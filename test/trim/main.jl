# Compiled with `juliac` in CI to check that the pool, typed queries, transactions,
# snapshots, concurrent reads, and the retry and error paths stay trim-safe, then run
# against the CI database (PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE).
using ScopedPostgres

const Item = @NamedTuple{id::String, name::String, size::Union{Int,Nothing}}

function check(name::String, ok::Bool)::Bool
    println(Core.stdout, ok ? "ok   " : "FAIL ", name)
    return ok
end

function run_checks()::Bool
    ScopedPostgres.execute("DROP TABLE IF EXISTS trim_items")
    ScopedPostgres.execute("CREATE TABLE trim_items (id TEXT PRIMARY KEY, name TEXT NOT NULL, size INT)")
    ScopedPostgres.execute("INSERT INTO trim_items VALUES (\$1, \$2, \$3)", ("a", "Alpha", 3))
    ScopedPostgres.execute("INSERT INTO trim_items VALUES (\$1, \$2, \$3)", ("b", "Bravo", nothing))
    ok = check("query", ScopedPostgres.query(Item, "SELECT id, name, size FROM trim_items WHERE id = \$1", ("a",)) ==
        (id = "a", name = "Alpha", size = 3))
    ok &= check("queryall", length(ScopedPostgres.queryall(Item, "SELECT id, name, size FROM trim_items")) == 2)
    rolled_back = try
        ScopedPostgres.transaction() do
            ScopedPostgres.execute("INSERT INTO trim_items VALUES ('c', 'Charlie', NULL)")
            error("roll back")
        end
        false
    catch
        ScopedPostgres.query(Item, "SELECT id, name, size FROM trim_items WHERE id = 'c'") === nothing
    end
    ok &= check("transaction rolls back", rolled_back)
    level = ScopedPostgres.read_snapshot() do
        ScopedPostgres.query(@NamedTuple{level::String},
            "SELECT current_setting('transaction_isolation') AS level").level
    end
    ok &= check("read_snapshot", level == "repeatable read")
    spawned = ScopedPostgres.spawn_queryall(Item, "SELECT id, name, size FROM trim_items", nothing)
    ok &= check("spawn_queryall", length(fetch(spawned)) == 2)
    code = try
        ScopedPostgres.execute("INSERT INTO trim_items VALUES ('a', 'Again', NULL)")
        nothing
    catch e
        println(Core.stdout, "     ", ScopedPostgres.error_brief(e))
        ScopedPostgres.sqlstate(e)
    end
    ok &= check("sqlstate", code == "23505")
    count = ScopedPostgres.with_query_counter() do
        ScopedPostgres.query(@NamedTuple{n::Int}, "SELECT 1 AS n")
        ScopedPostgres.query_count()
    end
    ok &= check("query counter", count == 1)
    ScopedPostgres.execute("DROP TABLE trim_items")
    return ok
end

function (@main)(args::Vector{String})::Cint
    port = something(tryparse(Int, get(ENV, "PGPORT", "5432")), 5432)
    ok = ScopedPostgres.with_db_pool(run_checks, get(ENV, "PGHOST", "localhost"), get(ENV, "PGUSER", "postgres"),
        get(ENV, "PGPASSWORD", ""), get(ENV, "PGDATABASE", "postgres"), port; limit = 4)
    println(Core.stdout, ok ? "all checks passed" : "some checks failed")
    return ok ? Cint(0) : Cint(1)
end
