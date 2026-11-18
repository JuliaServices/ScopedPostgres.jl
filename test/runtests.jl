using Test, Dates, ScopedPostgres, DBInterface
using ScopedPostgres: Postgres
const SP = ScopedPostgres

const conn_reset = ErrorException("tls read failed: unexpected TLS failure [SystemError: read: Connection reset by peer]")
const conn_timeout = ErrorException("tls read failed: unexpected TLS failure [SystemError: read: Operation timed out]")
const auth_failed = ErrorException("password authentication failed for user \"app\"")
const stale_stmt = ErrorException("prepared statement does not exist")

# The driver reports a backend killed mid-query as the server's ErrorResponse.
_pg_error(code, message) = Postgres.API.Error("FATAL", code, message, (nothing for _ = 1:14)...)
const backend_killed = _pg_error("57P01", "terminating connection due to administrator command")
const conn_failure = _pg_error("08006", "connection failure")
const unique_violation = _pg_error("23505", "duplicate key value violates unique constraint")
const stale_row_shape = _pg_error("0A000", "cached plan must not change result type")

@testset "ScopedPostgres" begin

@testset "query parameters" begin
    @test SP._normalize_params((Date(2026, 7, 19),)) == ["2026-07-19"]
    @test SP._normalize_params((DateTime(2026, 7, 19, 12, 34, 56),)) == ["2026-07-19T12:34:56"]
    @test SP._normalize_params((DateTime(2026, 7, 19, 12, 34, 56, 7),)) == ["2026-07-19T12:34:56.007"]
    @test SP._normalize_params(([1, 2, 3],)) == ["{1, 2, 3}"]
    @test isequal(SP._normalize_params((nothing, missing, true)), [missing, missing, "true"])
    @test SP._normalize_params(ntuple(identity, 26)) == string.(1:26)
    @test SP._normalize_params(Any["a", 1]) == ["a", "1"]
    @test SP._normalize_params(()) == Union{String,Missing}[]
    @test_throws ArgumentError SP._normalize_params((:symbol,))
    @test_throws ArgumentError SP._normalize_params(ntuple(identity, 27))
end

@testset "pool settings" begin
    scoped_pool = Ref{SP.DBPool}()
    SP.with_db_pool("localhost", "user", "password", "database", 5432;
            limit = 3, idle_evict_seconds = 0, application_name = "test-app") do
        scoped_pool[] = SP._get_db_pool()
        @test SP.pool_limit() == 3
        @test SP.pool_stats() == (limit = 3, in_use = 0, in_pool = 0)
        @test SP._get_db_pool().idle_evict_seconds == 0
        @test SP._get_db_pool().application_name == "test-app"
        @test SP._get_db_pool().schema_generation == 0
        @test SP.invalidate_schema!("other-database") == (pools = 0, closed_idle = 0, retiring_active = 0)
        @test SP.invalidate_schema!("database") == (pools = 1, closed_idle = 0, retiring_active = 0)
        @test SP._get_db_pool().schema_generation == 1
    end
    @test scoped_pool[].draining[]

    # A persistent pool is still closed when its block throws.
    failed_pool = Ref{SP.DBPool}()
    @test_throws ErrorException SP.with_db_pool("localhost", "user", "password", "database"; persist = true) do
        failed_pool[] = SP._get_db_pool()
        error("startup failed")
    end
    @test failed_pool[].draining[]

    SP.with_db_pool("localhost", "user", "password", "database") do
        pool = SP._get_db_pool()
        @test (pool.port, pool.limit, pool.idle_evict_seconds, pool.application_name) ==
            (5432, 100, 300.0, "ScopedPostgres")
        @test (pool.statement_cache_maxsize, pool.prefer_ipv4, pool.time_zone, pool.timestamps_as_text) ==
            (1024, false, "UTC", false)
    end
    for bad in ((; limit = 0), (; idle_evict_seconds = -1), (; statement_cache_maxsize = 0),
                (; time_zone = "UTC'; DROP TABLE users; --"))
        @test_throws ArgumentError SP.with_db_pool(() -> nothing, "localhost", "user", "password", "database"; bad...)
    end
    @test_throws ErrorException SP.query(Int, "SELECT 1")
    @test_throws ErrorException SP.current_connection()
end

@testset "retry policy" begin
    @testset "connection errors" begin
        @test SP._is_connection_error(conn_reset)
        @test SP._is_connection_error(conn_timeout)
        @test SP._is_connection_error(EOFError())
        @test SP._is_connection_error(ErrorException("Broken pipe"))
        @test !SP._is_connection_error(auth_failed)
        @test !SP._is_connection_error(ErrorException("duplicate key value"))
        @test SP._is_connection_error(backend_killed)
        @test SP._is_connection_error(conn_failure)
        @test !SP._is_connection_error(unique_violation)
    end

    @testset "statement verbs" begin
        @test SP._is_select("SELECT * FROM users")
        @test SP._is_select("\n  select 1")
        @test !SP._is_select("INSERT INTO users VALUES (1) RETURNING *")
        @test !SP._is_select("UPDATE users SET x = 1")
        @test !SP._is_select("WITH x AS (SELECT 1) UPDATE users SET y = 1")
    end

    @testset "retry decisions" begin
        # Failed before running: retry any statement.
        @test SP._should_retry(SP.StatementNotRun(conn_reset), "INSERT INTO t VALUES (1)")
        @test SP._should_retry(SP.StatementNotRun(conn_timeout), "SELECT 1")
        @test !SP._should_retry(SP.StatementNotRun(auth_failed), "SELECT 1")
        # Died while running: only reads retry.
        @test SP._should_retry(conn_reset, "SELECT * FROM users WHERE id = \$1")
        @test !SP._should_retry(conn_reset, "INSERT INTO payments VALUES (\$1)")
        @test !SP._should_retry(EOFError(), "UPDATE registrations SET status = 'confirmed'")
        @test !SP._should_retry(conn_timeout, "UPDATE registrations SET status = 'confirmed'")
        # The session lost its statements: the statement never ran.
        @test SP._should_retry(stale_stmt, "INSERT INTO t VALUES (1)")
        # What PostgreSQL actually reports for a statement the session no longer has.
        @test SP._should_retry(_pg_error("26000", "prepared statement \"s3\" does not exist"), "INSERT INTO t VALUES (1)")
        @test SP._should_retry(stale_row_shape, "UPDATE events SET name = 'new'")
        @test !SP._should_retry(_pg_error("0A000", "feature not supported"), "SELECT 1")
        @test SP._should_retry(backend_killed, "SELECT * FROM users")
        @test !SP._should_retry(backend_killed, "UPDATE users SET x = 1")
        decode_failure = TypeError(:typeassert, "", String, nothing)
        @test SP._should_retry(decode_failure, "SELECT * FROM users")
        @test !SP._should_retry(decode_failure, "INSERT INTO t VALUES (1) RETURNING *")
        # Real query errors never retry.
        @test !SP._should_retry(ErrorException("duplicate key value"), "SELECT 1")
        @test !SP._should_retry(auth_failed, "SELECT 1")
        @test !SP._should_retry(unique_violation, "SELECT 1")
    end

    @testset "which errors spoil a connection" begin
        for poisoned in (conn_reset, EOFError(), stale_stmt, backend_killed, conn_failure, stale_row_shape,
                         ErrorException("TypeError: in typeassert, expected String, got a value of type Nothing"))
            @test SP._is_connection_error(poisoned) || SP._is_stale_session_error(poisoned) ||
                  SP._is_result_decode_error(poisoned)
        end
        for clean in (unique_violation, ErrorException("boom"), ArgumentError("bad input"))
            @test !(SP._is_connection_error(clean) || SP._is_stale_session_error(clean) ||
                    SP._is_result_decode_error(clean))
        end
    end
end

@testset "errors" begin
    # Byte 1,500 is the middle of a two-byte character.
    @test SP.error_brief(ErrorException("é"^1000)) == "é"^1000
    @test SP.error_brief(ErrorException("x"^2000)) == "x"^1500
    @test SP.error_brief(unique_violation) == "PG 23505: duplicate key value violates unique constraint"
    @test SP.error_brief(KeyError(:x)) == "KeyError: key not found"
    @test occursin("Connection reset by peer", sprint(showerror, SP.StatementNotRun(conn_reset)))
    @test SP.sqlstate(unique_violation) == "23505"
    @test SP.sqlstate(ErrorException("x")) === nothing
    # An error raised while preparing reaches callers wrapped in StatementNotRun.
    @test SP.sqlstate(SP.StatementNotRun(_pg_error("42P01", "relation \"nope\" does not exist"))) == "42P01"
end

@testset "spawned reads" begin
    SP.with_db_pool("localhost", "user", "password", "database"; limit = 4) do
        @test fetch(SP.spawn_read(() -> 6 * 7, Int)) == 42
        # The caller sees the read's own exception.
        @test_throws ArgumentError("bad id") fetch(SP.spawn_read(() -> throw(ArgumentError("bad id")), Int))
        # Half the pool's limit of reads (2) run at once; the next runs before spawn_read returns.
        release = Channel{Nothing}(2)
        held = [SP.spawn_read(() -> (take!(release); 1), Int) for _ in 1:2]
        third = SP.spawn_read(() -> 3, Int)
        @test istaskdone(third.task)
        @test !any(read -> istaskdone(read.task), held)
        foreach(_ -> put!(release, nothing), 1:2)
        @test sum(fetch, held) + fetch(third) == 5
    end
end

# The rest needs a PostgreSQL server: set PGHOST (and PGPORT, PGUSER, PGPASSWORD,
# PGDATABASE as needed). CI runs them against a service container.
if isempty(get(ENV, "PGHOST", ""))
    @info "PGHOST is not set; skipping the tests that need a database"
else
    function with_test_pool(f; kwargs...)
        SP.with_db_pool(f, ENV["PGHOST"], get(ENV, "PGUSER", "postgres"), get(ENV, "PGPASSWORD", ""),
            get(ENV, "PGDATABASE", "postgres"), parse(Int, get(ENV, "PGPORT", "5432")); kwargs...)
    end

    @testset "against a database" begin
        with_test_pool(; limit = 4) do
            SP.execute("DROP TABLE IF EXISTS scopedpostgres_items")
            SP.execute("""
                CREATE TABLE scopedpostgres_items (
                    id TEXT PRIMARY KEY, name TEXT NOT NULL, size INT, added_at TIMESTAMPTZ NOT NULL DEFAULT now())""")
            Item = @NamedTuple{id::String, name::String, size::Union{Int,Nothing}}

            @testset "typed queries" begin
                SP.execute("INSERT INTO scopedpostgres_items (id, name, size) VALUES (\$1, \$2, \$3)", ("a", "Alpha", 3))
                SP.execute("INSERT INTO scopedpostgres_items (id, name, size) VALUES (\$1, \$2, \$3)", ("b", "Bravo", nothing))
                @test SP.query(Item, "SELECT id, name, size FROM scopedpostgres_items WHERE id = \$1", ("a",)) ==
                    (id = "a", name = "Alpha", size = 3)
                @test SP.query(Item, "SELECT id, name, size FROM scopedpostgres_items WHERE id = \$1", ("z",)) === nothing
                @test SP.queryall(Item, "SELECT id, name, size FROM scopedpostgres_items ORDER BY id") ==
                    [(id = "a", name = "Alpha", size = 3), (id = "b", name = "Bravo", size = nothing)]
                @test SP.query(@NamedTuple{n::Int}, "SELECT \$1::int + 1 AS n", (41,)).n == 42
            end

            @testset "transactions" begin
                @test_throws ErrorException SP.transaction() do
                    SP.execute("INSERT INTO scopedpostgres_items (id, name) VALUES ('c', 'Charlie')")
                    @test SP.in_transaction()
                    error("roll it back")
                end
                @test SP.query(Item, "SELECT id, name, size FROM scopedpostgres_items WHERE id = 'c'") === nothing
                count = SP.transaction() do
                    SP.execute("INSERT INTO scopedpostgres_items (id, name) VALUES ('c', 'Charlie')")
                    # A nested transaction joins the outer one, on the same session.
                    inner = SP.transaction() do
                        SP.query(@NamedTuple{pid::Int}, "SELECT pg_backend_pid() AS pid").pid
                    end
                    @test inner == SP.query(@NamedTuple{pid::Int}, "SELECT pg_backend_pid() AS pid").pid
                    SP.query(@NamedTuple{n::Int}, "SELECT count(*)::int AS n FROM scopedpostgres_items").n
                end
                @test count == 3
                @test !SP.in_transaction()
                err = try
                    SP.execute("INSERT INTO scopedpostgres_items (id, name) VALUES ('a', 'Again')")
                catch e
                    e
                end
                @test SP.sqlstate(err) == "23505"
                @test SP.constraint_name(err) == "scopedpostgres_items_pkey"
            end

            @testset "one row or none" begin
                @test_throws ArgumentError SP.query(@NamedTuple{n::Int}, "SELECT n FROM generate_series(1, 2) n")
                err = try
                    SP.query(@NamedTuple{n::Int}, "SELECT n FROM scopedpostgres_missing")
                catch e
                    e
                end
                @test SP.sqlstate(err) == "42P01"
            end

            @testset "a connection left inside a transaction is not lent out again" begin
                stray_pid = SP.with_db_connection() do
                    DBInterface.execute(SP.current_connection(), "BEGIN")
                    SP.query(@NamedTuple{pid::Int}, "SELECT pg_backend_pid() AS pid").pid
                end
                # It is closed rather than kept idle in its transaction, holding its locks.
                # Look from a second pool, which leaves this pool's idle connections alone.
                stray_session(pid) = with_test_pool() do
                    SP.query(@NamedTuple{n::Int},
                        "SELECT count(*)::int AS n FROM pg_stat_activity WHERE pid = \$1", (pid,)).n
                end
                for _ in 1:50
                    stray_session(stray_pid) == 0 && break
                    sleep(0.1)
                end
                @test stray_session(stray_pid) == 0
                SP.transaction() do
                    @test SP.query(@NamedTuple{pid::Int}, "SELECT pg_backend_pid() AS pid").pid != stray_pid
                    SP.execute("INSERT INTO scopedpostgres_items (id, name) VALUES ('t', 'Tango')")
                end
                seen = SP.with_db_connection(; forcenew = true) do
                    SP.query(Item, "SELECT id, name, size FROM scopedpostgres_items WHERE id = 't'")
                end
                @test seen == (id = "t", name = "Tango", size = nothing)
            end

            @testset "a statement the session lost is prepared again" begin
                SP.with_db_connection() do
                    @test SP.query(@NamedTuple{n::Int}, "SELECT 7 AS n").n == 7
                    SP.execute("DEALLOCATE ALL")
                end
                # The pool hands back the same session; its cached statement is gone.
                @test SP.query(@NamedTuple{n::Int}, "SELECT 7 AS n").n == 7
            end

            @testset "snapshots and one session" begin
                @test SP.read_snapshot() do
                    SP.query(@NamedTuple{level::String}, "SELECT current_setting('transaction_isolation') AS level").level
                end == "repeatable read"
                pids = SP.with_db_connection() do
                    [SP.query(@NamedTuple{pid::Int}, "SELECT pg_backend_pid() AS pid").pid for _ in 1:3]
                end
                @test allequal(pids)
            end

            @testset "counting statements" begin
                n = SP.with_query_counter() do
                    SP.query(@NamedTuple{n::Int}, "SELECT 1 AS n")
                    SP.queryall(@NamedTuple{n::Int}, "SELECT 1 AS n")
                    SP.execute("SELECT 1")
                    SP.query_count()
                end
                @test n == 3
                @test SP.query_count() == 0
            end

            @testset "connection settings" begin
                @test SP.query(@NamedTuple{zone::String}, "SELECT current_setting('TimeZone') AS zone").zone == "UTC"
                @test SP.query(@NamedTuple{at::DateTime}, "SELECT added_at AS at FROM scopedpostgres_items WHERE id = 'a'").at isa DateTime
                @test SP.pool_stats().in_pool >= 1
            end
            SP.execute("DROP TABLE scopedpostgres_items")
        end

        with_test_pool(; time_zone = "America/Denver", timestamps_as_text = true, application_name = "scopedpostgres-test") do
            @test SP.query(@NamedTuple{zone::String}, "SELECT current_setting('TimeZone') AS zone").zone == "America/Denver"
            @test SP.query(@NamedTuple{at::String}, "SELECT TIMESTAMPTZ '2026-01-02 03:04:05Z' AS at").at ==
                "2026-01-01 20:04:05-07"
            @test SP.query(@NamedTuple{name::String}, "SELECT current_setting('application_name') AS name").name ==
                "scopedpostgres-test"
        end

        @testset "a pool cannot open inside another pool's connection" begin
            with_test_pool() do
                SP.with_db_connection() do
                    @test_throws ArgumentError with_test_pool(() -> SP.query(@NamedTuple{n::Int}, "SELECT 1 AS n"))
                end
                SP.transaction() do
                    @test_throws ArgumentError with_test_pool(() -> nothing)
                end
                # Outside a connection block, an inner pool is fine and uses its own connections.
                @test with_test_pool(() -> SP.pool_stats().in_use) == 0
            end
        end

        @testset "a migration retires the pool's sessions" begin
            with_test_pool() do
                first_pid = SP.query(@NamedTuple{pid::Int}, "SELECT pg_backend_pid() AS pid").pid
                @test SP.invalidate_schema!(get(ENV, "PGDATABASE", "postgres")).closed_idle == 1
                @test SP.query(@NamedTuple{pid::Int}, "SELECT pg_backend_pid() AS pid").pid != first_pid
            end
        end
    end
end

end
