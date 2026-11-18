# ScopedPostgres.jl

Run [Postgres.jl](https://github.com/JuliaDatabases/Postgres.jl) queries on pooled
connections that a scope carries. `with_db_pool` sets the pool for a block of code, and
`query`, `queryall`, `execute`, and `transaction` inside it check a connection out, use
it, and give it back. No connection object is passed around.

- A bounded pool: callers past the limit wait for a connection. Idle connections older
  than a set age are closed instead of reused.
- Typed rows: ask for a `NamedTuple` (or any struct Postgres.jl can decode into).
- Transactions on one connection, nested ones joining the outer one, and repeatable-read
  snapshots for reads that must agree with each other.
- Safe retries: a statement that failed before it could have run (a dead pooled
  connection, a lost prepared statement) is retried once on a fresh connection. A
  statement starting with `SELECT` is also retried when the connection dies while it
  runs; a write is not, since it may already have committed. A `SELECT` with a side
  effect (`nextval`, a function that writes) counts as a read, so run it inside
  `transaction`, where nothing is retried.
- Concurrent reads (`spawn_read`) that overlap their round trips without taking every
  connection.
- A statement counter, for tests that pin how many queries a request makes.
- Written to compile with `juliac --trim`. CI builds a trimmed program and runs it.

## Install

```julia
using Pkg
Pkg.add(url = "https://github.com/JuliaServices/ScopedPostgres.jl")
```

## Example

```julia
using ScopedPostgres

const Item = @NamedTuple{id::String, name::String, size::Union{Int,Nothing}}

ScopedPostgres.with_db_pool("localhost", "app", ENV["PGPASSWORD"], "appdb"; limit = 10) do
    ScopedPostgres.execute("CREATE TABLE IF NOT EXISTS items (id TEXT PRIMARY KEY, name TEXT NOT NULL, size INT)")
    ScopedPostgres.execute("INSERT INTO items VALUES (\$1, \$2, \$3)", ("a", "Alpha", 3))

    # One row as a NamedTuple, or `nothing` when there is none.
    item = ScopedPostgres.query(Item, "SELECT id, name, size FROM items WHERE id = \$1", ("a",))

    # Commits when the block returns, rolls back when it throws.
    ScopedPostgres.transaction() do
        ScopedPostgres.execute("UPDATE items SET size = size + 1 WHERE id = \$1", ("a",))
        ScopedPostgres.execute("INSERT INTO items VALUES (\$1, \$2, \$3)", ("b", "Bravo", nothing))
    end

    ScopedPostgres.queryall(Item, "SELECT id, name, size FROM items ORDER BY id")
end
```

Parameters are a tuple (up to 26 values) or a `Vector{Any}` of `String`, `Int`,
`Int32`, `Float64`, `Bool`, `Date`, `DateTime`, bytes (`Vector{UInt8}`), `Vector`s of
`String`, `Int`, or `Float64`, and `nothing` or `missing` for NULL. Other types throw an
`ArgumentError`.

A task started inside `with_db_connection` or `transaction` inherits its connection;
finish it before the block ends. `spawn_read` inside one of those blocks runs its read
before returning for this reason.

## Pool settings

`with_db_pool(f, host, user, password, dbname, port = 5432; kwargs...)` takes:

| Keyword | Default | Meaning |
|---|---|---|
| `limit` | `100` | Most connections open at once. |
| `idle_evict_seconds` | `300` | Close a connection unused this long instead of reusing it (`0`: never). |
| `application_name` | `"ScopedPostgres"` | Shown in `pg_stat_activity`. |
| `statement_cache_maxsize` | `1024` | Prepared statements each connection keeps. |
| `prefer_ipv4` | `false` | Connect to the host's IPv4 address, keeping the name for TLS (SNI). |
| `time_zone` | `"UTC"` | Session time zone (`nothing`: the server's). |
| `timestamps_as_text` | `false` | Decode TIMESTAMP and TIMESTAMPTZ as `String`. |
| `persist` | `false` | Keep the pool open after `f` returns, until `drain!()`. |

## Trimming

The package's own code passes `juliac --trim=safe`. Two paths in Postgres.jl's
dependencies do not on stock Julia yet: Base64's pipes, used by SCRAM password sign-in
(SASLAuth), and Dates' `CompoundPeriod`, used to parse INTERVAL values. CI builds the
trimmed program with `--trim=unsafe-warn`, checks that those are the only verifier
errors, and runs it against a database that signs in without SCRAM.

## Tests

`julia --project -e 'using Pkg; Pkg.test()'`. The tests that need a database run when
`PGHOST` is set (with `PGPORT`, `PGUSER`, `PGPASSWORD`, and `PGDATABASE` as needed).
