"""
    ScopedPostgres

Run [Postgres.jl](https://github.com/JuliaDatabases/Postgres.jl) queries on pooled
connections that a scope carries: [`with_db_pool`](@ref) sets the pool for a block of
code, and [`query`](@ref), [`queryall`](@ref), [`execute`](@ref), [`transaction`](@ref),
and [`read_snapshot`](@ref) inside it check a connection out, use it, and give it back.
Queries that fail before they could have run are retried once on a fresh connection.
The code is written to compile with `juliac --trim=safe`.
"""
module ScopedPostgres

using Dates, Postgres, DBInterface, Sockets
using Base.ScopedValues: ScopedValue, @with

# The concrete connection and statement types, so pool code stays statically typed.
const PgConnection = Postgres.Connection{Postgres.Statement{Postgres.PostgresStyle}, Postgres.PostgresStyle}
const PgStatement = Postgres.Statement{Postgres.PostgresStyle}

"""
The SQLSTATE code of a PostgreSQL error, such as `\"23505\"`, or `nothing` for other errors.
Looks through a [`StatementNotRun`](@ref) to the error it wraps.
"""
sqlstate(err)::Union{String,Nothing} = (e = _pg_cause(err)) isa Postgres.Error ? e.code : nothing

"The constraint a PostgreSQL error names, or `nothing`. Looks through a [`StatementNotRun`](@ref)."
constraint_name(err)::Union{String,Nothing} = (e = _pg_cause(err)) isa Postgres.Error ? e.constraint : nothing

_pg_cause(err) = err isa StatementNotRun ? err.cause : err

"A bounded pool of connections to one database, made by [`with_db_pool`](@ref)."
mutable struct DBPool
    host::String
    user::String
    password::String
    dbname::String
    port::Int
    application_name::String
    idle_evict_seconds::Float64
    limit::Int
    statement_cache_maxsize::Int
    prefer_ipv4::Bool
    time_zone::Union{String,Nothing}
    timestamps_as_text::Bool
    condition::Threads.Condition
    idle::Vector{PgConnection}
    active::IdDict{PgConnection,Nothing}
    schema_generation::Int
    connection_generations::IdDict{PgConnection,Int}
    in_use::Int
    draining::Threads.Atomic{Bool}
end

const DB_POOL = ScopedValue{DBPool}()
const DB_CONNECTION = ScopedValue{PgConnection}()
const QUERY_COUNTER = ScopedValue{Threads.Atomic{Int}}()

"""
    with_query_counter(f)

Run `f()` counting the [`query`](@ref), [`queryall`](@ref), and [`execute`](@ref) calls
inside it; read the count with [`query_count`](@ref). A retry counts again.
"""
with_query_counter(f) = @with QUERY_COUNTER => Threads.Atomic{Int}(0) f()

"The number of statements run so far inside the current [`with_query_counter`](@ref), or 0 outside one."
query_count() = isassigned(QUERY_COUNTER) ? QUERY_COUNTER[][] : 0
_count_query!() = isassigned(QUERY_COUNTER) && Threads.atomic_add!(QUERY_COUNTER[], 1)

const _LIVE_POOLS = IdDict{DBPool,Nothing}()
const _LIVE_POOLS_LOCK = ReentrantLock()

# Run `f()` holding the pool's lock. `lock(f, ::Threads.Condition)` hands `f` on
# without specializing on it, which Julia 1.14's trim verifier cannot resolve.
function _locked(f::F, pool::DBPool) where {F}
    lock(pool.condition)
    try
        return f()
    finally
        unlock(pool.condition)
    end
end

function _register_pool!(pool::DBPool)
    lock(() -> (_LIVE_POOLS[pool] = nothing), _LIVE_POOLS_LOCK)
    return pool
end

"""
    with_db_pool(f, host, user, password, dbname, port = 5432; kwargs...)

Run `f()` with a pool of connections to the database, so the [`query`](@ref),
[`queryall`](@ref), [`execute`](@ref), and [`transaction`](@ref) calls inside it use the
pool. Connections open on first use. When `f` returns or throws, the pool closes its
connections, unless `persist = true` and `f` returned normally (a server keeps its pool
until [`drain!`](@ref)).

Keywords:
- `limit = 100`: the most connections open at once. Callers past it wait for one.
- `idle_evict_seconds = 300`: a connection unused this long is closed instead of reused,
  for servers or proxies that drop idle sessions. `0` keeps idle connections.
- `application_name = "ScopedPostgres"`: shown in `pg_stat_activity`.
- `statement_cache_maxsize = 1024`: prepared statements each connection keeps. Set it
  above the number of distinct SQL strings the program runs, or hot statements get
  prepared again.
- `prefer_ipv4 = false`: resolve the host to an IPv4 address and connect to that, keeping
  the host name for TLS (SNI). Falls back to the name when it does not resolve.
- `time_zone = "UTC"`: the session time zone set on each new connection (`nothing`
  leaves the server default). TIMESTAMPTZ values are rendered in it.
- `timestamps_as_text = false`: decode TIMESTAMP and TIMESTAMPTZ columns as `String`
  instead of `DateTime`, for programs that parse them themselves.
"""
function with_db_pool(f, host::String, user::String, password::String, dbname::String, port::Int = 5432;
                      persist::Bool = false, limit::Int = 100, idle_evict_seconds::Real = 300.0,
                      application_name::String = "ScopedPostgres", statement_cache_maxsize::Int = 1024,
                      prefer_ipv4::Bool = false, time_zone::Union{String,Nothing} = "UTC",
                      timestamps_as_text::Bool = false)
    # The block's queries would otherwise run on the outer pool's connection.
    isassigned(DB_CONNECTION) && throw(ArgumentError(
        "with_db_pool cannot open inside a with_db_connection or transaction block"))
    limit > 0 || throw(ArgumentError("limit must be a positive integer, got $limit"))
    idle_evict_seconds >= 0 ||
        throw(ArgumentError("idle_evict_seconds must be a non-negative number, got $idle_evict_seconds"))
    statement_cache_maxsize > 0 ||
        throw(ArgumentError("statement_cache_maxsize must be a positive integer, got $statement_cache_maxsize"))
    time_zone === nothing || occursin(r"^[A-Za-z0-9_+\-/:.]+$", time_zone) ||
        throw(ArgumentError("time_zone must be a time zone name such as \"UTC\" or \"America/Denver\""))
    pool = _register_pool!(DBPool(
        host,
        user,
        password,
        dbname,
        port,
        application_name,
        Float64(idle_evict_seconds),
        limit,
        statement_cache_maxsize,
        prefer_ipv4,
        time_zone,
        timestamps_as_text,
        Threads.Condition(),
        PgConnection[],
        IdDict{PgConnection,Nothing}(),
        0,
        IdDict{PgConnection,Int}(),
        0,
        Threads.Atomic{Bool}(false),
    ))
    completed = false
    try
        result = @with DB_POOL => pool f()
        completed = true
        return result
    finally
        (!persist || !completed) && drain!(pool)
    end
end

function _get_db_pool()
    isassigned(DB_POOL) || error("No database pool set. Run this inside `with_db_pool`.")
    return DB_POOL[]
end

"The current pool's `limit`."
pool_limit() = _get_db_pool().limit

"The current pool's `limit`, the connections checked out (`in_use`), and the idle ones kept (`in_pool`)."
function pool_stats()
    pool = _get_db_pool()
    return _locked(pool) do
        (limit = pool.limit, in_use = pool.in_use, in_pool = length(pool.idle))
    end
end

function connect(pool::DBPool)::PgConnection
    pool.draining[] && error("Database pool is shutting down")
    # With `prefer_ipv4`, resolve with libc and dial the IPv4 address, keeping TLS
    # SNI on the host name (some hosted databases route by SNI). On hosts with an
    # unreliable IPv6 route or resolver, libc and IPv4 work consistently. Falls back
    # to the host name when resolution fails (it is already an IP, or a local name).
    resolved = pool.prefer_ipv4 ? try
        string(Sockets.getaddrinfo(pool.host, Sockets.IPv4))
    catch
        nothing
    end : nothing
    # two typed dial calls (not one with a Union-typed sslservername kwarg,
    # whose NamedTuple is then built via runtime apply_type — dynamic under --trim)
    conn = resolved === nothing ?
        _dial_no_sni(pool, pool.host) : _dial_with_sni(pool, resolved, pool.host)
    try
        # PostgreSQL renders TIMESTAMPTZ values in the session time zone.
        zone = pool.time_zone
        zone === nothing || DBInterface.execute(conn, string("SET TIME ZONE '", zone, "'"))
        if pool.timestamps_as_text
            # TIMESTAMP (1114) and TIMESTAMPTZ (1184) arrive as the server's text.
            conn.type_registry[1114] = Postgres.API.TypeInfo(String, nothing)
            conn.type_registry[1184] = Postgres.API.TypeInfo(String, nothing)
        end
        return conn
    catch
        _close_connection!(conn)
        rethrow()
    end
end

function _dial_with_sni(pool::DBPool, host::String, sslservername::String)::PgConnection
    return DBInterface.connect(
        Postgres.Connection,
        host,
        pool.user,
        pool.password;
        sslservername,
        dbname=pool.dbname,
        port=pool.port,
        application_name=pool.application_name,
        statement_cache_maxsize=pool.statement_cache_maxsize,
    )::PgConnection
end

function _dial_no_sni(pool::DBPool, host::String)::PgConnection
    return DBInterface.connect(
        Postgres.Connection,
        host,
        pool.user,
        pool.password;
        dbname=pool.dbname,
        port=pool.port,
        application_name=pool.application_name,
        statement_cache_maxsize=pool.statement_cache_maxsize,
    )::PgConnection
end

# A server or proxy can reap idle connections, which may still look open locally.
# `idle_evict_seconds` closes them first; keep it longer than a normal gap between
# requests so statement caches survive routine pauses. 0 relies entirely on the
# first-use retry policy.
const _LAST_RELEASED = Dict{UInt,Float64}()
const _LAST_RELEASED_LOCK = ReentrantLock()

function _mark_released!(conn::PgConnection)
    lock(() -> (_LAST_RELEASED[objectid(conn)] = time()), _LAST_RELEASED_LOCK)
    return nothing
end
_idle_seconds(conn::PgConnection) =
    time() - lock(() -> get(_LAST_RELEASED, objectid(conn), time()), _LAST_RELEASED_LOCK)

function _close_connection!(conn::PgConnection)
    try
        close(conn)
    catch e
        @debug "failed to close database connection" exception = e
    end
    lock(() -> delete!(_LAST_RELEASED, objectid(conn)), _LAST_RELEASED_LOCK)
    return false
end

"""
Checkout validity: open, not stuck inside a transaction a previous occupant
abandoned (reusing such a connection surfaces as protocol desyncs —
"unexpected message type: E" — on unrelated later requests), and not idle
past `idle_evict_seconds`. NOTE: probing the socket buffer here
(bytesavailable) blocks on the TLS layer and livelocks the pool — don't.
Invalid connections are closed outside the pool lock and replaced.
"""
function _connection_ready(pool::DBPool, conn::PgConnection)
    isopen(conn) || return false
    (pool.idle_evict_seconds > 0 && _idle_seconds(conn) > pool.idle_evict_seconds) && return false
    _locked(pool) do
        get(pool.connection_generations, conn, -1) == pool.schema_generation
    end || return false
    return !_in_transaction(conn)
end

# The server's flag also catches a transaction a raw `BEGIN` opened.
_in_transaction(conn::PgConnection) = try
    conn.in_transaction || conn.server_in_transaction
catch
    true
end

# Reserve one of the bounded checkout slots and optionally take an idle client.
# A forced-fresh checkout discards one idle client when available; otherwise a
# pool full of idle clients could open limit + 1 physical PostgreSQL sessions.
function _reserve_slot!(pool::DBPool; forcenew::Bool=false)
    lock(pool.condition)
    try
        while !pool.draining[] && pool.in_use >= pool.limit
            wait(pool.condition)
        end
        pool.draining[] && error("Database pool is shutting down")
        pool.in_use += 1
        conn = isempty(pool.idle) ? nothing : pop!(pool.idle)
        return forcenew ? (nothing, conn) : (conn, nothing)
    finally
        unlock(pool.condition)
    end
end

function _take_idle(pool::DBPool)
    return _locked(pool) do
        pool.draining[] && return (nothing, false)
        return (isempty(pool.idle) ? nothing : pop!(pool.idle), true)
    end
end

function _activate_connection!(pool::DBPool, conn::PgConnection)
    return _locked(pool) do
        pool.draining[] && return false
        generation = get(pool.connection_generations, conn, pool.schema_generation)
        generation == pool.schema_generation || return false
        pool.connection_generations[conn] = generation
        pool.active[conn] = nothing
        return true
    end
end

function _forget_connection!(pool::DBPool, conn::PgConnection)
    _locked(pool) do
        delete!(pool.connection_generations, conn)
    end
    return nothing
end

function _release_slot!(pool::DBPool)
    _locked(pool) do
        pool.in_use > 0 || error("Cannot release a database slot when none are checked out")
        pool.in_use -= 1
        notify(pool.condition; all=false)
    end
    return nothing
end

# Never close a TLS/Postgres socket while holding `condition`: close can wait
# on network teardown, and doing that under the lock stalls every checkout.
function _acquire_connection(dbpool::DBPool; forcenew::Bool=false)::PgConnection
    dbpool.draining[] && error("Database pool is shutting down")
    conn, discarded = _reserve_slot!(dbpool; forcenew)
    if discarded !== nothing
        _forget_connection!(dbpool, discarded)
        _close_connection!(discarded)
    end
    try
        if !forcenew
            while conn !== nothing
                if _connection_ready(dbpool, conn) && !dbpool.draining[]
                    _activate_connection!(dbpool, conn) && return conn
                end
                _forget_connection!(dbpool, conn)
                _close_connection!(conn)
                conn, allowed = _take_idle(dbpool)
                allowed || error("Database pool is shutting down")
            end
        end
        conn = connect(dbpool)
        if !_activate_connection!(dbpool, conn)
            _close_connection!(conn)
            error("Database pool is shutting down")
        end
        return conn
    catch
        _release_slot!(dbpool)
        rethrow()
    end
end

function _release_connection!(pool::DBPool, conn::Union{PgConnection,Nothing})
    return_to_pool = false
    _locked(pool) do
        pool.in_use > 0 || error("Cannot release a database connection when none are checked out")
        pool.in_use -= 1
        conn === nothing || delete!(pool.active, conn)
        if conn !== nothing
            current_schema = get(pool.connection_generations, conn, -1) == pool.schema_generation
            if !pool.draining[] && current_schema
                push!(pool.idle, conn)
                return_to_pool = true
            else
                delete!(pool.connection_generations, conn)
            end
        end
        notify(pool.condition; all=false)
    end
    return_to_pool || (conn === nothing || _close_connection!(conn))
    return nothing
end

# Remove a broken connection from the pool's books and free its slot. The
# socket is closed outside the pool lock because TLS teardown can block on the
# network.
function _discard_connection!(pool::DBPool, conn::PgConnection)
    _locked(pool) do
        pool.in_use > 0 || error("Cannot release a database connection when none are checked out")
        pool.in_use -= 1
        delete!(pool.active, conn)
        delete!(pool.connection_generations, conn)
        notify(pool.condition; all=false)
    end
    _close_connection!(conn)
    return nothing
end

"""
Decide whether a checked-out connection is safe to pool again after `f` threw.

A server-reported SQL error (unique violation, a validation trigger's RAISE, a
permission failure) leaves the protocol synced: the driver parsed the
ErrorResponse and consumed through ReadyForQuery before throwing, so the
session is healthy. Closing it would pay a full TLS reconnect and lose the
session's prepared-statement cache on every routine constraint rejection.
Application exceptions raised between statements (an HTTP 409 thrown inside a
transaction block, say) also leave a healthy session once the rollback ran.

Transport failures, protocol desyncs, and stale-session errors are the
opposite — that session can never be trusted again. Result-decode failures are
also discarded: the error interrupts result consumption mid-stream, so the
protocol position is unknown. The final open/no-transaction check catches a
rollback that itself failed.
"""
function _reusable_after_error(conn::PgConnection, @nospecialize(e))
    cause = e isa StatementNotRun ? e.cause : e
    (_is_connection_error(cause) || _is_stale_session_error(cause)) && return false
    _is_result_decode_error(cause) && return false
    return try
        isopen(conn) && !_in_transaction(conn)
    catch
        false
    end
end

"""
    with_db_connection(f; forcenew = false)

Run `f()` on one checked-out connection, so every statement inside it uses the same
session. Nested calls reuse the outer connection. `forcenew = true` opens a fresh one.
A task started inside the block inherits the connection: finish it before the block
ends, or its statements run on a connection the pool has lent to someone else.
"""
function with_db_connection(f::F; forcenew::Bool=false) where {F}
    isassigned(DB_CONNECTION) && return f()
    pool = _get_db_pool()
    conn = _acquire_connection(pool; forcenew)
    reusable = true
    try
        result = @with DB_CONNECTION => conn f()
        # A transaction `f` opened with raw SQL and left open would keep its locks
        # while the connection sat idle in the pool.
        reusable = !_in_transaction(conn)
        return result
    catch e
        reusable = _reusable_after_error(conn, e)
        rethrow()
    finally
        if reusable
            pool.draining[] || _mark_released!(conn)
            _release_connection!(pool, conn)
        else
            _discard_connection!(pool, conn)
        end
    end
end

function _get_db_connection()
    isassigned(DB_CONNECTION) || error("No database connection set. Run this inside `with_db_connection` or `transaction`.")
    return DB_CONNECTION[]
end

"""
    current_connection() -> Postgres.Connection

The connection the current [`with_db_connection`](@ref) or [`transaction`](@ref) block
holds, for driver calls such as `DBInterface.execute(conn, "SAVEPOINT s")`. Throws
outside one.
"""
current_connection() = _get_db_connection()

"""
    drain!(pool) -> Int
    drain!() -> Int

Close a pool's connections, or every live pool's, and stop it handing out more. Returns
the number closed. Runs for every pool when the process exits.
"""
function drain!(dbpool::DBPool)
    connections = PgConnection[]
    _locked(dbpool) do
        dbpool.draining[] = true
        append!(connections, dbpool.idle)
        append!(connections, keys(dbpool.active))
        empty!(dbpool.idle)
        empty!(dbpool.active)
        empty!(dbpool.connection_generations)
        notify(dbpool.condition; all=true)
    end
    for conn in connections
        _close_connection!(conn)
    end
    lock(() -> delete!(_LIVE_POOLS, dbpool), _LIVE_POOLS_LOCK)
    return length(connections)
end

"""
    invalidate_schema!(dbname) -> (pools, closed_idle, retiring_active)

Advance the schema generation for every live pool connected to `dbname`.
Idle sessions are closed immediately because their driver and server-side
prepared statements may describe pre-migration row shapes. Checked-out
sessions are marked stale and closed when released instead of returning to the
pool. Call this after an external migration runner completes while the process
remains alive; normal deployments restart the process after migrations.
"""
function invalidate_schema!(dbname::AbstractString)
    pools = lock(() -> [pool for pool in keys(_LIVE_POOLS)
                        if pool.dbname == dbname], _LIVE_POOLS_LOCK)
    closed_idle = 0
    retiring_active = 0
    invalidated = 0
    for pool in pools
        connections = PgConnection[]
        active_count = 0
        did_invalidate = _locked(pool) do
            pool.draining[] && return false
            pool.schema_generation += 1
            append!(connections, pool.idle)
            empty!(pool.idle)
            foreach(conn -> delete!(pool.connection_generations, conn), connections)
            active_count = length(pool.active)
            true
        end
        did_invalidate || continue
        for conn in connections
            _close_connection!(conn)
        end
        invalidated += 1
        closed_idle += length(connections)
        retiring_active += active_count
    end
    return (pools = invalidated, closed_idle, retiring_active)
end

function drain!()
    pools = lock(() -> collect(keys(_LIVE_POOLS)), _LIVE_POOLS_LOCK)
    closed = 0
    for pool in pools
        closed += drain!(pool)
    end
    return closed
end

function __init__()
    atexit(drain!)
end

const IN_TRANSACTION = ScopedValue{Bool}(false)

"Whether the current task is inside a [`transaction`](@ref) (or a [`read_snapshot`](@ref))."
in_transaction() = IN_TRANSACTION[]

"""
    transaction(f; mock = false)

Run `f()` inside one database transaction on one connection: it commits when `f` returns
and rolls back when `f` throws. A nested `transaction` joins the outer one. If the
connection fails before the transaction begins, it is retried once on a fresh connection.
Statements inside a transaction are never retried. A task started inside the block
inherits the transaction's connection: finish it before the block ends.
`mock = true` runs `f()` without a transaction.
"""
function transaction(f::F; mock::Bool=false) where {F}
    (IN_TRANSACTION[] || mock) && return f()
    for attempt in 1:2
        begun = Ref(false)
        try
            return @with IN_TRANSACTION => true begin
                with_db_connection(; forcenew = attempt == 2) do
                    DBInterface.transaction(_get_db_connection()) do
                        begun[] = true
                        return f()
                    end
                end
            end
        catch e
            # A protocol desync or dead socket at BEGIN — before the body ran —
            # means we drew a corrupted pooled connection (see _connection_ready).
            # with_db_connection already closed it; the whole transaction is safe
            # to retry exactly once on a guaranteed-fresh connection.
            retryable = attempt == 1 && !begun[] &&
                (_is_connection_error(e) || _is_stale_session_error(e))
            retryable || rethrow()
        end
    end
    error("transaction retry loop exited without returning or throwing")
end

"""
Run a read projection against one repeatable PostgreSQL snapshot.

Public responses commonly assemble one logical document from several tables. A
normal READ COMMITTED transaction can observe a regeneration between those
queries and return a mixture of old and new schedule state. Keep the snapshot
boundary explicit at the projection entry point; nested callers reuse an
existing transaction because PostgreSQL isolation must be selected before its
first statement.
"""
function read_snapshot(f; mock::Bool=false)
    (IN_TRANSACTION[] || mock) && return f()
    return transaction() do
        execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        return f()
    end
end

"A read started by [`spawn_read`](@ref); `fetch` returns its `T` result."
struct SpawnedRead{T}
    task::Task
end

"Reads `spawn_read` is running concurrently, across all requests."
const _CONCURRENT_READS = Threads.Atomic{Int}(0)

"""
    spawn_read(f, T) -> SpawnedRead{T}

Start the read-only `f()` on its own task, so its database round trips overlap
the caller's other queries. `fetch` waits for it and returns its result, which
must be a `T`, or throws the exception `f` threw.

The task runs on the caller's thread: reads overlap while they wait on the
database but never compile code at the same moment on two threads, which
Julia 1.12's compiler does not always survive.

A concurrent read checks out its own pool connections, one at a time. At most
half the pool's limit of reads run concurrently, so they never take the
connections that requests running their queries in turn need. Past that limit,
and inside a transaction or a `with_db_connection` block (whose connection runs
one statement at a time), `f` finishes before `spawn_read` returns.
"""
function spawn_read(f::F, ::Type{T}) where {F,T}
    if !isassigned(DB_CONNECTION)
        if Threads.atomic_add!(_CONCURRENT_READS, 1) < pool_limit() ÷ 2
            return SpawnedRead{T}(@async try
                f()
            finally
                Threads.atomic_sub!(_CONCURRENT_READS, 1)
            end)
        end
        Threads.atomic_sub!(_CONCURRENT_READS, 1)
    end
    read = SpawnedRead{T}(@async f())
    fetch(read)
    return read
end

"[`queryall`](@ref) as a [`spawn_read`](@ref): `fetch` returns the `Vector{T}`."
spawn_queryall(::Type{T}, sql::String, params) where {T} =
    spawn_read(() -> queryall(T, sql, params), Vector{T})

function Base.fetch(read::SpawnedRead{T}) where {T}
    wait(read.task; throw = false)
    # The read's own exception, not a TaskFailedException, decides the error response.
    istaskfailed(read.task) && throw(read.task.exception)
    return fetch(read.task)::T
end

# ── Retry policy ─────────────────────────────────────────────────────────────
# Servers and connection poolers can silently kill idle connections; the death
# surfaces on the NEXT use as a TLS read failure / reset / EOF. Checkout catches
# closed sockets, but a socket can look open and still be dead server-side, so
# query/queryall/execute retry ONCE on a fresh connection — with idempotency
# discipline:
#   • a failure at PREPARE time (wrapped in StatementNotRun) provably ran
#     nothing — safe to retry any statement, and this is where an idle-killed
#     connection actually fails (first bytes on the dead socket);
#   • stale-session errors (pooler dropped prepared statements / bind
#     mismatches) mean the statement never executed — also safe;
#   • a connection death DURING execute is ambiguous for writes (the server
#     may have committed before the link died), so only plain SELECTs retry —
#     writes fail loudly;
#   • inside a transaction the connection is pinned: never retry, let the
#     transaction fail.

"Marker: the failure happened before the statement could possibly run."
struct StatementNotRun <: Exception
    cause::Exception
end
Base.showerror(io::IO, e::StatementNotRun) = showerror(io, e.cause)

# Typed message extraction instead of `sprint(showerror, ::Any)`: the generic
# showerror fallback is a pile of unresolvable dynamic dispatch under
# `juliac --trim`, and every string these classifiers sniff lives in a typed
# field of a known driver-stack exception anyway. Unknown types return "" and
# classify as non-retryable — the safe default.
function _error_message(@nospecialize(e))::String
    e isa ErrorException && begin
        m = e.msg
        # ErrorException.msg is declared AbstractString; narrow before returning
        return m isa String ? m : "ErrorException"
    end
    e isa Base.IOError && return e.msg
    e isa Postgres.Reseau.TLS.TLSError && return e.message
    e isa Postgres.API.Error && return e.message
    if e isa ArgumentError
        m = e.msg
        return m isa String ? m : ""
    end
    return ""
end

@noinline function _is_connection_error(@nospecialize(e))::Bool
    e isa EOFError && return true
    # the (fixed) driver surfaces a backend that died mid-query as the
    # server's own ErrorResponse rather than a raw EOF: SQLSTATE class 08
    # (connection exception) or 57P01–57P03 (operator intervention)
    if e isa Postgres.API.Error
        return startswith(e.code, "08") ||
               e.code == "57P01" || e.code == "57P02" || e.code == "57P03" ||
               _is_connection_error_message(e.message)
    end
    return _is_connection_error_message(_error_message(e))
end

_is_connection_error_message(message::String) =
    occursin("tls read failed", message) ||
    occursin("Connection reset by peer", message) ||
    occursin("Operation timed out", message) ||
    occursin("Broken pipe", message) ||
    occursin("broken pipe", message) ||
    occursin("read end of file", message) ||
    occursin("connection is closed", message)

"Server/session-state desyncs where our statement provably didn't execute."
@noinline function _is_stale_session_error(@nospecialize(e))::Bool
    message = _error_message(e)
    cached_plan_shape_changed = e isa Postgres.API.Error && e.code == "0A000" &&
        occursin("cached plan must not change result type", message)
    # 26000: the session no longer has the prepared statement.
    statement_missing = e isa Postgres.API.Error && e.code == "26000"
    return cached_plan_shape_changed || statement_missing ||
           occursin("unexpected message type: E", message) ||
           occursin("bind message supplies", message) ||
           occursin("prepared statement does not exist", message) ||
           occursin("number of parameters provided", message)
end

# Row decoding failed while consuming results — the statement already ran on
# the server, so this is NOT proof of a never-executed statement. Retrying a
# write here would double-execute it; only reads may retry.
@noinline function _is_result_decode_error(@nospecialize(e))::Bool
    e isa TypeError && e.expected === String && e.got === nothing && return true
    return occursin("in typeassert, expected String, got a value of type Nothing",
                    _error_message(e))
end

_is_select(sql::AbstractString) = startswith(uppercase(lstrip(sql)), "SELECT")

function _should_retry(@nospecialize(e), sql::AbstractString)
    # a pinned connection means we're inside a transaction (or an explicit
    # with_db_connection block): a retry would re-run on the same dead socket
    # and re-execute part of a broken transaction — fail loudly instead
    isassigned(DB_CONNECTION) && return false
    if e isa StatementNotRun
        return _is_connection_error(e.cause) || _is_stale_session_error(e.cause)
    end
    _is_stale_session_error(e) && return true
    _is_result_decode_error(e) && return _is_select(sql)
    # Execute-phase connection deaths (EOF/reset/timeout) are ambiguous for
    # writes — the server may have committed before the link died, and a
    # blind retry double-executes (observed as off-by-one capacity counts
    # under parallel load). Only reads retry; writes fail loudly. The idle
    # eviction above keeps reaped connections out of circulation, so this
    # path is rare.
    return _is_connection_error(e) && _is_select(sql)
end

# ── trim-safe query-parameter normalization ─────────────────────────────────
# Callers pass params as tuples whose elements are often Union{Nothing,String}
# (nullable ids). Tuples with union-typed elements are not dispatch types, so a
# specialized query call per params-tuple can't be statically resolved under
# `juliac --trim` — one flagged call per call site.
# Normalize once at this boundary into the driver's native
# Vector{Union{String,Missing}} (reusing the driver's `Postgres._param`
# stringification rules); the arity + element narrowing below keeps the barrier
# itself fully static for the app's parameter shapes. Unsupported element types
# throw — add a branch to `_param_narrowed` rather than widening a call site.
"""
    error_brief(e) -> String

A short, trim-safe description of a database error for logs: the SQLSTATE, message, and
detail of a PostgreSQL error, otherwise the message or exception type. At most 1,500
characters.
"""
@noinline function error_brief(@nospecialize(e))::String
    s = if e isa Postgres.API.Error
        d = e.detail
        string("PG ", e.code, ": ", e.message, d isa String ? string(" (", d, ")") : "")
    elseif e isa Postgres.PostgresInterfaceError
        e.msg
    elseif e isa StatementNotRun
        c = e.cause
        c isa Postgres.API.Error ? string("PG ", c.code, ": ", c.message) : "statement not run"
    elseif e isa ArgumentError
        m = e.msg
        m isa String ? m : m isa SubString{String} ? String(m) : "invalid argument"
    elseif e isa ErrorException
        m = e.msg
        m isa String ? m : m isa SubString{String} ? String(m) : "error"
    elseif e isa KeyError
        "KeyError: key not found"
    elseif e isa MethodError
        "MethodError"
    elseif e isa TypeError
        "TypeError"
    elseif e isa BoundsError
        "BoundsError"
    else
        # Rendering arbitrary exception payloads is dynamic under --trim.
        String(nameof(typeof(e)))
    end
    return first(s, 1500)
end

# hand-formatted pg date/timestamp literals: the driver's generic _param falls
# back to string(::Date/::DateTime), which routes through Dates' dynamic
# show machinery under --trim
function _pg_date_string(d::Date)::String
    return string(lpad4(Dates.year(d)), '-', lpad2(Dates.month(d)), '-', lpad2(Dates.day(d)))
end

function _pg_datetime_string(dt::DateTime)::String
    ms = Dates.millisecond(dt)
    base = string(lpad4(Dates.year(dt)), '-', lpad2(Dates.month(dt)), '-', lpad2(Dates.day(dt)),
                  'T', lpad2(Dates.hour(dt)), ':', lpad2(Dates.minute(dt)), ':', lpad2(Dates.second(dt)))
    return ms == 0 ? base : string(base, '.', lpad3(ms))
end

lpad2(x::Int)::String = x < 10 ? string('0', x) : string(x)
lpad3(x::Int)::String = x < 10 ? string("00", x) : x < 100 ? string('0', x) : string(x)
lpad4(x::Int)::String = x < 10 ? string("000", x) : x < 100 ? string("00", x) : x < 1000 ? string('0', x) : string(x)

@noinline function _param_narrowed(@nospecialize(p))::Union{String, Missing}
    p === nothing && return missing
    p === missing && return missing
    p isa String && return p
    p isa SubString{String} && return Postgres._param(p)
    p isa Int && return Postgres._param(p)
    p isa Int32 && return Postgres._param(p)
    p isa Float64 && return Postgres._param(p)
    p isa Bool && return Postgres._param(p)
    p isa Date && return _pg_date_string(p)
    p isa DateTime && return _pg_datetime_string(p)
    p isa Vector{UInt8} && return Postgres._param(p)
    p isa Vector{String} && return Postgres._param(p)
    p isa Vector{Int} && return Postgres._param(p)
    p isa Vector{Float64} && return Postgres._param(p)
    throw(ArgumentError("unsupported query parameter type: pass a String, Int, Int32, Float64, Bool, Date, DateTime, Vector{UInt8}, or a Vector of String, Int, or Float64"))
end

@noinline function _normalize_params(@nospecialize(params))::Union{Nothing, Vector{Union{String, Missing}}}
    params === nothing && return nothing
    params isa Vector{Union{String, Missing}} && return params
    params isa Tuple{} && return Union{String, Missing}[]
    if params isa NTuple{1, Any}
        out = Vector{Union{String, Missing}}(undef, 1)
        out[1] = _param_narrowed(params[1])
        return out
    elseif params isa NTuple{2, Any}
        out = Vector{Union{String, Missing}}(undef, 2)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        return out
    elseif params isa NTuple{3, Any}
        out = Vector{Union{String, Missing}}(undef, 3)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        return out
    elseif params isa NTuple{4, Any}
        out = Vector{Union{String, Missing}}(undef, 4)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        return out
    elseif params isa NTuple{5, Any}
        out = Vector{Union{String, Missing}}(undef, 5)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        out[5] = _param_narrowed(params[5])
        return out
    elseif params isa NTuple{6, Any}
        out = Vector{Union{String, Missing}}(undef, 6)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        out[5] = _param_narrowed(params[5])
        out[6] = _param_narrowed(params[6])
        return out
    elseif params isa NTuple{7, Any}
        out = Vector{Union{String, Missing}}(undef, 7)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        out[5] = _param_narrowed(params[5])
        out[6] = _param_narrowed(params[6])
        out[7] = _param_narrowed(params[7])
        return out
    elseif params isa NTuple{8, Any}
        out = Vector{Union{String, Missing}}(undef, 8)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        out[5] = _param_narrowed(params[5])
        out[6] = _param_narrowed(params[6])
        out[7] = _param_narrowed(params[7])
        out[8] = _param_narrowed(params[8])
        return out
    elseif params isa NTuple{9, Any}
        out = Vector{Union{String, Missing}}(undef, 9)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        out[5] = _param_narrowed(params[5])
        out[6] = _param_narrowed(params[6])
        out[7] = _param_narrowed(params[7])
        out[8] = _param_narrowed(params[8])
        out[9] = _param_narrowed(params[9])
        return out
    elseif params isa NTuple{10, Any}
        out = Vector{Union{String, Missing}}(undef, 10)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        out[5] = _param_narrowed(params[5])
        out[6] = _param_narrowed(params[6])
        out[7] = _param_narrowed(params[7])
        out[8] = _param_narrowed(params[8])
        out[9] = _param_narrowed(params[9])
        out[10] = _param_narrowed(params[10])
        return out
    elseif params isa NTuple{11, Any}
        out = Vector{Union{String, Missing}}(undef, 11)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        out[5] = _param_narrowed(params[5])
        out[6] = _param_narrowed(params[6])
        out[7] = _param_narrowed(params[7])
        out[8] = _param_narrowed(params[8])
        out[9] = _param_narrowed(params[9])
        out[10] = _param_narrowed(params[10])
        out[11] = _param_narrowed(params[11])
        return out
    elseif params isa NTuple{12, Any}
        out = Vector{Union{String, Missing}}(undef, 12)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        out[5] = _param_narrowed(params[5])
        out[6] = _param_narrowed(params[6])
        out[7] = _param_narrowed(params[7])
        out[8] = _param_narrowed(params[8])
        out[9] = _param_narrowed(params[9])
        out[10] = _param_narrowed(params[10])
        out[11] = _param_narrowed(params[11])
        out[12] = _param_narrowed(params[12])
        return out
    elseif params isa NTuple{13, Any}
        out = Vector{Union{String, Missing}}(undef, 13)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        out[5] = _param_narrowed(params[5])
        out[6] = _param_narrowed(params[6])
        out[7] = _param_narrowed(params[7])
        out[8] = _param_narrowed(params[8])
        out[9] = _param_narrowed(params[9])
        out[10] = _param_narrowed(params[10])
        out[11] = _param_narrowed(params[11])
        out[12] = _param_narrowed(params[12])
        out[13] = _param_narrowed(params[13])
        return out
    elseif params isa NTuple{14, Any}
        out = Vector{Union{String, Missing}}(undef, 14)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        out[5] = _param_narrowed(params[5])
        out[6] = _param_narrowed(params[6])
        out[7] = _param_narrowed(params[7])
        out[8] = _param_narrowed(params[8])
        out[9] = _param_narrowed(params[9])
        out[10] = _param_narrowed(params[10])
        out[11] = _param_narrowed(params[11])
        out[12] = _param_narrowed(params[12])
        out[13] = _param_narrowed(params[13])
        out[14] = _param_narrowed(params[14])
        return out
    elseif params isa NTuple{15, Any}
        out = Vector{Union{String, Missing}}(undef, 15)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        out[5] = _param_narrowed(params[5])
        out[6] = _param_narrowed(params[6])
        out[7] = _param_narrowed(params[7])
        out[8] = _param_narrowed(params[8])
        out[9] = _param_narrowed(params[9])
        out[10] = _param_narrowed(params[10])
        out[11] = _param_narrowed(params[11])
        out[12] = _param_narrowed(params[12])
        out[13] = _param_narrowed(params[13])
        out[14] = _param_narrowed(params[14])
        out[15] = _param_narrowed(params[15])
        return out
    elseif params isa NTuple{16, Any}
        out = Vector{Union{String, Missing}}(undef, 16)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        out[5] = _param_narrowed(params[5])
        out[6] = _param_narrowed(params[6])
        out[7] = _param_narrowed(params[7])
        out[8] = _param_narrowed(params[8])
        out[9] = _param_narrowed(params[9])
        out[10] = _param_narrowed(params[10])
        out[11] = _param_narrowed(params[11])
        out[12] = _param_narrowed(params[12])
        out[13] = _param_narrowed(params[13])
        out[14] = _param_narrowed(params[14])
        out[15] = _param_narrowed(params[15])
        out[16] = _param_narrowed(params[16])
        return out
    elseif params isa NTuple{17, Any}
        out = Vector{Union{String, Missing}}(undef, 17)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        out[5] = _param_narrowed(params[5])
        out[6] = _param_narrowed(params[6])
        out[7] = _param_narrowed(params[7])
        out[8] = _param_narrowed(params[8])
        out[9] = _param_narrowed(params[9])
        out[10] = _param_narrowed(params[10])
        out[11] = _param_narrowed(params[11])
        out[12] = _param_narrowed(params[12])
        out[13] = _param_narrowed(params[13])
        out[14] = _param_narrowed(params[14])
        out[15] = _param_narrowed(params[15])
        out[16] = _param_narrowed(params[16])
        out[17] = _param_narrowed(params[17])
        return out
    elseif params isa NTuple{18, Any}
        out = Vector{Union{String, Missing}}(undef, 18)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        out[5] = _param_narrowed(params[5])
        out[6] = _param_narrowed(params[6])
        out[7] = _param_narrowed(params[7])
        out[8] = _param_narrowed(params[8])
        out[9] = _param_narrowed(params[9])
        out[10] = _param_narrowed(params[10])
        out[11] = _param_narrowed(params[11])
        out[12] = _param_narrowed(params[12])
        out[13] = _param_narrowed(params[13])
        out[14] = _param_narrowed(params[14])
        out[15] = _param_narrowed(params[15])
        out[16] = _param_narrowed(params[16])
        out[17] = _param_narrowed(params[17])
        out[18] = _param_narrowed(params[18])
        return out
    elseif params isa NTuple{19, Any}
        out = Vector{Union{String, Missing}}(undef, 19)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        out[5] = _param_narrowed(params[5])
        out[6] = _param_narrowed(params[6])
        out[7] = _param_narrowed(params[7])
        out[8] = _param_narrowed(params[8])
        out[9] = _param_narrowed(params[9])
        out[10] = _param_narrowed(params[10])
        out[11] = _param_narrowed(params[11])
        out[12] = _param_narrowed(params[12])
        out[13] = _param_narrowed(params[13])
        out[14] = _param_narrowed(params[14])
        out[15] = _param_narrowed(params[15])
        out[16] = _param_narrowed(params[16])
        out[17] = _param_narrowed(params[17])
        out[18] = _param_narrowed(params[18])
        out[19] = _param_narrowed(params[19])
        return out
    elseif params isa NTuple{20, Any}
        out = Vector{Union{String, Missing}}(undef, 20)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        out[5] = _param_narrowed(params[5])
        out[6] = _param_narrowed(params[6])
        out[7] = _param_narrowed(params[7])
        out[8] = _param_narrowed(params[8])
        out[9] = _param_narrowed(params[9])
        out[10] = _param_narrowed(params[10])
        out[11] = _param_narrowed(params[11])
        out[12] = _param_narrowed(params[12])
        out[13] = _param_narrowed(params[13])
        out[14] = _param_narrowed(params[14])
        out[15] = _param_narrowed(params[15])
        out[16] = _param_narrowed(params[16])
        out[17] = _param_narrowed(params[17])
        out[18] = _param_narrowed(params[18])
        out[19] = _param_narrowed(params[19])
        out[20] = _param_narrowed(params[20])
        return out
    elseif params isa NTuple{21, Any}
        out = Vector{Union{String, Missing}}(undef, 21)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        out[5] = _param_narrowed(params[5])
        out[6] = _param_narrowed(params[6])
        out[7] = _param_narrowed(params[7])
        out[8] = _param_narrowed(params[8])
        out[9] = _param_narrowed(params[9])
        out[10] = _param_narrowed(params[10])
        out[11] = _param_narrowed(params[11])
        out[12] = _param_narrowed(params[12])
        out[13] = _param_narrowed(params[13])
        out[14] = _param_narrowed(params[14])
        out[15] = _param_narrowed(params[15])
        out[16] = _param_narrowed(params[16])
        out[17] = _param_narrowed(params[17])
        out[18] = _param_narrowed(params[18])
        out[19] = _param_narrowed(params[19])
        out[20] = _param_narrowed(params[20])
        out[21] = _param_narrowed(params[21])
        return out
    elseif params isa NTuple{22, Any}
        out = Vector{Union{String, Missing}}(undef, 22)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        out[5] = _param_narrowed(params[5])
        out[6] = _param_narrowed(params[6])
        out[7] = _param_narrowed(params[7])
        out[8] = _param_narrowed(params[8])
        out[9] = _param_narrowed(params[9])
        out[10] = _param_narrowed(params[10])
        out[11] = _param_narrowed(params[11])
        out[12] = _param_narrowed(params[12])
        out[13] = _param_narrowed(params[13])
        out[14] = _param_narrowed(params[14])
        out[15] = _param_narrowed(params[15])
        out[16] = _param_narrowed(params[16])
        out[17] = _param_narrowed(params[17])
        out[18] = _param_narrowed(params[18])
        out[19] = _param_narrowed(params[19])
        out[20] = _param_narrowed(params[20])
        out[21] = _param_narrowed(params[21])
        out[22] = _param_narrowed(params[22])
        return out
    elseif params isa NTuple{23, Any}
        out = Vector{Union{String, Missing}}(undef, 23)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        out[5] = _param_narrowed(params[5])
        out[6] = _param_narrowed(params[6])
        out[7] = _param_narrowed(params[7])
        out[8] = _param_narrowed(params[8])
        out[9] = _param_narrowed(params[9])
        out[10] = _param_narrowed(params[10])
        out[11] = _param_narrowed(params[11])
        out[12] = _param_narrowed(params[12])
        out[13] = _param_narrowed(params[13])
        out[14] = _param_narrowed(params[14])
        out[15] = _param_narrowed(params[15])
        out[16] = _param_narrowed(params[16])
        out[17] = _param_narrowed(params[17])
        out[18] = _param_narrowed(params[18])
        out[19] = _param_narrowed(params[19])
        out[20] = _param_narrowed(params[20])
        out[21] = _param_narrowed(params[21])
        out[22] = _param_narrowed(params[22])
        out[23] = _param_narrowed(params[23])
        return out
    elseif params isa NTuple{24, Any}
        out = Vector{Union{String, Missing}}(undef, 24)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        out[5] = _param_narrowed(params[5])
        out[6] = _param_narrowed(params[6])
        out[7] = _param_narrowed(params[7])
        out[8] = _param_narrowed(params[8])
        out[9] = _param_narrowed(params[9])
        out[10] = _param_narrowed(params[10])
        out[11] = _param_narrowed(params[11])
        out[12] = _param_narrowed(params[12])
        out[13] = _param_narrowed(params[13])
        out[14] = _param_narrowed(params[14])
        out[15] = _param_narrowed(params[15])
        out[16] = _param_narrowed(params[16])
        out[17] = _param_narrowed(params[17])
        out[18] = _param_narrowed(params[18])
        out[19] = _param_narrowed(params[19])
        out[20] = _param_narrowed(params[20])
        out[21] = _param_narrowed(params[21])
        out[22] = _param_narrowed(params[22])
        out[23] = _param_narrowed(params[23])
        out[24] = _param_narrowed(params[24])
        return out
    elseif params isa NTuple{25, Any}
        out = Vector{Union{String, Missing}}(undef, 25)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        out[5] = _param_narrowed(params[5])
        out[6] = _param_narrowed(params[6])
        out[7] = _param_narrowed(params[7])
        out[8] = _param_narrowed(params[8])
        out[9] = _param_narrowed(params[9])
        out[10] = _param_narrowed(params[10])
        out[11] = _param_narrowed(params[11])
        out[12] = _param_narrowed(params[12])
        out[13] = _param_narrowed(params[13])
        out[14] = _param_narrowed(params[14])
        out[15] = _param_narrowed(params[15])
        out[16] = _param_narrowed(params[16])
        out[17] = _param_narrowed(params[17])
        out[18] = _param_narrowed(params[18])
        out[19] = _param_narrowed(params[19])
        out[20] = _param_narrowed(params[20])
        out[21] = _param_narrowed(params[21])
        out[22] = _param_narrowed(params[22])
        out[23] = _param_narrowed(params[23])
        out[24] = _param_narrowed(params[24])
        out[25] = _param_narrowed(params[25])
        return out
    elseif params isa NTuple{26, Any}
        out = Vector{Union{String, Missing}}(undef, 26)
        out[1] = _param_narrowed(params[1])
        out[2] = _param_narrowed(params[2])
        out[3] = _param_narrowed(params[3])
        out[4] = _param_narrowed(params[4])
        out[5] = _param_narrowed(params[5])
        out[6] = _param_narrowed(params[6])
        out[7] = _param_narrowed(params[7])
        out[8] = _param_narrowed(params[8])
        out[9] = _param_narrowed(params[9])
        out[10] = _param_narrowed(params[10])
        out[11] = _param_narrowed(params[11])
        out[12] = _param_narrowed(params[12])
        out[13] = _param_narrowed(params[13])
        out[14] = _param_narrowed(params[14])
        out[15] = _param_narrowed(params[15])
        out[16] = _param_narrowed(params[16])
        out[17] = _param_narrowed(params[17])
        out[18] = _param_narrowed(params[18])
        out[19] = _param_narrowed(params[19])
        out[20] = _param_narrowed(params[20])
        out[21] = _param_narrowed(params[21])
        out[22] = _param_narrowed(params[22])
        out[23] = _param_narrowed(params[23])
        out[24] = _param_narrowed(params[24])
        out[25] = _param_narrowed(params[25])
        out[26] = _param_narrowed(params[26])
        return out
    elseif params isa Vector{Any}
        out = Vector{Union{String, Missing}}(undef, length(params))
        for i in eachindex(params)
            out[i] = _param_narrowed(params[i])
        end
        return out
    end
    throw(ArgumentError("unsupported params shape: pass a tuple (arity <= 26) or Vector{Any}"))

end

function _query_once(::Type{T}, sql::String, params=nothing; forcenew::Bool=false) where {T}
    _count_query!()
    with_db_connection(; forcenew) do
        try
            return execute_prepared(_get_db_connection(), sql, params, T)
        catch e
            # isa-narrow before occursin: ArgumentError.msg is declared
            # AbstractString, and an abstract-typed occursin is a dynamic
            # call under --trim (ours is always a String literal)
            if e isa ArgumentError
                m = e.msg
                if m isa String && occursin("Collection is empty", m)
                    return nothing
                end
            end
            rethrow()
        end
    end
end

"""
    query(T, sql, params = nothing) -> Union{T, Nothing}

Run `sql` with `params` (a tuple, up to 26 values, or a `Vector{Any}`) and decode its
row as a `T`, or return `nothing` when there are no rows. More than one row throws an
`ArgumentError`; use [`queryall`](@ref) for those. `\$1`, `\$2`, ... in `sql`
are the parameters. `nothing` and `missing` parameters are SQL NULL.
"""
function query(::Type{T}, sql::String, @nospecialize(params=nothing)) where {T}
    params = _normalize_params(params)
    try
        return _query_once(T, sql, params)
    catch e
        _should_retry(e, sql) || rethrow()
        return _query_once(T, sql, params; forcenew=true)
    end
end

function _queryall_once(::Type{T}, sql::String, params=nothing; forcenew::Bool=false) where {T}
    _count_query!()
    with_db_connection(; forcenew) do
        return execute_prepared(_get_db_connection(), sql, params, Vector{T})
    end
end

"""
    queryall(T, sql, params = nothing) -> Vector{T}

Run `sql` with `params`, like [`query`](@ref), and decode every row as a `T`.
"""
function queryall(::Type{T}, sql::String, @nospecialize(params=nothing)) where {T}
    params = _normalize_params(params)
    try
        return _queryall_once(T, sql, params)
    catch e
        _should_retry(e, sql) || rethrow()
        return _queryall_once(T, sql, params; forcenew=true)
    end
end

function _execute_once(sql::String, params=nothing; forcenew::Bool=false)
    _count_query!()
    with_db_connection(; forcenew) do
        return execute_prepared(_get_db_connection(), sql, params, Any)
    end
end

# prepare + execute + close! is THREE network round trips per query; over a
# WAN link to a hosted database that dominates every request. The driver's
# DBInterface.prepare is itself a per-connection statement cache
# (conn.statements: repeat calls are a dict hit, LRU-bounded by
# statement_cache_maxsize, re-prepared transparently after a reconnect), so
# the steady state is ONE round trip per query. Statements are session-scoped
# server-side and die with the connection, as does the driver's cache.
function execute_prepared(conn::PgConnection, sql::String, params, ::Type{T}) where {T}
    stmt = try
        # assert the concrete statement type: prepare's return otherwise widens
        # under --trim, making the execute call below dynamic
        DBInterface.prepare(conn, sql)::PgStatement
    catch e
        # nothing has executed yet — mark it so the retry policy knows
        # this failure is safe to retry for ANY statement
        e isa Exception ? throw(StatementNotRun(e)) : rethrow()
    end
    return DBInterface.execute(stmt, params, T)
end

"""
    execute(sql, params = nothing)

Run `sql` with `params`, like [`query`](@ref), for statements whose rows you do not need.
"""
function execute(sql::String, @nospecialize(params=nothing))
    params = _normalize_params(params)
    try
        return _execute_once(sql, params)
    catch e
        _should_retry(e, sql) || rethrow()
        return _execute_once(sql, params; forcenew=true)
    end
end

end # module ScopedPostgres
