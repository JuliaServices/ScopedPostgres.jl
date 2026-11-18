# Reads a `juliac --trim=unsafe-warn` build log and fails if any trim verifier error
# comes from somewhere other than the two known upstream paths in Postgres.jl's
# dependencies, which stock Julia cannot trim yet:
# - Base64's pipes (they store their stream as an abstract `IO`), reached through
#   SASLAuth's SCRAM sign-in;
# - Dates' `CompoundPeriod` sorting, reached through Postgres.jl's INTERVAL parsing.
# Usage: julia test/trim/check_errors.jl build.log
const KNOWN = ("SASLAuth", "Dates.Period")

blocks = split(read(ARGS[1], String), r"\n(?=(?:Error|Warning) #\d+:)")
errors = filter(block -> occursin(r"^(?:Error|Warning) #\d+:", block), blocks)
unknown = filter(block -> !any(marker -> occursin(marker, block), KNOWN), errors)
println("trim verifier errors: ", length(errors), " (", length(errors) - length(unknown),
    " from Base64 via SASLAuth or Dates via Postgres INTERVAL parsing)")
for block in unknown
    println("\nUnexpected trim verifier error:\n", block)
end
isempty(unknown) || exit(1)
