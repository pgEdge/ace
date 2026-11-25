# Reviewing a change to ACE

ACE compares and repairs data across the nodes of a Spock cluster: table-diff
and repset-diff by block hashes, schema-diff, spock-diff, Merkle trees kept
current through CDC, and table-repair that writes the chosen rows back to the
other nodes, in Spock repair mode so the writes are not replicated. It runs
as a CLI, an API server and a scheduler, in Go with pgx against several nodes
at once. A wrong comparison hides divergence, and a wrong repair spreads it
to every node, which is why the bar is high. Review a change
assuming it has a defect until you have checked the places one would hide.

## How to run a review

- On demand, in a Claude Code session: `/ace-commit-review`. It reads the
  staged change and reviews it against this file.
- As a pre-commit gate: the `ace-commit-review` skill installs a git hook
  (`.claude/skills/ace-commit-review/scripts/install-hook.sh`) that hands
  the reviewer the staged change and the committed copy of this file. It is
  opt-in and off by default; enable it with `git config --global
  ace.commitReview true`, or for one commit with `ACE_REVIEW=on`.
  `ACE_REVIEW=skip` bypasses one commit while it is on.
- By hand: work through the checklist below against the diff and the code
  around every hunk.

This file is the one place the checklist lives. The skill under
`.claude/skills/ace-commit-review` holds only the hook mechanics and the
output format the hook parses.

A diff alone is not enough. It rarely shows the caller, the SQL template, the
goroutine that owns the connection, or the type switch that decides whether
the change is right, so open the functions around each hunk before judging
it.

## What to check, in this order

1. **Comparison correctness.** Anything that can report two nodes equal when
   a row differs, or different when they are equal: block boundaries and the
   rows at them, composite and non-integer primary keys and their ordering
   across nodes, NULLs, numeric scale, arrays, bytea, timestamps and time
   zones, collation, and the row hash and block hash algorithms. The hash
   version stored with a Merkle tree must name the algorithm of every leaf
   in it; a path that mixes versions is critical.
2. **Repair safety.** A repair must change exactly the rows it reported, on
   exactly the nodes it reported, inside Spock repair mode, in a transaction
   that either applies fully or not at all. Check the source-of-truth choice,
   dry-run, the generated INSERT, UPDATE and DELETE statements, upsert on
   conflict, foreign keys and triggers, and what happens when a node fails
   midway. A repair that can delete or overwrite the wrong row is critical.
3. **SQL construction.** Table, schema and column names are user and
   catalog input: every identifier must be quoted, every value a bound
   parameter, and `pg_catalog` queries must handle quoted and mixed-case
   names, partitioned tables and foreign tables the way the code claims.
   Check each SQL template under `db/queries` that a hunk touches.
4. **Concurrency and resources.** Goroutines, worker pools and errgroups:
   is every connection returned to its pool and every pool closed, does
   cancellation of the context stop the work, can a shared map or slice be
   written by two goroutines, is a failure in one worker reported rather
   than lost, and is the memory for a large table bounded. Look for `go`
   statements without a wait, channels that can block forever, and
   `defer` inside loops.
5. **Spock and CDC interaction.** Node lists, `spock.node` and
   `spock.subscription` lookups, replication set membership, the CDC drain
   order before a Merkle tree update, truncate and stale leaf handling, and
   the single-node and failed-node paths the tests cover. Nothing may leave
   a tree marked current when a leaf was not recomputed.
6. **Error handling and exit paths.** Wrapped errors keep their cause,
   nothing is swallowed with `_`, partial failures are reported with the
   node name, CLI exit codes and API responses reflect the result, and
   the scheduler does not retry a repair that half applied.
7. **Configuration and compatibility.** Changes to `ace.sample.yaml`, the
   default config, flags or the API change user-facing behaviour: check
   defaults, validation, the docs, and whether an existing config or
   stored Merkle tree still works.
8. **Tests.** A fix without a test that fails before it and passes after it
   is incomplete unless the commit says why none is possible. Pure logic
   gets a unit test next to the code; behaviour against a cluster gets an
   integration test under `tests/integration`, exercising the changed path,
   without sleeps where a wait on a condition would do. The code must be
   gofmt clean and `go vet` clean, and must build with `go build ./...`.
9. **Project conventions.** Comments short and factual, no em dashes
   anywhere in code, comments, SQL, tests, docs or messages. Commit subject:
   one capitalized imperative line ending with a period, prefixed
   `Issue (#N): ` when it fixes a GitHub issue, body in plain paragraphs
   explaining the problem and the
   fix, no attribution trailers (no `Co-Authored-By`, no "Generated with").
10. **Scope.** Changes mixed into the commit that the message does not cover.

## How to judge

Be concrete and adversarial. For each finding, show the line, state what
happens (the sequence of events, the input, the configuration), and what the
consequence is. "Could be a problem" is not a finding; "with a composite key
whose second column is text, the block boundary query orders by the Go
string comparison while the server uses the collation, so rows near the
boundary land in different blocks on different nodes" is. If you reject, say
exactly what change or evidence would make you approve. If you approve, say
what you checked and found sound, because an approval with no evidence is
the same as not reviewing.

Do not pad. Do not list style nits ahead of correctness. Do not soften a
rejection because the change is small, the author is senior, or the commit
message sounds confident. Do not approve to be agreeable.

## Verdict

Report a single `APPROVE` or `REJECT`, a one or two sentence summary, the
findings most severe first (file:line, what happens, consequence, fix), what
you checked and found sound, and whether the commit message is accurate.

Severity: critical = a repair that writes or deletes the wrong rows, a diff
that hides a real difference, SQL injection, a Merkle tree left inconsistent
with its version; high = a crash, a leaked or exhausted connection pool, a
wrong result on some supported key type or configuration, a hang; medium =
correctness that needs an unusual condition, or a test that proves nothing;
low = convention.

Reject on any critical or high finding, on a fix with no test and no stated
reason, and on a commit message that misstates what the change does. Approve
only when you have opened the surrounding code for every hunk and found
nothing of medium severity or above.
