# RideFlow — Learning Journal

**A running record of what was learned, what broke, and why decisions were made.**

> **Why this file exists.** Six months from now, in an interview, you will be asked *why* you partitioned Kafka by `trip_id` or *why* the marts use `delete+insert`. The reasoning will have faded; the code will not explain itself. This journal captures the reasoning **at the moment it happened**, when the alternatives were still fresh and the trade-off still felt real.
>
> **Write the entry the same day.** A decision reconstructed a week later becomes a justification. Only same-day notes record the option you *nearly* chose, which is usually the most interesting part.

---

## How to use this file

Append a new entry per working session. Do not edit old entries to make past reasoning look better — a decision that later turned out wrong is more valuable than one retconned into correctness, because explaining a mistake and its fix is a stronger interview answer than describing a straight line.

### Entry template

```markdown
## YYYY-MM-DD — <Milestone> — <Session focus>

**Time spent:** Xh

### What I learned
-

### Problems faced
| Problem | Root cause | Resolution | Time lost |
|---|---|---|---|

### Decisions made
| Decision | Alternatives considered | Why this one | Reversible? |
|---|---|---|---|

### What I'd do differently

### Resources
-

### Open questions
-
```

---

## 2026-08-05 — M0 — Environment assessment and project planning

**Time spent:** ~2h

### What I learned

- **A repository skeleton is not a project.** The repo had 13 directories and 7 files, all zero bytes. Directory names implied an architecture, but implication is not design. Reading the environment before writing anything surfaced three blockers that would otherwise have appeared mid-build.
- **`python --version` is not enough.** The interpreter resolved to `C:\Users\Rajat Pandey\Desktop\AI & ML\python.exe` — a path containing both a space and an ampersand. `&` is a shell metacharacter; unquoted tooling on Windows will break on it in ways that produce confusing errors far from the cause.
- **Docker being *installed* and Docker *running* are different states.** `docker --version` returned 28.4.0 happily; `docker info` failed with `open //./pipe/docker_engine: The system cannot find the file specified` — the daemon was not started.
- **Stating non-objectives is as valuable as stating objectives.** Writing down what RideFlow deliberately will *not* do (sub-second latency, multi-region, ML, real PII) made the scope defensible and gave a ready answer to "what are the weaknesses of your design?"

### Problems faced

| Problem | Root cause | Resolution | Time lost |
|---|---|---|---|
| Every file in the repo was empty | Scaffold only, no content committed | Confirmed via `git log --stat` — `7 files changed, 0 insertions` | 10 min |
| Docker daemon unreachable | Docker Desktop installed but not started | Logged as an M0 blocker; does not block M1–M2 | — |
| Python 3.13.5 may be too new for dbt / Airflow | New Python releases outrun data-tooling support | Logged as blocking item **A6**. **Did not assume compatibility either way** — must be verified against the package index at install time | — |

### Decisions made

| Decision | Alternatives considered | Why this one | Reversible? |
|---|---|---|---|
| Streaming (Kafka) over batch | Batch files, micro-batch hybrid | Exercises real distributed-systems problems — ordering, duplicates, replay | Hard |
| DuckDB as warehouse | Postgres, Snowflake, BigQuery | Zero setup means a reviewer can clone and run real SQL immediately | Medium — Parquet keeps it portable |
| dbt for transformation | Raw SQL + Python, PySpark | DAG from `ref()`, tests as first-class, defined incremental semantics | Medium |
| Airflow for orchestration | Prefect, Dagster, cron | Concurrency control enforces the single-writer invariant; strongest hiring signal | Medium |
| **Landing-zone pattern** | Swap DuckDB out; coordinate the lock | **See below — the most important decision so far** | Hard |

### The decision that shaped everything

DuckDB is a **single-writer** embedded database. A Kafka consumer writing into it continuously would hold an exclusive lock forever, and dbt — a separate process — could never acquire it.

Three options: replace DuckDB with Postgres (loses columnar performance and zero-setup), coordinate lock handoff (fragile, couples two systems that should know nothing about each other), or **decouple ingestion from transformation with an immutable landing zone**.

The third won. The consumer writes only Parquet; dbt is the sole DuckDB writer; Airflow serialises dbt runs.

**The lesson: a real constraint produced a better architecture than an unconstrained design would have.** This is the answer to "tell me about a difficult technical decision" — it has a genuine constraint, three evaluated options, a documented trade-off, and an outcome that improved on the original plan.

### What I'd do differently

Check `docker info`, not just `docker --version`, as the first environment step. The version string tells you a binary exists, not that the service is usable.

### Open questions

- Does `dbt-core` / `dbt-duckdb` support Python 3.13? **Verify, do not assume.** (A6)
- Lookback window width for late arrivals — must exceed max simulated lateness. (A5)
- Kafka deployment mode: KRaft or ZooKeeper? *(Resolved 08-06: KRaft.)*

---

## 2026-08-06 — M1 — Event contract, reference data, data dictionary, sample dataset

**Time spent:** ~5h

### What I learned

- **`event_timestamp` and `ingested_at` must both exist, and it is the highest-leverage modelling decision in the whole contract.** Event time is business truth (when it happened on the device); ingestion time is arrival truth (when we learned about it). Lateness is *exactly* their difference — with one timestamp it cannot be computed at all.

  The failure modes are asymmetric and both severe: use ingestion time for business metrics and a network outage looks like a demand spike; use event time for incremental loading and every late event is silently missed **forever**, because the high-water mark has already advanced past it. No error, no warning — just a number that is quietly wrong.

- **Event names and state names are different things.** `RideRequested` is a transition that happened; `REQUESTED` is a state a trip is in. Conflating them produces a contract where you cannot tell whether a value is describing an occurrence or a condition.

- **Past tense is not a style preference.** An event is an immutable statement that something happened. `RideRequested` is an event; `RequestRide` is a *command*. Commands can be rejected; events cannot. Getting the tense wrong signals the wrong mental model.

- **Deduplication needs two passes, and only one of them is a guarantee.** The consumer's in-memory window catches rapid redelivery. It **cannot** catch a duplicate arriving hours later after a network partition, or a full replay after a bug fix. The staging window function — recomputed from the immutable landing zone every run — is the actual correctness mechanism. Treating the consumer pass as sufficient is a subtle and common error.

- **Dedup must keep the *earliest* arrival, not the latest.** Both copies have identical payloads by definition, so the choice looks arbitrary — but earliest makes the result deterministic, and determinism is what makes re-runs byte-identical. Keeping the latest would give the same payload with a different `ingested_at`, making lateness metrics non-reproducible across runs.

- **Nulls have three distinct meanings** and conflating them misreports all three: *meaningful absence* (`driver_id` before a match — the null **is** the fact), *not yet applicable* (`completed_at` on an in-flight trip), and *did not exist yet* (a v1.1 column on a v1.0 event).

- **Every dimension needs an `UNKNOWN` row at key `-1`.** A null foreign key silently drops rows from every inner join — the loss appears in no count and no error log. `-1` keeps the row, keeps the join, and makes unknowns *countable*, so a rising unknown rate becomes an alert instead of invisible attrition.

- **Local time is not a formatting concern; it is a correctness concern.** Events store UTC. 08:12 IST is 02:42 UTC. Bucketing hour-of-day on UTC would place the Bengaluru morning peak at 2 a.m. — and the chart looks plausible enough that you would rationalise it rather than investigate.

- **Point-in-time snapshots beat dimension joins for historical attributes.** A rider who was `SILVER` in March and `GOLD` in August must show as `SILVER` on their March trips. Joining to the *current* tier drags today's value backwards over history and systematically inflates the apparent performance of higher tiers.

- **Non-additive measures are a real trap.** `AVG(surge_multiplier)` averages ratios: a ₹50 trip at 3.0× and a ₹3000 trip at 1.1× average to 2.05×, which describes nothing real. Storing `surge_amount` as currency makes the correct revenue-weighted calculation the easy one — the best defence against analytical error is making the right query the obvious one.

### Problems faced

| Problem | Root cause | Resolution | Time lost |
|---|---|---|---|
| `PaymentCompleted` cannot express a payment failure | An event named for success cannot carry a `FAILED` status without lying | Modelled retry-then-success via `attempt_number` + `previous_attempt_failure_reason`. **Documented terminal failure as Gap 2**, needing `PaymentFailed` in v1.1 | 30 min |
| `EXPIRED` is in the state machine but has no event | Nine specified event types do not include `RideExpired` | Must be *inferred* from a timeout — which makes it indistinguishable from lost events. **Documented as Gap 1**; every mart must label it as derived | 20 min |
| `DriverOnline`/`DriverOffline` do not fit the trip topic | They are supply-side, keyed by `driver_id`, not `trip_id` | **Forced an architecture change**: two topics, two consumer paths, two fact tables | 45 min |
| Data dictionary examples did not match the sample data | Wrote docs and data separately, then cross-checked | Wrote a throwaway validator; it found **4 errors** | 30 min |

### The validator was worth every minute

Rather than eyeballing 30 events of fare arithmetic, I wrote a script asserting F1–F7, T1–T2, I2/I3, C1, S4, and every stored duration against its own timestamps.

It found four mismatches — and one was a genuine **logic** error, not a typo: I had written `event_count` as ranging 2–7, but `RideCancelled` and `RideCompleted` are mutually exclusive (rule S4), so no trip can emit all seven event types. **The real maximum is 6.**

That error would have survived any amount of re-reading. It only surfaced because something mechanical checked the claim against the data.

**Lesson: verify, do not assume — especially your own documentation.** "It looks right" is not a test.

### Decisions made

| Decision | Alternatives considered | Why this one | Reversible? |
|---|---|---|---|
| Envelope + payload | Flat structure (as sketched in the plan) | Separates transport concerns from business content; the consumer can route without understanding the type | Hard |
| PascalCase past-tense event names | The plan's state names | Events are transitions, not states | Hard |
| Two Kafka topics | One shared topic | One partitioning strategy per topic; sharing would lose per-driver *or* per-trip ordering | Hard |
| `causation_id` on every event | Rely on timestamps alone | Reconstructs true causal order independent of both arrival order and unreliable device clocks | Easy |
| Unknown enums accepted, unknown `event_type` rejected | Reject all unknowns | Enables independent producer/consumer deploys; but an unknown type has no inferable payload shape | Medium |
| `dim_date` + `dim_time` split | One combined dimension | Combined needs 1,440 rows/day; split keeps `dim_time` at 1,440 rows **total**, reused across all dates | Medium |
| Order sample events by `ingested_at` | Order by trip for readability | Arrival order is what the consumer actually sees — it makes the out-of-order case visible rather than tidied away | Easy |

### What I'd do differently

Write the validator **first**, then generate the data against it. I generated data, then validated, then fixed docs. Reversing the order would have caught the `event_count` logic error before it reached a document at all.

### Resources

- Kimball, *The Data Warehouse Toolkit* — grain declaration, conformed dimensions, the bus matrix
- dbt docs — incremental strategies and idempotency semantics
- Kafka docs — partition ordering guarantees and delivery semantics
- DuckDB docs — Parquet reading and the single-writer constraint

### Open questions

- **A6 still blocking:** dbt-core / dbt-duckdb on Python 3.13. Must verify at install.
- Lookback window: starting at 48h, needs empirical tuning in M4.
- Zone count and granularity per city — needed before M2 can generate coordinates.

---

## 2026-08-06 — M1 — Design documentation (architecture, schema, ETL, Kafka, Airflow, testing)

**Time spent:** ~4h

### What I learned

- **Kafka partition count should be chosen for its divisors.** 12 divides evenly by 1, 2, 3, 4, 6, and 12 — so the consumer group scales to any of those counts with perfectly even distribution. A count like 10 leaves most consumer counts unbalanced. Partitions can be *increased* later but never decreased, and increasing them breaks key-to-partition mapping for in-flight keys.

- **Kafka's exactly-once does not apply here, and saying so is stronger than claiming it does.** EOS covers Kafka-to-Kafka read-process-write transactions. RideFlow's sink is the **filesystem**, which is not a transactional participant — there is no atomic "write Parquet and commit offset". The honest architecture is at-least-once delivery plus idempotent downstream processing, which yields *effectively-once*.

- **The offset must be committed even for a poison message.** Not committing creates a poison-pill loop: the consumer retries the same unparseable bytes forever and the partition never advances. One malformed message would halt an entire partition indefinitely. The message goes to the DLQ — quarantined, not discarded.

- **`cleanup.policy=compact` would silently destroy the event log.** Compaction keeps only the latest value per key. Keyed by `trip_id`, that means retaining *one* event per trip and deleting the rest of the lifecycle. Correct for changelog topics, catastrophic for event logs.

- **Retry policy must vary by failure class.** Retrying a dbt *test* failure is pointless — the same data tested again fails again — and it delays the alert by the full backoff sequence, turning a 1-minute notification into a 15-minute one. Transient failures get exponential backoff; quality gates get **zero** retries.

- **`catchup=False` plus explicit backfill.** With `catchup=True` and `max_active_runs=1`, deploying a DAG with a three-month-old start date queues ~2,000 runs that execute *serially* for days while the current hour goes unprocessed. Recovery should be a decision, not an automatic stampede.

- **Mount the landing zone read-only.** Immutability enforced by the OS beats immutability enforced by convention. A bug that tries to delete a landed file gets a permission error instead of silently destroying the system of record.

- **Drill-across queries need aggregate-then-join.** Joining `fct_trips` and `fct_driver_sessions` directly through shared dimensions creates a fan trap — a driver with 3 sessions and 12 trips yields 36 rows and every measure inflates. Aggregate each fact to the common grain first, *then* join.

- **Freshness claims must be honest about what they measure.** "< 15 min" is *pipeline processing* latency. Real wall-clock data age on an hourly DAG is worst-case ~71 minutes, because scheduling latency dominates. Conflating them would let the dashboard claim 15-minute freshness while showing hour-old data.

### Problems faced

| Problem | Root cause | Resolution | Time lost |
|---|---|---|---|
| Power BI conflicts with the plan's Streamlit choice | Requirement changed after the plan was written | Adopted Power BI; **documented the two guarantees it weakens** (§6.7 reproducibility, §6.4 maintainability) with binding mitigations | 30 min |
| Requested table names clash with dbt conventions | `Fact_Rides` vs `fct_trips` | Used snake_case with an explicit mapping table and stated reasoning | 15 min |
| `Dim_Time` as a single dimension scales badly | Minute-grain date-time needs 525,600 rows/year | Split into `dim_date` + `dim_time`; documented why | 20 min |

### The Power BI trade-off, recorded honestly

Power BI is a materially stronger hiring signal than Streamlit. It also:

- is **Windows-only** on Desktop, so a macOS/Linux reviewer cannot open the report;
- produces a **binary `.pbix`** — no diffs, no code review, no CI testing;
- makes it easy to write DAX that becomes business logic *outside* dbt.

The third is the dangerous one, because it silently recreates the "logic scattered across dashboards" problem the whole architecture was built to prevent. Mitigation is binding: **no business logic in DAX**, marts exported to Parquet so the analytical layer stays engine-neutral, and every measure documented in `dashboard/measures.md` with its dbt equivalent.

There is a genuine engineering argument that Streamlit was the better choice. Being able to state that clearly is a better interview answer than pretending the trade-off does not exist.

### Decisions made

| Decision | Alternatives considered | Why this one | Reversible? |
|---|---|---|---|
| Power BI over Streamlit | Streamlit, Tableau, Metabase, Superset | Enterprise BI signal; also **validates the star schema** — a bad model fails visibly in Power BI's model view | Medium |
| Parquet mart export | Direct DuckDB ODBC only | Preserves non-Windows access; fewer moving parts than an ODBC driver dependency | Easy |
| 12 / 6 partitions | 8, 10, 16 | Divisor structure enables even scale-out at every realistic consumer count | Hard to reduce |
| KRaft over ZooKeeper | ZooKeeper | One fewer container; the direction Kafka itself is moving | Easy |
| `delete+insert`, not `append` | `append`, `merge` | Append double-counts on retry, defeating the idempotency objective outright | Medium |
| Zero retries on quality gates | Uniform retry policy | Deterministic failures gain nothing from retry and delay the alert | Easy |
| Docs-consistency check in CI | Manual review | Documentation drift is a defect class — the docs are the interface the team codes against | Easy |

### What I'd do differently

Resolve the BI-tool choice **before** writing `PROJECT_PLAN.md` §7.8. Changing it afterwards meant a supersede note in `architecture.md` and a revision to the plan's guarantees. Cheap now; expensive after the dashboard exists.

### Open questions

- Does the DuckDB ODBC driver work reliably with Power BI? **Verify in M10 — do not assume.** Parquet export is the recommended path precisely because it avoids this unknown.
- Compaction threshold for small files — measure in M4 before adding a compaction step.
- Are 12 partitions over-provisioned for local single-consumer development? Probably, but reducing later is impossible, so over-provisioning is the cheap direction to err in.

---

## 2026-08-06 — M2 — Event generator

**Time spent:** ~6h

### What I learned

- **A simulation can be individually correct and collectively wrong.** Every trip the generator produced was contract-valid, every fare invariant held, all 72 tests passed — and the marketplace was still nonsense, because the *aggregate* was undersupplied. Unit correctness does not imply system realism. Only measuring the emergent statistics exposed it.

- **Surge must compare like with like.** I originally divided a 10-minute request *count* by a *stock* of online drivers. That is a flow over a stock — dimensionally meaningless, and the resulting ratio never crossed any threshold, so surge sat permanently at 1.0. The fix was to convert both sides to per-hour rates: demand rate vs. driver capacity rate (drivers × ~1.9 trips/hour).

- **A parameter that scales with the wrong quantity silently destroys a signal.** My zone-mobility term added 15% of the *city-wide* fleet to each zone's supply. So doubling the fleet doubled every zone's apparent supply, and localised scarcity — the only thing that actually produces surge — was averaged away. Making it a multiplier on *local* supply fixed it. The lesson generalises: check what each term scales with, not just its magnitude.

- **Don't surge on small numbers.** Once surge worked, 3 a.m. showed 1.18×: a zone with one request and two idle drivers computed a ratio above threshold. Real platforms don't raise prices because one person opened the app. A minimum-demand floor is not a hack; it's part of the model.

- **Fleet sizing is a rate problem, not a headcount problem.** Sizing drivers from instantaneous demand worked for a 1-hour window and collapsed to 16% completion over 24 hours, because sessions end and no new ones start. Session count has to scale with `window / average_session_length`.

- **`uuid.uuid4()` cannot be seeded.** It draws from `os.urandom`. Using it anywhere would have silently broken byte-identical reproducibility while every other value matched. Deriving UUIDs from the seeded RNG is the fix, and there is now a test specifically for it.

- **Round components before summing, never the total independently.** Rounding each fare component to 2dp and deriving the taxable subtotal as the sum of those rounded values makes invariant F1 hold *exactly*, not within tolerance. Rounding the total separately from its parts is what produces the classic one-paisa drift.

- **Docs can be executable.** Rather than copying the JSON Schemas out of `event_contract.md` into `.json` files, the test suite parses the fenced blocks out of the markdown at runtime. There is exactly one copy of each schema, so doc/code drift is not merely detected — it is impossible.

### Problems faced

| Problem | Root cause | Resolution | Time lost |
|---|---|---|---|
| Completion rate 46% (should be ~85%) | 17% of driver sessions ended before the window opened | Extend shifts that would expire pre-window | 40 min |
| Completion collapsed to 16.6% at 24h | Fleet sized for instantaneous demand; no shift churn | Scale session count by `window / avg_session` | 30 min |
| Surge permanently 1.0 | Flow ÷ stock, plus city-wide mobility term | Rate ÷ rate; mobility as a multiple of local supply | 50 min |
| Surge on 92% of trips, 1.18× at 3 a.m. | No floor on demand or supply counts | `MIN_DEMAND_FOR_SURGE`, `MIN_EFFECTIVE_SUPPLY`, threshold 0.72 | 25 min |
| Faker pinned but never imported | Assumed a name generator would be needed | Removed — the no-PII design means identities are opaque UUIDs | 5 min |

### The bug worth remembering

The surge model was wrong **three times in a row**, each fix revealing the next error:

1. Flow ÷ stock → surge never fires.
2. Fixed the units, but the city-wide mobility term erased local imbalance → still never fires.
3. Fixed mobility, but no small-number floor → surge fires *everywhere*, including 3 a.m.

None of these were caught by unit tests, because each individual function did exactly what it said. They were caught by plotting average surge by local hour and asking "does this look like a real city?" **Aggregate plausibility checks are a distinct category of test, and data projects need them.** That is now `test_surge_and_demand.py`.

### Decisions made

| Decision | Alternatives considered | Why this one | Reversible? |
|---|---|---|---|
| Schemas parsed from the contract markdown | Duplicate them as `.json` files | One copy means drift is impossible, not just detectable | Easy |
| Distance from real geocoded centroids | Sample from a distribution | Uses the real OSM coordinates; distances reflect actual Bengaluru geography | Medium |
| Surge as a separate pass over aggregate demand | Compute per trip | Surge is a property of zone-level imbalance; per-trip makes it a function of nothing | Hard |
| Anomalies mutate a known-good chain | Generate bad data inline | A malformed event is always deliberate, never a business-logic bug | Easy |
| Anomaly tags kept off the wire | Serialise them | Tags are for tests; a real consumer must not be told which events are bad | Easy |
| Nominatim geocoding for zones | Hand-typed coordinates | Real places deserve real coordinates; also catches out-of-bounds errors | Easy |

### What I'd do differently

Plot the aggregate statistics *before* writing the unit tests. I wrote 72 passing tests against a marketplace that was economically nonsense. A single hour-by-hour histogram would have caught all three surge bugs in the first ten minutes.

### Open questions

- TLC calibration is still pending — every parameter is currently `hand_tuned`. The provenance test enforces honest labelling, so this cannot be misrepresented, but the "calibrated" claim is not yet earned.
- `ZONE_MOBILITY = 0.35` and `TRIPS_PER_DRIVER_HOUR = 1.9` are estimates. Both should be re-derived from real data.
- Morning peak surges less than evening (1.07 vs 1.27). Plausible — shift starts cluster at 05:00–07:00, so mornings are better supplied — but unverified against reality.

---

## 2026-08-06 — M3/M4 — Kafka, and the ingestion consumer

**Time spent:** ~7h

### What I learned

- **A failing test is a hypothesis, not a verdict.** An integration test reported "lost 180 of 240 events". Before changing any producer code I counted what was actually in Kafka: 1,010 messages, all present. The producer was fine; the *test's consumer* was racy. Had I trusted the failure, I would have "fixed" working code and broken it.

- **`subscribe()` is asynchronous.** It returns immediately while the group join happens in the background. One `poll()` does not guarantee assignment. Combined with `auto.offset.reset=latest`, anything produced before assignment completes lands behind the consumer's start offset and is invisible forever. Waiting on `assignment()` is the deterministic fix; sleeping is a guess.

- **After a hard crash, a resuming consumer waits out `session.timeout.ms`.** The broker still considers the SIGKILLed member alive for 45 seconds, so the new consumer joins the group but receives **no partitions** until the dead member's session expires. My first crash test used a 12-second idle timeout and reported 10,662 missing events — which looked exactly like catastrophic data loss and was pure impatience.

- **Measure throughput over the processing window, not wall clock.** My first number was 297 ev/s. The real rate was 4,368 ev/s; the difference was a 12-second idle timeout being counted as processing time. A performance number is only as good as its denominator.

- **Duplicates after a crash are the correct outcome, not a bug.** 13 duplicate rows appeared after the SIGKILL test. That is the uncommitted batch being redelivered — exactly what at-least-once means. A pipeline that showed *zero* duplicates there would be more suspicious, because it would suggest offsets were committed before the write.

- **Commit the offset for rejected messages too.** Otherwise one unparseable byte creates a poison-pill loop: the consumer retries the same message forever and the partition never advances. The message is quarantined in the DLQ, not discarded — nothing is lost, and the stream keeps moving.

- **Write to a temp name, then rename.** A crash mid-write would otherwise leave a truncated `.parquet` in the landing zone. A *corrupt* file in the system of record is far worse than a missing one, because the missing one just gets redelivered.

- **Declare the Arrow schema explicitly.** Inference reads types from the first batch, so a batch where every `causation_id` happened to be null would infer a null-typed column and the next batch would fail to append.

- **Git Bash silently rewrites Unix-looking paths.** `/opt/kafka/bin/...` becomes `C:/Program Files/Git/opt/kafka/bin/...` before Docker sees it. This produced a false "0 messages" reading that briefly looked like total data loss. `MSYS_NO_PATHCONV=1` is required.

- **`&` backgrounds the whole `&&` chain.** Variables assigned earlier in the chain end up in a subshell, so a later command sees them as empty. My first crash test silently used the *wrong consumer group* and re-read the entire topic instead of resuming — a test that passed while proving nothing.

### Problems faced

| Problem | Root cause | Resolution | Time lost |
|---|---|---|---|
| "lost 180 of 240 events" | Test consumer not yet assigned partitions | Wait on `assignment()` before producing | 35 min |
| Crash test showed 10,662 missing | Resume gave up before `session.timeout.ms` expired | Idle timeout raised above 45s | 40 min |
| Throughput reported 297 ev/s | Idle time counted as processing time | Separate `processing_sec` from wall clock | 15 min |
| Crash test used the wrong group | `&` scoped variables into a subshell | Moved to a script file | 20 min |
| `GetOffsetShell` not found | Removed in Kafka 4.x | Use `kafka-get-offsets.sh` | 10 min |
| Container path mangled | Git Bash MSYS conversion | `MSYS_NO_PATHCONV=1` | 15 min |

### The pattern in all of this

Five of the six problems above **looked like data loss and were not**. Every one was resolved by measuring the system directly — counting messages in Kafka, reading committed offsets, comparing distinct IDs — rather than reasoning from the symptom.

The one time I did reason from the symptom (assuming the round-trip failure was a producer bug), I was wrong.

**Check the data before changing the code.**

### Decisions made

| Decision | Alternatives considered | Why this one | Reversible? |
|---|---|---|---|
| Consumer parses schemas from `event_contract.md` | Generate `.json` files at build time | One copy means drift is impossible, not merely detectable. Cost: the markdown must ship with the consumer. | Medium |
| `payload_json` as a string column | Flatten into typed columns | Nine event types, nine shapes - flattening gives a very wide, very sparse table. dbt extracts types where the shape is known. | Medium |
| Partition on `ingested_at` | Partition on `event_timestamp` | Arrival time is monotonic, so partitions stay append-only. Event time would need writes into closed partitions. | Hard |
| Batch flush on size **or** age | Size only | Size alone stalls forever in low traffic; time alone makes tiny files under load | Easy |
| Kafka coordinates in every row | Omit them | Makes any warehouse row traceable to the exact message that produced it | Easy |

### What I'd do differently

Write the *verification* script before the crash test. I ran the crash test three times before it was measuring anything real — twice it silently proved nothing (wrong group, premature timeout) and once it reported a catastrophe that did not exist.

### Open questions

- Consumer-side dedup caught 2,854 duplicates in one run, but only because the whole topic fitted in a single batch. With realistic batch sizes it will catch far fewer. That is fine — staging is authoritative — but the metric will look worse and should not be mistaken for a regression.
- `session.timeout.ms=45000` makes crash recovery slow. Production-reasonable, but worth revisiting if fast failover matters more than avoiding spurious rebalances.

---

## 2026-08-06 — M5 — DuckDB warehouse and the dbt staging layer

**Time spent:** ~3h

### What I learned

- **A view stores SQL text, not results — so an embedded relative path is a landmine.** dbt baked `../data/raw` into the staging views, and DuckDB resolves relative paths against whoever *queries* the view, not against the dbt project. It worked from `transformation/` and failed everywhere else, including from the project root and (eventually) Power BI. The fix is an absolute path; the lesson is that anything baked into a view definition must be location-independent.

- **Jinja inside a `vars:` value in `dbt_project.yml` does not behave like jinja elsewhere.** `landing_zone: "{{ env_var('X', 'y') }}"` was substituted textually into the source definition and lost a level of braces, producing `{ env_var(...) }` in the compiled SQL and a parse error. The `env_var` lookup belongs where the value is *used*, in the source YAML.

- **A failing test is not automatically a bug in the data.** One relationship test failed: a `DriverOffline` with no matching `DriverOnline`. Rather than relax it, I traced the session through the raw Parquet (absent) and then into the DLQ, where I found it:
  `SCHEMA_VIOLATION: payload[]: 'driver_status' is a required property` — the generator's deliberate `drop_required` anomaly, correctly rejected by the consumer.

- **Which meant the test's *severity* was wrong, not the test.** An orphaned child is the designed consequence of quarantining its parent. Erroring there would mean the DLQ can never be exercised without failing the entire warehouse build — the quality gate punishing the pipeline for working. Changed to `warn`, with the investigation recorded in the YAML so the next person does not re-derive it.

- **The financial invariants belong in SQL, not just in Python.** F1, F2, F3, F4, F7 are now dbt singular tests at `severity: error`. They block publication. Everything else warns. That split is the whole point of a graded quality gate: publishing a wrong revenue number is worse than publishing none, but blocking a build over one unknown enum value gets the gate disabled.

- **Materialise the invariant, then test it.** `stg_ride_completed` computes `fare_component_sum` and `payout_split_sum` as columns. The test then compares two columns instead of re-deriving the arithmetic inside the test — so the test cannot disagree with the model about what the rule *is*.

- **`try_cast`, not `cast`, when extracting from JSON.** A malformed value should surface as NULL and be caught by a `not_null` test. `cast` aborts the whole model, so one bad row would take down the entire warehouse build.

- **Guard against a test that passes vacuously.** "Staging row count equals distinct landed events" would pass trivially if the landing zone happened to contain no duplicates. A second test asserts the duplicates were actually there, so the first one is proving something.

### Problems faced

| Problem | Root cause | Resolution | Time lost |
|---|---|---|---|
| Warehouse only queryable from `transformation/` | Relative path baked into the view | Absolute path via `RIDEFLOW_LANDING_ZONE` | 25 min |
| `syntax error at or near "RIDEFLOW_LANDING_ZONE"` | Jinja in a `vars:` value substituted textually | Move `env_var` into `_sources.yml` | 15 min |
| DuckDB could not read `/c/Users/...` | Git Bash `pwd` returns a MSYS path | `pwd -W` for a Windows path | 10 min |
| Relationship test failed on a driver session | Parent event was legitimately DLQ'd | Severity `warn`, with the DLQ evidence documented | 20 min |
| Heredoc aborted mid-file | Bash quoting in a multi-file `cat` | Use the file tool, stop fighting the shell | 10 min |

### Decisions made

| Decision | Alternatives considered | Why this one | Reversible? |
|---|---|---|---|
| Staging as views | Tables | Recomputed from the immutable landing zone every run, which is what makes pass-2 dedup *authoritative* rather than best-effort | Easy |
| Base model + 9 typed models | One wide flattened model | Nine event types, nine shapes — one table would be very wide and very sparse | Medium |
| Financial tests `error`, quality signals `warn` | Uniform severity | An over-blocking gate gets disabled; a never-blocking gate is decoration | Easy |
| `profiles.yml` inside the project | `~/.dbt/profiles.yml` | Version-controlled, so a clean clone works with no per-machine setup. No secrets to protect — DuckDB is a local file. | Easy |
| Read-only connections everywhere but dbt | Shared read-write | A second read-write connection takes the exclusive lock and breaks the next dbt run | Hard |

### What I'd do differently

Set the landing-zone path absolute from the first line. I wrote eleven models on top of a source definition that only worked from one directory, and only caught it because I happened to run a verification script from the project root.

### Open questions

- 1,786 open driver sessions out of 2,288. Plausible — most shifts extend past the observation window — but M6 must not treat an open session as a zero-length one.
- `dbt source freshness` passes now only because data was just ingested. Its thresholds are untested against genuinely stale data.

---

## Running list of things I got wrong

Kept deliberately. **"Tell me about a mistake you made"** is a standard interview question, and a specific answer with a concrete fix beats a vague one every time.

| # | Mistake | How it surfaced | Fix | Lesson |
|---|---|---|---|---|
| 1 | Stated max `event_count` per trip as 7 | Sample-data validator | Corrected to 6 — `RideCancelled` XOR `RideCompleted` | Mechanical checks catch what re-reading cannot |
| 2 | Data dictionary examples drifted from actual sample values | Same validator | Corrected 3 example values | Docs need tests too |
| 3 | Sketched a flat event structure in the plan | Formalising M1 | Moved to envelope + payload | A sketch is not a contract |
| 4 | Chose Streamlit before confirming the BI requirement | Requirement change | Superseded to Power BI, documented the cost | Confirm requirements before committing in writing |
| 5 | Assumed presence events could share the trip topic | Writing the partitioning section | Split into two topics | Grain determines partitioning, and grain differences are easy to miss |

---

## Concepts to be able to explain from memory

Working checklist. Tick when you can explain it **without notes**, including the trade-off.

- [ ] Why event time and ingestion time must both exist, and the failure mode of each if swapped
- [ ] Why Kafka gives at-least-once and why exactly-once does not extend to a filesystem sink
- [ ] Why deduplication needs two passes and which one is the actual guarantee
- [ ] Why dedup keeps the earliest arrival rather than the latest
- [ ] Why the incremental filter uses `ingested_at` but grouping uses `event_timestamp`
- [ ] Why `delete+insert` and not `append` for incremental marts
- [ ] Why partition count should be chosen for its divisors
- [ ] Why `cleanup.policy=compact` would destroy an event log
- [ ] Why the offset is committed for poison messages
- [ ] Why every dimension needs an `UNKNOWN` row at `-1`
- [ ] Why point-in-time attributes beat dimension joins for history
- [ ] Why `AVG(surge_multiplier)` is meaningless and what to compute instead
- [ ] Why drill-across needs aggregate-then-join
- [ ] Why the landing zone is the only unrecoverable component
- [ ] Why quality gates get zero retries
- [ ] Why `catchup=False` with explicit backfill
- [ ] Why DuckDB's single-writer constraint improved the architecture
