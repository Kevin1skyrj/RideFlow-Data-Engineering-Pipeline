# RideFlow — Interview Notes

**A question bank derived from this project's actual design decisions.**

| Field | Value |
|---|---|
| Version | `1.0.0` |
| Questions | **186** |
| Last updated | 2026-08-06 |

---

## 0. How to use this file

Every question here traces to a real decision in RideFlow, so every answer can be grounded in something you built rather than something you read.

### The three-layer answer

Weak answers stop at layer 1. Strong answers reach layer 3.

| Layer | Content | Example |
|---|---|---|
| **1 — What** | The choice | "I used Kafka for streaming ingestion." |
| **2 — Why** | The mechanism | "Because it's a replayable log — retained events can be re-consumed, so a consumer bug is recoverable rather than fatal." |
| **3 — Trade-off** | What you gave up, and when the choice stops being right | "The cost is operational weight. Below ~100 events/sec with no replay requirement, appending to files is the correct answer and Kafka would be resume-driven development." |

**Layer 3 is the differentiator.** Anyone can name a technology. Very few candidates can say when their own choice would be wrong.

### The two rules

1. **Never claim a guarantee you cannot demonstrate.** If asked "how do you know there's no data loss?", the answer is a test that kills the consumer mid-batch and reconciles counts — not "because we commit offsets after writing."
2. **Volunteer the weakness before they find it.** Saying "the biggest limitation is that a terminal payment failure isn't representable in v1 — here's the fix" is far stronger than being caught by it.

---

## 1. Apache Kafka

**Why used:** Durable, replayable log decoupling producer from consumer. Per-partition ordering, consumer-group scale-out, backpressure absorption.

**Alternatives rejected:** RabbitMQ (queue, not log — no replay), Kinesis/Pub-Sub (cloud-locked, costs money, reviewer can't run it), Pulsar (technically strong, weaker ecosystem and hiring signal), direct file writes (sidesteps every problem worth demonstrating).

**Trade-off accepted:** Operational weight. Below ~100 events/sec with no replay requirement, Kafka is overhead.

### Questions

1. Why Kafka and not RabbitMQ?
2. Why not Kinesis or Pub/Sub?
3. Why not Pulsar? *(Honest answer: ecosystem and hiring signal, not technical merit.)*
4. What does Kafka actually guarantee about ordering?
5. Why did you key by `trip_id`?
6. What would happen if you keyed by `city_id` instead?
7. What if you sent messages with a null key?
8. Why 12 partitions specifically?
9. Can you decrease partition count later? What breaks if you increase it?
10. What is a consumer group and why did you use two?
11. What happens during a rebalance? What must the consumer do before releasing a partition?
12. Why `enable.auto.commit=false`?
13. What exactly goes wrong with auto-commit enabled?
14. Walk me through your offset commit sequence.
15. What happens if the consumer crashes *after* the Parquet write but *before* the commit?
16. Why is `acks=all` set on the producer?
17. What does `enable.idempotence=true` do, and why is it different from your consumer-side dedup?
18. Why is `max.in.flight.requests=5` safe here but dangerous without idempotence?
19. Why 7-day retention and not 1 or 30?
20. Why is `cleanup.policy=compact` wrong for this topic?
21. What is your DLQ, and what specifically goes into it?
22. Why do you commit the offset for a DLQ'd message?
23. Why store raw base64 bytes in the DLQ instead of the parsed message?
24. How would you replay a DLQ message after fixing the producer?
25. What's the most important Kafka metric to monitor, and why?
26. What happens when consumer lag exceeds retention?
27. Why KRaft instead of ZooKeeper?
28. Why replication factor 1, and what would you change in production?
29. Why did you disable topic auto-creation?
30. How would you handle a schema change without breaking consumers?

### Follow-up chains to expect

> Why Kafka? → Why not RabbitMQ? → So what does replay actually buy you? → Give me a concrete incident where it saved you → How long is your replay window and why that number? → What if the bug is found after retention expires?

> Why key by `trip_id`? → What ordering does that guarantee? → Does Kafka ordering guarantee *causal* order? → No — so how do you actually reconstruct sequence? → *(`causation_id`, resolved at the intermediate layer)*

---

## 2. Apache Parquet

**Why used:** Columnar, compressed, self-describing, partition-prunable, and **engine-neutral** — every future migration path depends on that last property.

**Alternatives rejected:** JSON/CSV (no schema, no compression, full scans), Avro (row-oriented — right for streaming, wrong for analytics), Delta/Iceberg (genuinely better; complexity not yet justified).

**Trade-off accepted:** Small-file proliferation, and no ACID or time travel.

### Questions

31. Why Parquet and not CSV?
32. Why not JSON, when your Kafka messages are already JSON?
33. What does columnar storage actually buy on your query patterns?
34. Why is Avro right for Kafka but wrong for the landing zone?
35. Why not Delta Lake or Iceberg? *(Strongest "why not X" question here.)*
36. What would Iceberg give you that Parquet alone doesn't?
37. How does Parquet support schema evolution?
38. What happens when a v1.0 file and a v1.1 file sit in the same directory?
39. Explain partition pruning with your `dt=`/`hour=` layout.
40. Why partition by ingestion date rather than event date?
41. What is the small-file problem and when does it bite?
42. How would you fix it?
43. Why does engine neutrality matter if you're only using DuckDB?
44. How does Parquet compression compare to gzipped JSON, and why?

---

## 3. DuckDB

**Why used:** Columnar OLAP performance, zero infrastructure, reads Parquet directly, portable SQL dialect. A reviewer can clone and run real analytics immediately.

**Alternatives rejected:** Postgres (row-oriented, needs a server), Snowflake/BigQuery (credentials + cost — no reviewer can run it), SQLite (transactional, not analytical), ClickHouse (needs a server).

**Trade-off accepted:** **Single writer, single node.** This constraint shaped the entire architecture.

### Questions

45. Why DuckDB and not PostgreSQL?
46. Why not Snowflake or BigQuery? *(Answer: a portfolio project a reviewer cannot run is worthless.)*
47. What's the difference between DuckDB and SQLite?
48. What does "embedded" actually mean, and what does it cost you?
49. **Walk me through the single-writer constraint and how you handled it.** *(The key question.)*
50. Why didn't you just switch to Postgres to get concurrent writes?
51. Why not have the consumer and dbt coordinate the lock?
52. How does Airflow enforce single-writer in practice?
53. What happens if two dbt runs start simultaneously?
54. When does DuckDB stop being the right choice?
55. How would you migrate to Snowflake, and what would change?
56. How do you query Parquet directly from DuckDB, and why does that matter architecturally?
57. What are DuckDB's memory characteristics on a full refresh?

---

## 4. dbt

**Why used:** Transformation as engineering — DAG from `ref()`, tests as first-class objects, defined incremental semantics, generated lineage.

**Alternatives rejected:** Raw SQL + Python runner (rebuild every dbt feature, badly), PySpark (distributed overhead, no benefit at this volume), Pandas (memory-bound, hard to test), SQLMesh (better lineage; ecosystem maturity).

**Trade-off accepted:** A framework dependency, and SQL-only transformation.

### Questions

58. Why dbt instead of writing SQL scripts?
59. What does dbt give you that a Python runner wouldn't?
60. How does dbt build its DAG?
61. Why is a `ref()`-derived DAG better than a hand-maintained one?
62. Explain your staging → intermediate → marts layering.
63. What is explicitly forbidden in a staging model, and why?
64. Why are staging models views rather than tables?
65. What is an incremental model?
66. Why `delete+insert` rather than `append`?
67. What breaks if you use `append` and a run is retried?
68. How do you handle late-arriving data in an incremental model?
69. Why filter on `ingested_at` but group on `event_timestamp`?
70. What happens if you filter on `event_timestamp` instead? *(Late events missed permanently, silently.)*
71. How wide is your lookback window and how did you pick it?
72. What's the difference between a generic test and a singular test?
73. How do you enforce that a test failure blocks publication?
74. Why do some tests have `severity: warn` and others `error`?
75. What is a dbt seed and why load reference data that way?
76. Why re-run seeds every cycle?
77. How would you implement SCD Type 2 in dbt?
78. How do you test idempotency?
79. Why not PySpark?
80. When would PySpark become the right choice?

---

## 5. Apache Airflow

**Why used:** Scheduling, retry policy, parameterised backfill, and — critically — concurrency control that enforces the single-writer invariant.

**Alternatives rejected:** Prefect and Dagster (both lighter and arguably better-designed; rejected on hiring signal — an honest answer), cron (no DAG, no retry, no backfill), dbt Cloud scheduler (orchestrates dbt only).

**Trade-off accepted:** Operational weight — multiple containers, real resource cost.

### Questions

81. Why Airflow and not Prefect or Dagster? *(Be honest: hiring signal. Prefect is arguably the better engineering choice at this size.)*
82. Why not just use cron?
83. What is `max_active_runs=1` doing in your DAG, and why is it architectural?
84. Why `catchup=False`?
85. What happens with `catchup=True` and a three-month-old start date?
86. Why must `start_date` be fixed rather than `days_ago()`?
87. Why is `dagrun_timeout` shorter than the schedule interval?
88. Walk me through your task dependencies.
89. Why does `dbt_test` sit between `dbt_run` and the export?
90. Why do quality gates get **zero** retries?
91. What's wrong with retrying a dbt test failure?
92. Why exponential backoff rather than fixed?
93. How does backfill work, and what makes it safe?
94. Why is idempotency a prerequisite for backfill?
95. Why doesn't Airflow orchestrate your Kafka consumer?
96. Which executor did you use and why?
97. Why is the landing zone mounted read-only?
98. What's in your Airflow metadata database?
99. Is the Airflow metadata DB the same as your warehouse? *(No — and confusing them is a common error.)*
100. How do you alert, and why is alert severity graded?
101. Your DAG runs hourly — is your data at most one hour old? *(No. Explain the ~71-minute worst case.)*

---

## 6. Docker

**Why used:** Declarative multi-service topology, reproducibility, health-check-based startup ordering, and Python-version isolation.

**Alternatives rejected:** Local installs (unreproducible, Kafka-on-Windows is painful), Kubernetes (disproportionate), VMs (heavier, no benefit).

### Questions

102. Why Docker and not local installs?
103. Why Compose and not Kubernetes?
104. What do health checks solve that a startup script wouldn't?
105. Which components did you containerise and which did you leave on the host? Why?
106. How does Docker help with your Python 3.13 compatibility risk?
107. Why is the landing zone mounted read-only into Airflow?
108. What would change to deploy this to production?
109. How do you handle secrets?

---

## 7. Power BI

**Why used:** Enterprise BI fluency is a hiring requirement, and Power BI's model view **validates the star schema** — a badly modelled warehouse fails visibly.

**Alternatives rejected:** Streamlit (better engineering choice — cross-platform, diffable, testable — but weaker signal), Tableau (no free tier), Metabase/Superset (weaker signal), Grafana (time-series monitoring, wrong tool).

**Trade-off accepted:** Windows-only Desktop, binary `.pbix`, untestable in CI, and DAX tempts business logic out of dbt.

### Questions

110. Why Power BI and not Streamlit?
111. What did you give up by choosing it? *(Volunteer this — reproducibility and CI testability.)*
112. How do you stop business logic leaking into DAX?
113. Why does that matter architecturally?
114. How does Power BI connect to DuckDB?
115. Why did you recommend the Parquet export path over ODBC?
116. How does a BI tool validate your dimensional model?
117. What happens in Power BI if your star schema has ambiguous relationships?
118. How does the dashboard show data freshness honestly?
119. How would you version-control a `.pbix`?
120. Would you make the same choice again? *(A genuinely good answer acknowledges Streamlit was better engineering.)*

---

## 8. Event Design & Contracts

121. Why did you split `event_timestamp` and `ingested_at`? *(The single most important modelling question in this project.)*
122. What breaks if you only keep event time?
123. What breaks if you only keep ingestion time?
124. Why is `ingested_at` set by the consumer and never the producer?
125. Why past-tense PascalCase event names?
126. What's the difference between an event and a command?
127. Why is `event_id` separate from `trip_id`?
128. What is `causation_id` for?
129. How does `causation_id` help when arrival order is scrambled?
130. Why do you version each event type independently?
131. What makes a change MAJOR versus MINOR?
132. Why is changing a field's *meaning* without renaming it the most dangerous change?
133. Why do you accept unknown enum values but reject unknown `event_type`?
134. How does that enable independent producer/consumer deployment?
135. How do you guarantee backward compatibility, and how is it enforced?
136. Why must your frozen compatibility corpus never be edited to make it pass?
137. Your `PaymentCompleted` can't express a failed payment — why not? *(Volunteer this gap.)*
138. Why not just add a `FAILED` status to it?
139. `EXPIRED` has no event — what's the consequence?
140. Why can't driver presence events share a topic with trip events?

---

## 9. Data Modelling

141. Why a star schema and not 3NF?
142. What is grain and why declare it first?
143. What's the grain of your main fact table, and why include cancellations?
144. What would you lose by restricting it to completed trips?
145. What are additive, semi-additive, and non-additive measures?
146. Why is `AVG(surge_multiplier)` meaningless?
147. What's the correct way to measure average surge?
148. What is a conformed dimension?
149. What is the bus matrix and what does it enable?
150. What is a fan trap, and how do drill-across queries avoid it?
151. Why is `dim_time` separate from `dim_date`?
152. Why are your date keys local rather than UTC?
153. What breaks if you bucket hour-of-day on UTC?
154. What is SCD Type 2 and when do you use it over Type 1?
155. What's your rule for deciding which type applies?
156. Why store `customer_tier` on the fact instead of joining `dim_rider`?
157. What error does joining to `current_tier_id` cause?
158. Why does every dimension have an `UNKNOWN` row at `-1`?
159. What goes wrong with a null foreign key?
160. What is a degenerate dimension? Give one from your model.
161. Why did you denormalise derived values like `matching_duration_sec`?
162. What's the risk of storing derived values, and how do you manage it?
163. Why is `fct_payments` separate from `fct_trips`?
164. Why keep `fct_trip_events` when `fct_trips` exists?
165. Why did you not snowflake your dimensions?

---

## 10. Reliability, Quality & Pipeline Design

166. How do you guarantee you don't double-count revenue?
167. Why do you need *two* deduplication passes?
168. Why isn't the consumer-side pass sufficient on its own?
169. Why keep the earliest arrival rather than the latest?
170. Why flag duplicates instead of deleting them?
171. How do you handle late-arriving data?
172. How do you handle out-of-order events?
173. What are the three kinds of null, and why does conflating them matter?
174. Why never coalesce a missing fare to zero?
175. What happens to a malformed event end to end?
176. What's an orphaned event and why quarantine rather than discard?
177. Why do financial invariant failures block the run while other issues only warn?
178. What is idempotency and why is it a prerequisite for retry?
179. How would you prove your pipeline is idempotent?
180. What's the one component you can't recover from losing?
181. If someone deleted your warehouse right now, what happens?
182. How do you know there's no data loss on consumer restart? *(Answer with the test, not the theory.)*

---

## 11. The questions that decide the interview

These four come up in almost every data engineering interview. Prepare them properly.

### 183. "Tell me about a difficult technical decision."

**Use the DuckDB single-writer constraint.** It has everything a strong answer needs: a real constraint, three evaluated options with reasons for rejecting two, a documented trade-off, and an outcome *better* than the unconstrained design.

> DuckDB is single-writer — it takes an exclusive file lock. A consumer writing continuously would hold that lock forever, so dbt could never write. I had three options: replace DuckDB with Postgres, which solves the lock but discards columnar performance and zero-setup reproducibility; coordinate lock handoff between the two, which is fragile and couples systems that should know nothing about each other; or decouple ingestion from transformation with an immutable landing zone. I chose the third. The consumer only writes Parquet, dbt is the sole DuckDB writer, and Airflow serialises runs. It turned out to be how production lakehouses actually separate streaming ingest from batch transform — the constraint pushed me toward a better architecture than I'd have designed without it.

**Why it lands:** it demonstrates that you treat constraints as information rather than obstacles.

### 184. "What are the weaknesses of your design?"

Volunteer them. Candidates who can articulate their own system's limits are rare, and the contrast registers immediately.

| Weakness | Impact | Fix |
|---|---|---|
| Terminal payment failure unrepresentable | Payment success rate looks like 100% | `PaymentFailed` in v1.1 |
| `EXPIRED` inferred, not observed | Indistinguishable from lost events | `RideExpired` in v1.1 |
| `POOL` trips modelled as independent | Over-counts vehicle-hours | Separate `fct_pool_segments` grain |
| Driver status only at session start | Intra-session transitions invisible | `DriverStatusChanged` event |
| Power BI weakens reproducibility | Binary `.pbix`, Windows-only | Parquet mart export mitigates |
| Single node, single region | Won't scale past one machine | Documented escape hatches |

### 185. "How would you scale this to 100×?"

Do **not** answer with a list of bigger technologies.

> I'd measure which bottleneck binds first. In this design it's small-file proliferation, then single-consumer throughput — both cheap fixes: larger batches with compaction, then scaling the consumer group, which needs no code change because I provisioned 12 partitions against 1 consumer. After that it's DuckDB memory on full refresh, then single-node CPU. The architecture defers the expensive changes by keeping the landing zone in an engine-neutral format, so moving transformation to a cloud warehouse is a re-point rather than a rebuild. I wouldn't introduce Spark before the data justified it — at this volume distributed compute would make the pipeline slower, not faster.

### 186. "Tell me about a mistake you made."

Use a real one from `LEARNING.md`:

> I documented the maximum event count per trip as seven — one per event type. When I wrote a validator for my sample dataset it flagged the inconsistency: `RideCancelled` and `RideCompleted` are mutually exclusive, so the real maximum is six. I'd re-read that document several times without catching it. The lesson was that documentation needs mechanical verification the same way code does — "it looks right" isn't a test. I now validate the docs against the data in CI.

---

## 12. Behavioural questions this project answers

| Question | Use |
|---|---|
| "Walk me through a project you're proud of." | RideFlow end to end — lead with the business questions, not the tech list |
| "How do you handle ambiguity?" | Requirements were a folder skeleton; I wrote the contract first and documented the gaps |
| "How do you make technical decisions?" | Every choice in `architecture.md` records its rejected alternatives |
| "How do you ensure quality?" | Tests as gates, graded severity, chaos tests for each reliability guarantee |
| "Tell me about a time you disagreed with a requirement." | Power BI over Streamlit — adopted it, documented the two guarantees it weakened, made the mitigation binding |
| "How do you document your work?" | Ten design documents, cross-validated in CI |
| "How do you plan?" | Milestones with binding exit criteria, plus explicit non-objectives to control scope |

---

## 13. Questions to ask them

Good questions signal seniority as clearly as good answers.

1. How do you handle late-arriving data, and what's your lookback window?
2. What are your delivery semantics end to end, and where does exactly-once actually stop?
3. How do you detect a silently-wrong metric, as opposed to a failed pipeline?
4. Who owns data quality — the producing team or the platform team?
5. What happens when a producer ships a breaking schema change?
6. How do you handle backfills, and how often do you actually run them?
7. What's your most expensive pipeline, and do you know what it costs?
8. How do you decide when logic belongs in the warehouse versus the BI tool?
9. What's the biggest piece of technical debt in the platform right now?
10. When something is wrong in a dashboard, how long does it take to trace it back to the source event?

---

## 14. Preparation checklist

- [ ] Explain the DuckDB constraint decision in under 90 seconds
- [ ] Draw the architecture on a whiteboard from memory
- [ ] Explain event time vs ingestion time, including both failure modes
- [ ] Explain why exactly-once doesn't extend to a filesystem sink
- [ ] Explain the two dedup passes and which is the guarantee
- [ ] Explain why `delete+insert` and not `append`
- [ ] Explain the bus matrix and drill-across fan traps
- [ ] Name three weaknesses of your design unprompted
- [ ] Answer "how would you scale this" without naming a single new technology
- [ ] Have one specific mistake, with its fix, ready to tell
