# 15 — Final Revision Cheatsheet

## One-line architecture

Python events → Kafka → validating consumer/DLQ → immutable Parquet →
dbt/DuckDB → exported marts → Power BI, orchestrated by Airflow and gated by CI.

## Numbers

- 9 event types
- Kafka partitions: 12 trips, 6 presence, 3 per DLQ
- 7-day source retention; 30-day DLQ retention
- 48-hour normal incremental lookback
- 21 dbt models + 11 seeds + 135 tests = 167 resources
- 9 Airflow tasks
- 5 Power BI pages, 30 measures
- 90,186 full-data landed events
- 16,415 trips; 212 quarantined; 16,203 clean
- 13,897 completed; 85.77% completion
- ₹7,936,695.40 gross bookings
- ₹1,511,749.30 platform commission
- forced kill: 17,880 distinct recovered, 0 missing, 13 raw redeliveries
- recorded ingestion: 4,368 events/second

## Ten sentences to know

1. Kafka orders only inside one partition.
2. Trip and presence events need different partition keys, so they use separate topics.
3. Write-before-commit prevents loss but permits redelivery.
4. End-to-end semantics are at-least-once plus deterministic deduplication.
5. Raw Parquet is immutable and is the analytical system of record.
6. Business logic lives in dbt, not ingestion or Power BI.
7. Event, ingestion, and physical landing time solve different problems.
8. Grain is declared before facts and joins.
9. A green process is not proof of correct data; reconciliation is required.
10. DuckDB's single-writer constraint caused the Parquet decoupling architecture.

## Tool boundaries

| Tool | Does | Does not do |
|---|---|---|
| Kafka | durable transport and replay | transform business data |
| Consumer | validate and persist | assemble full trips |
| Parquet | durable columnar files | provide ACID tables alone |
| dbt | transform, test, document | schedule itself |
| DuckDB | execute OLAP locally | provide production HA |
| Airflow | orchestrate tasks | replace Kafka or dbt |
| Docker | package/run topology | create production readiness automatically |
| GitHub Actions | verify changes | prove every real-world scenario |
| Power BI | model and present metrics | own canonical transformations |

## Never overclaim

- Not production deployed.
- Not end-to-end exactly once.
- Not real Bengaluru trip data.
- TLC calibration not completed.
- One broker is not highly available.
- 48 hours is not the maximum accepted lateness.
- Power BI is not fully CI-testable.
- Some bad events remain in audit facts; business marts exclude quarantine.

## Three strongest stories

1. DuckDB lock → immutable landing-zone architecture.
2. Consumer SIGKILL → zero loss and expected deduplicated redelivery.
3. Clean CI failure → hidden local state and JSONL/Parquet mismatch fixed.

## Five questions to ask the interviewer

1. How do you handle late data and historical corrections?
2. Where does your end-to-end delivery guarantee stop?
3. How do you detect a silently wrong metric?
4. How are schema changes coordinated between producers and consumers?
5. What measured bottleneck is driving your current platform changes?

## Interview-day routine

- Draw the architecture once from memory.
- Speak the 30-second and 90-second answers aloud.
- Review the numbers above.
- Practise the three stories using STAR.
- Answer five random questions from `INTERVIEW_NOTES.md`.
- Stop studying shortly before the interview; clarity beats last-minute volume.
