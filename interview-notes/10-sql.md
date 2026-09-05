# 10 — SQL Used in RideFlow

## SQL's role

SQL expresses set-based transformations over landed events and warehouse
tables. dbt compiles and orders the SQL; DuckDB executes it. RideFlow uses SQL
for parsing payload fields, window-based deduplication, conditional aggregation,
trip assembly, joins, incremental filtering, dimensional keys, metrics, and
quality assertions.

## Window functions

Deduplication can assign:

```sql
row_number() over (
  partition by event_id
  order by landed_at, kafka_partition, kafka_offset
)
```

Then keep row 1. Unlike `distinct`, this states the duplicate key and the
deterministic winner.

## Conditional aggregation

Trip assembly converts many events into one trip row using expressions such as
`max(case when event_type = 'RideCompleted' then event_timestamp end)`. This
pivots lifecycle evidence while preserving null when a stage never occurred.

## Join discipline

Before every join, state left grain, right grain, expected cardinality, and
post-join grain. Dimension joins should normally be many facts to one
dimension. Joining facts requires pre-aggregation to a common grain.

## Null semantics

`NULL` means unknown, absent, or not applicable depending on the contract. It
does not equal anything, including another null; use `is null`. `coalesce` is
appropriate only when the replacement preserves business meaning. Replacing a
missing fare with zero would invent a measured zero.

## Time and money

Events are stored in UTC and converted explicitly to `Asia/Kolkata` when making
business date/time keys. Fixed-precision `decimal` is used for financial
arithmetic rather than floating point.

## Query planning concepts

Column projection, filter pushdown, partition pruning, join cardinality, and
early aggregation affect performance. `EXPLAIN` shows the plan; measurement is
better than guessing indexes or adding distributed tools.

## Questions and answers

### “`WHERE` versus `HAVING`?”

> `WHERE` filters input rows before aggregation; `HAVING` filters groups after
> aggregation. Filtering early usually reduces work, but the required business
> meaning decides placement.

### “Why window functions for dedup?”

> They preserve all columns while ranking rows inside each event ID and let me
> define an explicit stable winner. `GROUP BY` would require aggregation rules
> for every column, and `DISTINCT` would not remove duplicates whose load
> metadata differs.

### “How do joins create duplicates?”

> A one-to-many match multiplies the left row. If both sides contain repeated
> keys, multiplication can be many-to-many. I verify key uniqueness or
> aggregate to the target grain before joining, then reconcile row counts.

### “CTE versus subquery?”

> Both express intermediate relations; a CTE often improves readability and
> supports staged reasoning. Performance depends on the engine's optimizer, so
> I inspect plans rather than assuming every CTE materializes.

### “How would you optimize a slow RideFlow query?”

> Confirm required grain and correctness first, examine `EXPLAIN`, reduce scanned
> partitions/columns, push selective filters, pre-aggregate before large joins,
> remove repeated work, and measure again. I would not add an index or Spark
> without evidence that it addresses the actual bottleneck.
