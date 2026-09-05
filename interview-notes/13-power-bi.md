# 13 — Power BI

## What Power BI contributes

Power BI loads exported Parquet marts into a semantic model, relates facts to
dimensions, evaluates DAX measures under filter context, and presents five
business pages. It is the consumer layer, not the canonical transformation layer.

## Key concepts

- Power Query loads and shapes source files during refresh.
- Model relationships propagate filters from dimensions to facts.
- Calculated columns are computed per row at refresh.
- Measures compute aggregations at query time under current filter context.
- Star schemas reduce ambiguous relationships and simplify DAX.

## Design boundary

Stable row logic stays in dbt. DAX handles filter-aware aggregation. Every
important measure is documented and has a SQL-equivalent definition so the
binary PBIX is not the only source of meaning.

## Questions and answers

### “Why single-direction relationships?”

> Dimensions should filter facts predictably. Bi-directional filters can create
> ambiguous paths and surprising totals, especially with multiple facts.

### “Measure versus calculated column?”

> A column stores one result per row at refresh; a measure evaluates an
> aggregation for the current visual filters. Completion rate and gross bookings
> are measures. Reusable row-level business attributes belong upstream in dbt.

### “How did you verify the report?”

> I compared DAX outputs against warehouse SQL totals and documented the
> definitions. The full clean set is 16,203 trips, 13,897 completed,
> ₹7,936,695.40 gross bookings, and ₹1,511,749.30 commission.

### “What is the weakness of Power BI here?”

> The PBIX is binary, Windows-only, and not fully testable in CI. Exported
> Parquet, documented measures, metric SQL, and the static dashboard mitigate
> that limitation without pretending it disappears.
