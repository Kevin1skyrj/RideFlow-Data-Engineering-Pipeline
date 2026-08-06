"""RideFlow ingestion consumer.

Consumes from Kafka, validates against the event contract, deduplicates,
batches, and writes Parquet to the immutable landing zone.

The ordering rule that everything else depends on:

    poll -> validate -> dedupe -> write Parquet -> fsync -> COMMIT OFFSET

Offsets are committed only after a durable write. Committing earlier would make
delivery at-most-once and permit silent loss on crash; committing later than the
write means a crash produces duplicates, which are removable downstream. Losing
data is not recoverable; duplicating it is.
"""

__version__ = "0.1.0"
