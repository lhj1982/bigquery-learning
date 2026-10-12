Lab 10 — the concepts I would learn after this

Don't stop once the code works. For interviews, understand why these concepts exist:

Concept	You should be able to explain
PCollection	distributed collection of elements
ParDo	parallel transformation
GroupByKey	shuffle/group records
Windowing	grouping unbounded streaming data by time
Watermark	Dataflow's estimate of event-time progress
Late data	events arriving after their expected window
Batch vs streaming	bounded vs unbounded datasets
Partitioning	reduce BigQuery scan
Clustering	organize/filter within partitions
Dead-letter queue	isolate malformed records
Idempotency	tolerate retries/duplicates
Exactly-once vs at-least-once	delivery/processing semantics
Backfill	reprocess historical data
Schema evolution	adding/changing columns safely