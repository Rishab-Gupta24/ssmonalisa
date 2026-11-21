# ssmonalisa: Shopping Sessions in C++

A single static C++20 binary that implements `rules.md` (Databricks Shopping Sessions track) and ships in a `FROM scratch` image.

## Pipeline

1. **Metadata**: `manifest.json`, `products.csv`, `campaigns.csv`.
2. **Links**: parallel parse of `links/*.csv`, then union-find; each person is keyed by its byte-wise smallest ID.
3. **Events + clicks**: files are `mmap`ed and split into line-aligned chunks parsed by all threads with a strict CSV scanner (rules.md dialect) and an integer-only F1/F2/F3 timestamp parser. Events scatter into 256 partitions by `event_id`; valid clicks scatter into 256 person shards.
4. **Dedup**: each partition keeps the copy with the smallest `ingest_ts`, then drops it if invalid or late.
5. **Sessions + attribution**: per person shard, events sort by `(person, ts)` and clicks by `(person, ts, click_id)`; one pass builds sessions and user-day rows, and a sliding 7-day window over clicks gives the credited click and assisting campaigns.
6. **Output**: a built-in minimal Parquet writer (plain encoding, uncompressed, `DATE` for `day`).

IDs that look like `[a-z][0-9_a-z]{0,8}` are packed into order-preserving 64-bit keys; anything else is hashed and kept in a registry so byte-wise comparisons stay exact.

## Test locally (no Docker)

```sh
pip install polars
scripts/local_test.sh ~/ss/dev ~/ss/dev-answer   # builds, runs 36 fixtures, runs + checks the dev set
```
