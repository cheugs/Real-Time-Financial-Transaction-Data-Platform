# DE02 Acceptance Checklist: Evidence

[Français](../CHECKLIST.md) · **English**

For each item, this table says where to find the proof in the project.
<img src="../assets/icons/check.svg" width="16" alt="done"> = done and verified. All 83 items are done.

Test files:
- `unit`: tests/unit/
- `integ`: tests/integration/test_streaming_scenarios.py
- `e2e`: tests/e2e/test_pipeline.py
- `dbt`: dbt/models/**/schema.yml and dbt/tests/

## 1. Functional correctness

| Item | | Evidence |
|---|---|---|
| Realistic financial attributes | <img src="../assets/icons/check.svg" width="16" alt="done"> | `producer/generator.py`: account, merchant, amount (log-normal, local currency), currency by country, status, timestamps. `unit/test_generator.py::test_events_have_valid_financial_attributes` |
| Unique identifier | <img src="../assets/icons/check.svg" width="16" alt="done"> | UUID `event_id`, PK in `raw_transactions`. `test_every_event_has_a_unique_id`, dbt `unique` |
| Valid amounts, currencies, timestamps | <img src="../assets/icons/check.svg" width="16" alt="done"> | Spark validation + Postgres CHECK/FK + dbt `assert_amounts_are_valid`, `assert_no_future_events` |
| Ingested through Kafka | <img src="../assets/icons/check.svg" width="16" alt="done"> | `e2e::test_valid_events_flow_from_kafka_to_postgres` |
| Spark processes continuously | <img src="../assets/icons/check.svg" width="16" alt="done"> | Structured Streaming, 5 s trigger. `stream_progress` table |
| Aggregations mathematically correct | <img src="../assets/icons/check.svg" width="16" alt="done"> | `unit/test_aggregation.py` (counts, EUR sums, boundaries, status, delays) |
| PostgreSQL stores expected results | <img src="../assets/icons/check.svg" width="16" alt="done"> | e2e tests compare stored rows with sent events |
| Dashboard matches database | <img src="../assets/icons/check.svg" width="16" alt="done"> | `e2e::test_dashboard_figures_match_the_database` (same SQL module, one snapshot) |

## 2. Data quality and integrity

| Item | | Evidence |
|---|---|---|
| Missing mandatory fields detected | <img src="../assets/icons/check.svg" width="16" alt="done"> | `missing_*` reasons. `unit/test_validation.py` |
| Invalid amounts rejected | <img src="../assets/icons/check.svg" width="16" alt="done"> | `non_positive_amount`, `amount_too_large` + DB `CHECK amount > 0` |
| Invalid timestamps handled | <img src="../assets/icons/check.svg" width="16" alt="done"> | `invalid_event_time`, `event_time_in_future` |
| Duplicate IDs detected | <img src="../assets/icons/check.svg" width="16" alt="done"> | PK + `batch_log.duplicates_skipped`. `e2e::test_duplicate_events_are_stored_once` |
| Duplicates don't inflate aggregates | <img src="../assets/icons/check.svg" width="16" alt="done"> | `dropDuplicates`. `test_duplicates_do_not_inflate_aggregates`, `integ::test_duplicates_across_batches...`, dbt `assert_windows_not_inflated` |
| Invalid records isolated | <img src="../assets/icons/check.svg" width="16" alt="done"> | `bad_events` with `error_reason`. `e2e::test_invalid_events_are_isolated_with_a_reason` |
| Transformations preserve integrity | <img src="../assets/icons/check.svg" width="16" alt="done"> | dbt tests on staging/marts (unique, not_null, relationships, EUR = amount × rate) |
| DQ rules automatically tested | <img src="../assets/icons/check.svg" width="16" alt="done"> | dbt tests every 30 s + pytest + CI |

## 3. Real-time processing

| Item | | Evidence |
|---|---|---|
| No batch execution needed | <img src="../assets/icons/check.svg" width="16" alt="done"> | Long-running streaming queries |
| Event time vs processing time | <img src="../assets/icons/check.svg" width="16" alt="done"> | `event_time`, `kafka_time`, `processed_at` columns; windows use `event_time` |
| Out-of-order events | <img src="../assets/icons/check.svg" width="16" alt="done"> | `integ::test_out_of_order_events_land_in_the_right_window` |
| Late events with watermarks | <img src="../assets/icons/check.svg" width="16" alt="done"> | `integ::test_late_event_within_watermark...`, `...beyond_watermark...`, `e2e::test_late_event...` |
| Window aggregations accurate | <img src="../assets/icons/check.svg" width="16" alt="done"> | `unit/test_aggregation.py`, integration tests |
| Latency measurable | <img src="../assets/icons/check.svg" width="16" alt="done"> | `processed_at - kafka_time`. Dashboard KPI + chart. Benchmark p50/p95 |
| Throughput measurable | <img src="../assets/icons/check.svg" width="16" alt="done"> | `stream_progress.processed_rows_per_second`. Dashboard. Benchmark |
| Kafka consumer lag observable | <img src="../assets/icons/check.svg" width="16" alt="done"> | `stream_progress.kafka_lag` (latest offset − processed offset). Dashboard KPI + chart |

## 4. Reliability and fault tolerance

| Item | | Evidence |
|---|---|---|
| Recovers after restarts | <img src="../assets/icons/check.svg" width="16" alt="done"> | `restart: unless-stopped` + checkpoints. `e2e::test_spark_restart_does_not_lose_or_duplicate_events` |
| Checkpoints preserve progress | <img src="../assets/icons/check.svg" width="16" alt="done"> | `integ::test_restart_from_checkpoint_keeps_state` |
| Failed attempts retried | <img src="../assets/icons/check.svg" width="16" alt="done"> | `with_retry()` for DB writes, then the batch is re-run from the checkpoint. `e2e::test_postgres_outage...` |
| Duplicate processing doesn't corrupt results | <img src="../assets/icons/check.svg" width="16" alt="done"> | Idempotent upserts; `batch_log` in the same transaction |
| Historical events replayable | <img src="../assets/icons/check.svg" width="16" alt="done"> | `scripts/replay.*`. `e2e::test_replay_rebuilds_the_same_events` |
| No silent loss on recovery | <img src="../assets/icons/check.svg" width="16" alt="done"> | `failOnDataLoss=true`; e2e restart/outage tests count every event |
| Schema changes don't break processing | <img src="../assets/icons/check.svg" width="16" alt="done"> | v1/v2/v3 tests in unit, integration and e2e |
| Delivery guarantees documented and tested | <img src="../assets/icons/check.svg" width="16" alt="done"> | README "Delivery guarantees" table + the tests above |

## 5. Data architecture

| Item | | Evidence |
|---|---|---|
| Clear responsibilities | <img src="../assets/icons/check.svg" width="16" alt="done"> | README component table. Logic (`transformations.py`) separated from I/O (`streaming_job.py`) |
| Kafka topics/partitions configured | <img src="../assets/icons/check.svg" width="16" alt="done"> | `kafka-init`: 3 partitions, key = account_id, 7-day retention, auto-create off |
| Data flows traceable | <img src="../assets/icons/check.svg" width="16" alt="done"> | `kafka_partition`/`kafka_offset` on every row; `batch_log`, `stream_progress`, `pipeline_events` |
| Raw vs transformed separated | <img src="../assets/icons/check.svg" width="16" alt="done"> | `public` (raw/streaming) vs `analytics` (dbt) schemas |
| Tables properly structured | <img src="../assets/icons/check.svg" width="16" alt="done"> | PKs, NOT NULL, CHECK, FK to `fx_rates` (`sql/init.sql`) |
| Indexes justified | <img src="../assets/icons/check.svg" width="16" alt="done"> | Comments in `init.sql` + README "Indexes" |
| dbt modular | <img src="../assets/icons/check.svg" width="16" alt="done"> | staging → marts, sources, vars, singular tests |
| Supports future extensions | <img src="../assets/icons/check.svg" width="16" alt="done"> | New rules = new dbt model/var; new fields = schema field; config in `.env` |

## 6. Performance and scalability

See [PERFORMANCE.md](PERFORMANCE.md).

| Item | | Evidence |
|---|---|---|
| ≥ 10,000 transactions | <img src="../assets/icons/check.svg" width="16" alt="done"> | Covered by the 100k and 1M runs |
| Tested with 100,000 | <img src="../assets/icons/check.svg" width="16" alt="done"> | `benchmarks/benchmark_100000.json` |
| 1-million benchmark attempted | <img src="../assets/icons/check.svg" width="16" alt="done"> | `benchmarks/benchmark_1000000.json` |
| Throughput in events/s | <img src="../assets/icons/check.svg" width="16" alt="done"> | Produce, Spark and end-to-end events/s |
| Latency measured | <img src="../assets/icons/check.svg" width="16" alt="done"> | p50 / p95 / max |
| Resources monitored | <img src="../assets/icons/check.svg" width="16" alt="done"> | `docker stats` CPU/memory per container (in the JSON) |
| Bottlenecks identified | <img src="../assets/icons/check.svg" width="16" alt="done"> | PERFORMANCE.md "Bottlenecks" |
| Results documented | <img src="../assets/icons/check.svg" width="16" alt="done"> | PERFORMANCE.md |

## 7. Monitoring and observability

| Item | | Evidence |
|---|---|---|
| Ingestion monitored | <img src="../assets/icons/check.svg" width="16" alt="done"> | Dashboard KPIs, `batch_log`, input rate |
| Processing failures logged | <img src="../assets/icons/check.svg" width="16" alt="done"> | `pipeline_events.query_failed` + container logs (if Postgres itself is down, only the container log has it) |
| Kafka lag monitored | <img src="../assets/icons/check.svg" width="16" alt="done"> | Dashboard KPI + chart |
| Processing delays monitored | <img src="../assets/icons/check.svg" width="16" alt="done"> | Latency charts, arrival delay, late events |
| Duplicates counted | <img src="../assets/icons/check.svg" width="16" alt="done"> | "Duplicates skipped" KPI |
| Invalid events counted | <img src="../assets/icons/check.svg" width="16" alt="done"> | KPI + "invalid by reason" table |
| Recovery traceable | <img src="../assets/icons/check.svg" width="16" alt="done"> | `pipeline_events` (fresh_start / restart_from_checkpoint / replay_requested); `run_id` in `stream_progress`; "Spark restarts" KPI |
| Logs sufficient | <img src="../assets/icons/check.svg" width="16" alt="done"> | Python `logging` with batch id, valid/new/duplicate/invalid counts, retries |

## 8. Financial analytics

| Item | | Evidence |
|---|---|---|
| Volumes | <img src="../assets/icons/check.svg" width="16" alt="done"> | `txn_volume_per_minute.txn_count` |
| Total amounts | <img src="../assets/icons/check.svg" width="16" alt="done"> | `total_amount_eur` (always in one currency) |
| Distribution by status | <img src="../assets/icons/check.svg" width="16" alt="done"> | `analytics.status_per_minute`, approved/declined counts per window |
| Volumes by time window | <img src="../assets/icons/check.svg" width="16" alt="done"> | 1-minute event-time windows |
| Currency differences handled | <img src="../assets/icons/check.svg" width="16" alt="done"> | `fx_rates` + `amount_eur`; `volume_by_currency`; large-amount rule in EUR. `test_mixed_currencies_are_summed_in_eur` |
| Indicators generated | <img src="../assets/icons/check.svg" width="16" alt="done"> | `analytics.suspicious_activity` |
| Explainable rules | <img src="../assets/icons/check.svg" width="16" alt="done"> | Each row has a `details` sentence. Thresholds are dbt vars |
| No fraud claims | <img src="../assets/icons/check.svg" width="16" alt="done"> | README, dashboard caption, SQL comments |

## 9. Testing and validation

| Item | | Evidence |
|---|---|---|
| Unit tests pass | <img src="../assets/icons/check.svg" width="16" alt="done"> | 34 tests in `tests/unit` |
| Integration tests pass | <img src="../assets/icons/check.svg" width="16" alt="done"> | 7 streaming scenarios in `tests/integration` |
| End-to-end tests pass | <img src="../assets/icons/check.svg" width="16" alt="done"> | 11 tests in `tests/e2e` (9 normal + 2 slow) |
| Duplicate scenarios | <img src="../assets/icons/check.svg" width="16" alt="done"> | unit + integration + e2e |
| Late-event scenarios | <img src="../assets/icons/check.svg" width="16" alt="done"> | integration + e2e |
| Out-of-order scenarios | <img src="../assets/icons/check.svg" width="16" alt="done"> | integration |
| Application failure scenarios | <img src="../assets/icons/check.svg" width="16" alt="done"> | Spark restart, Postgres outage (e2e), checkpoint restart (integration) |
| Replay scenarios | <img src="../assets/icons/check.svg" width="16" alt="done"> | `e2e::test_replay_rebuilds_the_same_events` |
| Schema evolution scenarios | <img src="../assets/icons/check.svg" width="16" alt="done"> | unit + integration + e2e (v1, v2, unknown v3, missing version) |
| Dashboard validated against DB | <img src="../assets/icons/check.svg" width="16" alt="done"> | `e2e::test_dashboard_figures_match_the_database` |

## 10. Engineering and professional quality

| Item | | Evidence |
|---|---|---|
| Modular, maintainable | <img src="../assets/icons/check.svg" width="16" alt="done"> | generator / producer / transformations / streaming_job / queries / app |
| Configuration externalized | <img src="../assets/icons/check.svg" width="16" alt="done"> | `.env`, dbt vars |
| No secrets in git | <img src="../assets/icons/check.svg" width="16" alt="done"> | `.env` in `.gitignore`; only `.env.example` with a placeholder |
| Reproducible Docker services | <img src="../assets/icons/check.svg" width="16" alt="done"> | Pinned image and package versions; one `docker compose up` |
| CI runs tests | <img src="../assets/icons/check.svg" width="16" alt="done"> | `.github/workflows/ci.yml` runs unit, integration and end-to-end tests on every push, and passes on every commit |
| README explains architecture | <img src="../assets/icons/check.svg" width="16" alt="done"> | README |
| Decisions and trade-offs documented | <img src="../assets/icons/check.svg" width="16" alt="done"> | README "Design decisions and trade-offs" |
| Limitations acknowledged | <img src="../assets/icons/check.svg" width="16" alt="done"> | README "Known limitations" |
| Reproducible by another developer | <img src="../assets/icons/check.svg" width="16" alt="done"> | `cp .env.example .env && docker compose up -d --build` |
