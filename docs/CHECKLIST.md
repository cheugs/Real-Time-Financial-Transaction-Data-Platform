# DE02 Acceptance Checklist: Evidence

For each item, this table says where to find the proof in the project.
Status: ✅ done and verified · 🟡 done but not yet verified.

Test files:
- `unit`: tests/unit/
- `integ`: tests/integration/test_streaming_scenarios.py
- `e2e`: tests/e2e/test_pipeline.py
- `dbt`: dbt/models/**/schema.yml and dbt/tests/

## 1. Functional correctness

| Item | | Evidence |
|---|---|---|
| Realistic financial attributes | ✅ | `producer/generator.py`: account, merchant, amount (log-normal, local currency), currency by country, status, timestamps. `unit/test_generator.py::test_events_have_valid_financial_attributes` |
| Unique identifier | ✅ | UUID `event_id`, PK in `raw_transactions`. `test_every_event_has_a_unique_id`, dbt `unique` |
| Valid amounts, currencies, timestamps | ✅ | Spark validation + Postgres CHECK/FK + dbt `assert_amounts_are_valid`, `assert_no_future_events` |
| Ingested through Kafka | ✅ | `e2e::test_valid_events_flow_from_kafka_to_postgres` |
| Spark processes continuously | ✅ | Structured Streaming, 5 s trigger. `stream_progress` table |
| Aggregations mathematically correct | ✅ | `unit/test_aggregation.py` (counts, EUR sums, boundaries, status, delays) |
| PostgreSQL stores expected results | ✅ | e2e tests compare stored rows with sent events |
| Dashboard matches database | ✅ | `e2e::test_dashboard_figures_match_the_database` (same SQL module, one snapshot) |

## 2. Data quality and integrity

| Item | | Evidence |
|---|---|---|
| Missing mandatory fields detected | ✅ | `missing_*` reasons. `unit/test_validation.py` |
| Invalid amounts rejected | ✅ | `non_positive_amount`, `amount_too_large` + DB `CHECK amount > 0` |
| Invalid timestamps handled | ✅ | `invalid_event_time`, `event_time_in_future` |
| Duplicate IDs detected | ✅ | PK + `batch_log.duplicates_skipped`. `e2e::test_duplicate_events_are_stored_once` |
| Duplicates don't inflate aggregates | ✅ | `dropDuplicates`. `test_duplicates_do_not_inflate_aggregates`, `integ::test_duplicates_across_batches...`, dbt `assert_windows_not_inflated` |
| Invalid records isolated | ✅ | `bad_events` with `error_reason`. `e2e::test_invalid_events_are_isolated_with_a_reason` |
| Transformations preserve integrity | ✅ | dbt tests on staging/marts (unique, not_null, relationships, EUR = amount × rate) |
| DQ rules automatically tested | ✅ | dbt tests every 30 s + pytest + CI |

## 3. Real-time processing

| Item | | Evidence |
|---|---|---|
| No batch execution needed | ✅ | Long-running streaming queries |
| Event time vs processing time | ✅ | `event_time`, `kafka_time`, `processed_at` columns; windows use `event_time` |
| Out-of-order events | ✅ | `integ::test_out_of_order_events_land_in_the_right_window` |
| Late events with watermarks | ✅ | `integ::test_late_event_within_watermark...`, `...beyond_watermark...`, `e2e::test_late_event...` |
| Window aggregations accurate | ✅ | `unit/test_aggregation.py`, integration tests |
| Latency measurable | ✅ | `processed_at - kafka_time`. Dashboard KPI + chart. Benchmark p50/p95 |
| Throughput measurable | ✅ | `stream_progress.processed_rows_per_second`. Dashboard. Benchmark |
| Kafka consumer lag observable | ✅ | `stream_progress.kafka_lag` (latest offset − processed offset). Dashboard KPI + chart |

## 4. Reliability and fault tolerance

| Item | | Evidence |
|---|---|---|
| Recovers after restarts | ✅ | `restart: unless-stopped` + checkpoints. `e2e::test_spark_restart_does_not_lose_or_duplicate_events` |
| Checkpoints preserve progress | ✅ | `integ::test_restart_from_checkpoint_keeps_state` |
| Failed attempts retried | ✅ | `with_retry()` for DB writes, then the batch is re-run from the checkpoint. `e2e::test_postgres_outage...` |
| Duplicate processing doesn't corrupt results | ✅ | Idempotent upserts; `batch_log` in the same transaction |
| Historical events replayable | ✅ | `scripts/replay.*`. `e2e::test_replay_rebuilds_the_same_events` |
| No silent loss on recovery | ✅ | `failOnDataLoss=true`; e2e restart/outage tests count every event |
| Schema changes don't break processing | ✅ | v1/v2/v3 tests in unit, integration and e2e |
| Delivery guarantees documented and tested | ✅ | README "Delivery guarantees" table + the tests above |

## 5. Data architecture

| Item | | Evidence |
|---|---|---|
| Clear responsibilities | ✅ | README component table. Logic (`transformations.py`) separated from I/O (`streaming_job.py`) |
| Kafka topics/partitions configured | ✅ | `kafka-init`: 3 partitions, key = account_id, 7-day retention, auto-create off |
| Data flows traceable | ✅ | `kafka_partition`/`kafka_offset` on every row; `batch_log`, `stream_progress`, `pipeline_events` |
| Raw vs transformed separated | ✅ | `public` (raw/streaming) vs `analytics` (dbt) schemas |
| Tables properly structured | ✅ | PKs, NOT NULL, CHECK, FK to `fx_rates` (`sql/init.sql`) |
| Indexes justified | ✅ | Comments in `init.sql` + README "Indexes" |
| dbt modular | ✅ | staging → marts, sources, vars, singular tests |
| Supports future extensions | ✅ | New rules = new dbt model/var; new fields = schema field; config in `.env` |

## 6. Performance and scalability

See [PERFORMANCE.md](PERFORMANCE.md).

| Item | | Evidence |
|---|---|---|
| ≥ 10,000 transactions | ✅ | Covered by the 100k and 1M runs |
| Tested with 100,000 | ✅ | `benchmarks/benchmark_100000.json` |
| 1-million benchmark attempted | ✅ | `benchmarks/benchmark_1000000.json` |
| Throughput in events/s | ✅ | Produce, Spark and end-to-end events/s |
| Latency measured | ✅ | p50 / p95 / max |
| Resources monitored | ✅ | `docker stats` CPU/memory per container (in the JSON) |
| Bottlenecks identified | ✅ | PERFORMANCE.md "Bottlenecks" |
| Results documented | ✅ | PERFORMANCE.md |

## 7. Monitoring and observability

| Item | | Evidence |
|---|---|---|
| Ingestion monitored | ✅ | Dashboard KPIs, `batch_log`, input rate |
| Processing failures logged | ✅ | `pipeline_events.query_failed` + container logs (if Postgres itself is down, only the container log has it) |
| Kafka lag monitored | ✅ | Dashboard KPI + chart |
| Processing delays monitored | ✅ | Latency charts, arrival delay, late events |
| Duplicates counted | ✅ | "Duplicates skipped" KPI |
| Invalid events counted | ✅ | KPI + "invalid by reason" table |
| Recovery traceable | ✅ | `pipeline_events` (fresh_start / restart_from_checkpoint / replay_requested); `run_id` in `stream_progress`; "Spark restarts" KPI |
| Logs sufficient | ✅ | Python `logging` with batch id, valid/new/duplicate/invalid counts, retries |

## 8. Financial analytics

| Item | | Evidence |
|---|---|---|
| Volumes | ✅ | `txn_volume_per_minute.txn_count` |
| Total amounts | ✅ | `total_amount_eur` (always in one currency) |
| Distribution by status | ✅ | `analytics.status_per_minute`, approved/declined counts per window |
| Volumes by time window | ✅ | 1-minute event-time windows |
| Currency differences handled | ✅ | `fx_rates` + `amount_eur`; `volume_by_currency`; large-amount rule in EUR. `test_mixed_currencies_are_summed_in_eur` |
| Indicators generated | ✅ | `analytics.suspicious_activity` |
| Explainable rules | ✅ | Each row has a `details` sentence. Thresholds are dbt vars |
| No fraud claims | ✅ | README, dashboard caption, SQL comments |

## 9. Testing and validation

| Item | | Evidence |
|---|---|---|
| Unit tests pass | ✅ | 34 tests in `tests/unit` |
| Integration tests pass | ✅ | 7 streaming scenarios in `tests/integration` |
| End-to-end tests pass | ✅ | 11 tests in `tests/e2e` (9 normal + 2 slow) |
| Duplicate scenarios | ✅ | unit + integration + e2e |
| Late-event scenarios | ✅ | integration + e2e |
| Out-of-order scenarios | ✅ | integration |
| Application failure scenarios | ✅ | Spark restart, Postgres outage (e2e), checkpoint restart (integration) |
| Replay scenarios | ✅ | `e2e::test_replay_rebuilds_the_same_events` |
| Schema evolution scenarios | ✅ | unit + integration + e2e (v1, v2, unknown v3, missing version) |
| Dashboard validated against DB | ✅ | `e2e::test_dashboard_figures_match_the_database` |

## 10. Engineering and professional quality

| Item | | Evidence |
|---|---|---|
| Modular, maintainable | ✅ | generator / producer / transformations / streaming_job / queries / app |
| Configuration externalized | ✅ | `.env`, dbt vars |
| No secrets in git | ✅ | `.env` in `.gitignore`; only `.env.example` with a placeholder |
| Reproducible Docker services | ✅ | Pinned image and package versions; one `docker compose up` |
| CI runs tests | 🟡 | `.github/workflows/ci.yml` is written, but it only runs once the repo is on GitHub (not run yet) |
| README explains architecture | ✅ | README |
| Decisions and trade-offs documented | ✅ | README "Design decisions and trade-offs" |
| Limitations acknowledged | ✅ | README "Known limitations" |
| Reproducible by another developer | ✅ | `cp .env.example .env && docker compose up -d --build` |
