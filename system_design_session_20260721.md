# System Design Mentor — Daily Session
**Date:** 21-Jul-2026
**Topic:** Design a Logging & Monitoring System (like Datadog)
**Level:** SDE2/SDE3 | 60–150 LPA
**Mentor:** Arjun Mehta (40+ YOE)

---

## Opening Brief

Observability infrastructure is the nervous system of every large-scale product — and yet it is chronically under-designed in interviews. Datadog ingests trillions of data points per day across logs, metrics, and traces; Splunk indexes petabytes; Prometheus powers half the Kubernetes ecosystem. What makes this hard is not the volume alone — it is the combination of extreme write amplification (every service emits constantly), strict latency expectations on queries (your oncall engineer needs answers in seconds), and the fact that this system must stay alive precisely when everything else is on fire. Companies like Google (Monarch), Meta (Scuba), and Netflix (Atlas) have published extensively on this. If you can design this well, you can design anything.

---

## Warm-Up Questions (Easy)

*These establish baseline. A good SDE2 should answer all of these without hesitation.*

**Q1.** What is the difference between a log, a metric, and a trace? Give a concrete example of each from a payment service.

> **What a strong answer covers:**
> - **Log:** Structured or unstructured timestamped event record. Example: `{"ts":"2026-07-21T10:01:00Z","level":"ERROR","service":"payment","msg":"Charge failed","order_id":"ord_123","reason":"card_declined"}`
> - **Metric:** Numeric measurement aggregated over time. Example: `payment.charge.latency_p99 = 320ms` sampled every 10 seconds.
> - **Trace:** Causally linked chain of spans showing a request's path across services. Example: `traceId=abc` → span: API gateway (5ms) → span: payment service (280ms) → span: Stripe API call (270ms).
> - Candidate must articulate **cardinality** differences: logs are high-cardinality (order_id as a field), metrics are pre-aggregated low-cardinality, traces correlate logs and metrics via trace/span IDs.

> **Common weak answer:** "Logs are text files, metrics are numbers, traces are like logs with IDs." No mention of cardinality, aggregation, or how they complement each other operationally.

> **Mentor follow-up if they answer well:** "You said metrics are pre-aggregated. What exactly is lost when you aggregate? Give me a scenario where that lost information caused an incident."

---

**Q2.** A single microservice emits ~5,000 log lines per second at peak. Your company runs 200 such services. Estimate the daily storage required if you retain raw logs for 30 days. What compression ratio would you expect?

> **What a strong answer covers:**
> - Total log rate: 200 × 5,000 = 1,000,000 lines/sec = 1M lines/sec
> - Average log line: ~500 bytes (JSON with context fields)
> - Raw throughput: 1M × 500B = 500 MB/sec
> - Daily raw volume: 500 MB/s × 86,400s ≈ 43 TB/day
> - 30-day retention: 43 × 30 ≈ 1.3 PB raw
> - Compression: logs compress 5–10x with LZ4/Zstd (repetitive JSON keys, timestamps). Post-compression: ~130–260 TB for 30 days.
> - Candidate should mention **columnar storage** (Parquet/ORC) as another 3–5x improvement for archival tiers.
> - Should also flag: not all logs need 30-day raw retention — tiered storage (hot/warm/cold) is the real answer.

> **Mentor follow-up:** "You said 5–10x compression. What property of log data makes that possible, and why does that ratio degrade badly for encrypted or binary payloads?"

---

**Q3.** Should the ingestion pipeline for logs use a pull or push model? What are the trade-offs?

> **What a strong answer covers:**
> - **Push model** (service pushes logs to collector): Lower latency, simpler for ephemeral workloads (lambdas, containers), but requires services to handle backpressure and retry logic; collector becomes a bottleneck.
> - **Pull model** (collector scrapes endpoints — Prometheus-style): Collector controls rate, easier to detect dead targets, but requires stable service endpoints (hard for auto-scaling pods), adds scrape-interval lag (typically 15–60s).
> - **Hybrid** (Datadog agent): sidecar agent on each host buffers locally and pushes to aggregation tier — decouples service from central collector, handles spikes.
> - Should mention: pull is dominant for **metrics** (Prometheus), push is dominant for **logs** (Fluentd, Logstash, Vector).

> **Red flag answer:** "Pull is better because you can control it." — No reasoning about ephemeral workloads or the fundamental difference between metrics (time-series endpoints) and logs (event streams). Anyone who recommends pull-only for logs has not operated a real system.

---

## High-Level Design (Medium)

*The candidate should drive this. Expect them to draw components, identify data flows, pick protocols.*

**Q4.** Draw the high-level architecture for a Datadog-like system. Assume it must support logs, metrics, and distributed tracing. Start from the instrumented application and end at the query/dashboard layer.

> **Key components expected:**
> - Instrumented services with SDK/agent (DogStatsD, OpenTelemetry collector)
> - Edge aggregation / sidecar agent (buffering, local pre-aggregation)
> - Ingestion tier (horizontally scalable, Kafka-backed)
> - Stream processors (Flink/Spark Streaming for real-time alerting)
> - Storage: time-series DB for metrics (Prometheus/Cortex/VictoriaMetrics), columnar store for logs (ClickHouse, Druid, or Elasticsearch), distributed trace store (Jaeger/Tempo backed by object storage)
> - Query layer (Thanos/Grafana for metrics, custom query API for logs)
> - Alert engine (rule evaluation against metric streams)
> - Dashboard/UI layer

> **Architecture diagram (text):**
```
[App Service] ──(OTel SDK/DogStatsD)──► [Sidecar Agent]
                                               │
                            ┌──────────────────┤
                            │  local buffer    │
                            │  pre-aggregate   │
                            └──────────────────┘
                                               │
                                    ┌──────────▼──────────┐
                                    │  Ingestion Cluster  │
                                    │  (HTTP/gRPC fanin)  │
                                    └──────────┬──────────┘
                                               │
                              ┌────────────────▼─────────────────┐
                              │         Kafka (partitioned        │
                              │  by tenant×signal_type)          │
                              └──┬───────────────────────────┬───┘
                                 │                           │
                    ┌────────────▼────────┐     ┌───────────▼───────────┐
                    │  Stream Processor   │     │  Stream Processor     │
                    │  (Flink)            │     │  (Flink)              │
                    │  Alerting rules     │     │  Metric rollups       │
                    └────────────┬────────┘     └───────────┬───────────┘
                                 │                          │
                    ┌────────────▼──────┐    ┌─────────────▼────────┐
                    │  Alert Engine     │    │  Time-Series Store    │
                    │  (PagerDuty/OpsG) │    │  (Cortex/VictMetrics) │
                    └───────────────────┘    └──────────────────────┘
                                                          │
                              ┌───────────────────────────┼───────────────────┐
                              │                           │                   │
                   ┌──────────▼──────────┐   ┌───────────▼──────┐  ┌────────▼──────┐
                   │  Log Store          │   │  Trace Store      │  │  Query API    │
                   │  (ClickHouse/Druid) │   │  (Tempo+S3)       │  │  + Dashboard  │
                   └─────────────────────┘   └──────────────────┘  └───────────────┘
```

> **What separates SDE2 from SDE3 here:** An SDE2 draws a simple pipeline with Elasticsearch and Grafana and calls it done. An SDE3 separates the storage tier by signal type (metrics need time-series compression, logs need full-text index + columnar scan, traces need span-graph traversal with parent-child linkage), uses Kafka as the durability buffer between ingestion and storage to decouple write spikes from storage capacity, and explicitly partitions Kafka topics by tenant to prevent noisy-neighbor ingestion from starving another tenant's alerting.

---

**Q5.** Trace a single ERROR log line from the moment `logger.error("Payment failed", order_id=123)` is called in the application, all the way to it appearing in the Datadog UI search results.

> **Expected trace:**
> 1. App SDK formats the log as structured JSON, appends trace_id/span_id from active OTel context, emits to local sidecar agent via Unix socket (sub-millisecond).
> 2. Sidecar agent buffers in a ring buffer, batches by size (e.g., 512 events) or time (100ms), compresses with LZ4, and HTTP-POSTs to the regional ingestion endpoint.
> 3. Ingestion endpoint validates schema, authenticates via API key → tenant_id mapping, writes to Kafka topic `logs.raw` partition keyed by `tenant_id % num_partitions`.
> 4. Kafka consumer (ClickHouse Kafka table engine or Flink job) reads the batch, parses JSON, and inserts into ClickHouse's `logs` table with ReplicatedMergeTree engine. ClickHouse merges parts in background.
> 5. Separately, a Flink job evaluates the log against alert rules (e.g., "ERROR rate > 10/min for service=payment") using a tumbling window.
> 6. When the user queries in the UI: query hits the Query API → translates to ClickHouse SQL (`SELECT * FROM logs WHERE tenant_id=X AND level='ERROR' AND ts BETWEEN ... ORDER BY ts DESC LIMIT 500`) → ClickHouse prunes partitions by date, uses skip indexes on `level` and `service`, returns results → Query API paginates and returns to UI.

> **Tricky part:** Most candidates forget step 1 — the trace_id injection. Without it, your log and your trace are orphaned — you cannot correlate "this error log" with "this specific user's slow request trace." Also, candidates skip the ClickHouse merge-tree background compaction — queries arriving during a merge-storm see degraded performance, which is why ClickHouse has merge-on-insert settings and why Datadog runs dedicated merge threads.

---

**Q6.** Design the core API for log ingestion and log search. Define the key endpoints.

> **Expected API design:**
```
# Ingestion (write path — agent-facing, internal)
POST /v1/logs/ingest
Headers: DD-API-KEY: <tenant_api_key>
         Content-Encoding: gzip
         Content-Type: application/x-ndjson
Body: newline-delimited JSON log events (NDJSON)
Response: 202 Accepted {"intake_id": "uuid", "received": 4821}
  # 202 not 200 — async write; agent should not retry on 202

# Metrics ingestion
POST /v1/metrics
Body: {"series": [{"metric":"payment.latency","points":[[ts,val]],"tags":["env:prod","svc:payment"],"type":"gauge"}]}

# Log search (query path — user-facing)
GET /v1/logs/search
Params:
  q=service:payment AND level:ERROR   # Lucene-style query
  from=2026-07-21T10:00:00Z
  to=2026-07-21T11:00:00Z
  page_cursor=<opaque_cursor>          # cursor-based pagination — no OFFSET
  limit=200
Response: {"logs":[...], "next_cursor":"...", "total_matched": 48210}

# Metric query (Prometheus-compatible for ecosystem integration)  
GET /v1/query_range
Params: query=avg(payment.latency{env="prod"}) by (region)
        start=<unix_ts>&end=<unix_ts>&step=60s
```

> **What to push on:**
> - **Idempotency on ingestion:** Agent retries on network failure — is the same batch ingested twice? Answer: agent assigns a `batch_id`; ingestion layer deduplicates within a short window using Redis SET NX on `batch_id`. After the dedup window (5 min), duplicates are accepted — log storage is append-only and duplicates in analytics logs are acceptable (unlike financial transactions).
> - **Cursor-based pagination:** Never use `OFFSET` on 10B-row log tables. Cursor encodes `(ts, row_id)` — ClickHouse can seek efficiently.
> - **API versioning:** `/v1/` in path, not in header — easier to route at the API gateway layer and to deprecate wholesale.

---

## Data Modeling (Medium–Hard)

*This is where SDE3 candidates shine. Expect schema design, index choices, partitioning strategy.*

**Q7.** Design the storage schema for the logs table in ClickHouse (or a comparable columnar store). How do you model it to support the two dominant query patterns: (a) search by arbitrary fields and time range, (b) aggregate metrics over logs (e.g., count of ERRORs per service per minute)?

> **Expected schema:**
```sql
CREATE TABLE logs
(
    tenant_id     UInt32,
    ts            DateTime64(9, 'UTC'),   -- nanosecond precision
    level         LowCardinality(String), -- ERROR/WARN/INFO/DEBUG
    service       LowCardinality(String),
    host          String,
    trace_id      FixedString(16),        -- 128-bit stored as bytes
    span_id       FixedString(8),
    message       String,
    -- arbitrary structured fields stored as map
    attributes    Map(String, String),
    -- full message for FTS
    message_tokens String MATERIALIZED ...
)
ENGINE = ReplicatedMergeTree(
    '/clickhouse/tables/{shard}/logs',
    '{replica}'
)
PARTITION BY (tenant_id, toYYYYMM(ts))      -- partition prunes by month
ORDER BY (tenant_id, service, level, ts)     -- primary sort key = sparse index
SETTINGS index_granularity = 8192;

-- Skip index for fast predicate pushdown on level
ALTER TABLE logs ADD INDEX idx_level level TYPE set(4) GRANULARITY 1;

-- Materialized view for pre-aggregated error counts (avoids scanning raw logs)
CREATE MATERIALIZED VIEW log_error_counts_mv
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (tenant_id, service, level, window_start)
AS SELECT
    tenant_id,
    service,
    level,
    toStartOfMinute(ts) AS window_start,
    count() AS cnt
FROM logs
GROUP BY tenant_id, service, level, window_start;
```

> **Index choices and why:**
> - Primary sort key `(tenant_id, service, level, ts)` → ClickHouse's sparse primary index skips granules that don't match the WHERE clause. Queries filtered by tenant+service+level+time range scan a tiny fraction of data.
> - `LowCardinality(String)` for `level` and `service` → dictionary encoding; 4–8x compression and faster GROUP BY.
> - `Map(String, String)` for `attributes` → flexible schema without ALTER TABLE; cost is no index on map keys (full scan for rare attribute queries).
> - Skip index on `level` → for multi-tenant tables where a single granule has mixed services, the set-type skip index eliminates granules containing no ERRORs.

> **Partitioning key and why:**
> - `(tenant_id, toYYYYMM(ts))` → TTL and drop operations are partition-level. Dropping a tenant's data is `ALTER TABLE logs DROP PARTITION ...` — O(1) metadata op, no row-level deletes. Monthly granularity balances partition count vs. query pruning efficiency.

---

**Q8.** A customer queries: "Show me all ERROR logs for service=checkout in the last 15 minutes." At 1M log lines/sec across all tenants, how do you make this return in under 2 seconds?

> **Expected answer:**
> - **Partition pruning:** Query touches only current month's partition for this tenant. Eliminates 99%+ of data.
> - **Primary key skip:** ClickHouse's sparse index skips to the granule range matching `(tenant_id=X, service='checkout', level='ERROR', ts >= now()-15min)`. With index_granularity=8192 rows and ~500B/row, each granule is ~4MB. A 15-minute window at 5000 logs/sec/service = 4.5M rows = ~550 granules to scan — manageable.
> - **Materialized view for counts:** For aggregate queries (count per minute), the MV pre-aggregates; no raw scan needed.
> - **Query routing:** Multi-tenant cluster: query router hashes `tenant_id` to a dedicated shard pair. Queries for this tenant never hit other shards.
> - **Result cache:** Queries with `to=` in the past (closed windows) are deterministic → cache in Redis for 60 seconds keyed by `hash(query+params)`. Live queries (open `to=now`) are never cached.

> **Trap:** Naive answer: "Add an index on `level`." ClickHouse doesn't have traditional B-tree indexes. The primary key is a sparse index, and adding a Bloom filter on `message` for full-text search (which many candidates propose) has very high false-positive rates on high-cardinality text — you need a proper inverted index (ngrambf_v1 or tokenbf_v1 in ClickHouse) and even then FTS on raw message is a last resort, not the primary access path. Strong candidates push structured logging as the solution: if `service` and `level` are top-level columns (not buried in a JSON blob), they become part of the sort key and queries are fast.

---

**Q9.** Your metrics store needs to serve both real-time alerting (evaluate rules every 15 seconds) and historical dashboards (query 6 months of data). These have opposing consistency requirements. How do you handle this?

> **Expected answer:**
> - **Real-time alerting:** Uses the stream processor (Flink) directly on Kafka — data is evaluated before it hits storage. Latency: seconds. Consistency: exactly the data that has arrived; no historical completeness needed. Trade-off: cannot alert on historical anomaly baselines unless you join with a snapshot.
> - **Dashboard queries (recent, e.g., last 1 hour):** Hit the time-series store's hot tier (in-memory / SSD). Recent data may have partially-compacted blocks; reads tolerate eventual consistency (a 10-second lag is acceptable for a dashboard).
> - **Historical dashboards (weeks/months):** Hit pre-rolled-up downsampled metrics stored in cold storage (S3-backed Parquet). Raw 1-second samples are downsampled to 1-min, 5-min, 1-hour rollups by a background compaction job. Queries for long ranges automatically use the coarsest rollup that satisfies the requested resolution (Cortex/Thanos Compactor does this).
> - **CAP reasoning:** The metrics store is AP: during a network partition, ingesters continue writing and queriers read from what's available. You accept stale reads (up to the replication lag) rather than refusing queries. This is the right trade-off because an SRE during an incident must be able to query — a 503 from the monitoring system during an outage is catastrophic.

> **Mentor pushback:** "You said dashboards tolerate eventual consistency. But your alerting rule says 'alert if p99 latency > 500ms for 5 consecutive minutes.' If the stream processor has a 30-second lag, you miss a 4-minute spike. How do you fix this without making the alert path synchronous?"
> Expected fix: Use micro-batch watermarking in Flink — emit an alert only after the watermark (event-time progress marker) has advanced past the alert window end. Flink's event-time processing with allowed lateness handles late-arriving data without blocking.

---

## Low-Level Design (Hard)

*Pick the most interesting sub-component and go deep.*

**Q10.** Design the metric rollup / downsampling subsystem. Raw metrics arrive at 1-second resolution. You need to query 6-month trends in milliseconds. How does rollup work, and what are the failure modes?

> **Problem statement:** At 1M metric series × 1 data point/sec, raw storage is 86.4B points/day. A 6-month query at 1-second resolution would need to scan 15.6 trillion points — impossible in under 2 seconds. You need pre-computed rollups at multiple resolutions (1m, 5m, 1h, 1d) stored separately. Design the rollup pipeline.

> **Naive solution:** A cron job runs every hour, scans raw data for the previous hour, computes aggregates, writes to a rollup table.

> **Why naive fails at scale:**
> - Cron creates write spikes exactly when the system is under load (every hour on the hour, 1000 rollup jobs fire simultaneously).
> - A failed cron leaves a gap in rollup data — you won't notice until a user queries that range.
> - Reading the full previous hour of raw data (86.4B points/60 = 1.44B points per minute of raw) is a massive sequential scan that competes with live queries.

> **Expected optimal approach:**
> - **Streaming rollup with Flink tumbling windows:** For 1-minute rollups, use a Flink job with a 1-minute tumbling event-time window. For each `(tenant_id, metric_name, tag_set)` combination, the window computes `(min, max, sum, count, p50, p95, p99)` using a `HeapAggregator`. Window closes and emits when the watermark passes `window_end + allowed_lateness` (e.g., 30 seconds).
> - **Hierarchical rollup:** 5-minute rollups are computed from 1-minute rollup events (not raw data) — reduces re-computation by 5x. 1-hour from 5-minute, 1-day from 1-hour. Each tier has its own Kafka topic and Flink job.
> - **Storage layout per resolution tier:** VictoriaMetrics or Cortex stores each resolution in a separate object-storage block series. Query tier automatically selects the finest resolution that covers the requested range without exceeding a point budget (e.g., return at most 1000 data points per series per query).

> **Pseudo-code:**
```python
class RollupJob:
    def __init__(self, input_topic, output_topic, window_sec, allowed_lateness_sec):
        self.window = TumblingEventTimeWindow(window_sec)
        self.lateness = allowed_lateness_sec

    def process(self, event: MetricPoint):
        key = (event.tenant_id, event.metric_name, frozenset(event.tags.items()))
        self.window.add(key, event.value, event.ts)

    def on_window_close(self, key, window_start, values):
        rollup = RollupPoint(
            tenant_id=key[0],
            metric=key[1],
            tags=dict(key[2]),
            window_start=window_start,
            min=min(values),
            max=max(values),
            sum=sum(values),
            count=len(values),
            p99=percentile(values, 99),   # TDigest for memory efficiency
        )
        self.output_topic.emit(rollup)
```

> **Critical detail:** p99 across a rollup window cannot be computed by averaging p99s from sub-windows (non-additive). Use **t-Digest** or **DDSketch** — mergeable probabilistic data structures that allow computing approximate percentiles from partial aggregates with bounded error (~1% relative error at p99).

---

**Q11.** Two alert rule evaluations fire simultaneously for the same metric series at the same time. Both read p99=520ms (threshold: 500ms), both decide to fire, and both try to send a PagerDuty notification. How do you prevent duplicate pages?

> **Scenario:** Alert evaluation is distributed — 10 evaluator nodes share the rule set. Node A and Node B both happen to evaluate `payment.latency.p99 > 500ms` for tenant X within the same 15-second evaluation cycle. Both read from VictoriaMetrics, both see the violation, both invoke the PagerDuty API. The on-call engineer gets paged twice (or 10 times).

> **Expected fix:**
> - **Leader election per alert rule:** Assign each alert rule to exactly one evaluator node using consistent hashing on `(tenant_id, alert_rule_id)`. Only the assigned node evaluates and fires that rule. Redis with `SETNX alert_lock:{tenant}:{rule_id} {node_id} EX 30` provides a 30-second lease. The node renews every 10 seconds.
> - **Idempotency key on notification:** Even with leader election, a node can crash after deciding to fire but before recording that it fired. Solution: before calling PagerDuty, write `alert_fired:{tenant}:{rule_id}:{window_start}` to Redis with `SETNX` and a TTL matching the alert's re-notification interval (e.g., 4 hours). Only call PagerDuty if the SET succeeded. On crash-and-restart, the key exists → no duplicate.
> - **Deduplicated notification channel:** PagerDuty's Events API accepts a `dedup_key = hash(tenant+rule+window_start)` — PagerDuty itself deduplicates within an incident. This is the final safety net, not the primary mechanism.

> **Follow-up:** "What happens if the leader node dies while holding the alert_lock, mid-evaluation?"
> The lock TTL (30 seconds) expires. Another node acquires the lock and re-evaluates. If the previous node had already called PagerDuty but not yet written the idempotency key (the crash window), PagerDuty's own dedup_key prevents double-paging. The idempotency key in Redis closes the gap for internal actions (e.g., creating an internal incident record). This is a two-phase commit problem — the fix is: write the idempotency key first (with a "pending" state), then call PagerDuty, then mark it "confirmed." If you crash after "pending" but before "confirmed," the recovery path calls PagerDuty again with the same dedup_key (safe) and transitions to "confirmed."

---

**Q12.** A Kafka consumer reading from the `logs.raw` topic crashes mid-batch. When it restarts, it reprocesses 50,000 log events that were already written to ClickHouse. How do you handle this without duplicating log entries?

> **Scenario:** ClickHouse consumer reads a batch of 50K events, begins inserting into ClickHouse. ClickHouse insert succeeds for the first 30K rows. Consumer crashes. Kafka offset was not committed. Consumer restarts at the previous committed offset and re-sends all 50K.

> **Expected handling:**
> - **ClickHouse ReplacingMergeTree:** Use `ReplacingMergeTree(ts)` instead of MergeTree. Define a unique key per log event: `(tenant_id, trace_id, span_id, ts, xxHash32(message))`. On re-insert, ClickHouse marks the duplicate row for replacement during the next background merge. Reads use `FINAL` modifier or `SELECT DISTINCT` to deduplicate before merge completes.
>   - Trade-off: `FINAL` is expensive on large tables. Alternative: accept rare duplicates in the UI (log storage is analytics, not financial records) and build the UI to show a "deduplication note."
> - **Idempotent consumer with offset store:** Consumer writes `(kafka_topic, partition, offset)` to a separate ClickHouse table atomically with the log batch using ClickHouse's experimental transactions (22.4+) or by writing offset as a column in the same insert. On restart, consumer reads the last committed offset from ClickHouse (not Kafka) and seeks Kafka to that position, skipping already-processed offsets.
> - **Kafka exactly-once semantics:** If using Kafka transactions + ClickHouse Kafka engine's transactional mode, the consumer commits Kafka offset and ClickHouse insert atomically. This is the cleanest but adds ~10–20% latency overhead.

---

## Scaling to 10x / 100x (Hard)

**Q13.** Your system handles 1M log lines/sec today. A new enterprise customer onboards and adds 800K lines/sec from their monolith. Where does the system break first?

> **Expected answer:**
> - **First bottleneck: Kafka ingestion throughput.** At 1.8M × 500B = 900 MB/sec, you're approaching per-broker write saturation (typically 500MB/sec sustained on commodity hardware). Kafka writes are sequential per partition — adding producers doesn't help if partitions are the bottleneck. Fix: increase partition count for `logs.raw` topic, add broker nodes, or split into multiple Kafka clusters per tier.
> - **Second bottleneck: ClickHouse insert throughput.** ClickHouse's ReplicatedMergeTree has a maximum insert rate of ~300K-500K rows/sec per shard before merge-tree parts proliferate ("too many parts" error, degraded query performance). Fix: increase the consumer batch size (100K → 500K rows per insert), add ClickHouse shards horizontally, or use async_insert mode (ClickHouse buffers inserts internally and flushes at configurable intervals).
> - **Third bottleneck: Query layer fan-out.** If the new customer runs heavy dashboard queries, they compete with other tenants for ClickHouse query concurrency (typically 100 concurrent queries per shard). Fix: per-tenant query concurrency limits at the Query API layer (token bucket per tenant), dedicated shard pairs for top-tier customers.

> **Numbers to ground the answer:** ClickHouse on 32-core, NVMe: ~1M rows/sec insert throughput with optimal batch sizes; ~500 concurrent query seconds/sec query capacity. At 1.8M rows/sec, you need at minimum 4 ClickHouse shards for ingestion.

---

**Q14.** How do you shard the metrics time-series store across 100 nodes? What is your sharding key, and how do you handle hot series?

> **Expected sharding strategy:**
> - **Shard key: consistent hash of `(tenant_id, metric_name, sorted_tag_set)`** — the full series identifier. This distributes writes evenly across nodes for the typical case.
> - **Virtual nodes (vnodes):** Use 150 virtual nodes per physical node (consistent with Cassandra's default). This smooths out rebalancing when nodes are added/removed — only 1/N series need to move, not a full reshard.
> - **Problem with naive hashing:** A metric like `http.requests.total{env=prod}` emitted by 5000 pods all land on the same shard (same hash). This is a "hot series" — one physical node receives 5000 writes/sec for a single metric.
> - **Hot series detection:** Track per-series write rate at the ingestion tier. Any series exceeding a threshold (e.g., 1000 writes/sec) is flagged. The ingestion router adds a "shard suffix" to the series key (appending a random number 0-9), writing to 10 different shards. Query time: fan out to all 10 shards and merge. Effectively sharding a single hot series across 10 nodes.
> - **Adaptive sharding:** VictoriaMetrics uses this via vmselect/vminsert/vmstorage separation — inserts are load-balanced across vmstorage nodes, not content-addressed, which avoids the hot shard problem at the cost of requiring a global index (vmselect queries all shards for any query).

> **Hot spot problem:** The most common hot spot is not a single series but a tenant with millions of unique tag combinations (high cardinality). `payment.latency{order_id=<uuid>}` — each order is a different series. 1M orders/day = 1M new series/day, each needing a time-series index entry. Fix: enforce cardinality limits per tenant (reject series creation above 10M unique series) and educate customers to use attributes (logs) rather than tags (metrics) for high-cardinality dimensions.

---

**Q15.** Design a multi-tier caching strategy for the query layer. What do you cache, at which layer, and how do you invalidate?

> **Expected layered cache design:**
> - **L1 — In-process cache (query node local memory, LRU, 2GB):** Cache the results of metric rollup queries for closed time windows. Key: `SHA256(tenant_id + PromQL_query + start + end + step)`. TTL: closed windows never change, so TTL = indefinite (until eviction). Hit rate target: 60–70% for dashboard page loads (users reload the same dashboard repeatedly).
> - **L2 — Redis cluster (distributed, 100GB across 10 nodes):** Cache for queries that miss L1 (different query node handled the previous identical request). Same key scheme. TTL: 5 minutes for recent/open windows, 1 hour for historical. Serves cross-node deduplication.
> - **L3 — Materialized views / pre-computed rollups in the storage tier:** Not a traditional cache — pre-computed aggregations stored permanently. ClickHouse Materialized Views for log aggregations; VictoriaMetrics downsampled blocks for metrics. These are the foundation — L1/L2 accelerate individual queries; the rollups make queries possible at all on long ranges.
> - **CDN for static dashboard assets:** Not metrics data — the JS/CSS/chart configs. Cloudflare with 1-hour TTL. Not relevant to the core design but shows operational awareness.

> **Cache invalidation trap specific to this system:** Metrics can arrive late (a host was partitioned for 2 minutes, then reconnects and flushes backlog). A query result for `last_hour` was cached at T=0. At T=3min, the late data arrives and is ingested, changing the p99 for that hour. The L2 cache still serves the stale result for up to 5 more minutes. Fix: for any query window that ended less than 10 minutes ago, use a 1-minute TTL (or no cache). For windows older than 10 minutes, use a long TTL (1 hour). The 10-minute threshold matches the `allowed_lateness` configured in the Flink rollup jobs — after that window closes with lateness accounted for, results are stable.

---

**Q16.** Your raw log storage at 1.3 PB/30 days costs $35K/month on S3. How do you reduce this by 60% without reducing retention?

> **Expected answer:**
> - **Columnar compression:** Move from row-oriented storage (Elasticsearch's Lucene segments) to columnar (Parquet on S3 backed by Athena/ClickHouse S3 tables). Columnar stores achieve 10–20x compression on repetitive log data vs. Elasticsearch's 5x. Net: same data, 2x cheaper.
> - **Tiered storage with access-pattern-driven migration:** Define hot (last 3 days, SSD-backed ClickHouse local disk), warm (4–14 days, ClickHouse S3 tables on S3 Standard), cold (15–30 days, S3 Intelligent-Tiering or S3 Glacier Instant Retrieval). Cost difference: SSD = $0.10/GB/mo, S3 Standard = $0.023/GB/mo, S3-IA = $0.0125/GB/mo. Migrating days 4–30 from SSD to S3 saves 75% on storage for 90% of the data.
> - **Selective verbosity:** Let customers configure per-service log verbosity. DEBUG logs (60% of volume) are dropped at the edge agent for non-development environments. This is a configuration change, not an infrastructure change — potential 40% volume reduction for free.
> - **Sampling for high-volume INFO logs:** Tail-based sampling — if a trace has no errors and no slow spans, sample only 1% of its associated INFO logs. Store the trace itself but discard the verbose INFO logs. Transparent to users via a "sampling rate" metadata field.

---

## Mentor's 5 Hardest Questions (SDE3+ Differentiators)

*These are the questions that separate a strong SDE3 from a principal engineer. A candidate who answers even 3 of these well is exceptional.*

**H1.** ClickHouse's MergeTree relies on background merges to deduplicate `ReplacingMergeTree` rows and apply TTL deletions. At high ingest rates, the "too many parts" error occurs because parts are created faster than merges can compact them. Walk me through exactly why this happens, how ClickHouse's merge scheduler prioritizes, and what two configuration levers you would tune to push the ingest rate ceiling from 300K to 1M rows/sec per shard.

*(Expected depth: `max_insert_delayed_streams_for_parallel_write`, `merge_tree_max_rows_to_use_cache`, background merge thread pool sizes, the role of `min_bytes_for_wide_part` in avoiding small part proliferation, and why increasing `insert_block_size` trades latency for merge efficiency.)*

**H2.** You are designing this system for a GDPR-regulated European customer. A user exercises their right to erasure — "delete all my data within 30 days." But your logs contain their IP address, user_id, and action history scattered across 15 PB of immutable Parquet files on S3. How do you design for erasure from the start, without re-reading and rewriting petabytes?

*(Expected: Crypto-shredding — encrypt per-user data with a per-user key stored in a separate KMS. On erasure request, delete the user's KMS key. All their data in Parquet remains but is permanently unreadable. No file rewrite needed. Trade-off: key management complexity, auditability of erasure.)*

**H3.** You need to deploy a change to the ClickHouse schema — adding a new Materialized Column that derives a `normalized_service` field from the existing `attributes` Map. This MV must be populated for historical data too, not just new inserts. How do you do this with zero downtime and zero data loss on a 500TB table?

*(Expected: Create the new column as MATERIALIZED, which auto-populates for new inserts immediately. For historical backfill: `ALTER TABLE logs MATERIALIZE COLUMN normalized_service` — ClickHouse performs this as a background mutation, reading and rewriting parts without blocking reads or writes. Monitor with `SELECT * FROM system.mutations WHERE is_done=0`. Plan for 24–72 hours of background I/O on 500TB.)*

**H4.** You are the on-call engineer for the monitoring system itself. Your alerting system is firing alerts — but the alert notification pipeline (Kafka → Flink → PagerDuty) is also down (because you are monitoring everything, including the monitoring system). What metrics, traces, and synthetic checks do you instrument so you detect "the monitoring system is unhealthy" without relying on the monitoring system to detect it?

*(Expected: External synthetic canary — a separate simple process (not on the main Kafka cluster) that emits a test metric every 30 seconds and checks that an alert fires within 60 seconds, measured from an external vantage point. Heartbeat alerts — Prometheus "deadman's switch": an alert fires if a metric that should always be present stops arriving. Separate out-of-band paging channel (SMS via Twilio directly, not routed through PagerDuty) for the synthetic canary. Cross-region health check — a secondary region independently polls the primary's health API.)*

**H5.** You designed the system with ClickHouse as the log store. You are now at 50 PB of logs, and three things have changed: (a) customers want sub-second full-text search across raw message content, (b) your ClickHouse cluster costs $800K/month in cloud infra, and (c) a new open-source contender (Apache Iceberg + StarRocks) is promising 5x cost reduction. Walk me through how you evaluate the migration and execute it if you decide to proceed.

*(Expected: Evaluation — run parallel writes to both systems for 2 weeks, benchmark query latency at p50/p99 across representative query patterns, measure storage cost with identical datasets, validate schema migration paths. Decision criteria: latency SLA (must be ≤ current), cost reduction (must be ≥ 40% net of migration cost), ecosystem (connectors, Kafka integration, operational tooling). Migration execution: dual-write phase (2–4 weeks), historical backfill via a Spark job reading ClickHouse and writing Iceberg (run during off-peak), query traffic migration using feature flags at the Query API layer (route 1% → 5% → 20% → 100% over 4 weeks), ClickHouse kept as hot standby for 90 days post-migration before decommission.)*

---

## Mentor's Closing Notes

**Top 3 things most candidates get wrong on this topic:**

1. **Treating logs, metrics, and traces as the same problem.** They have fundamentally different storage requirements (logs = full-text + columnar, metrics = time-series compression, traces = graph traversal), different cardinality profiles, and different query patterns. Candidates who propose "put everything in Elasticsearch" are designing for 1/100th of the scale this problem requires. Elasticsearch is excellent at full-text search but collapses under time-series metric workloads.

2. **Ignoring the write path's backpressure story.** At 1M events/sec, the system must handle 10x spikes (deployment, incidents, load tests) gracefully. Candidates propose horizontal scaling but never address the intermediate buffer. Without Kafka as a durable, independently-scalable write buffer between ingestion and storage, a storage hiccup causes the entire pipeline to back-pressure into application services — which causes applications to slow down or drop logs precisely during the incident you're trying to debug.

3. **Alert deduplication as an afterthought.** Candidates design the happy path perfectly and wave at "we'll use PagerDuty's dedup feature" for the entire correctness argument. In production, a bug in the alert evaluator firing duplicate alerts at 3 AM will destroy your team's trust in the system overnight. The idempotency key pattern + leader election per rule must be designed upfront, not bolted on after the first incident.

**The one insight that makes an answer truly impressive:**

The understanding that **observability systems must be designed to remain queryable during the incidents they are meant to help resolve** — which means the query path must be isolated from the write path's failure modes. If log ingest backs up (Kafka consumer lag spikes), the query path should continue serving cached and stored data without degradation. This drives the architectural decision to separate ingestion, storage, and query into independently scalable and independently deployable services — not as a microservices fashion statement, but because during a major incident, you will want to scale up query capacity without touching the already-stressed ingest pipeline. Candidates who articulate this — that observability infrastructure has a unique "must work when everything else is broken" constraint that overrides normal latency/cost trade-offs — are thinking at the principal engineer level.

**Suggested follow-up reading:**
- Meta's **Scuba** paper (2013): "Scuba: Diving into Data at Facebook" — real-world design of an in-memory time-series analytics system; specifically the append-only table design and the trade-offs between memory pressure and query latency.
- **VictoriaMetrics architecture blog** (victoriametrics.com/blog): Deeply technical posts on their storage engine's row merging, compression (Gorilla + Zstd), and why they diverged from Prometheus's TSDB. Directly applicable to Q10 and Q14.

---

## How to Use This Session

1. **Solo mode:** Cover each question section by section. Write your answer, then read the expected answer. Grade yourself honestly.

2. **Interactive mode:** Paste this entire document into a new Claude conversation and say: *"You are Arjun Mehta. I am your student. Start with Q1 and don't reveal the expected answers — ask me the questions one at a time, push back on weak answers, and guide me to the right answer through follow-up questions."*

3. **Mock interview mode:** Set a timer. Answer only Q4–Q15 in 45 minutes as if it's a real interview. Then review.
