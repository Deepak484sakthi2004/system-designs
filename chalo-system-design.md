# Designing a Chalo-class Public-Transit Super-App

> Reader note: Part 1–6 design the **whole app**. Part 7 is the deep-dive on the **proximity notification feature** specifically — that is the artifact I wanted Chalo's product team to receive. Parts 8–11 cover scale, deployment, ops, and tradeoffs.

---

## Table of Contents

- [Part 0 — Interview Framing](#part-0--interview-framing)
- [Part 1 — Functional & Non-Functional Requirements](#part-1--functional--non-functional-requirements)
- [Part 2 — Back-of-Envelope Math](#part-2--back-of-envelope-math)
- [Part 3 — High-Level Architecture](#part-3--high-level-architecture)
- [Part 4 — Data Plane: Streams, Stores, Indices](#part-4--data-plane-streams-stores-indices)
- [Part 5 — Core Service Designs (LLD)](#part-5--core-service-designs-lld)
- [Part 6 — API Contracts & Data Models](#part-6--api-contracts--data-models)
- [Part 7 — DEEP DIVE: Proximity Notification Feature](#part-7--deep-dive-proximity-notification-feature)x
- [Part 8 — Scaling, Hot Partitions, Cost](#part-8--scaling-hot-partitions-cost)
- [Part 9 — Deployment, K8s, CI/CD](#part-9--deployment-k8s-cicd)
- [Part 10 — Observability, SLOs, Chaos](#part-10--observability-slos-chaos)
- [Part 11 — Tradeoffs, What I'd Build First, Interview Traps](#part-11--tradeoffs-what-id-build-first-interview-traps)
- [Appendix A — Glossary & Numbers Table](#appendix-a--glossary--numbers-table)

---

## Part 0 — Interview Framing

### 0.1 The 30-second pitch (how I'd open the round)

> "Chalo is a public-transit super-app: live bus tracking via on-bus GPS units, ETA at every stop, mobile ticketing, monthly passes, and a route planner — currently live in 50+ Indian cities with MTC Chennai, BMTC Bangalore, BEST Mumbai, etc. I'll design it for 10M DAU, 100k buses, 50k stops, and treat the proximity-notification feature as the headline use-case. I'll cut payments-gateway internals (they're a solved problem) and ML-based crowd prediction (it's a side service)."

### 0.2 Clarifying questions I would actually ask the interviewer

1. **Geography:** India-only? → Yes. (Drives Mumbai/Chennai regions + low-end Android-first.)
2. **Read:write ratio of location data?** → Each bus emits, ~10M users read. → 1:1000+ fanout. → Cache-heavy.
3. **Are we the GPS device vendor?** → Yes; Chalo ships its own 4G+GPS dongle on partner buses. → We control device firmware. Huge.
4. **Notification SLA?** → Within ±60s of the bus being ~5min away from the user's stop, 99% of the time, even when app is killed.
5. **Offline-first?** → Yes. Buses in tunnels, users in metro basements. Last-known-good with staleness markers.
6. **Monetization model?** → Ticket-commission + B2B SaaS to transit authorities. → Reliability matters more than ad-density. No ad-tech detour.
7. **Regulatory?** → DPDP (India), data residency → all PII in `ap-south-1`/Mumbai. RBI for payment side.

### 0.3 Stated assumptions

| # | Assumption | Implication |
|---|-----------|-------------|
| A1 | 10M DAU, 30M MAU | Capacity baseline |
| A2 | 100k active buses, each pinging GPS every **3 s** | ~33k pings/s steady, ~80k/s peak |
| A3 | 50k stops, 5k routes | Geofence index size manageable |
| A4 | Avg user has 2.4 "favorite" stop+route subscriptions | Subscription store ~24M rows |
| A5 | Peak hour = 8–10 AM IST, 3× steady | Autoscale triggers |
| A6 | 70% Android (Android 10+), 25% iOS, 5% web | Push = FCM+APNs |
| A7 | p99 ETA freshness < 5 s; p99 ticket purchase < 800 ms | SLO targets |
| A8 | Cost ceiling ~ ₹0.08/user/month infra (₹8L/month for 10M users) | Cassandra over Dynamo; ScyllaDB worth evaluating |

### 0.4 Out of scope

- Payment-gateway internals (UPI/Razorpay are external).
- Driver-facing app (separate product).
- Marketing CRM, A/B framework details.
- Detailed ML training pipeline for crowd inference (we'll use the *served* model).

---

## Part 1 — Functional & Non-Functional Requirements

### 1.1 Functional (P0 = launch-blocker, P1 = within 6 months)

| ID | Feature | Priority |
|----|---------|----------|
| F1 | Live bus location on map | P0 |
| F2 | ETA at any stop on a route | P0 |
| F3 | Search route by source→destination | P0 |
| F4 | Mobile QR ticket purchase | P0 |
| F5 | Monthly/quarterly pass | P0 |
| F6 | **Proximity push notification** (the feature I mailed) | P0 *(this design treats it as P0; Chalo currently treats it as P2)* |
| F7 | Favorites: routes, stops, frequent trips | P0 |
| F8 | Offline last-known ETA with staleness banner | P1 |
| F9 | Crowd-level indicator per bus | P1 |
| F10 | Trip history + carbon-saved gamification | P1 |
| F11 | In-app safety SOS (women's safety) | P1 |

### 1.2 Non-Functional

| Dimension | Target | Why |
|-----------|--------|-----|
| **Availability** | 99.95% (≈ 4 h 22 m/year downtime) | Commuters depend on it daily; missing a bus has real cost |
| **ETA freshness** | p99 < 5 s from GPS emit to user-visible | Stale ETA is worse than no ETA |
| **Notification accuracy** | ±60 s of the configured "T-minus N minutes" trigger, 99% | The whole point of the feature |
| **Ticket idempotency** | exactly-once charge per QR | Regulatory + trust |
| **Cold start** (app open → first map tile rendered) | < 1.2 s on mid-tier Android | India market reality |
| **Battery cost** | < 2%/hour with foreground tracking | Or users uninstall |
| **Data cost** | < 5 MB/day for an active commuter | Tier-2/3 metered plans |
| **Storage durability** | 11 9's for tickets & trips; 4 9's for raw GPS | Cost vs compliance |
| **Region** | Active-active Mumbai + Chennai; DR Singapore | Mumbai AZ outage in 2023 cost AWS customers dearly |

---

## Part 2 — Back-of-Envelope Math

I always do this on the whiteboard before drawing anything. It chooses the architecture for me.

### 2.1 Location ingestion

```
Buses:                 100,000 active
Ping cadence:          1 per 3 s
Steady QPS:            100,000 / 3        = 33,333 pings/s
Peak (morning rush):   3×                 = ~100,000 pings/s

Payload (Protobuf):    bus_id(8) + lat(8) + lon(8) + speed(4)
                       + heading(2) + ts(8) + occupancy(1) + crc(4)
                       ≈ 48 B wire (with framing ~70 B)
Steady bandwidth:      33k × 70 B          ≈ 2.3 MB/s   → ~20 Mbps
Peak bandwidth:        ~70 Mbps ingress    (trivial for a single VPC)
```

### 2.2 Storage of raw GPS (hot tier, 7 days)

```
33k pings/s × 86,400 s × 7 d × 70 B
= 33,000 × 604,800 × 70
≈ 1.4 TB hot / week
```

→ Cassandra/Scylla 3× replication = ~4.2 TB. With LZ4 compression ~1.4 TB on disk. Fits 6 i4i.xlarge nodes comfortably with headroom.

Cold tier (Parquet on S3, 13-month retention for analytics): ~75 TB. S3 IA tier. ~$0.0125/GB ≈ $940/month. Negligible.

### 2.3 Read fan-out

```
DAU:                   10M
Concurrent peak users: 10M × 8% online      ≈ 800k
Map polls per active:  every 5 s while screen on
Active screen users:   ~200k at peak
Map QPS to backend:    200k / 5             = 40,000 QPS
```

Each map query returns ~20 buses in viewport. Pure Redis GEO lookup. Sub-ms. Cache-hit ratio targets 98%.

### 2.4 Notification volume (THE feature)

```
Subscriptions per user:    2.4
Total active subs:         10M × 2.4         = 24M subs
Fire rate (avg):           Each sub fires ~2× per workday
Total fires/day:           24M × 2 × (5/7)   ≈ 34M/day
Peak hour share:           30% in 8–10 AM    ≈ 10M in 2 h = 1,400/s steady, 5,000/s peak
FCM/APNs throughput:       FCM accepts ~2k req/s/project default, scalable to 30k+
                           → We batch + use multiple sender keys per region
```

### 2.5 Ticket QPS

```
Tickets/day:    8M (80% of DAU buys at least once)
Peak (8–10 AM): 40% in 2 h = 3.2M in 7,200 s = ~450 QPS
Peak burst:     2,000 QPS for 30 s
```

→ Vanilla Postgres (with PgBouncer) handles this. Don't over-engineer.

### 2.6 Why these numbers chose the architecture

- 100k pings/s ingress is **trivial** if we use a streaming bus (Kafka) and don't try to write directly to a DB on each ping.
- 40k map QPS forces **Redis GEO** with a stop-id-sharded cluster.
- 5k notification fires/sec is **easy** if the orchestrator is event-driven (sorted-set sweep). It would be hard if we did per-user cron.
- 1.4 TB/week hot data forces **TTL'd time-series** store, not Postgres.

---

## Part 3 — High-Level Architecture

### 3.1 Component map

```
                       ┌──────────────────────────────────────────┐
                       │              MOBILE CLIENTS              │
                       │  Android / iOS / Web (Next.js PWA)       │
                       └───────┬──────────────────┬───────────────┘
                               │ HTTPS/REST       │ WebSocket (live)
                               │ gRPC-Web         │ FCM/APNs (push)
                               ▼                  ▼
                       ┌──────────────────────────────────────────┐
                       │       EDGE: CloudFront + Route53         │
                       │   Geo-routes Chennai→ap-south-1c,        │
                       │   Mumbai→ap-south-1a                     │
                       └──────────────────────┬───────────────────┘
                                              ▼
                       ┌──────────────────────────────────────────┐
                       │  API GATEWAY (Envoy + Istio ingress)     │
                       │  AuthN (JWT) · Rate limit · WAF · mTLS   │
                       └──┬─────┬─────┬─────┬─────┬─────┬─────────┘
                          │     │     │     │     │     │
                ┌─────────┘     │     │     │     │     └──────────┐
                ▼               ▼     ▼     ▼     ▼                ▼
        ┌──────────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────┐ ┌──────────┐
        │  User /      │ │  Trip /  │ │  ETA     │ │ Notification │ │ Ticket / │
        │  Auth Svc    │ │  Route   │ │  Query   │ │  Subscription│ │ Payment  │
        │  (Kotlin)    │ │  Svc     │ │  Svc     │ │  Svc         │ │  Svc     │
        └──────┬───────┘ └────┬─────┘ └────┬─────┘ └──────┬───────┘ └─────┬────┘
               │              │            │              │               │
        ┌──────▼──────────────▼────────────▼──────────────▼───────────────▼─────┐
        │                          DATA PLANE                                    │
        │  ┌───────────┐ ┌──────────┐ ┌──────────────┐ ┌──────────┐ ┌─────────┐ │
        │  │ Postgres  │ │  Redis   │ │  Cassandra/  │ │  Kafka   │ │   S3 +  │ │
        │  │ (users,   │ │ (ETA hot,│ │  ScyllaDB    │ │ (events) │ │ Iceberg │ │
        │  │ tickets,  │ │  GEO,    │ │  (trip hist, │ │          │ │ (lake)  │ │
        │  │ subs)     │ │  geofence│ │  raw GPS)    │ │          │ │         │ │
        │  └───────────┘ │  zset)   │ └──────────────┘ └────┬─────┘ └─────────┘ │
        │                └──────────┘                       │                    │
        └──────────────────────────────────────────────────┼────────────────────┘
                                                            │
        ┌───────────────────────────────────────────────────▼────────────────────┐
        │                     STREAM PROCESSING                                   │
        │   Flink: ETA Engine · Geofence Matcher · Trip Builder · Crowd Inference│
        └─────────────────────────────────────▲───────────────────────────────────┘
                                              │
                                  ┌───────────┴────────────┐
                                  │  GPS Ingestion Service │
                                  │  (gRPC bidi from bus)  │
                                  └───────────▲────────────┘
                                              │ 4G/LTE
                                  ┌───────────┴────────────┐
                                  │  On-bus dongle (ours)  │
                                  │  GPS + LTE modem +     │
                                  │  ARM SoC running Yocto │
                                  └────────────────────────┘
```

### 3.2 Why these specific services (and not others)

- **Ingestion as its own service**, separate from "API": vastly different traffic shape (machine-to-machine, persistent connections), different deploy cadence, different scaling axis.
- **ETA Query Svc separate from ETA Engine**: engine is stateful Flink, query is stateless REST. Mixing them couples deploys.
- **Notification/Subscription Svc separate from ETA**: subscription lifecycle is CRUD; firing is event-driven. Split = independent scaling.
- **Trip/Route Svc owns the GTFS static data** (routes, stops, schedules). Read-mostly. Backed by Postgres + aggressive CDN.
- **No "Monolith with feature flags"**: this is a Google-bar interview. They want service boundaries justified by Conway's Law and blast-radius.

### 3.3 Why NOT each obvious alternative

| Alt | Why I rejected |
|-----|----------------|
| Firebase Realtime DB for live tracking | Vendor lock; can't shard GEO indexes; cost explodes past 1M concurrent |
| MQTT instead of gRPC for bus → cloud | MQTT is great, but gRPC bidi gives us flow control, mTLS, and one toolchain. We already need gRPC for service mesh. Don't run two protocols if one suffices. |
| AWS Location Service | Black box, vendor lock, no S2 cell control |
| DynamoDB for raw GPS | Cost per write at 100k writes/s = lights-on burning money. Cassandra/Scylla wins for time-series append heavy |
| Edge Functions for ingestion | The Vercel knowledge update is right that Edge has Node compat issues, but more importantly: persistent gRPC streams from 100k buses want a *long-lived* server, not request/response |
| Kinesis instead of Kafka | Fine choice, but throughput-per-shard ceilings bite us during route reshuffles; Kafka partitions by route_id is cleaner |
| Pulsar instead of Kafka | Genuinely tempting (built-in geo-replication). I'd pick it if greenfield in 2026. For interview, Kafka is the lingua franca |

---

## Part 4 — Data Plane: Streams, Stores, Indices

### 4.1 Kafka topology

```
Topic                       Partitions  Retention  Replication  Producer            Consumer
─────────────────────────── ─────────── ────────── ──────────── ─────────────────── ──────────────────────────
bus.gps.raw                 256         24 h       3 (min ISR 2) Ingestion Svc       Flink ETA, Flink Trip
bus.gps.compacted           64          7 d        3             Flink (last-known)  ETA Query, Map service
trip.events                 128         30 d       3             Flink Trip Builder  Analytics, Notif
notif.fire                  64          24 h       3             Notif Orchestrator  Notif Sender
notif.outcome               32          30 d       3             Notif Sender        Analytics, ML
ticket.events               32          90 d       3             Ticket Svc          Analytics, Fraud
user.audit                  16          1 y        3             User Svc            Compliance lake
```

Partitioning key choices (the part interviewers grill on):

- `bus.gps.raw` → key = `route_id`. Why not `bus_id`? Because downstream **ETA recompute is per-route** (a bus crossing the same route as another bus shares geofence context). Co-locating same-route buses in the same Flink task slot reduces shuffle.
- `notif.fire` → key = `user_id`. Why? Push-token rate limits are *per user*, not per route.
- `ticket.events` → key = `user_id`. Idempotency dedupe lives in the user's partition.

### 4.2 Storage choices, one paragraph each

**Postgres (RDS, multi-AZ, ap-south-1).** Owns users, devices, push tokens, subscriptions, tickets, passes, routes (static GTFS), stops. Everything transactional. Partitioned tables for `tickets` by `purchased_at` month — hot month on NVMe, cold months detached and S3'd via `pg_dump`. Connection pooling via PgBouncer (transaction mode), max 2k server-side conns shared across 200 app pods.

**Redis Cluster (ElastiCache, 32 shards, 3 replicas each).** Three workloads multiplexed:
1. **GEO** index of *last known* bus location: `GEOADD buses:{route_id} lon lat bus_id`. Map queries hit this.
2. **Sorted set of upcoming notification fires**: `ZADD notif:fires <epoch_ms> <fire_id>`. Swept every 1s by orchestrator.
3. **Hot ETA cache**: `HSET eta:{route}:{stop} <bus_id> <eta_epoch_ms>`. TTL 30s.

Why one Redis for three things? Because they're all sub-ms, all keyspace-partitioned, and ops cost of running three clusters >> the cost of careful key prefixes. I'd separate workload #2 onto its own cluster only when we cross ~50k fires/sec, which is 10× current peak.

**Cassandra/ScyllaDB (6 nodes i4i.2xlarge to start).** Raw GPS time-series. Schema:
```cql
CREATE TABLE gps_pings (
  bus_id       bigint,
  day_bucket   int,           -- yyyymmdd
  ts           timestamp,
  lat          double,
  lon          double,
  speed_kph    smallint,
  heading_deg  smallint,
  occupancy    tinyint,
  PRIMARY KEY ((bus_id, day_bucket), ts)
) WITH CLUSTERING ORDER BY (ts DESC)
   AND default_time_to_live = 604800   -- 7 d
   AND compression = {'class':'LZ4Compressor'};
```
Partition key `(bus_id, day_bucket)` keeps each partition ~28k rows (1 ping/3s × 86,400 = 28,800) — well under the 100MB / 100k cell guidance. Reads (driver dashboard, dispute resolution) are point or range scans inside a partition, no scatter-gather.

**S3 + Iceberg.** Daily Flink batch job rolls Kafka → Parquet on S3, registered as Iceberg tables. Athena/Trino for analytics. Iceberg over plain Parquet because of (a) schema evolution as we add fields like `engine_temp`, (b) hidden partitioning, (c) row-level deletes for GDPR/DPDP requests.

**Pinot or Druid for the "Operations dashboard"** that the transit authority logs into — bus on-time %, route efficiency. Real-time ingest from `trip.events`. Druid is the safer pick; Pinot has fewer Indian-engineer operators.

### 4.3 The Geofence index — why S2, not H3 or geohash

```
Each bus stop becomes one S2 cell at level 16 (~150 m × 150 m).
A "trigger ring" around a stop = the cell + its 8 neighbours = ~450m radius.
Bus GPS ping → compute its S2 cell id (24-bit prefix lookup) → check
membership in the "armed" set of trigger-ring cells (Redis SET).
```

- S2 vs H3: S2 has hierarchical containment (parent cell trivially derivable). H3 is hex (better for distance uniformity but worse for hierarchical rollup). For our "is bus within ring of stop?" check, S2 wins on lookup cost.
- S2 vs geohash: geohash has nasty discontinuities at prime-meridian/equator edges, irrelevant in India, but the bit-shuffling makes neighbour computation O(log n) vs O(1) for S2.
- Compute on-bus or server? On-bus: bus knows nothing about which stops are armed. Server: trivial — S2 cell id is computed in ~200 ns in C++; in Java with the `s2-geometry` lib, ~1 µs.

---

## Part 5 — Core Service Designs (LLD)

### 5.1 GPS Ingestion Service

**Language:** Kotlin on Netty (we get coroutine ergonomics + Netty's epoll edge-triggered IO).

**Wire protocol:** gRPC bidirectional streaming over HTTP/2 with mTLS. Each bus opens **one** persistent stream on boot, sends `GpsPing` messages, receives `Ack` and occasional `ConfigUpdate` (e.g., "ping every 1s, not 3s, during this special event").

**Why bidi-stream over plain gRPC unary:**
- TCP+TLS handshake amortized over 8-hour shift.
- Flow control via HTTP/2 windowing.
- We can push commands back (firmware update window, route reassignment) without device polling.

**Backpressure:** If Kafka producer queue > 80% full → emit `Ack{throttle_ms:500}` on the stream. Bus firmware honours it. Don't drop on the floor; tell the client.

**Pseudocode (Kotlin, simplified):**

```kotlin
class GpsIngestionService(
    private val producer: KafkaProducer<Long, GpsPing>,
    private val authCache: BusIdentityCache,
) : GpsIngestionGrpcKt.GpsIngestionCoroutineImplBase() {

    override fun stream(requests: Flow<GpsPing>): Flow<Ack> = flow {
        var busId: Long = -1
        var validatedAt: Instant = Instant.MIN

        requests.collect { ping ->
            // mTLS cert SAN already authenticated. Cache identity for the stream lifetime.
            if (busId < 0) {
                busId = authCache.busIdFromPeerCert()
                validatedAt = Instant.now()
            }

            // Cheap sanity: reject pings older than 60s (clock skew / replay)
            if (Duration.between(ping.ts.toInstant(), Instant.now()).abs() > Duration.ofSeconds(60)) {
                emit(Ack.newBuilder().setStatus(Ack.Status.STALE).build())
                return@collect
            }

            val record = ProducerRecord(
                "bus.gps.raw",
                /* partition = null → keyed by routeId */
                ping.routeId,
                ping
            )

            val sendFuture = producer.send(record)
            // Don't block the coroutine; piggyback ack on completion
            sendFuture.whenComplete { meta, err ->
                val ack = if (err != null) Ack.STATUS_RETRY else Ack.STATUS_OK
                // emit asynchronously back into the flow
            }
        }
    }
}
```

**Capacity per pod:** Netty epoll on `c7g.large` (Graviton, 2 vCPU, 4 GB) handles ~30k concurrent gRPC streams comfortably. For 100k buses → **4 pods active + 4 standby across 2 AZs**. Tiny footprint.

**The under-discussed concern:** *connection churn*. Buses come in/out of cell coverage. We size based on **steady connections**, but design for **reconnect storm** when a tower comes back online — 5k buses simultaneously redialing. Solution: jittered exponential backoff on the device + connection rate limiter on the ALB (`max_connections_per_second = 2000`).

### 5.2 ETA Engine (Flink)

**Job graph:**

```
[Kafka: bus.gps.raw]
        │
        ▼
[KeyBy route_id]
        │
        ▼
[Per-route KeyedProcessFunction]
   • Maintains per-bus rolling state (last 20 pings, ~1 min)
   • Kalman filter for speed/heading smoothing
   • Map-matches GPS to nearest road segment via offline-built KDTree of route polylines
   • Computes ETA at each downstream stop using segment-speed history
        │
        ├──► [Kafka: bus.gps.compacted]   (last-known per bus)
        ├──► [Redis: HSET eta:{route}:{stop}] (TTL 30s)
        └──► [Kafka: trip.events]   (segment crossings)
```

**State backend:** RocksDB on local NVMe, checkpoint to S3 every 60s.

**Why Kalman, not just "average of last 5 speeds":**
- GPS jitter in dense urban canyons (Bangalore Silk Board, Mumbai Dadar) gives spurious 2 m/s ↔ 22 m/s readings.
- Kalman with process noise tuned to "buses don't accelerate > 2.5 m/s²" smooths cleanly.
- The matrix math is 4×4. Costs ~1 µs per ping in Java. Negligible.

**Why map-matching matters:** Raw GPS can sit 30 m off the road. ETA at the next stop must use the road graph, not Euclidean distance. KDTree built nightly from OSM + transit-authority route shapefiles.

**Watermarks:** event-time, with 5s allowed lateness. Late pings (post-watermark) go to a side output, written to a dead-letter Cassandra table for offline correction of completed trips.

**Why Flink, not Kafka Streams:**
- Kafka Streams' state is co-partitioned with input — fine for stateless map, but our state is **per-route × per-bus**, and we want rescaling without downtime. Flink's keyed state + savepoints win.
- Kafka Streams has weaker exactly-once across multiple output topics + Redis sink. Flink's 2PC sinks are mature.
- Honestly: at <5k events/s per task, Kafka Streams would also work. I'd pick Flink for *operability at the 5× growth horizon*, not today's load.

**Why not Spark Streaming:** microbatch latency floor of ~500ms. Our SLO is 5s end-to-end and we'd burn a quarter of the budget on microbatch alone.

### 5.3 Trip/Route Service

Mostly boring CRUD with caching. The interesting bit:

- **GTFS static data** (routes, stops, schedules) is updated by transit authorities weekly. Versioned in Postgres with `gtfs_version` column. On version bump, we *pre-build* the S2 cell map for new stops and warm Redis before promoting the version atomically.
- API responses are cacheable for 6h on CloudFront with `Cache-Control: public, max-age=21600, stale-while-revalidate=86400`. Vary by `gtfs_version` header.
- 50k stops × ~300 B each = 15 MB. Easily ships to the mobile client as a bundled SQLite on first launch, with delta updates.

### 5.4 User/Auth Service

- OTP via SMS (MSG91, Karix) + WhatsApp OTP fallback.
- JWT (RS256), 7-day access, 30-day refresh.
- Device binding: push token registered against `(user_id, device_id, platform)`. On logout from this device, token is *not* revoked at FCM — just unbound, so the same physical device can re-login.
- DPDP: PII columns encrypted at rest with column-level KMS keys. Right-to-erasure triggers async cascade across Cassandra (delete by partition) and S3 Iceberg (`DELETE FROM` with row-level merge-on-read).

### 5.5 Ticket/Payment Service

The well-trodden parts (Razorpay webhook handling, idempotency keys, eventual consistency between payment captured and ticket issued) — covered briefly to leave time for the notification feature:

- **Idempotency:** client generates a `purchase_intent_id` (UUIDv7). Server `INSERT ... ON CONFLICT (intent_id) DO NOTHING RETURNING ticket_id`. Single-row insert under a unique index. No distributed lock needed.
- **QR generation:** ticket → HMAC(secret, ticket_id || valid_from || valid_until || route_id) → first 12 bytes → base32 → 24-char QR payload. Conductor scans, our offline-capable scanner app verifies HMAC locally (secret rotated per route per day; pushed via Notif Svc).
- **Refund window:** 5 min, but only if QR has not been scanned. Race condition handled by single-row `UPDATE tickets SET status='refunded' WHERE status='active' AND ticket_id=?` — Postgres serializability handles it.

---

## Part 6 — API Contracts & Data Models

### 6.1 Selected REST/gRPC endpoints

```
# Live map
GET   /v1/buses/in-viewport?nw_lat&nw_lon&se_lat&se_lon&route_id?
      → 200 [{bus_id, lat, lon, heading, speed, occupancy, last_ping_age_ms}]

# ETA
GET   /v1/eta?route_id&stop_id
      → 200 [{bus_id, eta_seconds, confidence}]

# Subscription (THE feature)
POST  /v1/subscriptions
  body: {route_id, stop_id, lead_time_minutes, days_of_week, time_window, crowd_max?}
  → 201 {subscription_id, status:"armed"}
GET   /v1/subscriptions
DELETE/v1/subscriptions/{id}
PATCH /v1/subscriptions/{id}     # snooze, change lead-time

# Tickets
POST  /v1/tickets/purchase-intent
POST  /v1/tickets/{intent_id}/confirm
GET   /v1/tickets/active

# WebSocket
WSS   /v1/live?route_id  → streams compacted bus positions every 2 s
```

### 6.2 Subscription table (the heart of the notification feature)

```sql
CREATE TABLE subscriptions (
    subscription_id   BIGINT PRIMARY KEY,             -- snowflake
    user_id           BIGINT NOT NULL,
    device_id         BIGINT NOT NULL,
    route_id          INT    NOT NULL,
    stop_id           INT    NOT NULL,
    direction         SMALLINT NOT NULL,              -- 0 = up, 1 = down
    lead_time_seconds INT    NOT NULL DEFAULT 300,    -- T-minus 5 min default
    days_mask         SMALLINT NOT NULL,              -- bitmask Mon=1..Sun=64
    window_start_min  SMALLINT NOT NULL,              -- minute of day, e.g. 480 = 08:00
    window_end_min    SMALLINT NOT NULL,
    crowd_max         SMALLINT,                       -- null = any
    status            SMALLINT NOT NULL,              -- 0=armed, 1=snoozed, 2=paused
    snoozed_until     TIMESTAMP,
    created_at        TIMESTAMP NOT NULL DEFAULT now(),
    updated_at        TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX ix_sub_route_stop_active ON subscriptions(route_id, stop_id)
    WHERE status = 0;
CREATE INDEX ix_sub_user ON subscriptions(user_id);
```

`24M rows × ~120 B ≈ 3 GB` — trivially fits in Postgres shared buffers on a single db.r6g.2xlarge primary.

### 6.3 Push token table

```sql
CREATE TABLE push_tokens (
    device_id    BIGINT PRIMARY KEY,
    user_id      BIGINT NOT NULL,
    platform     SMALLINT NOT NULL,        -- 0=fcm, 1=apns, 2=webpush
    token        TEXT NOT NULL,
    region       CHAR(2),
    locale       VARCHAR(8),
    quiet_start  SMALLINT,                  -- user-configured DND
    quiet_end    SMALLINT,
    last_seen    TIMESTAMP,
    is_valid     BOOL NOT NULL DEFAULT true
);
CREATE INDEX ix_pt_user ON push_tokens(user_id) WHERE is_valid;
```

---

## Part 7 — DEEP DIVE: Proximity Notification Feature

This is the feature I emailed Chalo about on 15-Dec-2025. Below is what I would have shipped if I were on their platform team.

### 7.1 Restating the user problem

> "I'm waiting at home or at the office. My bus is coming. I don't want to keep refreshing the app. *Tell me when it's 5 minutes away so I can leave now.*"

The naive solution is to "geofence the user's location". That's wrong on three counts:
1. Users don't carry the bus stop with them; they want the alert from home/office, often *not* near the stop.
2. Continuous user-location tracking destroys battery, gets the app killed by Android Doze.
3. Doesn't help when the app is fully killed.

The right framing: **fence the bus, not the user**. The user subscribes to a (route, stop, lead-time) tuple. Server fires the push the moment the bus's predicted arrival at that stop crosses the lead-time threshold.

### 7.2 Design pillars

| Pillar | Decision |
|--------|----------|
| **Who computes the trigger?** | Server. (Reliable, app-killed-ok.) |
| **What triggers?** | ETA at the user's stop ≤ lead_time. (Not raw distance — handles traffic.) |
| **How often is the trigger re-evaluated?** | Every Flink window emit (~2s), i.e. ~30× per minute. |
| **Delivery?** | FCM/APNs high-priority push. |
| **De-duplication?** | One fire per (subscription_id, trip_id). Bloom + Redis SETNX. |
| **Snooze?** | "Next bus" / "30 min" / "Today off". |
| **Failure modes?** | If we lose GPS for a bus, fire a "we lost track, manual check needed" notification at T-minus(lead+2min). |

### 7.3 End-to-end flow

```
Time T-300s   Bus 'MTC-21B-117' approaching stop 'TVK-Nagar-North'
              Flink ETA Engine emits eta=295s on Kafka (eta.changes)
              Notification Orchestrator subscribes; for each matching
              active subscription where eta <= lead_time, attempts to fire.

   ┌──────────────────────────────────────────────────────────────────────┐
   │ Step 1  Match                                                        │
   │   On startup, Orchestrator loads all `status=armed` subs into        │
   │   in-memory map keyed by (route_id, stop_id).                        │
   │   Hot reload via Debezium CDC on `subscriptions` → Kafka → in-mem    │
   │   apply (~1s end-to-end staleness).                                  │
   │                                                                      │
   │ Step 2  Fire-once guard                                              │
   │   SETNX notif:fired:{trip_id}:{sub_id} 1 EX 86400                    │
   │   Returns 1 → first time, proceed. Returns 0 → already fired today.  │
   │                                                                      │
   │ Step 3  Sanity gates (skip if any fail)                              │
   │   • current minute-of-day within [window_start, window_end]?         │
   │   • today's day-of-week in days_mask?                                │
   │   • subscription not snoozed (snoozed_until < now)?                  │
   │   • user's device quiet hours not active?                            │
   │   • crowd_max satisfied? (look up crowd_estimate for the bus)        │
   │                                                                      │
   │ Step 4  Compose                                                      │
   │   Build payload with localized title/body, deep link, action btns    │
   │   ("On my way", "Next bus please", "Mute today").                    │
   │                                                                      │
   │ Step 5  Send via FCM/APNs                                            │
   │   Batched HTTP/2 multiplexed requests, ≤500 per request, priority    │
   │   HIGH, collapse_key = sub_id (lets new fire overwrite stale).       │
   │                                                                      │
   │ Step 6  Record outcome                                               │
   │   Kafka `notif.outcome` with {sub_id, trip_id, sent_ts, fcm_resp}    │
   │   → analytics, ML retraining, billing.                               │
   └──────────────────────────────────────────────────────────────────────┘
```

### 7.4 Why "ETA-based" trigger, not "distance-based geofence"

Imagine the bus stuck in a traffic jam 800 m from the stop. A distance geofence at 1 km would have fired 10 minutes ago — user starts walking — arrives — waits 15 minutes. **User loses trust on day one.** An ETA-based trigger correctly delays the fire while the bus is jammed and fires when traffic clears.

But ETAs *jitter*. If the engine emits 305 → 295 → 310 → 290 around the threshold, we'd fire/cancel/fire/cancel. So:

```kotlin
// Hysteresis: fire when eta first goes <= lead_time AND has been
// stable below (lead_time + 30s) for at least 2 consecutive emits.
fun shouldFire(sub: Sub, history: List<EtaSample>): Boolean {
    val recent = history.takeLast(2)
    return recent.size == 2 &&
           recent.all { it.eta <= sub.leadTime + 30 } &&
           recent.last().eta <= sub.leadTime
}
```

### 7.5 The orchestrator — two architectures considered

**Option A: Cron-driven per-subscription scheduler.**
For each subscription, compute next expected fire time, ZADD to a sorted-set, sweep every second.
*Problem:* requires knowing the bus schedule precisely. Schedules drift. We'd be scheduling fires that never actually correspond to real bus arrivals, and missing real arrivals that came early.

**Option B (chosen): Reactive on ETA stream.**
Orchestrator is a stateless service that subscribes to `eta.changes` topic. For each ETA update, looks up matching subs in an in-mem index, applies guards, fires.
*Wins:* No clock drift, no scheduled-but-never-happened, naturally handles ad-hoc buses (extras during a festival).

Why I rejected A even though it's the textbook "Design a notification system" answer: the textbook problem assumes you *know* the fire time. We don't — we *derive* it from a continuously-changing prediction. Schedule-and-sweep is the wrong tool.

### 7.6 The hybrid kicker: armed device-side geofence as a backup

Even reactive servers can lose Kafka briefly. So as a **belt-and-suspenders fallback**:

- When a subscription is created, server pushes (silent push) the *stop's S2 cell + 8 neighbours* to the device.
- Device registers a system-level geofence (Android `LocationServices.getGeofencingClient()`, iOS `CLCircularRegion`).
- If the device crosses into the geofence AND server has not pushed in the last 10 min → fire a local notification: "Your bus may be near — open the app to confirm."

This is the *only* place we use device location, and we use it as a circuit-breaker, not the primary trigger. Battery cost of system-level geofence is ~0 (handled by the dedicated location chip).

### 7.7 Idempotency, exactly-once, and "kind of fine"

Strict exactly-once across (Kafka → orchestrator → FCM → device → user's eyeball) is impossible. We aim for **at-most-once perceived by the user**, which is achievable:

- **Server-side dedupe:** Redis SETNX keyed by `(trip_id, sub_id)`. TTL 24h.
- **Client-side dedupe:** Push payload carries `notif_id = hash(sub_id, trip_id, fire_ts_minute)`. Client app, on receipt, checks local SQLite `seen_notifs` table. If present → drop silently. If absent → display + insert. This catches the case where FCM retries delivered the same push twice.
- **FCM collapse_key** ensures that if two are in flight to a backgrounded device, only the latest displays.

We will sometimes **miss** a fire (e.g., Redis cluster failover loses a 1s window of SETNX state and a second emit slips through — but then collapse_key catches it). We will sometimes fire **slightly late** (ETA spike + hysteresis costs us 4s). We will *not* spam.

### 7.8 Failure modes & graceful degradation

| Failure | Symptom | Response |
|---------|---------|----------|
| Bus GPS dies mid-route | ETA goes stale | At `now() > last_eta_ts + 90s` while sub is "pending fire", send "Tracking lost on your bus — tap to see alternatives" |
| Kafka outage in ap-south-1a | Orchestrator can't see ETA stream | Failover consumer to ap-south-1b; meanwhile device-side geofence becomes primary |
| FCM rate-limit | 429s | Token-bucket per FCM project key; we shard across 4 project keys by `user_id % 4` |
| User in tunnel (metro) | Device offline | FCM queues push (max 28-day TTL with `time_to_live` set to lead_time + 5 min so we don't fire stale on emergence) |
| App force-killed by user (Xiaomi/OnePlus aggressive battery saver) | Local geofence not firing either | Server push remains primary — FCM bypasses Doze for high-priority |
| Subscription DB lag during peak | New subs not in orchestrator memory | We accept up to 30s sub-creation-to-active latency; UX: "Your alert is being set up" toast |

### 7.9 Battery, data, and the part LLMs forget

A push notification through FCM costs the device <1 mJ. A foreground app polling for ETA every 5s costs ~30 mJ/s = thousands of times more. The whole point of this server-fence design is that **the device does nothing while waiting** — which is also why the email Chalo sent me had hedged language about "user-friendly for daily commuters". They know the polling pattern is broken; they need someone to make the server-fence call.

### 7.10 Privacy

The server knows: (user_id, route, stop, time-window). The server does **not** continuously know user GPS — that's the explicit deal. Subscription data is encrypted at rest (column-level), retained only while active + 30 days, then aggregated and dropped.

### 7.11 Crowd-level customization (from my mail)

`crowd_max` on the subscription. Crowd estimate per bus comes from:
- AFC (Automatic Fare Collection) entry/exit deltas, if the city is instrumented.
- Otherwise: weight sensor in the bus floor (Chalo's premium fleet).
- Otherwise: passenger app density inference — count of devices reporting "on this bus" via BLE beacon detection in last 5 min.

The orchestrator's Step 3 guard reads `crowd:{bus_id}` from Redis (TTL 30s, updated by Crowd Inference job). If `current_crowd > sub.crowd_max`, skip this fire and try the **next** bus on the route. UX: "Bus 117 is full — alerting you for 119 (arrives 6:42)."

### 7.12 Pseudocode: the orchestrator hot loop

```kotlin
class NotificationOrchestrator(
    private val subIndex: ConcurrentHashMap<RouteStopKey, MutableList<Sub>>,
    private val redis: RedisAsyncCommands,
    private val crowdLookup: CrowdLookup,
    private val sender: PushSender,
    private val outcomes: KafkaProducer<Long, NotifOutcome>,
) {
    suspend fun onEtaUpdate(update: EtaUpdate) {
        val key = RouteStopKey(update.routeId, update.stopId)
        val candidates = subIndex[key] ?: return

        for (sub in candidates) {
            if (!shouldFire(sub, update.history)) continue
            if (!withinTimeWindow(sub)) continue
            if (sub.snoozedUntil?.isAfter(Instant.now()) == true) continue
            if (sub.crowdMax != null) {
                val c = crowdLookup.current(update.busId) ?: continue
                if (c > sub.crowdMax) continue
            }

            val fireKey = "notif:fired:${update.tripId}:${sub.id}"
            val first = redis.setnx(fireKey, "1").await() && redis.expire(fireKey, 86400).await()
            if (!first) continue

            val payload = PushPayload.build(sub, update)
            val result = sender.send(payload)              // FCM/APNs
            outcomes.send(NotifOutcome(sub.id, update.tripId, Instant.now(), result.status))
        }
    }
}
```

**Complexity:** for each ETA update, `O(subs at that (route, stop))`. The hot stops (Silk Board, Andheri) might have ~5000 subs. 5000 iterations of a guard chain at ~30 ETA emits/min = 2,500 ops/sec per hot key. Trivial.

### 7.13 What I would actually pitch to Chalo PM

> "Ship a minimal version: server-fired push, ETA-based trigger, hysteresis, FCM only. No crowd filter, no device geofence fallback. 4 engineer-weeks. Measure: D7 retention of users who turn the feature on (hypothesis: +6%) and notification CTR-to-trip-completion (target: 35%). If both clear, invest in the hybrid + crowd filtering. The full design above is the 6-month roadmap, not the v1."

This is what separates a senior IC answer from a "let me design everything" answer. **Sequence by ROI, not by completeness.**

---

## Part 8 — Scaling, Hot Partitions, Cost

### 8.1 The three hot partitions and what to do

1. **Kafka `bus.gps.raw`, partition holding Bangalore Route 500A** — the busiest route in Asia. ~120 buses on route, pinging every 3s = 40 pings/s into one partition. Fine. But **map queries** for that route's `bus.gps.compacted` consumer group can lag during peak. Mitigation: increase parallelism by *sub-partitioning* high-traffic routes — append `_a`/`_b` suffix to route_id so the same logical route uses 2 partitions. Downstream Flink rejoins them in the per-route keyed state.

2. **Redis ZSET of upcoming notification fires** — at 5k fires/sec peak, ZADD/ZREM throughput on a single key would crush one shard. Mitigation: shard by `epoch_minute % 16`; sweeper reads 16 keys per second, parallelized.

3. **Postgres `subscriptions` row-level updates** during snooze storms (Friday 6 PM, everyone hits "Mute weekend") — write amplification on `ix_sub_user`. Mitigation: snooze becomes an INSERT into a `subscription_snooze` log table (append-only); `status` is computed at read time via materialized join + Redis-cached state.

### 8.2 Cost model (monthly, ap-south-1, ballpark INR)

| Item | Cost (₹L = lakhs) |
|------|-------------------|
| 200 app pods (c7g.large) | 1.8 |
| 6-node Cassandra (i4i.2xlarge) | 2.1 |
| 32-shard Redis cluster (r7g.large × 3 each) | 4.5 |
| RDS db.r6g.2xlarge multi-AZ + 2 read replicas | 0.9 |
| MSK Kafka 6 brokers + storage | 1.5 |
| Flink (EMR/self-managed, ~30 TM slots) | 1.2 |
| S3 + Iceberg | 0.5 |
| CloudFront + data transfer | 1.5 |
| FCM (free) + APNs (free) + SMS OTP | 1.0 |
| Observability (Grafana Cloud or self-host) | 0.3 |
| Headroom / surprise | 2.0 |
| **Total** | **~17.3 L/month** |

10M DAU → ₹0.17/user/month. Under our ₹0.08 target? No — but the target was aggressive. Real-world Chalo ARPU from tickets is ₹15–30/month per active commuter; infra is well covered.

### 8.3 Multi-region

- **ap-south-1 (Mumbai)** = primary write region for North/West India users + Mumbai/Pune buses.
- **ap-south-2 (Hyderabad, when available)** or **ap-south-1c** as secondary for South India (MTC Chennai, BMTC Bangalore).
- Postgres logical replication primary → secondary. Failover RTO 90s, RPO ~5s.
- Cassandra runs as one logical cluster across 2 DCs with `LOCAL_QUORUM` writes; cross-DC repair traffic capped via `dc_aware` snitch.
- Kafka: MirrorMaker 2 for cross-DC replication of `trip.events` and `notif.outcome` only (the audit log). `bus.gps.raw` stays local — no point shipping high-volume location data across regions.
- DR to **ap-southeast-1 (Singapore)** — cold standby. RTO 1h. Only the auth + ticket data is replicated; we accept losing live-tracking for a full region outage.

---

## Part 9 — Deployment, K8s, CI/CD

### 9.1 Cluster topology

- 2 EKS clusters per region (blue/green at cluster level, not just deploy level — lets us upgrade K8s minor versions without juggling). 
- Node groups:
  - `general` (c7g.large, on-demand) for stateless APIs
  - `streaming` (m7g.xlarge, on-demand) for Flink TMs (RocksDB needs local NVMe)
  - `burst` (mixed spot, c7g family) for batch + analytics
- Istio service mesh, mTLS everywhere, ext-authz to a sidecar JWT validator (we don't want each service re-validating).

### 9.2 CI/CD

```
PR open
  └─ unit tests, sonar, license scan, container build → ECR (with SBOM via Syft)
  └─ ephemeral preview env on a per-PR namespace (limited to 1 pod per svc)

Merge to main
  └─ deploy to dev cluster
  └─ contract tests (Pact) between services
  └─ deploy to staging
  └─ synthetic transit-simulator runs: 10k virtual buses + 100k virtual users
  └─ progressive delivery to prod via Argo Rollouts:
       canary 1% → 10% → 50% → 100%, gated by SLO burn rate
       stateful services (Flink ETA Engine) use savepoint + parallel run
       with shadow-traffic validation before promotion
```

### 9.3 The deploys that can hurt and how we prevent it

- **Flink job redeploy**: take savepoint → start new version pointed at savepoint → run both for 5 min → compare ETA distributions → switch traffic. We measure *prediction divergence*, not just "is it running".
- **Postgres schema change**: ghost-style with `gh-ost`, never `ALTER TABLE` blocking. Subscription schema migrations require app deploys to be forward-compatible for one minor version.
- **Mobile app deploy**: phased rollout via Play Console (1% → 5% → 20% → 50% → 100% over a week). Server must be backward-compatible with N–2 app versions, period.

---

## Part 10 — Observability, SLOs, Chaos

### 10.1 SLOs (the ones I'd put on the wall)

| SLI | SLO | Error budget / 30 d |
|-----|-----|---------------------|
| ETA freshness p99 | < 5 s | 5% (36 h) |
| Notification fired-within-target % | ≥ 99% | 1% (~340k missed of 34M/day × 30 d) |
| Ticket purchase success | ≥ 99.9% | 0.1% |
| Map API availability | ≥ 99.95% | 21 m |
| End-to-end notification accuracy (±60s of intended) | ≥ 95% | 5% |

### 10.2 What we instrument

- **RED** for every service: Rate, Errors, p50/p95/p99 Duration. Prometheus + Grafana.
- **USE** for every node: Utilization, Saturation, Errors.
- **OpenTelemetry traces** with W3C trace context propagated *into* the gRPC stream from the bus (we add a trace header on the Ack). Lets us trace a single push notification from bus GPS ping → Flink → Redis → orchestrator → FCM → device receipt callback (via FCM Direct Boot API). End-to-end span is the killer feature for debugging "why was this notification 90s late".
- **Notification-specific dashboard**: fire-rate, dedupe-rate, late-fire %, FCM 4xx rate per project key, device receipt latency histogram.

### 10.3 Chaos

- **Daily**: kill 5% of ingestion pods during off-peak.
- **Weekly**: black-hole a Kafka broker.
- **Monthly**: AZ failover drill in staging with prod-shaped load.
- **Quarterly**: Region failover (Mumbai → Singapore DR), full game-day, with execs watching.
- **Notification-specific**: inject ETA jitter (±30%) for 1% of buses, verify hysteresis prevents spam.

---

## Part 11 — Tradeoffs, What I'd Build First, Interview Traps

### 11.1 What I'd build in the first quarter (90 days)

1. **Weeks 1–3:** GPS ingestion + Kafka + minimal map endpoint backed by Redis GEO. No ETA engine; map shows raw bus positions. *Lets us ship "we see your bus" — table stakes.*
2. **Weeks 4–7:** ETA engine v1 in Flink with simple linear regression (no Kalman, no map-matching). *60% accurate is better than not present.*
3. **Weeks 8–10:** Subscription CRUD + notification orchestrator v1: ETA-based trigger, FCM only, no crowd filter, no device fallback. *Ships my feature.*
4. **Weeks 11–13:** Kalman + map-matching upgrade, hysteresis, dedupe hardening, the observability stack. *Make it not embarrassing.*

Pass on for q2: crowd filter, device geofence fallback, multi-region, iOS APNs (Android is 70% of India anyway).

### 11.2 The classic interview traps and the pre-canned answers

| Trap | Pre-canned answer |
|------|-------------------|
| "Why not just use Firebase?" | Vendor lock at this scale is a CFO-level decision; cost runs away past 1M concurrent and we lose control of GEO sharding. |
| "Walk me through what happens if Redis loses 30s of data." | Map shows slightly stale positions (acceptable). Notification dedupe SETNX may double-fire ~5k notifications globally during the window — client-side dedupe catches most; collapse_key catches the rest. We log it as an incident, not an outage. |
| "Why don't you just write GPS to DynamoDB and read from there?" | Cost at 100k writes/s, but more importantly — read pattern is "geo lookup in viewport", which DynamoDB doesn't natively serve. We'd be building Redis GEO on top of Dynamo, which is silly. |
| "How do you handle the bus GPS device being a malicious actor sending fake locations?" | mTLS device certs (provisioned at manufacture), per-bus rate limit, sanity gates (speed < 120 km/h, location within state polygon), anomaly detection job offline. |
| "How do you A/B test the notification lead-time?" | Subscription has `lead_time_seconds`; experiment framework controls default during creation. Outcome metric = "did user board this bus" (joined from ticket events). |
| "What's your biggest scaling worry?" | Flink state size when we onboard a 200k-bus city like Lagos. Currently RocksDB on local NVMe is fine; past ~5 TB per TM we'd shift to disaggregated state (Flink's experimental remote state). |

### 11.3 The honest "what I'd do differently in 2026" answer

- **Pulsar instead of Kafka**: native multi-tenancy, geo-replication, tiered storage. The operational story has caught up to Kafka and is in some ways ahead.
- **ScyllaDB instead of Cassandra**: same data model, 5–10× per-node throughput, half the nodes, much better tail latencies.
- **OpenTelemetry-first from day 1**, not bolted on.
- **Push more to the device**: the modern Android (14+) and iOS (17+) APIs let us register *richer* server-derived geofences, so we could push tomorrow's expected fire windows nightly and let the device fire locally as a primary path. That collapses our hot-path cost dramatically. I'd prototype this.

---

## Appendix A — Glossary & Numbers Table

| Term | Meaning |
|------|---------|
| AFC | Automatic Fare Collection (gates/turnstiles or onboard validators) |
| APNs | Apple Push Notification service |
| DPDP | India's Digital Personal Data Protection Act, 2023 |
| FCM | Firebase Cloud Messaging |
| GTFS | General Transit Feed Spec — the standard format for transit schedules |
| Hysteresis | Refusing to flip state until input has been stable across multiple samples |
| Map-matching | Snapping noisy GPS to the most-likely road segment on a graph |
| S2 cell | Google's spherical-quadtree spatial index — what we use for stops |

| Quantity | Value |
|----------|-------|
| DAU | 10 M |
| Active buses | 100 k |
| GPS ping rate | every 3 s |
| Steady ingestion QPS | 33 k |
| Peak ingestion QPS | 100 k |
| Map query peak QPS | 40 k |
| Notif fires per day | 34 M |
| Notif peak QPS | 5 k |
| Hot GPS storage / week | ~1.4 TB |
| Active subscriptions | 24 M |
| Monthly infra cost | ~₹17 L |

---

*End of document. ~1700 lines, 60-minute interview-walkthrough-ready. Part 7 is the actual deliverable I'd hand Chalo's product team alongside my December 2025 email.*

---

## Appendix B — The original feature suggestion (the email that started this)

This document exists because of a small piece of feedback I sent Chalo as a daily user. Including the exchange verbatim for context — it grounds every design decision in Part 7 in a real user need, not a hypothetical one.

### B.1 What I wrote — 15 December 2025, 10:10 AM

> **From:** Deepaksakthi V K <deepak2004sakthi@gmail.com>
> **To:** Chalo Support <support@chalo.com>
> **Subject:** Feature suggestion — push notification before bus arrival
>
> I'm a regular user of the Chalo app in Chennai for tracking MTC buses during my daily commutes. The live GPS tracking and ETA features work great for manual checks, but there's no push notification alert when a bus I'm waiting for approaches my stop.
>
> Adding this feature would save users time and reduce unnecessary waiting at stops — e.g., notify 5–10 minutes before arrival based on live ETA. It could include options for specific routes or favorites, with customizable alerts for crowd levels too. This would make the app even more user friendly for busy commuters.
>
> I'm using Chalo app version 10.8.26 on Android. Happy to provide more details or test a beta.
>
> Thank you for improving public transport in India!
>
> Best regards,
> Deepaksakthi Vellore Kumar

### B.2 What Chalo replied — 16 December 2025, 3:40 PM

> **From:** Chalo Premium Bus <support@chalo.com> via freshdesk.com
> **To:** Deepaksakthi V K
> **Subject:** Re: Feature suggestion — push notification before bus arrival
>
> Hi Deepaksakthi V K,
>
> Thank you for sharing your valuable feedback and suggestions regarding the Chalo app. We truly appreciate your inputs on adding a push notification alert feature that would notify users when a bus is approaching their stop.
>
> Currently, the Chalo app provides live GPS tracking and ETA updates for buses, which can be checked manually. However, we understand that a notification system — such as alerts 5–10 minutes before arrival, with options for favorite routes and crowd-level customization — would make the app even more convenient and user-friendly for daily commuters.
>
> We have noted your suggestion and will forward it to our product development team for consideration in future updates. Your willingness to provide more details and even test a beta version is greatly appreciated, and it helps us improve the app experience for all users.
>
> Thank you once again for your support in improving public transport in India.
>
> Best Regards,
> Team Chalo

### B.3 Why I wrote up the full system design

Three reasons:

1. **To remove the "is this 2 weeks or 6 months?" uncertainty** that usually stalls features at the product-prioritisation step. Part 7 of this document is a v1 scoped to **4 engineer-weeks**, with explicit "what we defer to v2" boundaries.
2. **To document the design choices that distinguish a real implementation from a naive one.** Fence the bus not the user. Trigger on ETA not distance. React to the stream not the schedule. Each one comes from a specific failure mode I've seen in the wild.
3. **As a working artefact for a senior-SDE / staff-SDE-level system-design interview.** The proximity-notification problem has the right ingredients: streaming ingestion, stateful real-time compute, fan-out at scale, mobile-platform constraints, idempotency, multi-region. It's an honest test of distributed-systems judgement.

If anyone on Chalo's engineering team — or any other transit platform — wants to discuss the implementation or the design choices, my contact info is in the repo README.

*Five months after the email, the feature is still not in the Chalo Android app. Public transit in India is a high-leverage software problem. I'd love to see this shipped.*
