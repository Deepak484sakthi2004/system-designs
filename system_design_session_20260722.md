# System Design Mentor — Daily Session
**Date:** 22-Jul-2026
**Topic:** Design a Distributed Transaction System (2PC / Saga Pattern)
**Level:** SDE2/SDE3 | 60–150 LPA
**Mentor:** Arjun Mehta (40+ YOE)

---

## Opening Brief

Distributed transactions are the hardest unsolved problem that most engineers think they've solved. Every microservices-based fintech, e-commerce, and logistics platform deals with this daily — money that debits but doesn't credit, orders that are confirmed but never fulfilled, inventory that's reserved but never released. Google built Spanner to solve a subset of this; Uber tore apart their monolith and had to rebuild guarantees the monolith gave them for free. The hard part isn't understanding 2PC on a whiteboard — it's knowing exactly when 2PC will kill your availability and when Sagas will let you paint yourself into a consistency corner.

---

## Warm-Up Questions (Easy)

*These establish baseline. A good SDE2 should answer all of these without hesitation.*

**Q1.** What is the ACID problem in a microservices architecture? Why can't you just use a database transaction that spans two services?

> **What a strong answer covers:**
> - Each microservice owns its own database (database-per-service pattern); no shared DB means no shared transaction coordinator
> - Network calls between services can fail independently; you can't hold DB locks across a network boundary without severe availability impact
> - Even if both services use Postgres, a two-phase commit across two separate Postgres instances requires XA transactions, which are notoriously slow and block resources
> - The fundamental tension: atomicity and isolation require coordination; coordination requires time; time means blocked resources; blocked resources mean reduced availability

> **Common weak answer:** "You can use a shared database" — this couples services at the data layer and defeats the entire purpose of microservices.

> **Mentor follow-up if they answer well:** "If database-per-service is the rule, how does an e-commerce system atomically debit a user's wallet AND decrement inventory AND create an order record — all in three separate services — without a shared transaction?"

---

**Q2.** Back-of-envelope: A payment service processes 50,000 transactions/second. Each transaction involves 3 microservices (wallet, ledger, notification). If a 2PC coordinator holds locks for an average of 150ms while waiting for all participants to respond, what is the maximum lock contention you'd expect, and why is this a problem?

> **What a strong answer covers:**
> - At 50K TPS × 3 services = 150K lock acquisitions/second
> - With 150ms lock hold time, each lock is held by 0.15s × 50K = 7,500 concurrent transactions at any moment
> - If transactions touch overlapping rows (same user's wallet), queuing occurs; tail latency explodes
> - A single slow participant (GC pause, network hiccup) blocks all other transactions touching the same rows
> - In practice, 2PC at 50K TPS with 150ms coordination latency is not viable for hot-row workloads

> **Mentor follow-up:** "Spanner does distributed transactions at global scale. What trick does it use to make 2PC bearable? Hint: think about what TrueTime gives them."

---

**Q3.** What is the difference between a Saga and 2PC? When would you choose one over the other?

> **What a strong answer covers:**
> - **2PC:** Synchronous, blocking protocol. Coordinator asks all participants to "prepare" (acquire locks, write to redo log), then issues "commit" or "rollback". Atomicity is guaranteed. Participants are locked until coordinator decides. Single point of failure: if coordinator crashes after "prepare" but before "commit", participants are blocked indefinitely (the "in-doubt" problem).
> - **Saga:** A sequence of local transactions, each with a compensating transaction. No global lock held. If step N fails, steps N-1 down to 1 are compensated (rolled back semantically). Eventually consistent — intermediate states are visible to other transactions.
> - **Choose 2PC when:** you need true atomicity, transactions are short-lived, you control all participants, and availability can tolerate coordinator overhead. Example: transferring between two accounts in the same bank's system.
> - **Choose Saga when:** you have long-lived transactions, cross-service boundaries with external systems (payment gateways), or availability is paramount. Example: booking a flight + hotel + car in one checkout flow.

> **Red flag answer:** "Saga is just a better 2PC" — they're solving different parts of the problem. Sagas give up isolation; 2PC gives up availability. Neither is strictly better.

---

## High-Level Design (Medium)

*The candidate should drive this. Expect them to draw components, identify data flows, pick protocols.*

**Q4.** Design a distributed transaction system that supports both Saga (choreography and orchestration) and 2PC for a large e-commerce platform. Draw the high-level architecture.

> **Key components expected:**
> - **Transaction Coordinator Service** — manages 2PC protocol, persists coordinator log to durable storage
> - **Saga Orchestrator** — a state machine service that drives Saga steps and compensations
> - **Event Bus (Kafka)** — for choreography-based Sagas; services react to domain events
> - **Participant Services** — Order, Inventory, Payment, Notification; each has its own DB and implements `prepare/commit/rollback` (for 2PC) or `execute/compensate` (for Saga)
> - **Outbox Pattern** — each participant writes events to a local outbox table in the same local DB transaction; a relay process publishes to Kafka
> - **Distributed State Store** — stores Saga/2PC state durably (Postgres or DynamoDB)
> - **Dead Letter Queue** — for failed compensation events that need manual intervention

> **Architecture diagram (text):**
```
[Client / API Gateway]
        |
        v
[Transaction Coordinator Service] ←──── persists ────→ [Coordinator Log DB (Postgres)]
        |
   ┌────┴────────────────────┐
   |                         |
   v                         v
[2PC Path]              [Saga Path]
   |                         |
   v                         v
[Prepare → all]    [Saga Orchestrator] ←── state ──→ [Saga State Store]
   |                    |         |
   v             [Step Execute]  [Compensation]
[Commit/Rollback]      |              |
   |             [Outbox Relay]  [Outbox Relay]
   v                   |              |
[Participant        [Kafka Bus]   [Kafka Bus]
 Services:              |
 Order, Inventory,  [Participant Services consume events]
 Payment, Notif]
        |
   [Local DB + Outbox Table]
```

> **What separates SDE2 from SDE3 here:** An SDE2 draws the services and says "use Kafka." An SDE3 immediately asks: "How does the Saga Orchestrator itself become durable? What happens if it crashes mid-Saga? Answer: the orchestrator must be stateless and re-derive its position from the durable state store. Every step transition must be idempotent."

---

**Q5.** Trace a complete Saga execution for an e-commerce order: user places an order involving Payment Service, Inventory Service, and Order Service. Walk through the happy path AND the failure path where payment succeeds but inventory reservation fails.

> **Expected trace (Happy Path):**
> 1. Client calls `POST /orders` → API Gateway → Saga Orchestrator
> 2. Orchestrator creates Saga record (state: `STARTED`) in state store
> 3. Orchestrator sends `ReserveInventory` command to Inventory Service
> 4. Inventory Service acquires local lock on SKU, decrements count, writes to outbox, commits local DB tx
> 5. Outbox relay publishes `InventoryReserved` event to Kafka
> 6. Orchestrator receives `InventoryReserved`, updates state to `INVENTORY_RESERVED`, sends `ChargePayment` command
> 7. Payment Service charges card, writes to outbox, commits local tx
> 8. Outbox relay publishes `PaymentCharged` event
> 9. Orchestrator receives `PaymentCharged`, updates state to `PAYMENT_CHARGED`, sends `CreateOrder` command
> 10. Order Service creates order record, publishes `OrderCreated`
> 11. Orchestrator marks Saga `COMPLETED`

> **Failure Path (inventory succeeds, payment fails):**
> 1–5. Same as above
> 6. Orchestrator sends `ChargePayment` command
> 7. Payment Service fails (card declined) → publishes `PaymentFailed` event
> 8. Orchestrator receives `PaymentFailed`, transitions to `COMPENSATING`
> 9. Orchestrator sends `ReleaseInventory` compensation command to Inventory Service
> 10. Inventory Service increments count back, commits local tx
> 11. Orchestrator marks Saga `COMPENSATED`

> **Tricky part:** Most candidates forget that the compensation itself can fail. What if `ReleaseInventory` times out? The Orchestrator must retry compensation idempotently. Each compensation command must carry the original Saga ID so the participant can detect and ignore duplicates.

---

**Q6.** Define the key APIs for the Saga Orchestrator service and the participant contract.

> **Expected API design:**
```
# Saga Orchestrator REST API
POST   /sagas                          # Start a new saga; body: {saga_type, context}
                                       # Returns: {saga_id, status}
GET    /sagas/{saga_id}                # Fetch current saga state and step history
POST   /sagas/{saga_id}/events         # Internal: participants post step results here

# Participant Contract (each service must implement)
POST   /saga-commands/{command_type}   # Execute a saga step
  Request: {saga_id, idempotency_key, payload}
  Response: {status: "SUCCESS"|"FAILURE", result}

# 2PC Coordinator API
POST   /transactions                   # Begin 2PC transaction; returns transaction_id
POST   /transactions/{tx_id}/prepare   # Coordinator triggers prepare phase
POST   /transactions/{tx_id}/commit    # Coordinator issues commit
POST   /transactions/{tx_id}/rollback  # Coordinator issues rollback
GET    /transactions/{tx_id}/status    # Query in-doubt transaction state
```

> **What to push on:**
> - **Idempotency:** Every command must carry an `idempotency_key` (typically `saga_id + step_name`). If the Orchestrator retries after a timeout, the participant must return the same result without re-executing.
> - **Async vs sync:** For long-running steps (payment gateway calls), participants should acknowledge receipt synchronously and post results asynchronously. The Orchestrator must support a callback/event-driven model, not just polling.
> - **Versioning:** Sagas can run for minutes. What if the Inventory Service deploys a new version mid-Saga? The `saga_type` should include a version so the Orchestrator can route to the correct step definitions.

---

## Data Modeling (Medium–Hard)

*This is where SDE3 candidates shine. Expect schema design, index choices, partitioning strategy.*

**Q7.** Design the data model for the Saga state store. It must support: querying all in-flight Sagas, resuming a crashed Orchestrator, and auditing a completed Saga's step history.

> **Expected schema:**
```sql
-- Core Saga state machine
CREATE TABLE sagas (
    saga_id         UUID PRIMARY KEY,
    saga_type       VARCHAR(100)   NOT NULL,  -- e.g., 'ORDER_PLACEMENT_V2'
    status          VARCHAR(50)    NOT NULL,  -- STARTED, COMPENSATING, COMPLETED, FAILED
    context         JSONB          NOT NULL,  -- initial payload (order_id, user_id, etc.)
    current_step    VARCHAR(100),
    created_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    version         BIGINT         NOT NULL DEFAULT 0  -- optimistic locking
);

-- Step-by-step audit trail
CREATE TABLE saga_steps (
    step_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    saga_id         UUID           NOT NULL REFERENCES sagas(saga_id),
    step_name       VARCHAR(100)   NOT NULL,  -- 'RESERVE_INVENTORY', 'CHARGE_PAYMENT'
    step_type       VARCHAR(20)    NOT NULL,  -- 'FORWARD' | 'COMPENSATION'
    status          VARCHAR(50)    NOT NULL,  -- PENDING, SUCCEEDED, FAILED, SKIPPED
    idempotency_key VARCHAR(255)   NOT NULL UNIQUE,
    request_payload JSONB,
    response_payload JSONB,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    retry_count     INT            NOT NULL DEFAULT 0
);

-- Outbox table (per-participant, in their own DB)
CREATE TABLE saga_outbox (
    event_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    saga_id         UUID           NOT NULL,
    event_type      VARCHAR(100)   NOT NULL,
    payload         JSONB          NOT NULL,
    published       BOOLEAN        NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ    NOT NULL DEFAULT now()
);
CREATE INDEX idx_saga_outbox_unpublished ON saga_outbox(created_at) WHERE published = FALSE;
```

> **Index choices and why:**
> - `sagas(status, updated_at)` — to find stale in-flight Sagas for the recovery sweep job
> - `saga_steps(saga_id)` — to reconstruct full step history for a given Saga
> - `saga_steps.idempotency_key UNIQUE` — to detect and ignore duplicate step submissions
> - `saga_outbox(published, created_at)` — partial index on `published = FALSE` for efficient relay polling

> **Partitioning key and why:** Partition `saga_steps` by `saga_id` hash — all steps for a Saga live on the same shard, making step history queries local. Partition `sagas` by `created_at` range for archival — completed Sagas older than 90 days move to cold storage.

---

**Q8.** The Orchestrator crashes mid-Saga after sending `ChargePayment` command but before recording the step as `PENDING`. On restart, how does it know whether to retry the step or assume it already ran?

> **Expected answer:**
> - The Orchestrator must write the `PENDING` step record to the Saga state store **before** sending the command to the participant — this is the "write-ahead" approach.
> - On restart, the recovery job queries `sagas WHERE status IN ('STARTED', 'COMPENSATING') AND updated_at < now() - interval '30 seconds'`
> - For each in-flight Saga, it inspects the last `saga_steps` record:
>   - `status = PENDING` and `started_at` is old → resend the command with the same `idempotency_key`; the participant's idempotency layer will deduplicate
>   - No step record for the expected current step → safe to send fresh command
> - The participant must handle this via the `idempotency_key` uniqueness constraint: if it sees a duplicate key, it returns the stored result without re-executing

> **Trap:** Naive answer — "just check if the payment went through by querying the Payment Service." This introduces coupling and the Payment Service might be down. The idempotency key approach makes recovery self-contained without querying participants.

---

**Q9.** In an Order Saga, the inventory is reserved (step 1 completes) and payment is charged (step 2 completes). Before the Orchestrator creates the Order record (step 3), another user queries the inventory count. They see reduced stock — but the Order doesn't exist yet. Is this acceptable? How do you reason about isolation in Sagas?

> **Expected answer:**
> - Sagas do **not** provide isolation (the "I" in ACID). Intermediate states are visible to concurrent transactions — this is the **"lost isolation" problem**.
> - The correct answer is to reason about which isolation anomalies your business can tolerate:
>   - **Dirty reads of intermediate state:** Acceptable in most e-commerce (inventory showing 0 is conservative; worst case: user sees "out of stock" briefly)
>   - **Lost updates during compensation:** Dangerous — if two Sagas both reserve the last unit and one compensates, the count must return to 1, not 2
> - Mitigation strategies:
>   - **Semantic lock pattern:** Mark the inventory row as `status = RESERVED` during the Saga; other transactions can read but should treat `RESERVED` stock as unavailable
>   - **Pivot transaction:** Identify the irreversible step (payment charge). Steps before pivot are "retryable forward"; steps after pivot are "retriable forward only, never compensated" (e.g., you issue a refund, not a charge reversal)
>   - **Countermeasure: re-read before commit** — the Order Service re-reads inventory state before creating the order to catch anomalies

> **Mentor pushback:** "Your Saga compensates inventory after a failed payment. But between the inventory reservation and the compensation, a flash sale triggered and 500 users saw stock as available. When compensation fires and adds 1 unit back, is the inventory count correct?" — Answer: yes, if the compensation is `UPDATE inventory SET count = count + 1` (relative update) rather than `SET count = original_value` (absolute update). Absolute updates cause lost updates under concurrency.

---

## Low-Level Design (Hard)

*Pick the most interesting sub-component and go deep. This tests implementation thinking.*

**Q10.** Design the Saga Orchestrator as a durable, crash-safe state machine. The Orchestrator process can crash at any point. How do you ensure Sagas resume correctly and exactly once?

> **Problem statement:** The Orchestrator must transition a Saga through N steps, each involving a network call to a participant. It can crash between any two operations. On restart, it must resume without duplicating completed steps or skipping failed ones.

> **Naive solution:** Keep Saga state in memory; on crash, lose all progress and restart all Sagas from step 1.

> **Why naive fails at scale:** Re-executing completed steps causes double-charges, duplicate inventory decrements. Users get charged twice. This is catastrophic in fintech.

> **Expected optimal approach:**
> - **Event-sourced state machine:** The Saga's state is derived from a log of immutable events in `saga_steps`. The current state is never stored directly — it is computed by replaying the step log.
> - **Write-ahead before act:** Before sending any command, append a `PENDING` step record to `saga_steps`. Only after receiving a confirmed result do you update that record to `SUCCEEDED`/`FAILED`.
> - **Idempotent participants:** Every command carries `idempotency_key = sha256(saga_id + step_name)`. Participants store results keyed by this — duplicates return cached results.
> - **Optimistic locking on Saga:** `sagas.version` increments on every transition. On concurrent orchestrator instances trying to advance the same Saga (after failover), only one wins the `UPDATE sagas SET version = version + 1 WHERE version = $expected` CAS operation.

> **Pseudo-code:**
```python
class SagaOrchestrator:
    def advance(self, saga_id: str):
        with db.transaction():
            saga = db.get_for_update(saga_id)         # SELECT FOR UPDATE
            step = self.next_step(saga)
            if step is None:
                saga.status = 'COMPLETED'
                db.save(saga)
                return

            # Write-ahead: record PENDING before sending
            step_record = SagaStep(
                saga_id=saga_id,
                step_name=step.name,
                status='PENDING',
                idempotency_key=f"{saga_id}:{step.name}",
                started_at=now()
            )
            db.insert(step_record)        # Commit this first
            saga.current_step = step.name
            saga.version += 1
            db.save(saga)

        # Send command AFTER committing the PENDING record
        try:
            result = participant.execute(
                command=step.command,
                idempotency_key=step_record.idempotency_key,
                payload=step.build_payload(saga.context)
            )
            self._record_result(saga_id, step.name, result)
        except TimeoutError:
            # Recovery job will retry using the PENDING record
            pass

    def recover(self):
        # Runs periodically on startup and as a background job
        stale_sagas = db.query(
            "SELECT * FROM sagas WHERE status IN ('STARTED','COMPENSATING')"
            " AND updated_at < now() - interval '30s'"
        )
        for saga in stale_sagas:
            self.advance(saga.saga_id)   # Idempotent; will retry PENDING step
```

---

**Q11.** In 2PC, describe the exact race condition that occurs when the coordinator crashes after sending `PREPARE` to all participants but before sending `COMMIT`. How do participant services resolve their in-doubt state?

> **Scenario:** Coordinator sends `PREPARE` to Inventory and Payment services. Both respond `PREPARED` (locks acquired, redo logs written). Coordinator crashes before persisting `COMMIT` decision or sending it. Both participants are now in-doubt: they hold locks, cannot commit unilaterally, cannot rollback unilaterally (since another participant might commit).

> **Expected fix:**
> - **Coordinator log (Write-Ahead Log):** Before sending `COMMIT`, coordinator durably writes its decision to a WAL on disk (or replicated DB). On restart, it reads the WAL and re-sends `COMMIT` to all participants.
> - **Participant timeout + coordinator query:** After a configurable timeout in the `PREPARED` state, participants query the coordinator (or a coordinator replica) for the transaction decision. This requires the coordinator to be HA (primary + replicas sharing the WAL).
> - **Presumed-abort protocol (optimization):** If a participant cannot reach the coordinator, it aborts after timeout. The coordinator, on recovery, checks its WAL: if the transaction was not logged as committed, it also aborts and re-sends rollback. This means participants can safely abort on timeout.
> - **Cooperative termination protocol:** Participants can query *each other* for their state. If any participant is in `ABORTED`, the group can abort. If any participant is in `COMMITTED`, the group must commit. Only if all are in `PREPARED` is the outcome truly unknown.

> **Follow-up:** "What if the lock holder (participant in PREPARED state) crashes before receiving COMMIT?" — On restart, the participant reads its own redo log, sees an uncommitted prepared transaction, queries the coordinator for the decision, and either commits or rolls back accordingly. This is why the coordinator WAL must outlive any individual participant restart.

---

**Q12.** Design the compensation retry mechanism for a Saga when a compensation step fails repeatedly (e.g., the Inventory Service is down for 2 hours during a compensation).

> **Scenario:** Order Saga step 1 (inventory reserve) succeeded. Step 2 (payment) failed. Orchestrator triggers compensation: `ReleaseInventory`. Inventory Service is down. Compensation fails 3 times in a row. What now?

> **Expected handling:**
> - **Exponential backoff with jitter:** Retry compensation with delays: 1s, 2s, 4s, 8s... up to a max (e.g., 5 min). Jitter prevents thundering herd on recovery.
> - **Compensation is idempotent:** Each retry uses the same `idempotency_key`. When Inventory Service comes back up, it processes the compensation exactly once.
> - **Durable retry queue:** The Orchestrator must persist the pending compensation to a durable queue (Kafka topic or a `pending_compensations` DB table) rather than holding it in memory. On Orchestrator restart, pending compensations resume.
> - **Dead Letter Queue (DLQ) after N retries:** After a configurable limit (e.g., 72 hours, 100 retries), move the Saga to `COMPENSATION_FAILED` status and push to a DLQ. Human operator must resolve — either manually trigger compensation, issue a refund through an alternate path, or flag for reconciliation.
> - **Alerting:** Sagas in `COMPENSATION_FAILED` must trigger a PagerDuty alert immediately — these represent financial or inventory inconsistencies requiring human resolution.
> - **Non-compensatable steps (pivot):** Some steps cannot be compensated (e.g., email sent). For these, document the "best effort" compensation (send a cancellation email) and accept that it may fail.

---

## Scaling to 10x / 100x (Hard)

*This is where senior candidates must show systems thinking, not just pattern matching.*

**Q13.** Where does a Saga Orchestrator break first when you scale from 10K to 1M concurrent Sagas?

> **Expected answer:**
> - **Bottleneck 1: Saga state store write throughput.** Every step transition writes to `sagas` and `saga_steps`. At 1M concurrent Sagas with average 5 steps each and 10-second completion time, that's `1M × 5 / 10 = 500K writes/second` to the state store. A single Postgres instance handles ~50K writes/second. You need horizontal sharding or a purpose-built system.
> - **Bottleneck 2: Orchestrator process itself.** A single Orchestrator process manages in-memory state and dispatches commands. At 1M Sagas, you need the Orchestrator to be stateless and horizontally scalable. Saga assignment must be partitioned (e.g., by `saga_id % num_orchestrators`) using consistent hashing.
> - **Bottleneck 3: Recovery sweep query.** `SELECT * FROM sagas WHERE status IN ('STARTED') AND updated_at < now() - 30s` is a full-table scan at 1M rows. Without a proper index on `(status, updated_at)`, this sweeps the whole table every 30 seconds.
> - **Bottleneck 4: Kafka consumer lag.** If choreography-based Sagas use Kafka, slow consumers fall behind. A participant processing 100K events/second with 500ms per event needs 50 consumer threads or partitions.

> **Numbers:** At 1M concurrent Sagas, assume 5 steps each, 10s average duration: 500K state transitions/second. Redis-backed Saga state (with async DB flush) can handle this; pure synchronous Postgres cannot.

---

**Q14.** How do you shard the Saga state store across multiple database nodes?

> **Expected sharding strategy:**
> - **Hash sharding on `saga_id`:** `saga_id` is a UUID; use consistent hashing to map each `saga_id` to a shard. All rows for a given Saga (`sagas` + `saga_steps`) land on the same shard — all step history queries are local.
> - **Why not range sharding:** Sagas arrive in time order; range sharding on `created_at` creates write hotspots on the most recent shard.
> - **Why not service-based sharding:** Different Saga types (ORDER, REFUND, SUBSCRIPTION) have wildly uneven volumes; sharding by Saga type creates unbalanced shards.
> - **Number of shards:** Start with 16 logical shards on 4 physical nodes (4 logical per node). This allows adding nodes by migrating 4 logical shards at a time without full resharding.

> **Hot spot problem:**
> - UUID v4 distributes uniformly — no hot spots from key distribution.
> - Hot spots occur when one customer generates 80% of Sagas (enterprise client). Detect via per-shard QPS monitoring; mitigate by giving high-volume saga_id prefixes their own dedicated shard.
> - Use virtual nodes (128 per physical node) so shard migration is fine-grained.

---

**Q15.** What should be cached in a distributed transaction system, and what must never be cached?

> **Expected layered cache design:**
> - **Cache (Redis):**
>   - **Idempotency key results:** `idempotency_key → result` with TTL = 24h. Prevents re-processing duplicate commands without hitting the DB.
>   - **Saga definition/config:** The step definitions for each `saga_type` (which steps, which participants, which compensations). This is static config — cache indefinitely with explicit invalidation on deploy.
>   - **Participant health status:** Whether a participant is up/degraded (used by Orchestrator to fail-fast rather than timeout). TTL = 5s.
> - **Do NOT cache:**
>   - **Saga current state (`sagas.status`, `sagas.current_step`):** This changes on every step; a stale cache causes the Orchestrator to re-execute completed steps. Always read from the source-of-truth DB with `SELECT FOR UPDATE`.
>   - **`saga_steps` records:** Step completion status must be authoritative; cache misses here cause duplicate execution.
>   - **Coordinator's commit/abort decision in 2PC:** This must be read from the WAL, never from cache. A stale cache of "ABORTED" when the real decision is "COMMITTED" causes data loss.

> **Cache invalidation trap:** The idempotency key cache is the one place where a false negative (cache miss when key exists in DB) is safe (causes a DB lookup), but a false positive (cache hit with stale data) is dangerous only if results change — which they shouldn't (idempotency results are immutable once written). Use write-through caching: write to DB first, then populate cache.

---

**Q16.** How do you reduce the infrastructure cost of the Saga state store at 100M Sagas/month?

> **Expected answer:**
> - **Tiered storage:** Active Sagas (status IN STARTED, COMPENSATING) stay in hot Postgres. Completed Sagas older than 7 days move to S3 (Parquet via a nightly job). 90-day archive query goes to Athena.
> - **Saga step compression:** `request_payload` and `response_payload` in `saga_steps` are JSONB blobs. Compress with zstd before storage; decompress on read. Typical savings: 60–70% on repetitive payloads.
> - **Batched outbox relay:** Instead of the relay polling every 100ms and publishing one event at a time, batch 500 events per Kafka produce call. Reduces Kafka broker write amplification by 500×.
> - **Partition pruning:** Monthly partition on `saga_steps(created_at)`. DROP old partitions instead of row-level deletes — zero I/O for archival.
> - **Reduce step granularity:** If a Saga type consistently succeeds without compensation in 99.9% of cases, consider collapsing it into a single step with an async reconciliation job for the 0.1% failure case. Fewer steps = fewer rows.

---

## Mentor's 5 Hardest Questions (SDE3+ Differentiators)

*These are the questions that separate a strong SDE3 from a principal engineer. A candidate who answers even 3 of these well is exceptional.*

**H1.** In choreography-based Sagas using Kafka, each service publishes events and reacts to others' events. How do you prevent a "Saga storm" — a cascade where one compensating event triggers another compensation, which triggers another, in an infinite cycle? What message schema change prevents this?

> **Expected answer:** Carry a `saga_id` and `step_sequence` in every event header. Each participant checks: "Have I already processed an event for this `saga_id` at this step?" using the idempotency store. More importantly, compensating events should be published to a *separate* Kafka topic (e.g., `saga.compensations`) that forward-flow consumers are not subscribed to. This breaks the event loop structurally. Additionally, the Saga definition should enumerate valid state transitions; any event that doesn't match a valid transition for the current state is ignored and logged — not re-published.

---

**H2.** Your Saga spans services across two geographic regions (India and US) due to data residency laws — user data must stay in-region but the Order service is global. How do you handle a Saga that must coordinate participants across regulatory boundaries?

> **Expected answer:** The Orchestrator must be aware of data-residency constraints in the Saga definition. Steps touching Indian user data route to the IN-region participant; steps touching global order data route to the global service. Cross-region calls must not carry PII in the payload — use a token/reference that the remote service resolves locally. The Saga state store itself may need to be split: PII-containing fields stay in the IN region; non-PII Saga metadata can live globally. Compensation flows must also respect residency — the IN-region compensation runs in-region, not triggered by a cross-region event carrying PII. This requires the Orchestrator to support "regional sub-orchestrators" that handle the PII-sensitive steps locally and report back only status (not data) to the global Orchestrator.

---

**H3.** You need to deploy a change to the Saga step definition — specifically, adding a new mandatory step (fraud check) between `RESERVE_INVENTORY` and `CHARGE_PAYMENT`. At the time of deploy, there are 50,000 in-flight Sagas. How do you deploy this without breaking or manually migrating them?

> **Expected answer:** Use **Saga versioning**. Each Saga record stores `saga_type = 'ORDER_PLACEMENT_V1'`. New Sagas created post-deploy use `ORDER_PLACEMENT_V2` which includes the fraud check step. In-flight V1 Sagas continue running to completion using the V1 step definition — the Orchestrator routes by `saga_type`. After all V1 Sagas drain (typically hours), remove V1 support. This requires the Orchestrator to load step definitions dynamically by version, not hardcode them. The fraud check service must also be idempotent-ready before the deploy, even if V2 is not yet creating Sagas — ensures no ordering issues. Use a feature flag to control V2 Saga creation rate (0% → 10% canary → 100%).

---

**H4.** What metrics and traces would you instrument on the Saga Orchestrator to detect and alert on: (a) a participant that is consistently slow and causing Saga tail latency, (b) compensation storms (unusual spike in compensation rate), and (c) in-doubt 2PC transactions older than 5 minutes?

> **Expected answer:**
> - **(a) Slow participant detection:**
>   - Histogram: `saga_step_duration_seconds{step_name, participant}` — p99 per step per participant
>   - Alert: `p99(saga_step_duration{participant="inventory"}) > 2s for 5m`
>   - Distributed trace: every Saga carries a `trace_id`; each step is a span. In Jaeger/Tempo, query spans with `duration > 2s` grouped by participant.
> - **(b) Compensation storm detection:**
>   - Counter: `saga_compensations_total{saga_type, compensating_step}`
>   - Rate alert: `rate(saga_compensations_total[5m]) / rate(sagas_started_total[5m]) > 0.05` (compensation rate > 5% of new Sagas)
>   - This ratio normalizes for traffic spikes; an absolute threshold fires false positives during load.
> - **(c) In-doubt 2PC transactions:**
>   - Gauge: `two_pc_indoubt_transactions{age_bucket}` — populated by the recovery sweep job
>   - Alert: `two_pc_indoubt_transactions{age_bucket="5m+"} > 0` — any transaction older than 5 minutes without a decision is a critical anomaly requiring immediate PagerDuty.
>   - Log: every coordinator WAL write includes structured log with `transaction_id`, `decision`, `participants`. Parse into a log-based metric in Datadog.

---

**H5.** You initially built the Order Saga using an orchestrator-based approach with Postgres as the Saga state store. At 10M Sagas/day, Postgres is saturating at 80% CPU. Your team proposes migrating to an event-sourced Kafka Streams-based Saga state machine. Walk through the migration plan without downtime and without losing in-flight Sagas.

> **Expected answer:**
> 1. **Dual-write phase:** Before switching, have the new Kafka Streams processor subscribe to the same Saga command/event topics but only *read* (shadow mode). Validate that it would produce the same state transitions as the Postgres orchestrator. Run for 2 weeks; compare state snapshots daily.
> 2. **Drain-and-switch for new Sagas:** Add a feature flag: new Sagas above a threshold (e.g., `saga_id hash % 100 < 10`) route to the new Kafka Streams processor. The old Postgres orchestrator continues handling existing + non-flagged Sagas. Gradually increase the percentage.
> 3. **In-flight migration:** Do NOT migrate in-flight Sagas mid-execution. Let them complete on the old system. Only new Sagas use the new system. The Postgres orchestrator stays live until all V1 Sagas drain.
> 4. **State bootstrap:** The Kafka Streams processor's state store (RocksDB local + Kafka topic as changelog) must be bootstrapped from the existing Postgres Saga records. Write a one-time migration job that replays all completed and active Sagas as events into the Kafka changelog topic. The processor rebuilds its RocksDB state from these events.
> 5. **Cutover:** Once 0 active Sagas remain on the old system, disable the Postgres orchestrator. Keep Postgres in read-only mode for 30 days for audit queries.
> 6. **Rollback plan:** The Kafka changelog topic is the source of truth. If rollback is needed, the Postgres orchestrator can re-read the Kafka changelog and reconstruct its state. Feature flag flips back to 0%.

---

## Mentor's Closing Notes

**Top 3 things most candidates get wrong on this topic:**

1. **Treating Sagas as "distributed transactions with retries."** Sagas are NOT transactions. They explicitly give up isolation. A candidate who doesn't proactively mention the lost-isolation problem and its mitigations (semantic locks, pivot transactions, countermeasures) hasn't understood Sagas at the SDE3 level.

2. **Forgetting that compensation can fail.** Every candidate draws the happy compensation path. Almost none think through: what if compensation itself is undeliverable for 2 hours? The answer (durable retry queue, DLQ, human escalation, non-compensatable step handling) is where the real engineering lives.

3. **2PC coordinator as a single point of failure.** Candidates who propose 2PC without immediately addressing coordinator HA (write-ahead log, standby coordinator, presumed-abort on timeout) have proposed a system that will deadlock in production. The coordinator WAL is the most critical piece of 2PC; most candidates never mention it.

**The one insight that makes an answer truly impressive:**

The **Outbox Pattern** as the foundational primitive for reliable event publishing. Most candidates propose "just publish to Kafka after the DB write" — which has a race condition: the DB write commits, the process crashes before Kafka publish, and the event is lost forever. Candidates who immediately reach for the Outbox Pattern (write event to a local outbox table in the same DB transaction as the business operation, then have a relay process publish it to Kafka with at-least-once semantics + idempotent consumers) demonstrate production-grade distributed systems thinking. This pattern eliminates the dual-write problem that plagues every event-driven Saga implementation.

**Suggested follow-up reading:**
- **"Saga" (1987) — Hector Garcia-Molina & Kenneth Salem:** The original paper. Two pages. Read it. Every modern Saga framework traces back here.
- **Kleppmann, *Designing Data-Intensive Applications*, Chapter 9:** The clearest treatment of 2PC, coordinator failure modes, and linearizability vs. serializability in print.
- **Uber Engineering Blog — "Designing Resilient Systems":** Covers how Uber's Cadence workflow engine (now Temporal) solved exactly this problem at 1M+ concurrent workflows.
- **Temporal.io documentation on workflow durability:** A production-grade open-source implementation of durable Saga execution — reading the "Workflow History" internals is worth more than any blog post on this topic.

---

## How to Use This Session

1. **Solo mode:** Cover each question section by section. Write your answer, then read the expected answer. Grade yourself honestly. Questions Q10–Q12 are where most SDE2s struggle — spend extra time there.

2. **Interactive mode:** Paste this entire document into a new Claude conversation and say: *"You are Arjun Mehta. I am your student. Start with Q1 and don't reveal the expected answers — ask me the questions one at a time, push back on weak answers, and guide me to the right answer through follow-up questions."*

3. **Mock interview mode:** Set a 45-minute timer. Answer only Q4–Q15 as if it's a real interview — whiteboard the architecture, trace the failure path, design the schema. Then review. If you couldn't explain the Outbox Pattern or compensation retry mechanics under time pressure, that's your gap for this week.
