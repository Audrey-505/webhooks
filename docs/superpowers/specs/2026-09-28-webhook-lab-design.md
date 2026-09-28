# Webhook Lab — Design Spec

**Date:** 2026-09-28
**Status:** Draft — awaiting review
**Companion notes:** [`docs/LEARNINGS.md`](../../LEARNINGS.md)

## 1. Purpose

A hands-on lab for learning how webhooks work end to end — and specifically for *seeing* idempotency, duplicate events, retries, and event handling. It simulates a real payment provider (think Stripe) sending webhooks to a store backend, with controls to inject failures and a live dashboard that shows what happens.

Code quality goal: clean, readable, maintainable implementations — small units with one clear purpose each.

### Success criteria

A user can, from the dashboard:
1. Create, pay, and refund orders and watch the resulting webhooks deliver.
2. Inject failures on either side and watch retries with backoff, dead letters, duplicates being skipped, and out-of-order events being deferred then applied.
3. Run each scenario preset and see the ledger end in the correct state (e.g. "charged 1×").
4. Turn dedupe off and *see* the double charge that idempotency prevents.
5. Replay any past event and see it skipped as a duplicate.

### Out of scope (v1)

- Subscription management (multiple endpoints, event-type filters). One endpoint, configured in settings.
- Authentication on the lab's own REST APIs and hubs.
- Multiple receiver instances / horizontal scaling.
- Browser end-to-end tests.
- Deployment. The lab runs locally.

## 2. Roles

| Component | Real-world analogue | Role |
|---|---|---|
| **Provider** | Stripe | Knows first that something happened; sends signed webhooks; retries |
| **Receiver** | Store backend | Verifies, dedupes, stores, processes webhooks into an order ledger |
| **Dashboard** | Admin screen | Drives both sides and visualizes what happened |

Webhooks (HTTP POST, server → server) carry the business events. SignalR (server → browser) carries observation updates to the dashboard only. Provider and Receiver never talk over SignalR; closing the dashboard does not affect delivery.

## 3. Architecture

```
┌──────────── Angular dashboard (:4200) ────────────┐
│  Provider panel │   Timeline   │  Receiver panel   │
└───────┬──────── SignalR ───────────── SignalR ──┬──┘
        │ REST                              REST  │
┌───────▼──────────┐   signed HTTP POST  ┌────────▼─────────┐
│ Provider (:5001) │ ──────────────────▶ │ Receiver (:5002) │
│ provider.db      │ ◀── 2xx / 5xx / ⏱ ──│ receiver.db      │
└──────────────────┘                     └──────────────────┘
```

- **Stack:** .NET 10 (ASP.NET Core minimal APIs), EF Core with SQLite, SignalR, Angular (standalone components, signals), xUnit.
- **Each service owns its own database and its own SignalR hub.** The Receiver knows nothing about the Provider except the shared signing secret.
- **Pattern symmetry:** the Provider uses an **outbox** (store, then send and retry from a background worker); the Receiver uses an **inbox** (store, acknowledge immediately, process in a background worker).

### Repository layout

```
Webhook-receiver/
├─ WebhookLab.sln
├─ src/
│  ├─ WebhookLab.Contracts/   event envelope, event type names, WebhookSignature, Backoff
│  ├─ WebhookLab.Provider/    order simulator, outbox, DeliveryDispatcher, chaos, ProviderHub
│  └─ WebhookLab.Receiver/    /webhooks endpoint, inbox, InboxProcessor, ledger, faults, ReceiverHub
├─ tests/
│  ├─ WebhookLab.Contracts.Tests/
│  ├─ WebhookLab.Provider.Tests/
│  └─ WebhookLab.Receiver.Tests/
├─ dashboard/                 Angular workspace
└─ docs/
   ├─ LEARNINGS.md
   └─ superpowers/specs/
```

## 4. Contracts (`WebhookLab.Contracts`)

### Event envelope

```json
{
  "id": "evt_01J...",
  "type": "payment.succeeded",
  "createdAt": "2026-09-28T10:00:01Z",
  "data": { "orderId": 42, "amount": 50.00 }
}
```

- `id` is globally unique per event and is the idempotency key. Retries and replays of the same event reuse the same `id`.

### Event types

| Type | Data | Meaning |
|---|---|---|
| `order.created` | `orderId`, `amount` | An order exists |
| `payment.succeeded` | `orderId`, `amount` | Customer was charged |
| `payment.refunded` | `orderId`, `amount` | Customer was refunded |

### Signature

Follows the [Standard Webhooks](https://www.standardwebhooks.com/) header convention:

| Header | Value |
|---|---|
| `webhook-id` | the event `id` |
| `webhook-timestamp` | Unix seconds when this attempt was signed |
| `webhook-signature` | `v1,{base64(HMAC-SHA256(secret, "{id}.{timestamp}.{body}"))}` |

`WebhookSignature` exposes `Sign(...)` and `Verify(...)`. Verification uses a constant-time comparison and rejects timestamps more than **5 minutes** from now. The secret comes from configuration (`Webhook:Secret`) in both services.

## 5. Provider (`WebhookLab.Provider`)

### Data model (`provider.db`)

| Table | Key columns |
|---|---|
| `Orders` | `Id`, `Amount`, `Status` (`Created` / `Paid` / `Refunded`) |
| `Events` | `Id` (evt_…), `Type`, `Payload` (JSON), `CreatedAt` |
| `Deliveries` | `Id`, `EventId`, `Status` (`Pending` / `Succeeded` / `Dead`), `AttemptCount`, `NextAttemptAt`, `IsReplay` |
| `DeliveryAttempts` | `Id`, `DeliveryId`, `AttemptNumber`, `StartedAt`, `DurationMs`, `StatusCode` (nullable), `Error` (nullable) |

### Flow

1. An order action (`create` / `pay` / `refund`) runs in **one transaction**: update `Orders`, insert `Events`, insert a `Pending` `Delivery` with `NextAttemptAt = now`.
2. `DeliveryDispatcher` (`BackgroundService`) runs every 1 s and loads `Pending` deliveries with `NextAttemptAt <= now`, oldest first.
3. For each delivery it applies chaos, signs, POSTs to `Webhook:TargetUrl` with a **5 s timeout**, and records a `DeliveryAttempt`.
4. On `2xx` → `Succeeded`. On anything else (any non-2xx status, timeout, connection error) → `AttemptCount++`. If `AttemptCount >= 5` → `Dead`; otherwise `NextAttemptAt = now + 2^AttemptCount s + jitter(0–500 ms)`.
5. Every state change is pushed to `ProviderHub`.

Backoff calculation lives in the small `Backoff` class in `WebhookLab.Contracts`, shared with the Receiver's deferred-event retries. Time comes from an injected `TimeProvider` so tests control the clock.

### Replay

`POST /api/events/{id}/replay` inserts a new `Pending` `Delivery` (`IsReplay = true`) for an existing event. The event `id` does not change.

### Chaos (sending-side failures)

Held in memory in a `ChaosSettings` singleton, editable via API:

| Setting | Effect |
|---|---|
| `DuplicateNext` (count) | The next N deliveries are sent twice, back to back |
| `DelaySeconds` | Wait this long before each send |
| `ReorderNext` (count) | Hold the next N deliveries, then release them in reverse order |
| `BadSignatureNext` (count) | Sign the next N sends with a wrong secret |

Counters decrement as they are used, so each toggle is a one-shot unless set again.

### API

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/orders` | Create an order (`amount`) |
| POST | `/api/orders/{id}/pay` | Pay |
| POST | `/api/orders/{id}/refund` | Refund |
| GET | `/api/deliveries` | Deliveries with attempts |
| POST | `/api/events/{id}/replay` | Replay |
| GET / PUT | `/api/chaos` | Read / update chaos settings |
| POST | `/api/reset` | Clear all data and chaos |
| Hub | `/hubs/provider` | Live updates |

## 6. Receiver (`WebhookLab.Receiver`)

### Data model (`receiver.db`)

| Table | Key columns |
|---|---|
| `InboxEvents` | `EventId` (**primary key** — the idempotency guarantee), `Type`, `Payload`, `ReceivedAt`, `Status` (`Received` / `Processed` / `Deferred` / `Failed`), `ProcessingAttempts`, `NextAttemptAt`, `LastError` |
| `LedgerOrders` | `OrderId`, `Amount`, `Status`, `ChargeCount`, `RefundCount` |

`ChargeCount` and `RefundCount` exist so the dashboard can show "charged 1×" (correct) versus "charged 2×" (the bug idempotency prevents).

### `POST /webhooks` — endpoint steps, in order

1. **Faults**: apply the `FailNext` fault (return `500`) if set.
2. **Verify signature** → `401` if invalid or expired. Nothing stored.
3. **Insert `InboxEvents`** row with `Status = Received`.
   - Unique-key violation → **duplicate**. Return `200` and push "duplicate skipped" to the hub. If the `DedupeOff` fault is on, instead reset the existing row to `Received` so it is processed again (demonstrates the double charge).
   - Database unavailable → `503`.
4. **Faults**: apply the `SlowNext` fault — the event is already stored, then the response is delayed 6 s (past the Provider's 5 s timeout). This simulates a "lost 200".
5. Return `200`.

The endpoint never processes business logic itself — it only verifies, stores, and acknowledges.

### `InboxProcessor` (`BackgroundService`)

Runs every 1 s and loads events with `Status in (Received, Deferred)` and `NextAttemptAt <= now`, oldest `ReceivedAt` first. For each event, in **one transaction**:
1. Dispatch to the handler for its `Type` (one small handler class per event type).
2. Update `LedgerOrders`.
3. Mark the event `Processed`.

If the `CrashNext` fault is set, it throws after step 2 → the transaction rolls back → the event stays `Received` and is retried on the next loop. The ledger is unaffected.

### Ledger rules (out-of-order handling)

| Event | Precondition | If unmet |
|---|---|---|
| `order.created` | none (existing order → no-op) | — |
| `payment.succeeded` | order exists | `Deferred` |
| `payment.refunded` | order exists and is `Paid` | `Deferred` |

`Deferred` events retry with the same backoff as the Provider. After 5 processing attempts → `Failed` (shown on the dashboard, never silently dropped).

### Faults (receiving-side failures)

Held in memory in a `FaultSettings` singleton:

| Setting | Effect |
|---|---|
| `FailNext` (count) | Return `500` for the next N requests |
| `SlowNext` (count) | Store, then respond after 6 s for the next N requests |
| `CrashNext` (count) | Throw mid-processing for the next N events |
| `DedupeOff` (bool) | Duplicates are re-processed instead of skipped |

### API

| Method | Path | Purpose |
|---|---|---|
| POST | `/webhooks` | Webhook endpoint |
| GET | `/api/inbox` | Inbox events |
| GET | `/api/ledger` | Ledger orders |
| GET / PUT | `/api/faults` | Read / update faults |
| POST | `/api/reset` | Clear all data and faults |
| Hub | `/hubs/receiver` | Live updates |

## 7. Hub messages

Both hubs push small, flat messages that all carry `eventId` and `at` (timestamp), so the dashboard can merge them into one timeline.

| Hub | Message | Extra fields |
|---|---|---|
| Provider | `EventCreated` | `type`, `orderId` |
| Provider | `AttemptStarted` | `deliveryId`, `attemptNumber`, `isReplay`, `isDuplicateSend` |
| Provider | `AttemptCompleted` | `deliveryId`, `attemptNumber`, `statusCode?`, `error?`, `durationMs` |
| Provider | `DeliveryStatusChanged` | `deliveryId`, `status`, `nextAttemptAt?` |
| Receiver | `WebhookReceived` | `outcome` (`Stored` / `Duplicate` / `DuplicateReprocessed` / `Rejected` / `FaultInjected`) |
| Receiver | `ProcessingCompleted` | `status` (`Processed` / `Deferred` / `Failed`, or `Received` after a crash rollback), `error?` |
| Receiver | `LedgerChanged` | `orderId`, `chargeCount`, `refundCount`, `status` |

## 8. Dashboard (`dashboard/`, Angular)

Layout: **Provider panel | Timeline | Receiver panel** (option B from brainstorming).

- **Provider panel:** order actions, chaos controls, outbox list (delivery status, attempt count, Replay button on each delivery).
- **Timeline (center):** a sequence-diagram view of the selected event. Each HTTP attempt is an arrow from Provider to Receiver, and each response is a return arrow with its status (`500`, `timeout`, `200`, `200 duplicate`). Receiver-side outcomes such as "stored", "deferred", and "processed" appear alongside the arrows. Clicking an event anywhere selects it and highlights it in all three columns.
- **Receiver panel:** fault controls, inbox list, ledger (with `charged N×`).
- **Scenario presets** and a **Reset lab** button (calls both `/api/reset`).

Structure:
- `ProviderApi`, `ReceiverApi` services — REST calls.
- `LabEventsService` — connects to both hubs and exposes a merged, `eventId`-keyed signal of timeline entries. This merge logic is the main thing that gets unit-tested.
- On hub reconnect, panels reload their current state from REST.

### Scenario presets

A preset is a data definition (`scenarios.ts`). The services know nothing about presets. Every preset runs the same way:

1. Reset both services.
2. Create an order and wait until the Receiver has `Processed` its `order.created`. This keeps the setup's one-shot counters from being used up by the create event.
3. Apply the preset's setup (chaos/faults).
4. Run the preset's actions.

| Preset | Setup | Actions | Expected outcome |
|---|---|---|---|
| Happy path | none | pay | 1 attempt, `200`, charged 1× |
| Duplicate send | `DuplicateNext=1` | pay | second copy skipped, charged 1× |
| Lost 200 | `SlowNext=1` | pay | timeout → retry → duplicate skipped, charged 1× |
| Receiver outage | `FailNext=3` | pay | 3 × `500` with growing backoff, then `200` |
| Dead letter + replay | `FailNext=5` | pay; after `Dead`, Replay | delivery `Dead`; replay succeeds, charged 1× |
| Refund before payment | `ReorderNext=2` | pay, refund | refund arrives first → `Deferred`, then applied after payment |
| Crash mid-processing | `CrashNext=1` | pay | processing rolls back, retries, charged 1× |
| Forged signature | `BadSignatureNext=5` | pay | `401` × 5, delivery `Dead`, nothing stored, order unpaid |
| Idempotency off | `DedupeOff=true`, `DuplicateNext=1` | pay | **charged 2×** — the bug made visible |

`ReorderNext=N` holds deliveries until N have been collected, so presets must always run at least N actions after setting it.

## 9. Error handling summary

| Where | Failure | Behavior |
|---|---|---|
| Provider → Receiver | 5xx / timeout / connection refused | Record attempt, backoff, `Dead` after 5 |
| Provider → Receiver | 4xx (e.g. `401`) | Same as above — retried, then `Dead` |
| Receiver endpoint | Bad/expired signature | `401`, not stored |
| Receiver endpoint | Duplicate `EventId` | `200`, logged as duplicate |
| Receiver endpoint | Database unavailable | `503` |
| InboxProcessor | Handler throws / crash fault | Transaction rolls back, retried next loop |
| InboxProcessor | Precondition unmet | `Deferred` → retry → `Failed` after 5 |
| Dashboard | Hub disconnect | Auto-reconnect, reload state from REST |

## 10. Testing

- **Unit (xUnit):** `WebhookSignature` (valid, tampered body, wrong secret, expired timestamp), `Backoff`, each ledger handler's rules.
- **Receiver integration** (`WebApplicationFactory`, real SQLite file per test) — one test per lesson:
  - `Same_event_twice_charges_customer_once`
  - `Lost_200_then_retry_is_skipped_as_duplicate`
  - `Refund_before_payment_is_deferred_then_applied`
  - `Crash_mid_processing_is_retried_without_double_charge`
  - `Forged_signature_is_rejected`
- **Provider dispatcher:** fake `HttpMessageHandler` as the receiver plus a fake `TimeProvider` — asserts retry schedule, dead-lettering, duplicate sends, reordering, replay.
- **Angular:** unit tests for `LabEventsService` merge logic.

## 11. Configuration defaults

| Setting | Default |
|---|---|
| Provider URL | `http://localhost:5001` |
| Receiver URL | `http://localhost:5002` |
| Dashboard URL | `http://localhost:4200` (CORS allowed on both services) |
| `Webhook:TargetUrl` (Provider) | `http://localhost:5002/webhooks` |
| `Webhook:Secret` | a dev-only value in `appsettings.Development.json` |
| Max delivery attempts | 5 |
| Delivery timeout | 5 s |
| Signature tolerance | 5 min |
| Dispatcher / processor poll interval | 1 s |
| Slow-fault response delay | 6 s |
