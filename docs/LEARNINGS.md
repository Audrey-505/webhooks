# Webhook Lab — Learnings

Notes for future reference and study. Foundations first, then the questions I asked while designing this project, with answers.

---

## 1. Foundations

### The big picture

```
Stripe (Provider) ──webhook──▶ Store backend (Receiver) ──SignalR──▶ Browser (Dashboard)
     server                         server                              human's screen
```

- **Webhooks carry the business data** — "the payment succeeded."
- **SignalR carries live updates for human eyes** — "attempt 2 got a 500," "duplicate skipped."
- Close the dashboard and the webhooks keep working exactly the same. SignalR is for *watching*; it is never part of delivery.

### Roles

| This project | Real world | Role |
|---|---|---|
| **Provider** | Stripe | Knows first that something happened and *notifies* others |
| **Receiver** | Your store's backend | Gets notified and *reacts* (marks order paid, ships it) |
| **Dashboard** | Admin screen / browser | A human watching what's happening |

Memory aid: the **Provider provides** the news; the **Receiver receives** it.

### Glossary

| Term | Meaning |
|---|---|
| **Polling** | Repeatedly asking the same question on a timer ("paid yet? paid yet?") whether or not anything changed. |
| **Webhook** | A plain HTTP POST from one server to another, sent *because an event happened*, to a URL the receiver registered in advance. Also called a "reverse API" or "HTTP callback." |
| **WebSocket** | A long-lived, two-way connection that stays open. A different protocol from ordinary request/response HTTP. |
| **SignalR** | A .NET library that gives you a persistent server↔browser connection. Uses WebSockets when possible, falls back to Server-Sent Events or long polling. Adds reconnection and groups. |
| **At-least-once delivery** | The guarantee webhooks give: an event will arrive *one or more* times — never "exactly once." |
| **Retry / backoff** | When a delivery fails, the provider tries again later, waiting longer each time (1s, 2s, 4s, …). |
| **Dead letter** | A delivery the provider gave up on after N failed attempts. |
| **Duplicate** | The same event arriving more than once (usually because of a retry). |
| **Idempotency** | Processing the same event twice has the same effect as processing it once. The receiver achieves this by remembering event IDs it has already handled. |
| **Out-of-order delivery** | Events can arrive in a different order than they happened (e.g. a refund before the payment). |
| **HMAC signature** | The provider signs each payload with a shared secret; the receiver recomputes the signature to prove the request is genuine and untampered. |
| **Outbox (sender side)** | Store each outgoing event/delivery in a database first; a background worker sends and retries from there. |
| **Inbox (receiver side)** | Store each incoming event first, reply `200` immediately, process it later in a background worker. |
| **Chaos (engineering)** | Deliberately injecting failures, under your control, to see how a system copes. Term popularized by Netflix's "Chaos Monkey." |
| **SQLite** | A SQL database engine with no server — a library inside your app; the whole database is a single file. |

### Why webhooks are hard (the reason this lab exists)

A webhook is just an HTTP call, so it inherits HTTP's problems: the receiver may be down, slow, or return an error. The sender can't be sure you processed it, so it **retries** → retries cause **duplicates** → duplicates require **idempotency**. Network timing also means events can arrive **out of order**.

---

## 2. Questions & Answers

### Q: Do I need two sources — an event source and a receiver?
Yes.
- **Event source (sender/provider):** when something happens (e.g. "order paid"), it sends an HTTP POST to a URL someone registered with it. Stripe, GitHub, and Shopify work this way.
- **Receiver:** your server exposes that URL, verifies the request is genuine, replies quickly with `2xx`, and does the real work separately.

### Q: I've seen SignalR on client projects — is that webhooks?
Not quite. SignalR keeps a **live connection** open between a server and browser clients to push messages in real time. Webhooks are **server-to-server**, one HTTP call per event, no persistent connection. They're often paired: a webhook arrives at your backend, then SignalR pushes the update to the browser. This project uses exactly that pattern.

### Q: What's the difference between SQL and SQLite?
**SQL** is the query language. **SQLite** is one specific database engine that you talk to using SQL.
- SQL Server / PostgreSQL / MySQL run as a **separate server process** you connect to (connection string, credentials, a running service).
- SQLite has **no server** — it's a library inside your app, and the entire database is one file (e.g. `receiver.db`).
- Benefits here: nothing to install; you can open the `.db` file in a viewer (DB Browser for SQLite, VS Code extension) and watch tables fill up; EF Core code is nearly identical to SQL Server — switching is mostly `UseSqlite` → `UseSqlServer`.
- Limits (single writer, not shared across machines) don't matter for a local lab.

### Q: Does "chaos" mean literal chaos?
No — it's from **chaos engineering**: deliberately injecting failures, on purpose and under your control, to see how the system copes. In this lab it means switches like "send this event twice" or "fail the next 3 requests." Nothing is random unless you ask for it.

### Q: Is the provider like Stripe and the receiver like the storefront?
Yes. Provider = Stripe (knows first, notifies). Receiver = your store's backend (gets notified, reacts). Dashboard = a human watching.

### Q: How are webhooks and SignalR separated in this structure?
Same idea — "push, don't poll" — on two different legs:
- **Webhook (server → server):** Stripe can't hold a connection open to every store on earth, so it makes one HTTP POST to your registered URL when something happens. No persistent connection. *Fire-and-retry*: if you're down, it tries again later.
- **SignalR (server → browser):** a browser has no public URL, so a server can't POST to it. Instead the browser opens a persistent connection to the server and the server pushes down it. If the browser isn't connected, the message is simply missed — no retries, because it's a live feed, not a delivery guarantee.

In this lab both Provider and Receiver host a SignalR hub **only so the dashboard can watch each side**. Provider and Receiver never talk to each other over SignalR.

### Q: Is SignalR a WebSocket? I thought you said SignalR isn't WebSockets.
The earlier statement was that SignalR isn't **webhooks** — easy to mix up because the words look alike.
- **Webhook** = an ordinary HTTP POST. One request, one response, done.
- **WebSocket** = a long-lived, two-way connection that stays open.
- **SignalR** = a .NET library providing a persistent connection. It **uses WebSockets when it can** and falls back to older techniques when it can't.

So SignalR ≈ a managed persistent WebSocket, open for as long as the browser tab is connected (not forever).

### Q: What's the benefit of a webhook over a standard API call between apps?
A webhook **is** a standard API call — technically just an HTTP POST with a JSON body. The name describes **who calls whom, and when**:
- **Normal API call:** *you* call *them* when *you* want something — "is payment 123 paid yet?"
- **Webhook:** *they* call *you* when *something happens* — you register a URL in advance, and they POST to it on the event.

The benefit is **no polling**. Without webhooks your store would ask "paid yet?" every few seconds for every open order — wasted calls, added delay, rate limits. With webhooks you hear about it once, right when it happens.

### Q: To confirm — webhooks are event-driven API calls (no WebSocket), and SignalR is a persistent server→browser connection?
Correct.
- **Webhook:** an ordinary HTTP API call triggered by an event — "something happened, and I'm configured to notify X." No WebSocket.
- **SignalR:** a persistent connection so that when something happens on the server, it's reflected in the browser immediately. Server → browser.

### Q: Why return 200 for a duplicate instead of an error?
The status code answers only one question for the provider: **"Did you get it?"** — not "Was it new?"

| Response | Provider's interpretation | What it does |
|---|---|---|
| `2xx` | "Got it" | Stops — delivery succeeded |
| anything else (`4xx`, `5xx`, timeout) | "Didn't get it" | Retries later |

For a duplicate, the honest answer is **yes, you already have it**. Returning an error (e.g. `409 Conflict`) backfires:
1. **The provider retries** → you get *more* duplicates of the same event.
2. **It eventually dead-letters** an event you actually processed fine → false alarms.
3. **Real providers punish repeated failures** — Stripe emails you and can **disable your endpoint**, so you'd miss real events.

Duplicates often happen *because* your first `200` got lost (you processed it but responded after the provider's timeout). The retry is really asking "did you get this?" — answering "yes" closes the loop.

**Rule of thumb for receivers:**
- `2xx` → received (new *or* duplicate). Record duplicates in **your own** logs/dashboard, not in the response.
- `401` → bad signature. A legitimate rejection; it's not from the provider.
- `5xx` / timeout → a genuine temporary problem on your side (e.g. database down) — here you *want* a retry.

---

## 3. Project Decisions (so far)

| Decision | Choice | Why |
|---|---|---|
| Backend stack | C# / ASP.NET Core (.NET 10) | Matches day-to-day client work |
| Dashboard | Angular | Visualize and drive both sides; Angular practice |
| Domain | Payments / orders | Duplicate processing = a visible double charge |
| Receiver storage | SQLite via EF Core | Unique constraint makes dedupe safe; state survives restarts |
| Delivery engine | Outbox + dispatcher loop | Every attempt is a visible row; retries survive restarts; replay is trivial |
| Concepts covered | HMAC signatures, retries + backoff + dead letter, duplicates/idempotency, out-of-order, inbox pattern, manual replay | All six |
| Dashboard layout | Provider panel \| live sequence timeline \| Receiver panel | Retries, timeouts, and duplicates visible as arrows as they happen |
| Scenario presets | 9 one-click scenarios incl. "Idempotency off" | Replay a lesson without manual setup; see the double-charge bug |
| Duplicate response | `200`, not an error | The provider only asks "did you get it?" — errors cause more retries |

Full design: [`docs/superpowers/specs/2026-09-28-webhook-lab-design.md`](superpowers/specs/2026-09-28-webhook-lab-design.md)
