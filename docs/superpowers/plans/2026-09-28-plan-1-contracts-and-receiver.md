# Webhook Lab — Plan 1: Contracts + Receiver Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the shared webhook contracts and the Receiver service: verify signed webhooks, store them in an inbox with idempotent dedupe, process them into an order ledger in a background worker, and expose fault injection, query, and reset APIs, plus a SignalR hub.

**Architecture:** `WebhookLab.Contracts` holds the event envelope, HMAC signature helper, and backoff math shared by both services. `WebhookLab.Receiver` is an ASP.NET Core minimal-API app. `POST /webhooks` only verifies, stores, and acknowledges. `InboxProcessor` applies events to the ledger inside a transaction. SQLite (EF Core) stores state, and the `EventId` primary key is the idempotency guarantee.

**Tech Stack:** .NET 10, ASP.NET Core minimal APIs, EF Core + SQLite, SignalR, xUnit, `WebApplicationFactory`, `FakeTimeProvider`.

**Spec:** `docs/superpowers/specs/2026-09-28-webhook-lab-design.md` (sections 4, 6, 7, 9, 10, 11). This is plan 1 of 3; Plan 2 = Provider, Plan 3 = Angular dashboard.

## Global Constraints

- Target framework: `net10.0` for every project. SDK pinned via `global.json`.
- Receiver URL: `http://localhost:5002`. Dashboard origin allowed by CORS: `http://localhost:4200`.
- Signature headers: `webhook-id`, `webhook-timestamp`, `webhook-signature`; value `v1,{base64(HMAC-SHA256(secret, "{id}.{timestamp}.{body}"))}`.
- Signature tolerance: 5 minutes. Max processing attempts: 5. Backoff: `2^attempts s + jitter(0–500 ms)`. Poll interval: 1 s. Slow-fault response delay: 6 s.
- Duplicates return `200`. Bad signature returns `401`. Database failure on store returns `503`. `FailNext` fault returns `500`.
- Enums are stored as strings in SQLite and serialized as strings in JSON and SignalR (readable in a DB viewer and on the dashboard).
- Store timestamps as UTC `DateTime` in entities (EF Core SQLite cannot compare/order `DateTimeOffset`).
- Dev secret (`appsettings.Development.json`): `dev-secret-change-me`.
- **Git:** the repository is not initialized yet. Run the commit steps only if `git status` works; otherwise skip them.
- All commands run from the repository root: `/Users/audreyalcantara/Documents/projects/Webhook-receiver`.

## File Map

```
global.json                                          SDK pin
.gitignore                                           dotnet + *.db + .superpowers/
WebhookLab.slnx                                      solution (.NET 10 default format)
scripts/send-webhook.sh                              sign + send a webhook by hand with curl

src/WebhookLab.Contracts/
  EventTypes.cs            event type name constants
  WebhookEnvelope.cs       envelope + OrderEventData records
  WebhookJson.cs           shared JsonSerializerOptions
  WebhookHeaders.cs        header name constants
  WebhookSignature.cs      Sign / Verify
  Backoff.cs               retry delay math + MaxAttempts

src/WebhookLab.Receiver/
  Program.cs                           composition root
  appsettings.json / appsettings.Development.json
  Properties/launchSettings.json
  Data/InboxEvent.cs, InboxStatus.cs   inbox entity
  Data/LedgerOrder.cs, LedgerStatus.cs ledger entity
  Data/ReceiverDbContext.cs
  Faults/FaultSettings.cs              one-shot fault counters + DedupeOff
  Api/QueryEndpoints.cs                GET /api/inbox, /api/ledger
  Api/LabControlEndpoints.cs           GET/PUT /api/faults, POST /api/reset
  Webhooks/WebhookOptions.cs           Webhook:Secret
  Webhooks/InboxStore.cs               insert-or-detect-duplicate
  Webhooks/WebhookEndpoint.cs          POST /webhooks
  Realtime/ReceiverHub.cs, ReceiverMessages.cs, ReceiverNotifier.cs
  Processing/IWebhookEventHandler.cs   handler contract + HandlerResult
  Processing/OrderCreatedHandler.cs, PaymentSucceededHandler.cs, PaymentRefundedHandler.cs
  Processing/InboxProcessor.cs         process due events in a transaction
  Processing/InboxProcessorWorker.cs   BackgroundService loop

tests/WebhookLab.Contracts.Tests/
  WebhookSignatureTests.cs, BackoffTests.cs
tests/WebhookLab.Receiver.Tests/
  ReceiverFactory.cs, TestEvents.cs, TestDb.cs
  QueryEndpointsTests.cs, LabControlTests.cs, WebhookEndpointTests.cs,
  LedgerHandlerTests.cs, InboxProcessorTests.cs, LessonTests.cs
```

---

### Task 1: Solution scaffold + Contracts

**Files:**
- Create: `global.json`, `.gitignore`, `WebhookLab.slnx`
- Create: `src/WebhookLab.Contracts/{EventTypes,WebhookEnvelope,WebhookJson,WebhookHeaders,WebhookSignature,Backoff}.cs`
- Test: `tests/WebhookLab.Contracts.Tests/{WebhookSignatureTests,BackoffTests}.cs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `EventTypes.OrderCreated|PaymentSucceeded|PaymentRefunded` (`const string`)
  - `record WebhookEnvelope(string Id, string Type, DateTimeOffset CreatedAt, OrderEventData Data)`
  - `record OrderEventData(int OrderId, decimal Amount)`
  - `WebhookJson.Options` (`JsonSerializerOptions`, web defaults / camelCase)
  - `WebhookHeaders.Id|Timestamp|Signature` (`const string`)
  - `WebhookSignature.Sign(string secret, string webhookId, long timestamp, string body) : string`
  - `WebhookSignature.Verify(string secret, string webhookId, string timestamp, string body, string signature, DateTimeOffset now) : bool`
  - `WebhookSignature.Tolerance` (`TimeSpan`, 5 min)
  - `Backoff.MaxAttempts` (`const int` 5), `Backoff.Delay(int attemptCount, Random random) : TimeSpan`

- [ ] **Step 1: Install the .NET 10 SDK**

Only .NET 9 is currently installed. Install .NET 10:

```bash
brew install --cask dotnet-sdk
dotnet --list-sdks
```

Expected: a `10.0.x` line is listed. (If Homebrew isn't used, install from https://dotnet.microsoft.com/download/dotnet/10.0.)

- [ ] **Step 2: Create the solution and projects**

```bash
dotnet new globaljson --sdk-version 10.0.100 --roll-forward latestFeature
dotnet new gitignore
printf '\n# Webhook Lab\n*.db\n*.db-shm\n*.db-wal\n.superpowers/\n' >> .gitignore
dotnet new sln -n WebhookLab
dotnet new classlib -n WebhookLab.Contracts -o src/WebhookLab.Contracts -f net10.0
dotnet new xunit -n WebhookLab.Contracts.Tests -o tests/WebhookLab.Contracts.Tests -f net10.0
rm src/WebhookLab.Contracts/Class1.cs tests/WebhookLab.Contracts.Tests/UnitTest1.cs
dotnet add tests/WebhookLab.Contracts.Tests reference src/WebhookLab.Contracts
dotnet sln add src/WebhookLab.Contracts tests/WebhookLab.Contracts.Tests
```

Expected: `WebhookLab.slnx` exists; `dotnet build` succeeds.

- [ ] **Step 3: Write the contract types**

`src/WebhookLab.Contracts/EventTypes.cs`:
```csharp
namespace WebhookLab.Contracts;

/// <summary>The webhook event types the Provider sends.</summary>
public static class EventTypes
{
    public const string OrderCreated = "order.created";
    public const string PaymentSucceeded = "payment.succeeded";
    public const string PaymentRefunded = "payment.refunded";
}
```

`src/WebhookLab.Contracts/WebhookEnvelope.cs`:
```csharp
namespace WebhookLab.Contracts;

/// <summary>
/// The JSON body of every webhook. <see cref="Id"/> is the idempotency key:
/// retries and replays of the same event reuse it.
/// </summary>
public sealed record WebhookEnvelope(string Id, string Type, DateTimeOffset CreatedAt, OrderEventData Data);

public sealed record OrderEventData(int OrderId, decimal Amount);
```

`src/WebhookLab.Contracts/WebhookJson.cs`:
```csharp
using System.Text.Json;

namespace WebhookLab.Contracts;

public static class WebhookJson
{
    /// <summary>camelCase JSON, matching what the Provider sends.</summary>
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);
}
```

`src/WebhookLab.Contracts/WebhookHeaders.cs`:
```csharp
namespace WebhookLab.Contracts;

/// <summary>Header names from the Standard Webhooks convention (standardwebhooks.com).</summary>
public static class WebhookHeaders
{
    public const string Id = "webhook-id";
    public const string Timestamp = "webhook-timestamp";
    public const string Signature = "webhook-signature";
}
```

- [ ] **Step 4: Write the failing signature tests**

`tests/WebhookLab.Contracts.Tests/WebhookSignatureTests.cs`:
```csharp
using WebhookLab.Contracts;

namespace WebhookLab.Contracts.Tests;

public class WebhookSignatureTests
{
    private const string Secret = "whsec_test";
    private const string Id = "evt_1";
    private const long Timestamp = 1_790_000_000;
    private const string Body = """{"a":1}""";
    private static readonly DateTimeOffset SignedAt = DateTimeOffset.FromUnixTimeSeconds(Timestamp);

    [Fact]
    public void Sign_matches_known_vector()
    {
        // Computed independently with: printf '%s' 'evt_1.1790000000.{"a":1}' | openssl dgst -sha256 -hmac whsec_test -binary | base64
        Assert.Equal("v1,wn9RxYh879SGYsmO/Vaiv4CLIqfB0qkd6S5loWxUSmM=", WebhookSignature.Sign(Secret, Id, Timestamp, Body));
    }

    [Fact]
    public void Verify_accepts_a_valid_signature()
    {
        var signature = WebhookSignature.Sign(Secret, Id, Timestamp, Body);

        Assert.True(WebhookSignature.Verify(Secret, Id, Timestamp.ToString(), Body, signature, SignedAt));
    }

    [Fact]
    public void Verify_rejects_a_tampered_body()
    {
        var signature = WebhookSignature.Sign(Secret, Id, Timestamp, Body);

        Assert.False(WebhookSignature.Verify(Secret, Id, Timestamp.ToString(), """{"a":2}""", signature, SignedAt));
    }

    [Fact]
    public void Verify_rejects_the_wrong_secret()
    {
        var signature = WebhookSignature.Sign("some-other-secret", Id, Timestamp, Body);

        Assert.False(WebhookSignature.Verify(Secret, Id, Timestamp.ToString(), Body, signature, SignedAt));
    }

    [Fact]
    public void Verify_rejects_a_timestamp_older_than_the_tolerance()
    {
        var signature = WebhookSignature.Sign(Secret, Id, Timestamp, Body);
        var now = SignedAt + WebhookSignature.Tolerance + TimeSpan.FromSeconds(1);

        Assert.False(WebhookSignature.Verify(Secret, Id, Timestamp.ToString(), Body, signature, now));
    }

    [Fact]
    public void Verify_rejects_a_timestamp_too_far_in_the_future()
    {
        var signature = WebhookSignature.Sign(Secret, Id, Timestamp, Body);
        var now = SignedAt - WebhookSignature.Tolerance - TimeSpan.FromSeconds(1);

        Assert.False(WebhookSignature.Verify(Secret, Id, Timestamp.ToString(), Body, signature, now));
    }

    [Theory]
    [InlineData("")]
    [InlineData("not-a-number")]
    public void Verify_rejects_a_malformed_timestamp(string timestamp)
    {
        var signature = WebhookSignature.Sign(Secret, Id, Timestamp, Body);

        Assert.False(WebhookSignature.Verify(Secret, Id, timestamp, Body, signature, SignedAt));
    }
}
```

- [ ] **Step 5: Run tests to verify they fail**

Run: `dotnet test tests/WebhookLab.Contracts.Tests`
Expected: build FAILS with `The name 'WebhookSignature' does not exist in the current context`.

- [ ] **Step 6: Implement `WebhookSignature`**

`src/WebhookLab.Contracts/WebhookSignature.cs`:
```csharp
using System.Security.Cryptography;
using System.Text;

namespace WebhookLab.Contracts;

/// <summary>
/// HMAC-SHA256 webhook signatures. The sender signs "{id}.{timestamp}.{body}" with a shared
/// secret; the receiver recomputes it to prove the request is genuine and unmodified.
/// Including the timestamp lets the receiver reject old requests replayed by an attacker.
/// </summary>
public static class WebhookSignature
{
    public static readonly TimeSpan Tolerance = TimeSpan.FromMinutes(5);

    public static string Sign(string secret, string webhookId, long timestamp, string body)
    {
        var payload = Encoding.UTF8.GetBytes($"{webhookId}.{timestamp}.{body}");
        var hash = HMACSHA256.HashData(Encoding.UTF8.GetBytes(secret), payload);
        return "v1," + Convert.ToBase64String(hash);
    }

    public static bool Verify(string secret, string webhookId, string timestamp, string body, string signature, DateTimeOffset now)
    {
        if (!long.TryParse(timestamp, out var sentAtSeconds))
            return false;

        var nowSeconds = now.ToUnixTimeSeconds();
        var toleranceSeconds = (long)Tolerance.TotalSeconds;
        if (sentAtSeconds < nowSeconds - toleranceSeconds || sentAtSeconds > nowSeconds + toleranceSeconds)
            return false;

        var expected = Encoding.UTF8.GetBytes(Sign(secret, webhookId, sentAtSeconds, body));
        // Constant-time comparison: doesn't leak how many leading characters matched.
        return CryptographicOperations.FixedTimeEquals(expected, Encoding.UTF8.GetBytes(signature));
    }
}
```

- [ ] **Step 7: Write the failing backoff tests**

`tests/WebhookLab.Contracts.Tests/BackoffTests.cs`:
```csharp
using WebhookLab.Contracts;

namespace WebhookLab.Contracts.Tests;

public class BackoffTests
{
    [Theory]
    [InlineData(1, 2)]
    [InlineData(2, 4)]
    [InlineData(3, 8)]
    [InlineData(4, 16)]
    public void Delay_doubles_with_each_attempt(int attemptCount, int expectedSeconds)
    {
        Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), Backoff.Delay(attemptCount, new FixedRandom(0)));
    }

    [Fact]
    public void Delay_adds_at_most_half_a_second_of_jitter()
    {
        var delay = Backoff.Delay(1, new FixedRandom(0.999));

        Assert.InRange(delay, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(2.5));
    }

    private sealed class FixedRandom(double value) : Random
    {
        public override double NextDouble() => value;
    }
}
```

- [ ] **Step 8: Run tests to verify they fail**

Run: `dotnet test tests/WebhookLab.Contracts.Tests`
Expected: build FAILS with `The name 'Backoff' does not exist in the current context`.

- [ ] **Step 9: Implement `Backoff`**

`src/WebhookLab.Contracts/Backoff.cs`:
```csharp
namespace WebhookLab.Contracts;

/// <summary>
/// Exponential backoff shared by the Provider's delivery retries and the Receiver's deferred events.
/// Jitter spreads retries out so many failed deliveries don't all retry at the same instant.
/// </summary>
public static class Backoff
{
    public const int MaxAttempts = 5;

    private static readonly TimeSpan MaxJitter = TimeSpan.FromMilliseconds(500);

    /// <param name="attemptCount">Attempts made so far (1 after the first failure → ~2 s).</param>
    public static TimeSpan Delay(int attemptCount, Random random) =>
        TimeSpan.FromSeconds(Math.Pow(2, attemptCount)) + MaxJitter * random.NextDouble();
}
```

- [ ] **Step 10: Run tests to verify they pass**

Run: `dotnet test tests/WebhookLab.Contracts.Tests`
Expected: PASS, 13 tests.

- [ ] **Step 11: Commit**

```bash
git add global.json .gitignore WebhookLab.slnx src/WebhookLab.Contracts tests/WebhookLab.Contracts.Tests
git commit -m "feat: add webhook contracts, HMAC signature, and backoff"
```

---

### Task 2: Receiver skeleton — data model, query API, test factory

**Files:**
- Create: `src/WebhookLab.Receiver/` (web project), `Program.cs`, `appsettings.json`, `appsettings.Development.json`, `Properties/launchSettings.json`
- Create: `src/WebhookLab.Receiver/Data/{InboxStatus,InboxEvent,LedgerStatus,LedgerOrder,ReceiverDbContext}.cs`
- Create: `src/WebhookLab.Receiver/Api/QueryEndpoints.cs`
- Test: `tests/WebhookLab.Receiver.Tests/{ReceiverFactory,QueryEndpointsTests}.cs`

**Interfaces:**
- Consumes: `WebhookLab.Contracts` (project reference only in this task).
- Produces:
  - `enum InboxStatus { Received, Processed, Deferred, Failed }`
  - `class InboxEvent { string EventId; string Type; string Payload; DateTime ReceivedAt; InboxStatus Status; int ProcessingAttempts; DateTime NextAttemptAt; string? LastError }`
  - `enum LedgerStatus { Created, Paid, Refunded }`
  - `class LedgerOrder { int OrderId; decimal Amount; LedgerStatus Status; int ChargeCount; int RefundCount }`
  - `ReceiverDbContext` with `InboxEvents`, `LedgerOrders`
  - `QueryEndpoints.MapQueryEndpoints(this IEndpointRouteBuilder)`
  - Test: `ReceiverFactory` (`WebApplicationFactory<Program>`) with `Time` (`FakeTimeProvider`), `Secret` const, `WithDbAsync<T>(Func<ReceiverDbContext, Task<T>>)`, `GetInboxEventAsync(string)`, `GetLedgerOrderAsync(int)`

- [ ] **Step 1: Create the projects**

```bash
dotnet new web -n WebhookLab.Receiver -o src/WebhookLab.Receiver -f net10.0
dotnet new xunit -n WebhookLab.Receiver.Tests -o tests/WebhookLab.Receiver.Tests -f net10.0
rm tests/WebhookLab.Receiver.Tests/UnitTest1.cs
dotnet add src/WebhookLab.Receiver reference src/WebhookLab.Contracts
dotnet add src/WebhookLab.Receiver package Microsoft.EntityFrameworkCore.Sqlite
dotnet add tests/WebhookLab.Receiver.Tests reference src/WebhookLab.Receiver
dotnet add tests/WebhookLab.Receiver.Tests package Microsoft.AspNetCore.Mvc.Testing
dotnet add tests/WebhookLab.Receiver.Tests package Microsoft.Extensions.TimeProvider.Testing
dotnet sln add src/WebhookLab.Receiver tests/WebhookLab.Receiver.Tests
```

- [ ] **Step 2: Configuration files**

`src/WebhookLab.Receiver/Properties/launchSettings.json` (replace the generated file):
```json
{
  "$schema": "https://json.schemastore.org/launchsettings.json",
  "profiles": {
    "http": {
      "commandName": "Project",
      "dotnetRunMessages": true,
      "launchBrowser": false,
      "applicationUrl": "http://localhost:5002",
      "environmentVariables": { "ASPNETCORE_ENVIRONMENT": "Development" }
    }
  }
}
```

`src/WebhookLab.Receiver/appsettings.json` (replace):
```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.AspNetCore": "Warning",
      "Microsoft.EntityFrameworkCore": "Warning"
    }
  },
  "AllowedHosts": "*",
  "ConnectionStrings": {
    "Receiver": "Data Source=receiver.db"
  },
  "Receiver": {
    "EnableBackgroundProcessing": true,
    "SlowResponseDelay": "00:00:06"
  },
  "Dashboard": {
    "Origin": "http://localhost:4200"
  }
}
```

`src/WebhookLab.Receiver/appsettings.Development.json` (replace):
```json
{
  "Webhook": {
    "Secret": "dev-secret-change-me"
  }
}
```

- [ ] **Step 3: Data model**

`src/WebhookLab.Receiver/Data/InboxStatus.cs`:
```csharp
namespace WebhookLab.Receiver.Data;

public enum InboxStatus
{
    /// <summary>Stored and acknowledged; waiting for the processor.</summary>
    Received,
    /// <summary>Applied to the ledger.</summary>
    Processed,
    /// <summary>Arrived before an event it depends on; will retry with backoff.</summary>
    Deferred,
    /// <summary>Gave up. Visible on the dashboard, never silently dropped.</summary>
    Failed,
}
```

`src/WebhookLab.Receiver/Data/InboxEvent.cs`:
```csharp
namespace WebhookLab.Receiver.Data;

/// <summary>One row per unique webhook event. EventId is the primary key, which is what makes dedupe safe.</summary>
public sealed class InboxEvent
{
    public required string EventId { get; init; }
    public required string Type { get; init; }
    public required string Payload { get; init; }
    public DateTime ReceivedAt { get; init; }
    public InboxStatus Status { get; set; } = InboxStatus.Received;
    public int ProcessingAttempts { get; set; }
    public DateTime NextAttemptAt { get; set; }
    public string? LastError { get; set; }
}
```

`src/WebhookLab.Receiver/Data/LedgerStatus.cs`:
```csharp
namespace WebhookLab.Receiver.Data;

public enum LedgerStatus
{
    Created,
    Paid,
    Refunded,
}
```

`src/WebhookLab.Receiver/Data/LedgerOrder.cs`:
```csharp
namespace WebhookLab.Receiver.Data;

/// <summary>
/// The store's view of an order. ChargeCount/RefundCount exist so you can *see* whether
/// a payment was applied once (correct) or twice (the bug idempotency prevents).
/// </summary>
public sealed class LedgerOrder
{
    public int OrderId { get; init; }
    public decimal Amount { get; init; }
    public LedgerStatus Status { get; set; } = LedgerStatus.Created;
    public int ChargeCount { get; set; }
    public int RefundCount { get; set; }
}
```

`src/WebhookLab.Receiver/Data/ReceiverDbContext.cs`:
```csharp
using Microsoft.EntityFrameworkCore;

namespace WebhookLab.Receiver.Data;

public sealed class ReceiverDbContext(DbContextOptions<ReceiverDbContext> options) : DbContext(options)
{
    public DbSet<InboxEvent> InboxEvents => Set<InboxEvent>();
    public DbSet<LedgerOrder> LedgerOrders => Set<LedgerOrder>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<InboxEvent>(inbox =>
        {
            // The idempotency guarantee: inserting the same event id twice fails at the database.
            inbox.HasKey(e => e.EventId);
            inbox.Property(e => e.Status).HasConversion<string>();
            inbox.HasIndex(e => new { e.Status, e.NextAttemptAt });
        });

        modelBuilder.Entity<LedgerOrder>(order =>
        {
            order.HasKey(o => o.OrderId);
            order.Property(o => o.OrderId).ValueGeneratedNever();
            order.Property(o => o.Status).HasConversion<string>();
        });
    }
}
```

- [ ] **Step 4: Query endpoints**

`src/WebhookLab.Receiver/Api/QueryEndpoints.cs`:
```csharp
using Microsoft.EntityFrameworkCore;
using WebhookLab.Receiver.Data;

namespace WebhookLab.Receiver.Api;

public static class QueryEndpoints
{
    public static IEndpointRouteBuilder MapQueryEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/api/inbox", async (ReceiverDbContext db) =>
            await db.InboxEvents.AsNoTracking().OrderByDescending(e => e.ReceivedAt).ToListAsync());

        app.MapGet("/api/ledger", async (ReceiverDbContext db) =>
            await db.LedgerOrders.AsNoTracking().OrderBy(o => o.OrderId).ToListAsync());

        return app;
    }
}
```

- [ ] **Step 5: `Program.cs`**

`src/WebhookLab.Receiver/Program.cs` (replace):
```csharp
using System.Text.Json.Serialization;
using Microsoft.EntityFrameworkCore;
using WebhookLab.Receiver.Api;
using WebhookLab.Receiver.Data;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<ReceiverDbContext>(options =>
    options.UseSqlite(builder.Configuration.GetConnectionString("Receiver")));
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.ConfigureHttpJsonOptions(options =>
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));
builder.Services.AddCors(options => options.AddDefaultPolicy(policy => policy
    .WithOrigins(builder.Configuration["Dashboard:Origin"] ?? "http://localhost:4200")
    .AllowAnyHeader()
    .AllowAnyMethod()
    .AllowCredentials()));

var app = builder.Build();

// A lab doesn't need migrations: create the schema if the database file is new.
using (var scope = app.Services.CreateScope())
    scope.ServiceProvider.GetRequiredService<ReceiverDbContext>().Database.EnsureCreated();

app.UseCors();
app.MapQueryEndpoints();

app.Run();

public partial class Program;
```

- [ ] **Step 6: Test factory**

`tests/WebhookLab.Receiver.Tests/ReceiverFactory.cs`:
```csharp
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Time.Testing;
using WebhookLab.Receiver.Data;

namespace WebhookLab.Receiver.Tests;

/// <summary>
/// Runs the real Receiver in memory against its own SQLite file. The background worker is off,
/// so tests call the processor explicitly, and the clock is fake, so backoff is instant.
/// </summary>
public sealed class ReceiverFactory : WebApplicationFactory<Program>
{
    public const string Secret = "test-secret";

    private readonly string _databasePath = Path.Combine(Path.GetTempPath(), $"receiver-test-{Guid.NewGuid():N}.db");

    public FakeTimeProvider Time { get; } = new(new DateTimeOffset(2026, 9, 28, 10, 0, 0, TimeSpan.Zero));

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Testing");
        builder.UseSetting("ConnectionStrings:Receiver", $"Data Source={_databasePath}");
        builder.UseSetting("Webhook:Secret", Secret);
        builder.UseSetting("Receiver:EnableBackgroundProcessing", "false");
        builder.UseSetting("Receiver:SlowResponseDelay", "00:00:00.200");
        builder.ConfigureTestServices(services =>
        {
            services.RemoveAll<TimeProvider>();
            services.AddSingleton<TimeProvider>(Time);
        });
    }

    public async Task<T> WithDbAsync<T>(Func<ReceiverDbContext, Task<T>> query)
    {
        using var scope = Services.CreateScope();
        return await query(scope.ServiceProvider.GetRequiredService<ReceiverDbContext>());
    }

    public Task<InboxEvent?> GetInboxEventAsync(string eventId) =>
        WithDbAsync(db => db.InboxEvents.AsNoTracking().SingleOrDefaultAsync(e => e.EventId == eventId));

    public Task<LedgerOrder?> GetLedgerOrderAsync(int orderId) =>
        WithDbAsync(db => db.LedgerOrders.AsNoTracking().SingleOrDefaultAsync(o => o.OrderId == orderId));

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        SqliteConnection.ClearAllPools();
        File.Delete(_databasePath);
    }
}
```

- [ ] **Step 7: Write the test**

`tests/WebhookLab.Receiver.Tests/QueryEndpointsTests.cs`:
```csharp
using System.Net.Http.Json;
using System.Text.Json;

namespace WebhookLab.Receiver.Tests;

public class QueryEndpointsTests
{
    [Theory]
    [InlineData("/api/inbox")]
    [InlineData("/api/ledger")]
    public async Task A_fresh_receiver_has_no_data(string path)
    {
        await using var lab = new ReceiverFactory();

        var items = await lab.CreateClient().GetFromJsonAsync<List<JsonElement>>(path);

        Assert.NotNull(items);
        Assert.Empty(items);
    }
}
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `dotnet test`
Expected: PASS (Contracts 13 + Receiver 2). If `EnsureCreated` or the factory is wrong, the query tests fail with a SQLite "no such table" error. That is the failure this step guards against.

- [ ] **Step 9: Commit**

```bash
git add src/WebhookLab.Receiver tests/WebhookLab.Receiver.Tests WebhookLab.slnx
git commit -m "feat: add receiver skeleton with inbox and ledger data model"
```

---

### Task 3: Fault settings + lab control API

**Files:**
- Create: `src/WebhookLab.Receiver/Faults/FaultSettings.cs`
- Create: `src/WebhookLab.Receiver/Api/LabControlEndpoints.cs`
- Modify: `src/WebhookLab.Receiver/Program.cs`
- Test: `tests/WebhookLab.Receiver.Tests/LabControlTests.cs`

**Interfaces:**
- Consumes: `ReceiverDbContext` (Task 2).
- Produces:
  - `record FaultSnapshot(int FailNext, int SlowNext, int CrashNext, bool DedupeOff)`
  - `FaultSettings` singleton: `Snapshot() : FaultSnapshot`, `Apply(FaultSnapshot)`, `Reset()`, `TryConsumeFail() : bool`, `TryConsumeSlow() : bool`, `TryConsumeCrash() : bool`, `bool DedupeOff { get; }`, `TimeSpan SlowResponseDelay { get; init; }`
  - `LabControlEndpoints.MapLabControlEndpoints(this IEndpointRouteBuilder)`: `GET /api/faults`, `PUT /api/faults`, `POST /api/reset`
  - Test: `ReceiverFactory.Faults` property

- [ ] **Step 1: Write the failing tests**

`tests/WebhookLab.Receiver.Tests/LabControlTests.cs`:
```csharp
using System.Net;
using System.Net.Http.Json;
using WebhookLab.Receiver.Data;
using WebhookLab.Receiver.Faults;

namespace WebhookLab.Receiver.Tests;

public class LabControlTests
{
    [Fact]
    public void One_shot_fault_counts_down_to_zero()
    {
        var faults = new FaultSettings();
        faults.Apply(new FaultSnapshot(FailNext: 2, SlowNext: 0, CrashNext: 0, DedupeOff: false));

        Assert.True(faults.TryConsumeFail());
        Assert.True(faults.TryConsumeFail());
        Assert.False(faults.TryConsumeFail());
    }

    [Fact]
    public async Task Faults_start_cleared()
    {
        await using var lab = new ReceiverFactory();

        var faults = await lab.CreateClient().GetFromJsonAsync<FaultSnapshot>("/api/faults");

        Assert.Equal(new FaultSnapshot(0, 0, 0, false), faults);
    }

    [Fact]
    public async Task Put_faults_updates_the_settings()
    {
        await using var lab = new ReceiverFactory();
        var client = lab.CreateClient();
        var update = new FaultSnapshot(FailNext: 2, SlowNext: 1, CrashNext: 3, DedupeOff: true);

        var response = await client.PutAsJsonAsync("/api/faults", update);

        response.EnsureSuccessStatusCode();
        Assert.Equal(update, await client.GetFromJsonAsync<FaultSnapshot>("/api/faults"));
    }

    [Fact]
    public async Task Reset_clears_data_and_faults()
    {
        await using var lab = new ReceiverFactory();
        var client = lab.CreateClient();
        await lab.WithDbAsync(db =>
        {
            db.InboxEvents.Add(new InboxEvent { EventId = "evt_1", Type = "order.created", Payload = "{}" });
            db.LedgerOrders.Add(new LedgerOrder { OrderId = 1, Amount = 50m });
            return db.SaveChangesAsync();
        });
        lab.Faults.Apply(new FaultSnapshot(1, 1, 1, true));

        var response = await client.PostAsync("/api/reset", null);

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
        Assert.Null(await lab.GetInboxEventAsync("evt_1"));
        Assert.Null(await lab.GetLedgerOrderAsync(1));
        Assert.Equal(new FaultSnapshot(0, 0, 0, false), lab.Faults.Snapshot());
    }
}
```

- [ ] **Step 2: Add the `Faults` helper to the factory**

In `tests/WebhookLab.Receiver.Tests/ReceiverFactory.cs`, add `using WebhookLab.Receiver.Faults;` and this member below the `Time` property:
```csharp
    public FaultSettings Faults => Services.GetRequiredService<FaultSettings>();
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `dotnet test tests/WebhookLab.Receiver.Tests`
Expected: build FAILS with `The type or namespace name 'Faults' does not exist in the namespace 'WebhookLab.Receiver'`.

- [ ] **Step 4: Implement `FaultSettings`**

`src/WebhookLab.Receiver/Faults/FaultSettings.cs`:
```csharp
namespace WebhookLab.Receiver.Faults;

public sealed record FaultSnapshot(int FailNext, int SlowNext, int CrashNext, bool DedupeOff);

/// <summary>
/// Receiving-side failures you can switch on from the dashboard. Counters are one-shot:
/// "FailNext = 3" fails the next three requests, then turns itself off.
/// Thread-safe because requests and the background processor use it concurrently.
/// </summary>
public sealed class FaultSettings
{
    private int _failNext;
    private int _slowNext;
    private int _crashNext;
    private volatile bool _dedupeOff;

    /// <summary>How late a "slow" response is. Longer than the Provider's 5 s timeout by default.</summary>
    public TimeSpan SlowResponseDelay { get; init; } = TimeSpan.FromSeconds(6);

    /// <summary>UNSAFE lab switch: duplicates are processed again instead of skipped.</summary>
    public bool DedupeOff => _dedupeOff;

    public FaultSnapshot Snapshot() =>
        new(Volatile.Read(ref _failNext), Volatile.Read(ref _slowNext), Volatile.Read(ref _crashNext), _dedupeOff);

    public void Apply(FaultSnapshot faults)
    {
        Volatile.Write(ref _failNext, faults.FailNext);
        Volatile.Write(ref _slowNext, faults.SlowNext);
        Volatile.Write(ref _crashNext, faults.CrashNext);
        _dedupeOff = faults.DedupeOff;
    }

    public void Reset() => Apply(new FaultSnapshot(0, 0, 0, false));

    public bool TryConsumeFail() => TryConsume(ref _failNext);
    public bool TryConsumeSlow() => TryConsume(ref _slowNext);
    public bool TryConsumeCrash() => TryConsume(ref _crashNext);

    private static bool TryConsume(ref int counter)
    {
        while (true)
        {
            var current = Volatile.Read(ref counter);
            if (current <= 0)
                return false;
            if (Interlocked.CompareExchange(ref counter, current - 1, current) == current)
                return true;
        }
    }
}
```

- [ ] **Step 5: Implement the lab control endpoints**

`src/WebhookLab.Receiver/Api/LabControlEndpoints.cs`:
```csharp
using Microsoft.EntityFrameworkCore;
using WebhookLab.Receiver.Data;
using WebhookLab.Receiver.Faults;

namespace WebhookLab.Receiver.Api;

public static class LabControlEndpoints
{
    public static IEndpointRouteBuilder MapLabControlEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/api/faults", (FaultSettings faults) => faults.Snapshot());

        app.MapPut("/api/faults", (FaultSnapshot update, FaultSettings faults) =>
        {
            faults.Apply(update);
            return faults.Snapshot();
        });

        app.MapPost("/api/reset", async (ReceiverDbContext db, FaultSettings faults) =>
        {
            await db.InboxEvents.ExecuteDeleteAsync();
            await db.LedgerOrders.ExecuteDeleteAsync();
            faults.Reset();
            return Results.NoContent();
        });

        return app;
    }
}
```

- [ ] **Step 6: Register in `Program.cs`**

Add `using WebhookLab.Receiver.Faults;` to the usings. After the `AddSingleton(TimeProvider.System);` line add:
```csharp
builder.Services.AddSingleton(new FaultSettings
{
    SlowResponseDelay = builder.Configuration.GetValue("Receiver:SlowResponseDelay", TimeSpan.FromSeconds(6)),
});
```
After `app.MapQueryEndpoints();` add:
```csharp
app.MapLabControlEndpoints();
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `dotnet test`
Expected: PASS (Receiver: 6 tests).

- [ ] **Step 8: Commit**

```bash
git add src/WebhookLab.Receiver tests/WebhookLab.Receiver.Tests
git commit -m "feat: add receiver fault injection and lab control API"
```

---

### Task 4: Webhook endpoint — verify, store, dedupe, acknowledge

**Files:**
- Create: `src/WebhookLab.Receiver/Webhooks/{WebhookOptions,InboxStore,WebhookEndpoint}.cs`
- Create: `src/WebhookLab.Receiver/Realtime/{ReceiverHub,ReceiverMessages,ReceiverNotifier}.cs`
- Modify: `src/WebhookLab.Receiver/Program.cs`
- Test: `tests/WebhookLab.Receiver.Tests/{TestEvents,WebhookEndpointTests}.cs`, modify `ReceiverFactory.cs`

**Interfaces:**
- Consumes: `WebhookSignature.Verify`, `WebhookEnvelope`, `WebhookJson.Options`, `WebhookHeaders` (Task 1); `ReceiverDbContext`, `InboxEvent`, `InboxStatus`, `LedgerOrder`, `LedgerStatus` (Task 2); `FaultSettings.TryConsumeFail/TryConsumeSlow/DedupeOff/SlowResponseDelay` (Task 3).
- Produces:
  - `enum StoreResult { Stored, Duplicate, DuplicateReprocessed }`
  - `InboxStore.StoreAsync(WebhookEnvelope envelope, string rawBody, bool reprocessDuplicates) : Task<StoreResult>`
  - `enum WebhookOutcome { Stored, Duplicate, DuplicateReprocessed, Rejected, FaultInjected }`
  - `ReceiverNotifier`: `WebhookReceivedAsync(string eventId, WebhookOutcome)`, `ProcessingCompletedAsync(string eventId, InboxStatus, string? error)`, `LedgerChangedAsync(string eventId, LedgerOrder)`
  - Hub at `/hubs/receiver`; messages `WebhookReceived`, `ProcessingCompleted`, `LedgerChanged`
  - Test: `TestEvents.Created/Paid/Refunded(int orderId) : WebhookEnvelope`; `ReceiverFactory.SendAsync(WebhookEnvelope, string? secret = null, DateTimeOffset? signedAt = null, string? headerId = null, CancellationToken ct = default) : Task<HttpResponseMessage>`

- [ ] **Step 1: Test helpers**

`tests/WebhookLab.Receiver.Tests/TestEvents.cs`:
```csharp
using WebhookLab.Contracts;

namespace WebhookLab.Receiver.Tests;

public static class TestEvents
{
    public static WebhookEnvelope Created(int orderId) => New(EventTypes.OrderCreated, orderId);
    public static WebhookEnvelope Paid(int orderId) => New(EventTypes.PaymentSucceeded, orderId);
    public static WebhookEnvelope Refunded(int orderId) => New(EventTypes.PaymentRefunded, orderId);

    public static WebhookEnvelope New(string type, int orderId) =>
        new($"evt_{Guid.NewGuid():N}", type, DateTimeOffset.UnixEpoch, new OrderEventData(orderId, 50m));
}
```

In `ReceiverFactory.cs`, add usings `System.Text`, `System.Text.Json`, `WebhookLab.Contracts`, and this method:
```csharp
    /// <summary>Sends a webhook signed the way the Provider signs it. Overrides let tests forge or break it.</summary>
    public Task<HttpResponseMessage> SendAsync(
        WebhookEnvelope envelope,
        string? secret = null,
        DateTimeOffset? signedAt = null,
        string? headerId = null,
        CancellationToken ct = default)
    {
        var body = JsonSerializer.Serialize(envelope, WebhookJson.Options);
        var id = headerId ?? envelope.Id;
        var timestamp = (signedAt ?? Time.GetUtcNow()).ToUnixTimeSeconds();

        var request = new HttpRequestMessage(HttpMethod.Post, "/webhooks")
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json"),
        };
        request.Headers.Add(WebhookHeaders.Id, id);
        request.Headers.Add(WebhookHeaders.Timestamp, timestamp.ToString());
        request.Headers.Add(WebhookHeaders.Signature, WebhookSignature.Sign(secret ?? Secret, id, timestamp, body));

        return CreateClient().SendAsync(request, ct);
    }
```

- [ ] **Step 2: Write the failing tests**

`tests/WebhookLab.Receiver.Tests/WebhookEndpointTests.cs`:
```csharp
using System.Diagnostics;
using System.Net;
using Microsoft.EntityFrameworkCore;
using WebhookLab.Receiver.Data;
using WebhookLab.Receiver.Faults;

namespace WebhookLab.Receiver.Tests;

public class WebhookEndpointTests
{
    [Fact]
    public async Task Valid_webhook_is_stored_as_received_and_acknowledged()
    {
        await using var lab = new ReceiverFactory();
        var created = TestEvents.Created(42);

        var response = await lab.SendAsync(created);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var stored = await lab.GetInboxEventAsync(created.Id);
        Assert.NotNull(stored);
        Assert.Equal(InboxStatus.Received, stored.Status);
        Assert.Equal(created.Type, stored.Type);
    }

    [Fact]
    public async Task Duplicate_webhook_is_acknowledged_and_stored_once()
    {
        await using var lab = new ReceiverFactory();
        var created = TestEvents.Created(42);
        await lab.SendAsync(created);

        var second = await lab.SendAsync(created);

        Assert.Equal(HttpStatusCode.OK, second.StatusCode);
        Assert.Equal(1, await lab.WithDbAsync(db => db.InboxEvents.CountAsync()));
    }

    [Fact]
    public async Task Wrong_secret_is_rejected_and_nothing_is_stored()
    {
        await using var lab = new ReceiverFactory();
        var created = TestEvents.Created(42);

        var response = await lab.SendAsync(created, secret: "attacker-guess");

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        Assert.Null(await lab.GetInboxEventAsync(created.Id));
    }

    [Fact]
    public async Task Old_timestamp_is_rejected()
    {
        await using var lab = new ReceiverFactory();

        var response = await lab.SendAsync(TestEvents.Created(42), signedAt: lab.Time.GetUtcNow().AddMinutes(-10));

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task Body_id_that_does_not_match_the_header_is_a_bad_request()
    {
        await using var lab = new ReceiverFactory();

        var response = await lab.SendAsync(TestEvents.Created(42), headerId: "evt_different");

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task FailNext_fault_returns_500_without_storing()
    {
        await using var lab = new ReceiverFactory();
        lab.Faults.Apply(new FaultSnapshot(FailNext: 1, SlowNext: 0, CrashNext: 0, DedupeOff: false));
        var created = TestEvents.Created(42);

        var response = await lab.SendAsync(created);

        Assert.Equal(HttpStatusCode.InternalServerError, response.StatusCode);
        Assert.Null(await lab.GetInboxEventAsync(created.Id));
    }

    [Fact]
    public async Task SlowNext_fault_stores_the_event_then_answers_late()
    {
        await using var lab = new ReceiverFactory();
        lab.Faults.Apply(new FaultSnapshot(FailNext: 0, SlowNext: 1, CrashNext: 0, DedupeOff: false));
        var created = TestEvents.Created(42);

        var stopwatch = Stopwatch.StartNew();
        var response = await lab.SendAsync(created);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.True(stopwatch.Elapsed >= lab.Faults.SlowResponseDelay);
        Assert.NotNull(await lab.GetInboxEventAsync(created.Id));
    }

    [Fact]
    public async Task DedupeOff_puts_a_duplicate_back_in_the_queue()
    {
        await using var lab = new ReceiverFactory();
        var created = TestEvents.Created(42);
        await lab.SendAsync(created);
        await lab.WithDbAsync(db => db.InboxEvents
            .Where(e => e.EventId == created.Id)
            .ExecuteUpdateAsync(s => s.SetProperty(e => e.Status, InboxStatus.Processed)));
        lab.Faults.Apply(new FaultSnapshot(FailNext: 0, SlowNext: 0, CrashNext: 0, DedupeOff: true));

        await lab.SendAsync(created);

        Assert.Equal(InboxStatus.Received, (await lab.GetInboxEventAsync(created.Id))!.Status);
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `dotnet test tests/WebhookLab.Receiver.Tests --filter WebhookEndpointTests`
Expected: FAIL. Each test gets `404 Not Found` (or the build fails if a helper is missing) because `/webhooks` doesn't exist yet.

- [ ] **Step 4: Options**

`src/WebhookLab.Receiver/Webhooks/WebhookOptions.cs`:
```csharp
namespace WebhookLab.Receiver.Webhooks;

public sealed class WebhookOptions
{
    public const string SectionName = "Webhook";

    /// <summary>Shared with the Provider. Used to verify each request's signature.</summary>
    public string Secret { get; set; } = "";
}
```

- [ ] **Step 5: Realtime notifier**

`src/WebhookLab.Receiver/Realtime/ReceiverHub.cs`:
```csharp
using Microsoft.AspNetCore.SignalR;

namespace WebhookLab.Receiver.Realtime;

/// <summary>Dashboard-only live feed. Webhook delivery never depends on it.</summary>
public sealed class ReceiverHub : Hub;
```

`src/WebhookLab.Receiver/Realtime/ReceiverMessages.cs`:
```csharp
using WebhookLab.Receiver.Data;

namespace WebhookLab.Receiver.Realtime;

public enum WebhookOutcome
{
    Stored,
    Duplicate,
    DuplicateReprocessed,
    Rejected,
    FaultInjected,
}

// Every message carries EventId + At so the dashboard can merge both services into one timeline.
public sealed record WebhookReceivedMessage(string EventId, DateTimeOffset At, WebhookOutcome Outcome);

public sealed record ProcessingCompletedMessage(string EventId, DateTimeOffset At, InboxStatus Status, string? Error);

public sealed record LedgerChangedMessage(
    string EventId, DateTimeOffset At, int OrderId, int ChargeCount, int RefundCount, LedgerStatus Status);
```

`src/WebhookLab.Receiver/Realtime/ReceiverNotifier.cs`:
```csharp
using Microsoft.AspNetCore.SignalR;
using WebhookLab.Receiver.Data;

namespace WebhookLab.Receiver.Realtime;

public sealed class ReceiverNotifier(IHubContext<ReceiverHub> hub, TimeProvider time)
{
    public Task WebhookReceivedAsync(string eventId, WebhookOutcome outcome) =>
        hub.Clients.All.SendAsync("WebhookReceived", new WebhookReceivedMessage(eventId, time.GetUtcNow(), outcome));

    public Task ProcessingCompletedAsync(string eventId, InboxStatus status, string? error) =>
        hub.Clients.All.SendAsync("ProcessingCompleted", new ProcessingCompletedMessage(eventId, time.GetUtcNow(), status, error));

    public Task LedgerChangedAsync(string eventId, LedgerOrder order) =>
        hub.Clients.All.SendAsync("LedgerChanged", new LedgerChangedMessage(
            eventId, time.GetUtcNow(), order.OrderId, order.ChargeCount, order.RefundCount, order.Status));
}
```

- [ ] **Step 6: Inbox store**

`src/WebhookLab.Receiver/Webhooks/InboxStore.cs`:
```csharp
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using WebhookLab.Contracts;
using WebhookLab.Receiver.Data;

namespace WebhookLab.Receiver.Webhooks;

public enum StoreResult
{
    Stored,
    Duplicate,
    DuplicateReprocessed,
}

/// <summary>
/// Inserts an incoming event, letting the database detect duplicates. Insert-and-catch is used
/// instead of check-then-insert: two copies arriving at the same instant can't both pass.
/// </summary>
public sealed class InboxStore(ReceiverDbContext db, TimeProvider time)
{
    private const int SqliteConstraintPrimaryKey = 1555;
    private const int SqliteConstraintUnique = 2067;

    public async Task<StoreResult> StoreAsync(WebhookEnvelope envelope, string rawBody, bool reprocessDuplicates)
    {
        var now = time.GetUtcNow().UtcDateTime;
        db.InboxEvents.Add(new InboxEvent
        {
            EventId = envelope.Id,
            Type = envelope.Type,
            Payload = rawBody,
            ReceivedAt = now,
            NextAttemptAt = now,
        });

        // No cancellation token on purpose: once a genuine event arrives, keep it even if the sender hangs up.
        try
        {
            await db.SaveChangesAsync();
            return StoreResult.Stored;
        }
        catch (DbUpdateException ex) when (IsDuplicateKey(ex))
        {
            db.ChangeTracker.Clear();
        }

        if (!reprocessDuplicates)
            return StoreResult.Duplicate;

        // UNSAFE lab mode: queue the already-stored event again, so it gets applied twice.
        await db.InboxEvents
            .Where(e => e.EventId == envelope.Id)
            .ExecuteUpdateAsync(s => s
                .SetProperty(e => e.Status, InboxStatus.Received)
                .SetProperty(e => e.NextAttemptAt, now));
        return StoreResult.DuplicateReprocessed;
    }

    private static bool IsDuplicateKey(DbUpdateException ex) =>
        ex.InnerException is SqliteException { SqliteExtendedErrorCode: SqliteConstraintPrimaryKey or SqliteConstraintUnique };
}
```

- [ ] **Step 7: Webhook endpoint**

`src/WebhookLab.Receiver/Webhooks/WebhookEndpoint.cs`:
```csharp
using System.Data.Common;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using WebhookLab.Contracts;
using WebhookLab.Receiver.Faults;
using WebhookLab.Receiver.Realtime;

namespace WebhookLab.Receiver.Webhooks;

public static class WebhookEndpoint
{
    public static IEndpointRouteBuilder MapWebhookEndpoint(this IEndpointRouteBuilder app)
    {
        app.MapPost("/webhooks", HandleAsync);
        return app;
    }

    /// <summary>
    /// Verify → store → acknowledge. No business logic here: that happens later in InboxProcessor,
    /// so this responds fast and the Provider doesn't time out and retry needlessly.
    /// </summary>
    private static async Task<IResult> HandleAsync(
        HttpRequest request,
        InboxStore inbox,
        FaultSettings faults,
        IOptions<WebhookOptions> options,
        TimeProvider time,
        ReceiverNotifier notifier)
    {
        using var reader = new StreamReader(request.Body);
        var body = await reader.ReadToEndAsync();
        var eventId = request.Headers[WebhookHeaders.Id].ToString();

        if (faults.TryConsumeFail())
        {
            await notifier.WebhookReceivedAsync(eventId, WebhookOutcome.FaultInjected);
            return Results.StatusCode(StatusCodes.Status500InternalServerError);
        }

        var isGenuine = WebhookSignature.Verify(
            options.Value.Secret,
            eventId,
            request.Headers[WebhookHeaders.Timestamp].ToString(),
            body,
            request.Headers[WebhookHeaders.Signature].ToString(),
            time.GetUtcNow());
        if (!isGenuine)
        {
            await notifier.WebhookReceivedAsync(eventId, WebhookOutcome.Rejected);
            return Results.Unauthorized();
        }

        var envelope = TryParse(body);
        if (envelope is null || envelope.Id != eventId || envelope.Data is null)
            return Results.BadRequest("Body must be a webhook envelope whose id matches the webhook-id header.");

        StoreResult result;
        try
        {
            result = await inbox.StoreAsync(envelope, body, faults.DedupeOff);
        }
        catch (Exception ex) when (ex is DbException or DbUpdateException)
        {
            // Temporary problem on our side: ask the Provider to retry later.
            return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
        }

        await notifier.WebhookReceivedAsync(eventId, result switch
        {
            StoreResult.Stored => WebhookOutcome.Stored,
            StoreResult.Duplicate => WebhookOutcome.Duplicate,
            _ => WebhookOutcome.DuplicateReprocessed,
        });

        if (faults.TryConsumeSlow())
            await Task.Delay(faults.SlowResponseDelay); // stored already; the sender just won't hear back in time

        // 200 for new AND duplicate events: the Provider only asks "did you get it?"
        return Results.Ok();
    }

    private static WebhookEnvelope? TryParse(string body)
    {
        try
        {
            return JsonSerializer.Deserialize<WebhookEnvelope>(body, WebhookJson.Options);
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
```

- [ ] **Step 8: Register in `Program.cs`**

Add usings `WebhookLab.Receiver.Realtime` and `WebhookLab.Receiver.Webhooks`. After the `FaultSettings` registration add:
```csharp
builder.Services.AddOptions<WebhookOptions>()
    .Bind(builder.Configuration.GetSection(WebhookOptions.SectionName))
    .Validate(o => !string.IsNullOrWhiteSpace(o.Secret), "Webhook:Secret must be configured.")
    .ValidateOnStart();
builder.Services.AddScoped<InboxStore>();
builder.Services.AddSingleton<ReceiverNotifier>();
builder.Services.AddSignalR().AddJsonProtocol(options =>
    options.PayloadSerializerOptions.Converters.Add(new JsonStringEnumConverter()));
```
After `app.MapLabControlEndpoints();` add:
```csharp
app.MapWebhookEndpoint();
app.MapHub<ReceiverHub>("/hubs/receiver");
```

- [ ] **Step 9: Run tests to verify they pass**

Run: `dotnet test`
Expected: PASS (Receiver: 14 tests).

- [ ] **Step 10: Commit**

```bash
git add src/WebhookLab.Receiver tests/WebhookLab.Receiver.Tests
git commit -m "feat: add signed webhook endpoint with idempotent inbox store"
```

---

### Task 5: Ledger handlers

**Files:**
- Create: `src/WebhookLab.Receiver/Processing/{IWebhookEventHandler,OrderCreatedHandler,PaymentSucceededHandler,PaymentRefundedHandler}.cs`
- Test: `tests/WebhookLab.Receiver.Tests/{TestDb,LedgerHandlerTests}.cs`

**Interfaces:**
- Consumes: `OrderEventData`, `EventTypes` (Task 1); `ReceiverDbContext`, `LedgerOrder`, `LedgerStatus` (Task 2).
- Produces:
  - `enum HandlerResult { Applied, Deferred }`
  - `interface IWebhookEventHandler { string EventType { get; } Task<HandlerResult> HandleAsync(OrderEventData data, ReceiverDbContext db, CancellationToken ct); }`
  - `OrderCreatedHandler`, `PaymentSucceededHandler`, `PaymentRefundedHandler`. Handlers change tracked entities only; the **caller** calls `SaveChangesAsync`.

- [ ] **Step 1: Test database helper**

`tests/WebhookLab.Receiver.Tests/TestDb.cs`:
```csharp
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using WebhookLab.Receiver.Data;

namespace WebhookLab.Receiver.Tests;

/// <summary>An in-memory SQLite database that lives as long as this object.</summary>
public sealed class TestDb : IDisposable
{
    private readonly SqliteConnection _connection = new("DataSource=:memory:");

    public TestDb()
    {
        _connection.Open();
        Context = new ReceiverDbContext(new DbContextOptionsBuilder<ReceiverDbContext>().UseSqlite(_connection).Options);
        Context.Database.EnsureCreated();
    }

    public ReceiverDbContext Context { get; }

    public void Dispose()
    {
        Context.Dispose();
        _connection.Dispose();
    }
}
```

- [ ] **Step 2: Write the failing tests**

`tests/WebhookLab.Receiver.Tests/LedgerHandlerTests.cs`:
```csharp
using WebhookLab.Contracts;
using WebhookLab.Receiver.Data;
using WebhookLab.Receiver.Processing;

namespace WebhookLab.Receiver.Tests;

public class LedgerHandlerTests
{
    private static readonly OrderEventData Order42 = new(OrderId: 42, Amount: 50m);

    [Fact]
    public async Task OrderCreated_adds_the_order_to_the_ledger()
    {
        using var test = new TestDb();

        var result = await new OrderCreatedHandler().HandleAsync(Order42, test.Context, default);
        await test.Context.SaveChangesAsync();

        Assert.Equal(HandlerResult.Applied, result);
        var order = await test.Context.LedgerOrders.FindAsync(42);
        Assert.Equal(LedgerStatus.Created, order!.Status);
        Assert.Equal(50m, order.Amount);
    }

    [Fact]
    public async Task OrderCreated_for_an_existing_order_changes_nothing()
    {
        using var test = new TestDb();
        test.Context.LedgerOrders.Add(new LedgerOrder { OrderId = 42, Amount = 50m, Status = LedgerStatus.Paid, ChargeCount = 1 });
        await test.Context.SaveChangesAsync();

        var result = await new OrderCreatedHandler().HandleAsync(Order42, test.Context, default);
        await test.Context.SaveChangesAsync();

        Assert.Equal(HandlerResult.Applied, result);
        Assert.Equal(LedgerStatus.Paid, (await test.Context.LedgerOrders.FindAsync(42))!.Status);
    }

    [Fact]
    public async Task PaymentSucceeded_marks_the_order_paid_and_counts_the_charge()
    {
        using var test = new TestDb();
        test.Context.LedgerOrders.Add(new LedgerOrder { OrderId = 42, Amount = 50m });
        await test.Context.SaveChangesAsync();

        var result = await new PaymentSucceededHandler().HandleAsync(Order42, test.Context, default);
        await test.Context.SaveChangesAsync();

        Assert.Equal(HandlerResult.Applied, result);
        var order = await test.Context.LedgerOrders.FindAsync(42);
        Assert.Equal(LedgerStatus.Paid, order!.Status);
        Assert.Equal(1, order.ChargeCount);
    }

    [Fact]
    public async Task PaymentSucceeded_before_the_order_exists_is_deferred()
    {
        using var test = new TestDb();

        var result = await new PaymentSucceededHandler().HandleAsync(Order42, test.Context, default);

        Assert.Equal(HandlerResult.Deferred, result);
    }

    [Fact]
    public async Task PaymentRefunded_marks_a_paid_order_refunded()
    {
        using var test = new TestDb();
        test.Context.LedgerOrders.Add(new LedgerOrder { OrderId = 42, Amount = 50m, Status = LedgerStatus.Paid, ChargeCount = 1 });
        await test.Context.SaveChangesAsync();

        var result = await new PaymentRefundedHandler().HandleAsync(Order42, test.Context, default);
        await test.Context.SaveChangesAsync();

        Assert.Equal(HandlerResult.Applied, result);
        var order = await test.Context.LedgerOrders.FindAsync(42);
        Assert.Equal(LedgerStatus.Refunded, order!.Status);
        Assert.Equal(1, order.RefundCount);
    }

    [Fact]
    public async Task PaymentRefunded_before_the_payment_is_deferred()
    {
        using var test = new TestDb();
        test.Context.LedgerOrders.Add(new LedgerOrder { OrderId = 42, Amount = 50m });
        await test.Context.SaveChangesAsync();

        var result = await new PaymentRefundedHandler().HandleAsync(Order42, test.Context, default);

        Assert.Equal(HandlerResult.Deferred, result);
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `dotnet test tests/WebhookLab.Receiver.Tests --filter LedgerHandlerTests`
Expected: build FAILS with `The type or namespace name 'Processing' does not exist`.

- [ ] **Step 4: Implement the handlers**

`src/WebhookLab.Receiver/Processing/IWebhookEventHandler.cs`:
```csharp
using WebhookLab.Contracts;
using WebhookLab.Receiver.Data;

namespace WebhookLab.Receiver.Processing;

public enum HandlerResult
{
    Applied,
    /// <summary>An event this one depends on hasn't been processed yet (out-of-order delivery).</summary>
    Deferred,
}

/// <summary>
/// Applies one event type to the ledger. Handlers only modify tracked entities; the caller saves,
/// so the ledger change and the inbox status commit in the same transaction.
/// </summary>
public interface IWebhookEventHandler
{
    string EventType { get; }

    Task<HandlerResult> HandleAsync(OrderEventData data, ReceiverDbContext db, CancellationToken ct);
}
```

`src/WebhookLab.Receiver/Processing/OrderCreatedHandler.cs`:
```csharp
using WebhookLab.Contracts;
using WebhookLab.Receiver.Data;

namespace WebhookLab.Receiver.Processing;

public sealed class OrderCreatedHandler : IWebhookEventHandler
{
    public string EventType => EventTypes.OrderCreated;

    public async Task<HandlerResult> HandleAsync(OrderEventData data, ReceiverDbContext db, CancellationToken ct)
    {
        if (await db.LedgerOrders.FindAsync([data.OrderId], ct) is null)
            db.LedgerOrders.Add(new LedgerOrder { OrderId = data.OrderId, Amount = data.Amount });

        return HandlerResult.Applied;
    }
}
```

`src/WebhookLab.Receiver/Processing/PaymentSucceededHandler.cs`:
```csharp
using WebhookLab.Contracts;
using WebhookLab.Receiver.Data;

namespace WebhookLab.Receiver.Processing;

public sealed class PaymentSucceededHandler : IWebhookEventHandler
{
    public string EventType => EventTypes.PaymentSucceeded;

    public async Task<HandlerResult> HandleAsync(OrderEventData data, ReceiverDbContext db, CancellationToken ct)
    {
        var order = await db.LedgerOrders.FindAsync([data.OrderId], ct);
        if (order is null)
            return HandlerResult.Deferred;

        order.Status = LedgerStatus.Paid;
        // Deliberately not idempotent on its own: the inbox dedupe is what stops a double charge.
        order.ChargeCount++;
        return HandlerResult.Applied;
    }
}
```

`src/WebhookLab.Receiver/Processing/PaymentRefundedHandler.cs`:
```csharp
using WebhookLab.Contracts;
using WebhookLab.Receiver.Data;

namespace WebhookLab.Receiver.Processing;

public sealed class PaymentRefundedHandler : IWebhookEventHandler
{
    public string EventType => EventTypes.PaymentRefunded;

    public async Task<HandlerResult> HandleAsync(OrderEventData data, ReceiverDbContext db, CancellationToken ct)
    {
        var order = await db.LedgerOrders.FindAsync([data.OrderId], ct);
        if (order is not { Status: LedgerStatus.Paid })
            return HandlerResult.Deferred;

        order.Status = LedgerStatus.Refunded;
        order.RefundCount++;
        return HandlerResult.Applied;
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `dotnet test tests/WebhookLab.Receiver.Tests --filter LedgerHandlerTests`
Expected: PASS, 6 tests.

- [ ] **Step 6: Commit**

```bash
git add src/WebhookLab.Receiver/Processing tests/WebhookLab.Receiver.Tests
git commit -m "feat: add ledger handlers with out-of-order deferral"
```

---

### Task 6: Inbox processor + background worker

**Files:**
- Create: `src/WebhookLab.Receiver/Processing/{InboxProcessor,InboxProcessorWorker}.cs`
- Modify: `src/WebhookLab.Receiver/Program.cs`, `tests/WebhookLab.Receiver.Tests/ReceiverFactory.cs`
- Test: `tests/WebhookLab.Receiver.Tests/InboxProcessorTests.cs`

**Interfaces:**
- Consumes: `Backoff`, `WebhookEnvelope`, `WebhookJson` (Task 1); data model (Task 2); `FaultSettings.TryConsumeCrash` (Task 3); `ReceiverNotifier` (Task 4); `IWebhookEventHandler`, `HandlerResult`, the three handlers (Task 5).
- Produces:
  - `InboxProcessor.ProcessDueAsync(CancellationToken ct = default) : Task<int>`. Processes due `Received`/`Deferred` events oldest-first and returns how many were attempted.
  - `InboxProcessorWorker` (`BackgroundService`, 1 s loop), registered only when `Receiver:EnableBackgroundProcessing` is true.
  - Test: `ReceiverFactory.ProcessInboxAsync() : Task<int>`, `ReceiverFactory.SendAndProcessAsync(WebhookEnvelope) : Task`

- [ ] **Step 1: Add factory helpers**

In `ReceiverFactory.cs` add `using WebhookLab.Receiver.Processing;` and these methods:
```csharp
    /// <summary>Runs one pass of the inbox processor, like one tick of the background worker.</summary>
    public async Task<int> ProcessInboxAsync()
    {
        using var scope = Services.CreateScope();
        return await scope.ServiceProvider.GetRequiredService<InboxProcessor>().ProcessDueAsync();
    }

    public async Task SendAndProcessAsync(WebhookEnvelope envelope)
    {
        (await SendAsync(envelope)).EnsureSuccessStatusCode();
        await ProcessInboxAsync();
    }
```

- [ ] **Step 2: Write the failing tests**

`tests/WebhookLab.Receiver.Tests/InboxProcessorTests.cs`:
```csharp
using WebhookLab.Contracts;
using WebhookLab.Receiver.Data;
using WebhookLab.Receiver.Faults;

namespace WebhookLab.Receiver.Tests;

public class InboxProcessorTests
{
    private const int OrderId = 42;

    [Fact]
    public async Task Received_event_is_applied_and_marked_processed()
    {
        await using var lab = new ReceiverFactory();
        var created = TestEvents.Created(OrderId);

        await lab.SendAndProcessAsync(created);

        Assert.Equal(InboxStatus.Processed, (await lab.GetInboxEventAsync(created.Id))!.Status);
        Assert.NotNull(await lab.GetLedgerOrderAsync(OrderId));
    }

    [Fact]
    public async Task Deferred_event_is_not_retried_before_its_backoff_elapses()
    {
        await using var lab = new ReceiverFactory();
        await lab.SendAndProcessAsync(TestEvents.Paid(OrderId)); // no order yet → deferred, retry in ~2 s

        lab.Time.Advance(TimeSpan.FromSeconds(1));

        Assert.Equal(0, await lab.ProcessInboxAsync());
    }

    [Fact]
    public async Task Deferred_event_is_applied_once_its_dependency_arrives()
    {
        await using var lab = new ReceiverFactory();
        var payment = TestEvents.Paid(OrderId);
        await lab.SendAndProcessAsync(payment);
        Assert.Equal(InboxStatus.Deferred, (await lab.GetInboxEventAsync(payment.Id))!.Status);

        await lab.SendAndProcessAsync(TestEvents.Created(OrderId));
        lab.Time.Advance(TimeSpan.FromSeconds(3));
        await lab.ProcessInboxAsync();

        Assert.Equal(InboxStatus.Processed, (await lab.GetInboxEventAsync(payment.Id))!.Status);
        Assert.Equal(1, (await lab.GetLedgerOrderAsync(OrderId))!.ChargeCount);
    }

    [Fact]
    public async Task Deferred_event_fails_after_max_attempts()
    {
        await using var lab = new ReceiverFactory();
        var payment = TestEvents.Paid(OrderId); // its order never arrives
        await lab.SendAsync(payment);

        for (var attempt = 1; attempt <= Backoff.MaxAttempts; attempt++)
        {
            await lab.ProcessInboxAsync();
            lab.Time.Advance(TimeSpan.FromSeconds(20)); // longer than any backoff step
        }

        var stored = await lab.GetInboxEventAsync(payment.Id);
        Assert.Equal(InboxStatus.Failed, stored!.Status);
        Assert.Equal(Backoff.MaxAttempts, stored.ProcessingAttempts);
    }

    [Fact]
    public async Task Unknown_event_type_is_marked_failed()
    {
        await using var lab = new ReceiverFactory();
        var mystery = TestEvents.New("customer.teleported", OrderId);

        await lab.SendAndProcessAsync(mystery);

        var stored = await lab.GetInboxEventAsync(mystery.Id);
        Assert.Equal(InboxStatus.Failed, stored!.Status);
        Assert.Contains("customer.teleported", stored.LastError);
    }

    [Fact]
    public async Task Crash_rolls_back_the_ledger_change_and_leaves_the_event_queued()
    {
        await using var lab = new ReceiverFactory();
        await lab.SendAndProcessAsync(TestEvents.Created(OrderId));
        lab.Faults.Apply(new FaultSnapshot(FailNext: 0, SlowNext: 0, CrashNext: 1, DedupeOff: false));
        var payment = TestEvents.Paid(OrderId);

        await lab.SendAndProcessAsync(payment);

        var stored = await lab.GetInboxEventAsync(payment.Id);
        Assert.Equal(InboxStatus.Received, stored!.Status);
        Assert.NotNull(stored.LastError);
        Assert.Equal(0, (await lab.GetLedgerOrderAsync(OrderId))!.ChargeCount);
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `dotnet test tests/WebhookLab.Receiver.Tests --filter InboxProcessorTests`
Expected: build FAILS with `The type or namespace name 'InboxProcessor' could not be found`.

- [ ] **Step 4: Implement `InboxProcessor`**

`src/WebhookLab.Receiver/Processing/InboxProcessor.cs`:
```csharp
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using WebhookLab.Contracts;
using WebhookLab.Receiver.Data;
using WebhookLab.Receiver.Faults;
using WebhookLab.Receiver.Realtime;

namespace WebhookLab.Receiver.Processing;

/// <summary>
/// Applies stored events to the ledger. Each event's ledger change and its inbox status are
/// committed in one transaction, so a crash can never leave "charged but not marked processed".
/// </summary>
public sealed class InboxProcessor(
    ReceiverDbContext db,
    IEnumerable<IWebhookEventHandler> handlers,
    FaultSettings faults,
    TimeProvider time,
    ReceiverNotifier notifier,
    ILogger<InboxProcessor> logger)
{
    private readonly Dictionary<string, IWebhookEventHandler> _handlers = handlers.ToDictionary(h => h.EventType);

    /// <summary>Processes every due event, oldest first. Returns how many were attempted.</summary>
    public async Task<int> ProcessDueAsync(CancellationToken ct = default)
    {
        var now = time.GetUtcNow().UtcDateTime;
        var dueEventIds = await db.InboxEvents
            .Where(e => (e.Status == InboxStatus.Received || e.Status == InboxStatus.Deferred) && e.NextAttemptAt <= now)
            .OrderBy(e => e.ReceivedAt)
            .Select(e => e.EventId)
            .ToListAsync(ct);

        foreach (var eventId in dueEventIds)
            await ProcessOneAsync(eventId, ct);

        return dueEventIds.Count;
    }

    private async Task ProcessOneAsync(string eventId, CancellationToken ct)
    {
        db.ChangeTracker.Clear();
        var inboxEvent = await db.InboxEvents.SingleAsync(e => e.EventId == eventId, ct);

        if (!_handlers.TryGetValue(inboxEvent.Type, out var handler))
        {
            inboxEvent.Status = InboxStatus.Failed;
            inboxEvent.LastError = $"No handler for event type '{inboxEvent.Type}'.";
            await db.SaveChangesAsync(ct);
            await notifier.ProcessingCompletedAsync(eventId, inboxEvent.Status, inboxEvent.LastError);
            return;
        }

        var data = JsonSerializer.Deserialize<WebhookEnvelope>(inboxEvent.Payload, WebhookJson.Options)!.Data;

        try
        {
            await using var transaction = await db.Database.BeginTransactionAsync(ct);

            var result = await handler.HandleAsync(data, db, ct);
            await db.SaveChangesAsync(ct);

            if (faults.TryConsumeCrash())
                throw new InvalidOperationException("Injected crash: the ledger was written but never committed.");

            RecordResult(inboxEvent, result);
            await db.SaveChangesAsync(ct);
            await transaction.CommitAsync(ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Leaving the try block disposed the uncommitted transaction, which rolled it back.
            db.ChangeTracker.Clear();
            logger.LogWarning(ex, "Processing {EventId} failed; it stays queued for retry", eventId);
            await db.InboxEvents
                .Where(e => e.EventId == eventId)
                .ExecuteUpdateAsync(s => s.SetProperty(e => e.LastError, ex.Message), ct);
            await notifier.ProcessingCompletedAsync(eventId, InboxStatus.Received, ex.Message);
            return;
        }

        await notifier.ProcessingCompletedAsync(eventId, inboxEvent.Status, inboxEvent.LastError);
        if (inboxEvent.Status == InboxStatus.Processed
            && await db.LedgerOrders.AsNoTracking().SingleOrDefaultAsync(o => o.OrderId == data.OrderId, ct) is { } order)
        {
            await notifier.LedgerChangedAsync(eventId, order);
        }
    }

    private void RecordResult(InboxEvent inboxEvent, HandlerResult result)
    {
        inboxEvent.ProcessingAttempts++;

        if (result == HandlerResult.Applied)
        {
            inboxEvent.Status = InboxStatus.Processed;
            inboxEvent.LastError = null;
        }
        else if (inboxEvent.ProcessingAttempts >= Backoff.MaxAttempts)
        {
            inboxEvent.Status = InboxStatus.Failed;
            inboxEvent.LastError = $"Still waiting on an earlier event after {Backoff.MaxAttempts} attempts.";
        }
        else
        {
            inboxEvent.Status = InboxStatus.Deferred;
            inboxEvent.NextAttemptAt = time.GetUtcNow().UtcDateTime + Backoff.Delay(inboxEvent.ProcessingAttempts, Random.Shared);
            inboxEvent.LastError = "Waiting on an earlier event (arrived out of order).";
        }
    }
}
```

- [ ] **Step 5: Implement the worker**

`src/WebhookLab.Receiver/Processing/InboxProcessorWorker.cs`:
```csharp
namespace WebhookLab.Receiver.Processing;

/// <summary>Runs the inbox processor once a second for the lifetime of the app.</summary>
public sealed class InboxProcessorWorker(IServiceScopeFactory scopes, ILogger<InboxProcessorWorker> logger) : BackgroundService
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(1);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(PollInterval);
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                using var scope = scopes.CreateScope();
                await scope.ServiceProvider.GetRequiredService<InboxProcessor>().ProcessDueAsync(stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Inbox processing pass failed; trying again next tick");
            }
        }
    }
}
```

- [ ] **Step 6: Register in `Program.cs`**

Add `using WebhookLab.Receiver.Processing;`. After `builder.Services.AddScoped<InboxStore>();` add:
```csharp
builder.Services.AddSingleton<IWebhookEventHandler, OrderCreatedHandler>();
builder.Services.AddSingleton<IWebhookEventHandler, PaymentSucceededHandler>();
builder.Services.AddSingleton<IWebhookEventHandler, PaymentRefundedHandler>();
builder.Services.AddScoped<InboxProcessor>();
if (builder.Configuration.GetValue("Receiver:EnableBackgroundProcessing", true))
    builder.Services.AddHostedService<InboxProcessorWorker>();
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `dotnet test`
Expected: PASS (Receiver: 26 tests).

- [ ] **Step 8: Commit**

```bash
git add src/WebhookLab.Receiver tests/WebhookLab.Receiver.Tests
git commit -m "feat: add transactional inbox processor with deferral and crash rollback"
```

---

### Task 7: Lesson tests, manual sender script, end-to-end check

**Files:**
- Create: `tests/WebhookLab.Receiver.Tests/LessonTests.cs`
- Create: `scripts/send-webhook.sh`
- Modify: `tests/WebhookLab.Receiver.Tests/ReceiverFactory.cs`

**Interfaces:**
- Consumes: everything above.
- Produces: `ReceiverFactory.WaitUntilStoredAsync(string eventId) : Task`; runnable `scripts/send-webhook.sh <type> <orderId> [eventId]`.

- [ ] **Step 1: Add the wait helper**

In `ReceiverFactory.cs` add:
```csharp
    /// <summary>Polls until an event is in the inbox (for requests the client stopped waiting on).</summary>
    public async Task WaitUntilStoredAsync(string eventId)
    {
        for (var i = 0; i < 200; i++)
        {
            if (await GetInboxEventAsync(eventId) is not null)
                return;
            await Task.Delay(10);
        }
        throw new TimeoutException($"{eventId} was never stored.");
    }
```

- [ ] **Step 2: Write the lesson tests**

`tests/WebhookLab.Receiver.Tests/LessonTests.cs`:
```csharp
using System.Net;
using WebhookLab.Receiver.Data;
using WebhookLab.Receiver.Faults;

namespace WebhookLab.Receiver.Tests;

/// <summary>Each test is one of the lab's lessons, stated as the behavior it proves.</summary>
public class LessonTests
{
    private const int OrderId = 42;

    [Fact]
    public async Task Same_event_twice_charges_customer_once()
    {
        await using var lab = new ReceiverFactory();
        await lab.SendAndProcessAsync(TestEvents.Created(OrderId));
        var payment = TestEvents.Paid(OrderId);

        await lab.SendAndProcessAsync(payment);
        await lab.SendAndProcessAsync(payment);

        Assert.Equal(1, (await lab.GetLedgerOrderAsync(OrderId))!.ChargeCount);
    }

    [Fact]
    public async Task Lost_200_then_retry_is_skipped_as_duplicate()
    {
        await using var lab = new ReceiverFactory();
        await lab.SendAndProcessAsync(TestEvents.Created(OrderId));
        lab.Faults.Apply(new FaultSnapshot(FailNext: 0, SlowNext: 1, CrashNext: 0, DedupeOff: false));
        var payment = TestEvents.Paid(OrderId);

        // Attempt 1: the Receiver stores the event, but answers after the sender has given up.
        using var senderTimeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => lab.SendAsync(payment, ct: senderTimeout.Token));
        await lab.WaitUntilStoredAsync(payment.Id);

        // Attempt 2: the sender never heard back, so it retries the same event id.
        var retry = await lab.SendAsync(payment);
        await lab.ProcessInboxAsync();

        Assert.Equal(HttpStatusCode.OK, retry.StatusCode);
        Assert.Equal(1, (await lab.GetLedgerOrderAsync(OrderId))!.ChargeCount);
    }

    [Fact]
    public async Task Refund_before_payment_is_deferred_then_applied()
    {
        await using var lab = new ReceiverFactory();
        await lab.SendAndProcessAsync(TestEvents.Created(OrderId));
        var payment = TestEvents.Paid(OrderId);
        var refund = TestEvents.Refunded(OrderId);

        await lab.SendAsync(refund); // arrives first
        lab.Time.Advance(TimeSpan.FromSeconds(1));
        await lab.SendAsync(payment);
        await lab.ProcessInboxAsync();

        Assert.Equal(InboxStatus.Deferred, (await lab.GetInboxEventAsync(refund.Id))!.Status);
        Assert.Equal(LedgerStatus.Paid, (await lab.GetLedgerOrderAsync(OrderId))!.Status);

        lab.Time.Advance(TimeSpan.FromSeconds(3));
        await lab.ProcessInboxAsync();

        var order = await lab.GetLedgerOrderAsync(OrderId);
        Assert.Equal(LedgerStatus.Refunded, order!.Status);
        Assert.Equal(1, order.RefundCount);
    }

    [Fact]
    public async Task Crash_mid_processing_is_retried_without_double_charge()
    {
        await using var lab = new ReceiverFactory();
        await lab.SendAndProcessAsync(TestEvents.Created(OrderId));
        lab.Faults.Apply(new FaultSnapshot(FailNext: 0, SlowNext: 0, CrashNext: 1, DedupeOff: false));

        await lab.SendAndProcessAsync(TestEvents.Paid(OrderId)); // crashes, rolls back
        await lab.ProcessInboxAsync();                           // retried

        Assert.Equal(1, (await lab.GetLedgerOrderAsync(OrderId))!.ChargeCount);
    }

    [Fact]
    public async Task Forged_signature_is_rejected()
    {
        await using var lab = new ReceiverFactory();
        var forged = TestEvents.Paid(OrderId);

        var response = await lab.SendAsync(forged, secret: "not-the-real-secret");

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        Assert.Null(await lab.GetInboxEventAsync(forged.Id));
    }

    [Fact]
    public async Task Dedupe_off_charges_customer_twice()
    {
        await using var lab = new ReceiverFactory();
        await lab.SendAndProcessAsync(TestEvents.Created(OrderId));
        lab.Faults.Apply(new FaultSnapshot(FailNext: 0, SlowNext: 0, CrashNext: 0, DedupeOff: true));
        var payment = TestEvents.Paid(OrderId);

        await lab.SendAndProcessAsync(payment);
        await lab.SendAndProcessAsync(payment);

        // The bug idempotency prevents, made visible.
        Assert.Equal(2, (await lab.GetLedgerOrderAsync(OrderId))!.ChargeCount);
    }
}
```

- [ ] **Step 3: Run the lesson tests**

Run: `dotnet test tests/WebhookLab.Receiver.Tests --filter LessonTests`
Expected: PASS, 6 tests. These exercise behavior already built in Tasks 3–6. A failure here means a real integration bug in those tasks: fix it there, don't weaken the test.

- [ ] **Step 4: Manual sender script**

`scripts/send-webhook.sh`:
```bash
#!/usr/bin/env bash
# Sign and send one webhook to the Receiver by hand — see exactly what goes over the wire.
# Usage: scripts/send-webhook.sh <type> <orderId> [eventId]
#   Reuse an eventId to send a duplicate.
set -euo pipefail

TYPE=${1:?type, e.g. order.created | payment.succeeded | payment.refunded}
ORDER_ID=${2:?orderId}
EVENT_ID=${3:-evt_$(uuidgen | tr -d '-' | tr '[:upper:]' '[:lower:]')}
SECRET=${WEBHOOK_SECRET:-dev-secret-change-me}
URL=${RECEIVER_URL:-http://localhost:5002/webhooks}

TIMESTAMP=$(date +%s)
BODY=$(printf '{"id":"%s","type":"%s","createdAt":"%s","data":{"orderId":%s,"amount":50.00}}' \
  "$EVENT_ID" "$TYPE" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$ORDER_ID")
SIGNATURE=$(printf '%s' "$EVENT_ID.$TIMESTAMP.$BODY" | openssl dgst -sha256 -hmac "$SECRET" -binary | base64)

curl -sS -i -X POST "$URL" \
  -H "content-type: application/json" \
  -H "webhook-id: $EVENT_ID" \
  -H "webhook-timestamp: $TIMESTAMP" \
  -H "webhook-signature: v1,$SIGNATURE" \
  -d "$BODY"
echo
echo "event id: $EVENT_ID"
```

```bash
chmod +x scripts/send-webhook.sh
```

- [ ] **Step 5: Full test run**

Run: `dotnet test`
Expected: PASS. Contracts 13, Receiver 32, 0 failed.

- [ ] **Step 6: End-to-end manual check**

Terminal 1:
```bash
dotnet run --project src/WebhookLab.Receiver
```
Expected: `Now listening on: http://localhost:5002`.

Terminal 2:
```bash
scripts/send-webhook.sh order.created 42
scripts/send-webhook.sh payment.succeeded 42 evt_manual_pay
scripts/send-webhook.sh payment.succeeded 42 evt_manual_pay   # duplicate
sleep 2
curl -s http://localhost:5002/api/ledger
curl -s http://localhost:5002/api/inbox
WEBHOOK_SECRET=wrong scripts/send-webhook.sh order.created 43
```
Expected:
- The first three sends return `HTTP/1.1 200 OK`.
- The ledger shows order 42 `"status":"Paid"` with `"chargeCount":1`.
- The inbox has 2 events, both `"status":"Processed"`.
- The wrong-secret send returns `HTTP/1.1 401 Unauthorized`.

Run `curl -X POST http://localhost:5002/api/reset`, then stop the Receiver (Ctrl+C). The database file `receiver.db` is created relative to the working directory `dotnet run` was launched from: with `dotnet run --project src/WebhookLab.Receiver` (as used throughout this plan) that's `src/WebhookLab.Receiver/`, not the repo root. Open it in DB Browser for SQLite to see the `InboxEvents` and `LedgerOrders` tables. It is git-ignored.

- [ ] **Step 7: Commit**

```bash
git add tests/WebhookLab.Receiver.Tests/LessonTests.cs tests/WebhookLab.Receiver.Tests/ReceiverFactory.cs scripts/send-webhook.sh
git commit -m "test: add lesson scenarios and manual webhook sender script"
```
