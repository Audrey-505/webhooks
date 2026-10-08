# Webhook Lab — Plan 2: Provider Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Provider ("mini Stripe"). It owns orders, records a webhook event plus a pending delivery for every order change (the outbox), and runs a background dispatcher that signs and POSTs those events to the Receiver. Failed sends are retried with exponential backoff and dead-lettered after 5 attempts. The dispatcher supports sending-side chaos (duplicate, delay, reorder, bad signature) and replay, exposes a REST API, and pushes live updates through a SignalR hub.

**Architecture:** `WebhookLab.Provider` is an ASP.NET Core minimal-API app on port 5001 with its own SQLite database (`provider.db`). It reuses `WebhookLab.Contracts` (envelope, `WebhookSignature`, `Backoff`) from Plan 1.
- `OrderService` changes an order and writes the matching `WebhookEvent` and a `Pending` `Delivery` in **one transaction**.
- `DeliveryDispatcher` sends due deliveries **outside** any transaction through `WebhookSender`, a typed `HttpClient` with a 5 s timeout. It records every `DeliveryAttempt` and schedules retries.
- `DeliveryDispatcherWorker` runs the dispatcher once a second.

**Tech Stack:** .NET 10, ASP.NET Core minimal APIs, EF Core + SQLite, `IHttpClientFactory` typed client, SignalR, xUnit, `WebApplicationFactory`, `FakeTimeProvider`, a fake `HttpMessageHandler` standing in for the Receiver.

**Spec:** `docs/superpowers/specs/2026-09-28-webhook-lab-design.md` (sections 5, 7, 9, 10, 11). This is plan 2 of 3. Plan 1 (Contracts + Receiver) is merged on `main`. Plan 3 = Angular dashboard.

## Global Constraints

- Target framework: `net10.0`. The SDK is pinned by the existing `global.json`. Add new projects to the existing `WebhookLab.slnx`.
- Provider URL: `http://localhost:5001`. Dashboard origin allowed by CORS: `http://localhost:4200`. `Webhook:TargetUrl` default: `http://localhost:5002/webhooks`.
- Dev secret (`appsettings.Development.json`): `dev-secret-change-me`. It must equal the Receiver's.
- Signature: `WebhookSignature.Sign(secret, id, unixSeconds, body)` from Contracts. Headers come from `WebhookHeaders`. The exact stored `Payload` string is what is sent and signed on every attempt.
- Delivery timeout: 5 s. Max attempts: `Backoff.MaxAttempts` (5). Retry delay: `Backoff.Delay(attemptCount, Random.Shared)`. Dispatcher poll interval: 1 s.
- Every attempt increments `AttemptCount`. `2xx` → `Succeeded`. Anything else (non-2xx including 4xx, timeout, connection error) is a failure: `AttemptCount >= 5` → `Dead`, otherwise reschedule with backoff.
- The HTTP send is never inside a database transaction. Order change + event + delivery are written in one transaction.
- Enums are stored as strings in SQLite and serialized as strings in JSON and SignalR. Timestamps are stored as UTC `DateTime`, with the UTC read-back converter (same as the Receiver).
- Chaos counters are one-shot (decrement on use); `DelaySeconds` applies to every send while set. Negative values are clamped to 0.
- **Git:** work happens on branch `plan-2-provider`. Every commit message ends with the Co-Authored-By attribution your own system instructions specify.
- All commands run from the repository root: `/Users/audreyalcantara/Documents/projects/Webhook-receiver`.

## File Map

```
src/WebhookLab.Provider/
  Program.cs                              composition root
  appsettings.json / appsettings.Development.json
  Properties/launchSettings.json          port 5001
  Data/OrderStatus.cs, Order.cs           the Provider's own orders
  Data/WebhookEvent.cs                    an event + its exact JSON payload
  Data/DeliveryStatus.cs, Delivery.cs     one "get this event to the Receiver" job
  Data/DeliveryAttempt.cs                 one HTTP POST and its result
  Data/ProviderDbContext.cs
  Api/QueryEndpoints.cs, DeliveryViews.cs GET /api/orders, GET /api/deliveries
  Api/LabControlEndpoints.cs              GET/PUT /api/chaos, POST /api/reset
  Api/OrderEndpoints.cs                   POST /api/orders, /pay, /refund
  Api/EventEndpoints.cs                   POST /api/events/{id}/replay
  Chaos/ChaosSettings.cs                  sending-side chaos switches
  Chaos/ReorderBuffer.cs                  deliveries held back by ReorderNext
  Orders/OrderService.cs                  order change + event + delivery in one transaction
  Dispatch/WebhookOptions.cs              Webhook:Secret, TargetUrl, Timeout
  Dispatch/WebhookSender.cs               sign + POST one webhook → SendResult
  Dispatch/DeliveryDispatcher.cs          send due deliveries, record attempts, retry/dead-letter, chaos
  Dispatch/DeliveryDispatcherWorker.cs    BackgroundService loop
  Realtime/ProviderHub.cs, ProviderMessages.cs, ProviderNotifier.cs

tests/WebhookLab.Provider.Tests/
  ProviderFactory.cs, FakeReceiver.cs
  QueryEndpointsTests.cs, ChaosTests.cs (unit + API), OrderEndpointsTests.cs,
  WebhookSenderTests.cs, DeliveryDispatcherTests.cs, DispatcherChaosTests.cs, DeliveryLessonTests.cs
```

---

### Task 1: Provider skeleton — data model, query API, test factory

**Files:**
- Create: `src/WebhookLab.Provider/` (web project), `Program.cs`, `appsettings.json`, `appsettings.Development.json`, `Properties/launchSettings.json`
- Create: `src/WebhookLab.Provider/Data/{OrderStatus,Order,WebhookEvent,DeliveryStatus,Delivery,DeliveryAttempt,ProviderDbContext}.cs`
- Create: `src/WebhookLab.Provider/Api/{QueryEndpoints,DeliveryViews}.cs`
- Test: `tests/WebhookLab.Provider.Tests/{ProviderFactory,QueryEndpointsTests}.cs`

**Interfaces:**
- Consumes: `WebhookLab.Contracts` (project reference).
- Produces:
  - `enum OrderStatus { Created, Paid, Refunded }`; `class Order { int Id; decimal Amount; OrderStatus Status }`
  - `class WebhookEvent { string Id; string Type; string Payload; DateTime CreatedAt }` (table `Events`)
  - `enum DeliveryStatus { Pending, Succeeded, Dead }`
  - `class Delivery { int Id; string EventId; DeliveryStatus Status; int AttemptCount; DateTime NextAttemptAt; bool IsReplay; DateTime CreatedAt; List<DeliveryAttempt> Attempts }`
  - `class DeliveryAttempt { int Id; int DeliveryId; int AttemptNumber; bool IsDuplicateSend; DateTime StartedAt; long DurationMs; int? StatusCode; string? Error }`
  - `ProviderDbContext` with `Orders`, `Events`, `Deliveries`, `DeliveryAttempts`
  - `record DeliveryView(...)`, `record AttemptView(...)`; `QueryEndpoints.MapQueryEndpoints`
  - Test: `ProviderFactory` with `Secret`, `Time` (`FakeTimeProvider`), `Json` (web options + string enums), `WithDbAsync<T>`

- [ ] **Step 1: Create the branch and projects**

```bash
git checkout -b plan-2-provider
dotnet new web -n WebhookLab.Provider -o src/WebhookLab.Provider -f net10.0
dotnet new xunit -n WebhookLab.Provider.Tests -o tests/WebhookLab.Provider.Tests -f net10.0
rm tests/WebhookLab.Provider.Tests/UnitTest1.cs
dotnet add src/WebhookLab.Provider reference src/WebhookLab.Contracts
dotnet add src/WebhookLab.Provider package Microsoft.EntityFrameworkCore.Sqlite --version 10.0.12
dotnet add tests/WebhookLab.Provider.Tests reference src/WebhookLab.Provider
dotnet add tests/WebhookLab.Provider.Tests package Microsoft.AspNetCore.Mvc.Testing --version 10.0.12
dotnet add tests/WebhookLab.Provider.Tests package Microsoft.Extensions.TimeProvider.Testing --version 10.10.0
dotnet sln add src/WebhookLab.Provider tests/WebhookLab.Provider.Tests
```

Expected: `dotnet build` succeeds; `WebhookLab.slnx` lists the two new projects under `/src/` and `/tests/`.

- [ ] **Step 2: Configuration files**

`src/WebhookLab.Provider/Properties/launchSettings.json` (replace the generated file):
```json
{
  "$schema": "https://json.schemastore.org/launchsettings.json",
  "profiles": {
    "http": {
      "commandName": "Project",
      "dotnetRunMessages": true,
      "launchBrowser": false,
      "applicationUrl": "http://localhost:5001",
      "environmentVariables": { "ASPNETCORE_ENVIRONMENT": "Development" }
    }
  }
}
```

`src/WebhookLab.Provider/appsettings.json` (replace):
```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.AspNetCore": "Warning",
      "Microsoft.EntityFrameworkCore": "Warning",
      "System.Net.Http.HttpClient": "Warning"
    }
  },
  "AllowedHosts": "*",
  "ConnectionStrings": {
    "Provider": "Data Source=provider.db"
  },
  "Provider": {
    "EnableBackgroundDispatch": true
  },
  "Webhook": {
    "TargetUrl": "http://localhost:5002/webhooks",
    "Timeout": "00:00:05"
  },
  "Dashboard": {
    "Origin": "http://localhost:4200"
  }
}
```

`src/WebhookLab.Provider/appsettings.Development.json` (replace):
```json
{
  "Webhook": {
    "Secret": "dev-secret-change-me"
  }
}
```

- [ ] **Step 3: Data model**

`src/WebhookLab.Provider/Data/OrderStatus.cs`:
```csharp
namespace WebhookLab.Provider.Data;

public enum OrderStatus
{
    Created,
    Paid,
    Refunded,
}
```

`src/WebhookLab.Provider/Data/Order.cs`:
```csharp
namespace WebhookLab.Provider.Data;

/// <summary>
/// The Provider's own record of an order — its source of truth. The Receiver only ever learns
/// about it through webhooks.
/// </summary>
public sealed class Order
{
    public int Id { get; init; }
    public decimal Amount { get; init; }
    public OrderStatus Status { get; set; } = OrderStatus.Created;
}
```

`src/WebhookLab.Provider/Data/WebhookEvent.cs`:
```csharp
namespace WebhookLab.Provider.Data;

/// <summary>Something that happened at the Provider, to be delivered to the Receiver as a webhook.</summary>
public sealed class WebhookEvent
{
    public required string Id { get; init; }
    public required string Type { get; init; }

    /// <summary>The exact JSON body. Every attempt and replay sends — and signs — this same string.</summary>
    public required string Payload { get; init; }

    public DateTime CreatedAt { get; init; }
}
```

`src/WebhookLab.Provider/Data/DeliveryStatus.cs`:
```csharp
namespace WebhookLab.Provider.Data;

public enum DeliveryStatus
{
    /// <summary>Not delivered yet; the dispatcher sends it once NextAttemptAt has passed.</summary>
    Pending,
    /// <summary>The Receiver answered 2xx.</summary>
    Succeeded,
    /// <summary>Gave up after Backoff.MaxAttempts failures (a "dead letter"). Can still be replayed.</summary>
    Dead,
}
```

`src/WebhookLab.Provider/Data/Delivery.cs`:
```csharp
namespace WebhookLab.Provider.Data;

/// <summary>
/// One job of "get this event to the Receiver". The outbox row the dispatcher works from.
/// A replay creates a new Delivery for the same event (same event id), with IsReplay = true.
/// </summary>
public sealed class Delivery
{
    public int Id { get; init; }
    public required string EventId { get; init; }
    public DeliveryStatus Status { get; set; } = DeliveryStatus.Pending;

    /// <summary>Attempts made so far, including the one that succeeded.</summary>
    public int AttemptCount { get; set; }

    public DateTime NextAttemptAt { get; set; }
    public bool IsReplay { get; init; }
    public DateTime CreatedAt { get; init; }
    public List<DeliveryAttempt> Attempts { get; } = [];
}
```

`src/WebhookLab.Provider/Data/DeliveryAttempt.cs`:
```csharp
namespace WebhookLab.Provider.Data;

/// <summary>One HTTP POST to the Receiver and what came back. The dashboard timeline is drawn from these.</summary>
public sealed class DeliveryAttempt
{
    public int Id { get; init; }
    public int DeliveryId { get; init; }
    public int AttemptNumber { get; init; }

    /// <summary>True for the extra copy sent by the DuplicateNext chaos setting.</summary>
    public bool IsDuplicateSend { get; init; }

    public DateTime StartedAt { get; init; }
    public long DurationMs { get; init; }

    /// <summary>Null when no response arrived (timeout, connection refused).</summary>
    public int? StatusCode { get; init; }

    public string? Error { get; init; }
}
```

`src/WebhookLab.Provider/Data/ProviderDbContext.cs`:
```csharp
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace WebhookLab.Provider.Data;

public sealed class ProviderDbContext(DbContextOptions<ProviderDbContext> options) : DbContext(options)
{
    public DbSet<Order> Orders => Set<Order>();
    public DbSet<WebhookEvent> Events => Set<WebhookEvent>();
    public DbSet<Delivery> Deliveries => Set<Delivery>();
    public DbSet<DeliveryAttempt> DeliveryAttempts => Set<DeliveryAttempt>();

    protected override void ConfigureConventions(ModelConfigurationBuilder configurationBuilder)
    {
        // Same fix as the Receiver: SQLite hands DateTime back as Unspecified, which JSON writes without
        // a trailing "Z". We only ever store UTC, so stamp the Kind back on read. Stored values are untouched.
        configurationBuilder.Properties<DateTime>().HaveConversion<UtcDateTimeConverter>();
    }

    private sealed class UtcDateTimeConverter() : ValueConverter<DateTime, DateTime>(
        v => v,
        v => DateTime.SpecifyKind(v, DateTimeKind.Utc));

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(order => order.Property(o => o.Status).HasConversion<string>());

        modelBuilder.Entity<WebhookEvent>(webhookEvent => webhookEvent.ToTable("Events"));

        modelBuilder.Entity<Delivery>(delivery =>
        {
            delivery.Property(d => d.Status).HasConversion<string>();
            // The dispatcher's "what's due?" query filters on these two columns every second.
            delivery.HasIndex(d => new { d.Status, d.NextAttemptAt });
            delivery.HasOne<WebhookEvent>().WithMany().HasForeignKey(d => d.EventId);
            delivery.HasMany(d => d.Attempts).WithOne().HasForeignKey(a => a.DeliveryId);
        });
    }
}
```

- [ ] **Step 4: Query endpoints**

`src/WebhookLab.Provider/Api/DeliveryViews.cs`:
```csharp
using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Api;

/// <summary>What GET /api/deliveries returns: a delivery, its event type, and every attempt in order.</summary>
public sealed record DeliveryView(
    int Id,
    string EventId,
    string EventType,
    DeliveryStatus Status,
    int AttemptCount,
    DateTime NextAttemptAt,
    bool IsReplay,
    DateTime CreatedAt,
    IReadOnlyList<AttemptView> Attempts)
{
    public static DeliveryView From(Delivery delivery, string eventType) => new(
        delivery.Id,
        delivery.EventId,
        eventType,
        delivery.Status,
        delivery.AttemptCount,
        delivery.NextAttemptAt,
        delivery.IsReplay,
        delivery.CreatedAt,
        delivery.Attempts
            .OrderBy(a => a.Id)
            .Select(a => new AttemptView(a.AttemptNumber, a.IsDuplicateSend, a.StartedAt, a.DurationMs, a.StatusCode, a.Error))
            .ToList());
}

public sealed record AttemptView(
    int AttemptNumber,
    bool IsDuplicateSend,
    DateTime StartedAt,
    long DurationMs,
    int? StatusCode,
    string? Error);
```

`src/WebhookLab.Provider/Api/QueryEndpoints.cs`:
```csharp
using Microsoft.EntityFrameworkCore;
using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Api;

public static class QueryEndpoints
{
    public static IEndpointRouteBuilder MapQueryEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/api/orders", async (ProviderDbContext db) =>
            await db.Orders.AsNoTracking().OrderBy(o => o.Id).ToListAsync());

        app.MapGet("/api/deliveries", async (ProviderDbContext db) =>
        {
            var deliveries = await db.Deliveries.AsNoTracking()
                .Include(d => d.Attempts)
                .OrderByDescending(d => d.Id)
                .ToListAsync();
            var eventTypes = await db.Events.AsNoTracking().ToDictionaryAsync(e => e.Id, e => e.Type);
            return deliveries.Select(d => DeliveryView.From(d, eventTypes[d.EventId]));
        });

        return app;
    }
}
```

- [ ] **Step 5: `Program.cs`**

`src/WebhookLab.Provider/Program.cs` (replace):
```csharp
using System.Text.Json.Serialization;
using Microsoft.EntityFrameworkCore;
using WebhookLab.Provider.Api;
using WebhookLab.Provider.Data;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<ProviderDbContext>(options =>
    options.UseSqlite(builder.Configuration.GetConnectionString("Provider")));
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.ConfigureHttpJsonOptions(options =>
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));
builder.Services.AddCors(options => options.AddDefaultPolicy(policy => policy
    .WithOrigins(builder.Configuration["Dashboard:Origin"] ?? "http://localhost:4200")
    .AllowAnyHeader()
    .AllowAnyMethod()
    .AllowCredentials()));

var app = builder.Build();

// A lab doesn't need migrations: create the schema if the database file is new. EnsureCreated never
// alters an existing provider.db, so after changing an entity's shape, delete the file and rerun.
using (var scope = app.Services.CreateScope())
    scope.ServiceProvider.GetRequiredService<ProviderDbContext>().Database.EnsureCreated();

app.UseCors();
app.MapQueryEndpoints();

app.Run();

public partial class Program;
```

- [ ] **Step 6: Test factory**

`tests/WebhookLab.Provider.Tests/ProviderFactory.cs`:
```csharp
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Time.Testing;
using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Tests;

/// <summary>
/// Runs the real Provider in memory against its own SQLite file. The background dispatcher is off,
/// so tests trigger dispatch passes explicitly, and the clock is fake, so backoff is instant.
/// </summary>
public sealed class ProviderFactory : WebApplicationFactory<Program>
{
    public const string Secret = "test-secret";

    /// <summary>Matches the app's JSON: camelCase, enums as strings.</summary>
    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter() },
    };

    private readonly string _databasePath = Path.Combine(Path.GetTempPath(), $"provider-test-{Guid.NewGuid():N}.db");

    public FakeTimeProvider Time { get; } = new(new DateTimeOffset(2026, 10, 8, 10, 0, 0, TimeSpan.Zero));

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("Testing");
        builder.UseSetting("ConnectionStrings:Provider", $"Data Source={_databasePath}");
        builder.UseSetting("Webhook:Secret", Secret);
        builder.UseSetting("Webhook:TargetUrl", "http://receiver.test/webhooks");
        builder.UseSetting("Provider:EnableBackgroundDispatch", "false");
        builder.ConfigureTestServices(services =>
        {
            services.RemoveAll<TimeProvider>();
            services.AddSingleton<TimeProvider>(Time);
        });
    }

    public async Task<T> WithDbAsync<T>(Func<ProviderDbContext, Task<T>> query)
    {
        using var scope = Services.CreateScope();
        return await query(scope.ServiceProvider.GetRequiredService<ProviderDbContext>());
    }

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        SqliteConnection.ClearAllPools();
        File.Delete(_databasePath);
    }
}
```

- [ ] **Step 7: Write the tests**

`tests/WebhookLab.Provider.Tests/QueryEndpointsTests.cs`:
```csharp
using System.Net.Http.Json;
using System.Text.Json;
using WebhookLab.Provider.Api;
using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Tests;

public class QueryEndpointsTests
{
    [Theory]
    [InlineData("/api/orders")]
    [InlineData("/api/deliveries")]
    public async Task A_fresh_provider_has_no_data(string path)
    {
        await using var lab = new ProviderFactory();

        var items = await lab.CreateClient().GetFromJsonAsync<List<JsonElement>>(path);

        Assert.NotNull(items);
        Assert.Empty(items);
    }

    [Fact]
    public async Task Deliveries_include_the_event_type_and_attempts_in_order()
    {
        await using var lab = new ProviderFactory();
        await lab.WithDbAsync(db =>
        {
            db.Events.Add(new WebhookEvent { Id = "evt_1", Type = "order.created", Payload = "{}" });
            var delivery = new Delivery { EventId = "evt_1", AttemptCount = 2, Status = DeliveryStatus.Succeeded };
            delivery.Attempts.Add(new DeliveryAttempt { AttemptNumber = 1, StatusCode = 500 });
            delivery.Attempts.Add(new DeliveryAttempt { AttemptNumber = 2, StatusCode = 200 });
            db.Deliveries.Add(delivery);
            return db.SaveChangesAsync();
        });

        var deliveries = await lab.CreateClient().GetFromJsonAsync<List<DeliveryView>>("/api/deliveries", ProviderFactory.Json);

        var view = Assert.Single(deliveries!);
        Assert.Equal("order.created", view.EventType);
        Assert.Equal(DeliveryStatus.Succeeded, view.Status);
        Assert.Equal(new int?[] { 500, 200 }, view.Attempts.Select(a => a.StatusCode));
    }
}
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `dotnet test`
Expected: PASS. Contracts 13, Receiver 37, Provider 3. A wrong schema or factory shows up as a SQLite "no such table" failure, which is what this step guards against.

- [ ] **Step 9: Commit**

```bash
git add WebhookLab.slnx src/WebhookLab.Provider tests/WebhookLab.Provider.Tests
git commit -m "feat: add provider skeleton with outbox data model and query API"
```

---

### Task 2: Chaos settings, reorder buffer, lab control API

**Files:**
- Create: `src/WebhookLab.Provider/Chaos/{ChaosSettings,ReorderBuffer}.cs`
- Create: `src/WebhookLab.Provider/Api/LabControlEndpoints.cs`
- Modify: `src/WebhookLab.Provider/Program.cs`, `tests/WebhookLab.Provider.Tests/ProviderFactory.cs`
- Test: `tests/WebhookLab.Provider.Tests/ChaosTests.cs`

**Interfaces:**
- Consumes: `ProviderDbContext` and entities (Task 1).
- Produces:
  - `record ChaosSnapshot(int DuplicateNext, int DelaySeconds, int ReorderNext, int BadSignatureNext)`
  - `ChaosSettings` (singleton): `Snapshot()`, `Apply(ChaosSnapshot)` (clamps negatives to 0), `Reset()`, `TimeSpan Delay`, `bool ReorderPending`, `TryConsumeDuplicate()`, `TryConsumeReorder()`, `TryConsumeBadSignature()`
  - `ReorderBuffer` (singleton): `Hold(int deliveryId)`, `IsHeld(int)`, `bool HasHeld`, `ReleaseReversed() : IReadOnlyList<int>`, `Clear()`
  - Endpoints: `GET /api/chaos`, `PUT /api/chaos`, `POST /api/reset` (204)
  - Test: `ProviderFactory.Chaos`, `ProviderFactory.Reorder`

- [ ] **Step 1: Factory helpers**

In `ProviderFactory.cs` add `using WebhookLab.Provider.Chaos;` and below `Time`:
```csharp
    public ChaosSettings Chaos => Services.GetRequiredService<ChaosSettings>();
    public ReorderBuffer Reorder => Services.GetRequiredService<ReorderBuffer>();
```

- [ ] **Step 2: Write the failing tests**

`tests/WebhookLab.Provider.Tests/ChaosTests.cs`:
```csharp
using System.Net;
using System.Net.Http.Json;
using Microsoft.EntityFrameworkCore;
using WebhookLab.Provider.Chaos;
using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Tests;

public class ChaosTests
{
    [Fact]
    public void One_shot_chaos_counts_down_to_zero()
    {
        var chaos = new ChaosSettings();
        chaos.Apply(new ChaosSnapshot(DuplicateNext: 2, DelaySeconds: 0, ReorderNext: 0, BadSignatureNext: 0));

        Assert.True(chaos.TryConsumeDuplicate());
        Assert.True(chaos.TryConsumeDuplicate());
        Assert.False(chaos.TryConsumeDuplicate());
    }

    [Fact]
    public void Negative_values_are_treated_as_off()
    {
        var chaos = new ChaosSettings();

        chaos.Apply(new ChaosSnapshot(DuplicateNext: -3, DelaySeconds: -1, ReorderNext: -2, BadSignatureNext: -1));

        Assert.Equal(new ChaosSnapshot(0, 0, 0, 0), chaos.Snapshot());
        Assert.Equal(TimeSpan.Zero, chaos.Delay);
    }

    [Fact]
    public void Reorder_buffer_releases_newest_first_and_empties()
    {
        var reorder = new ReorderBuffer();
        reorder.Hold(1);
        reorder.Hold(2);
        reorder.Hold(3);

        Assert.Equal([3, 2, 1], reorder.ReleaseReversed());
        Assert.False(reorder.HasHeld);
        Assert.False(reorder.IsHeld(1));
    }

    [Fact]
    public async Task Chaos_starts_cleared()
    {
        await using var lab = new ProviderFactory();

        var chaos = await lab.CreateClient().GetFromJsonAsync<ChaosSnapshot>("/api/chaos");

        Assert.Equal(new ChaosSnapshot(0, 0, 0, 0), chaos);
    }

    [Fact]
    public async Task Put_chaos_updates_the_settings()
    {
        await using var lab = new ProviderFactory();
        var client = lab.CreateClient();
        var update = new ChaosSnapshot(DuplicateNext: 1, DelaySeconds: 2, ReorderNext: 3, BadSignatureNext: 4);

        var response = await client.PutAsJsonAsync("/api/chaos", update);

        response.EnsureSuccessStatusCode();
        Assert.Equal(update, await client.GetFromJsonAsync<ChaosSnapshot>("/api/chaos"));
    }

    [Fact]
    public async Task Reset_clears_data_chaos_and_held_deliveries()
    {
        await using var lab = new ProviderFactory();
        await lab.WithDbAsync(db =>
        {
            db.Orders.Add(new Order { Amount = 50m });
            db.Events.Add(new WebhookEvent { Id = "evt_1", Type = "order.created", Payload = "{}" });
            var delivery = new Delivery { EventId = "evt_1" };
            delivery.Attempts.Add(new DeliveryAttempt { AttemptNumber = 1, StatusCode = 500 });
            db.Deliveries.Add(delivery);
            return db.SaveChangesAsync();
        });
        lab.Chaos.Apply(new ChaosSnapshot(1, 1, 1, 1));
        lab.Reorder.Hold(1);

        var response = await lab.CreateClient().PostAsync("/api/reset", null);

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
        Assert.Equal(0, await lab.WithDbAsync(db => db.Orders.CountAsync()));
        Assert.Equal(0, await lab.WithDbAsync(db => db.Events.CountAsync()));
        Assert.Equal(0, await lab.WithDbAsync(db => db.Deliveries.CountAsync()));
        Assert.Equal(0, await lab.WithDbAsync(db => db.DeliveryAttempts.CountAsync()));
        Assert.Equal(new ChaosSnapshot(0, 0, 0, 0), lab.Chaos.Snapshot());
        Assert.False(lab.Reorder.HasHeld);
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `dotnet test tests/WebhookLab.Provider.Tests --filter ChaosTests`
Expected: build FAILS with `The type or namespace name 'Chaos' does not exist in the namespace 'WebhookLab.Provider'`.

- [ ] **Step 4: Implement `ChaosSettings`**

`src/WebhookLab.Provider/Chaos/ChaosSettings.cs`:
```csharp
namespace WebhookLab.Provider.Chaos;

public sealed record ChaosSnapshot(int DuplicateNext, int DelaySeconds, int ReorderNext, int BadSignatureNext);

/// <summary>
/// Sending-side failures you can switch on from the dashboard — the Provider's mirror of the
/// Receiver's FaultSettings. Counters are one-shot ("DuplicateNext = 2" duplicates the next two
/// deliveries, then turns itself off); DelaySeconds applies to every send while it is set.
/// Thread-safe because the API and the dispatcher use it concurrently.
/// </summary>
public sealed class ChaosSettings
{
    private int _duplicateNext;
    private int _delaySeconds;
    private int _reorderNext;
    private int _badSignatureNext;

    /// <summary>How long to wait before each send.</summary>
    public TimeSpan Delay => TimeSpan.FromSeconds(Volatile.Read(ref _delaySeconds));

    /// <summary>True while ReorderNext is still collecting deliveries to hold back.</summary>
    public bool ReorderPending => Volatile.Read(ref _reorderNext) > 0;

    public ChaosSnapshot Snapshot() => new(
        Volatile.Read(ref _duplicateNext),
        Volatile.Read(ref _delaySeconds),
        Volatile.Read(ref _reorderNext),
        Volatile.Read(ref _badSignatureNext));

    public void Apply(ChaosSnapshot chaos)
    {
        // Negative numbers mean nothing here; store them as "off" so the API never reports nonsense.
        Volatile.Write(ref _duplicateNext, Math.Max(0, chaos.DuplicateNext));
        Volatile.Write(ref _delaySeconds, Math.Max(0, chaos.DelaySeconds));
        Volatile.Write(ref _reorderNext, Math.Max(0, chaos.ReorderNext));
        Volatile.Write(ref _badSignatureNext, Math.Max(0, chaos.BadSignatureNext));
    }

    public void Reset() => Apply(new ChaosSnapshot(0, 0, 0, 0));

    public bool TryConsumeDuplicate() => TryConsume(ref _duplicateNext);
    public bool TryConsumeReorder() => TryConsume(ref _reorderNext);
    public bool TryConsumeBadSignature() => TryConsume(ref _badSignatureNext);

    // Same compare-and-swap loop as the Receiver's FaultSettings: two threads can't both use the last one.
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

- [ ] **Step 5: Implement `ReorderBuffer`**

`src/WebhookLab.Provider/Chaos/ReorderBuffer.cs`:
```csharp
namespace WebhookLab.Provider.Chaos;

/// <summary>
/// Deliveries held back by the ReorderNext chaos setting. Once the last one is collected, the
/// dispatcher releases them all newest-first — so the Receiver sees events in the wrong order.
/// </summary>
public sealed class ReorderBuffer
{
    private readonly Lock _gate = new();
    private readonly List<int> _held = [];

    public bool HasHeld
    {
        get { lock (_gate) return _held.Count > 0; }
    }

    public void Hold(int deliveryId)
    {
        lock (_gate) _held.Add(deliveryId);
    }

    public bool IsHeld(int deliveryId)
    {
        lock (_gate) return _held.Contains(deliveryId);
    }

    /// <summary>Returns the held delivery ids, newest first, and empties the buffer.</summary>
    public IReadOnlyList<int> ReleaseReversed()
    {
        lock (_gate)
        {
            var released = new List<int>(_held);
            released.Reverse();
            _held.Clear();
            return released;
        }
    }

    public void Clear()
    {
        lock (_gate) _held.Clear();
    }
}
```

- [ ] **Step 6: Lab control endpoints**

`src/WebhookLab.Provider/Api/LabControlEndpoints.cs`:
```csharp
using Microsoft.EntityFrameworkCore;
using WebhookLab.Provider.Chaos;
using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Api;

public static class LabControlEndpoints
{
    public static IEndpointRouteBuilder MapLabControlEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/api/chaos", (ChaosSettings chaos) => chaos.Snapshot());

        app.MapPut("/api/chaos", (ChaosSnapshot update, ChaosSettings chaos) =>
        {
            chaos.Apply(update);
            return chaos.Snapshot();
        });

        app.MapPost("/api/reset", async (ProviderDbContext db, ChaosSettings chaos, ReorderBuffer reorder) =>
        {
            // One transaction so a reset can't be observed half-done. Children before parents:
            // SQLite enforces the foreign keys (attempts → deliveries → events).
            await using var transaction = await db.Database.BeginTransactionAsync();
            await db.DeliveryAttempts.ExecuteDeleteAsync();
            await db.Deliveries.ExecuteDeleteAsync();
            await db.Events.ExecuteDeleteAsync();
            await db.Orders.ExecuteDeleteAsync();
            await transaction.CommitAsync();
            chaos.Reset();
            reorder.Clear();
            return Results.NoContent();
        });

        return app;
    }
}
```

- [ ] **Step 7: Register in `Program.cs`**

Add `using WebhookLab.Provider.Chaos;`. After `builder.Services.AddSingleton(TimeProvider.System);` add:
```csharp
builder.Services.AddSingleton<ChaosSettings>();
builder.Services.AddSingleton<ReorderBuffer>();
```
After `app.MapQueryEndpoints();` add:
```csharp
app.MapLabControlEndpoints();
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `dotnet test tests/WebhookLab.Provider.Tests`
Expected: PASS, 9 tests.

- [ ] **Step 9: Commit**

```bash
git add src/WebhookLab.Provider tests/WebhookLab.Provider.Tests
git commit -m "feat: add provider chaos settings, reorder buffer, and lab control API"
```

---

### Task 3: Orders (outbox writes) + realtime notifier

**Files:**
- Create: `src/WebhookLab.Provider/Realtime/{ProviderHub,ProviderMessages,ProviderNotifier}.cs`
- Create: `src/WebhookLab.Provider/Orders/OrderService.cs`
- Create: `src/WebhookLab.Provider/Api/OrderEndpoints.cs`
- Modify: `src/WebhookLab.Provider/Program.cs`, `tests/WebhookLab.Provider.Tests/ProviderFactory.cs`
- Test: `tests/WebhookLab.Provider.Tests/OrderEndpointsTests.cs`

**Interfaces:**
- Consumes: data model (Task 1); `WebhookEnvelope`, `OrderEventData`, `EventTypes`, `WebhookJson` (Contracts).
- Produces:
  - `ProviderNotifier` (singleton): `EventCreatedAsync(WebhookEvent, int orderId)`, `AttemptStartedAsync(Delivery, int attemptNumber, bool isDuplicateSend)`, `AttemptCompletedAsync(Delivery, int attemptNumber, int? statusCode, string? error, long durationMs)`, `DeliveryStatusChangedAsync(Delivery)`
  - Hub `/hubs/provider`; messages `EventCreated`, `AttemptStarted`, `AttemptCompleted`, `DeliveryStatusChanged`
  - `enum OrderActionStatus { Done, NotFound, InvalidState }`; `record OrderActionResult(OrderActionStatus Status, Order? Order = null, string? EventId = null)`
  - `OrderService` (scoped): `CreateAsync(decimal) / PayAsync(int) / RefundAsync(int) : Task<OrderActionResult>`
  - `record CreateOrderRequest(decimal Amount)`; `record OrderActionResponse(int OrderId, OrderStatus Status, string EventId)`
  - Endpoints: `POST /api/orders` (201 / 400), `POST /api/orders/{id}/pay`, `POST /api/orders/{id}/refund` (200 / 404 / 409)
  - Test: `ProviderFactory.CreateOrderAsync(decimal amount = 50m) : Task<OrderActionResponse>`, `ProviderFactory.PayAsync(int orderId) : Task<OrderActionResponse>`

- [ ] **Step 1: Factory helpers**

In `ProviderFactory.cs` add usings `System.Net.Http.Json` and `WebhookLab.Provider.Api`, and:
```csharp
    public async Task<OrderActionResponse> CreateOrderAsync(decimal amount = 50m)
    {
        var response = await CreateClient().PostAsJsonAsync("/api/orders", new CreateOrderRequest(amount));
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<OrderActionResponse>(Json))!;
    }

    public async Task<OrderActionResponse> PayAsync(int orderId)
    {
        var response = await CreateClient().PostAsync($"/api/orders/{orderId}/pay", null);
        response.EnsureSuccessStatusCode();
        return (await response.Content.ReadFromJsonAsync<OrderActionResponse>(Json))!;
    }
```

- [ ] **Step 2: Write the failing tests**

`tests/WebhookLab.Provider.Tests/OrderEndpointsTests.cs`:
```csharp
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using WebhookLab.Contracts;
using WebhookLab.Provider.Api;
using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Tests;

public class OrderEndpointsTests
{
    [Fact]
    public async Task Create_saves_the_order_and_queues_an_order_created_webhook()
    {
        await using var lab = new ProviderFactory();

        var response = await lab.CreateClient().PostAsJsonAsync("/api/orders", new CreateOrderRequest(50m));

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        var created = (await response.Content.ReadFromJsonAsync<OrderActionResponse>(ProviderFactory.Json))!;
        Assert.Equal(OrderStatus.Created, created.Status);

        var webhookEvent = await lab.WithDbAsync(db => db.Events.SingleAsync(e => e.Id == created.EventId));
        Assert.Equal(EventTypes.OrderCreated, webhookEvent.Type);
        var envelope = JsonSerializer.Deserialize<WebhookEnvelope>(webhookEvent.Payload, WebhookJson.Options)!;
        Assert.Equal(created.EventId, envelope.Id);
        Assert.Equal(new OrderEventData(created.OrderId, 50m), envelope.Data);

        var delivery = await lab.WithDbAsync(db => db.Deliveries.SingleAsync(d => d.EventId == created.EventId));
        Assert.Equal(DeliveryStatus.Pending, delivery.Status);
        Assert.Equal(0, delivery.AttemptCount);
        Assert.False(delivery.IsReplay);
        Assert.Equal(lab.Time.GetUtcNow().UtcDateTime, delivery.NextAttemptAt);
    }

    [Fact]
    public async Task Pay_marks_the_order_paid_and_queues_payment_succeeded()
    {
        await using var lab = new ProviderFactory();
        var order = await lab.CreateOrderAsync();

        var paid = await lab.PayAsync(order.OrderId);

        Assert.Equal(OrderStatus.Paid, paid.Status);
        Assert.NotEqual(order.EventId, paid.EventId);
        Assert.Equal(EventTypes.PaymentSucceeded, (await lab.WithDbAsync(db => db.Events.SingleAsync(e => e.Id == paid.EventId))).Type);
        Assert.Equal(1, await lab.WithDbAsync(db => db.Deliveries.CountAsync(d => d.EventId == paid.EventId)));
    }

    [Fact]
    public async Task Refund_after_payment_queues_payment_refunded()
    {
        await using var lab = new ProviderFactory();
        var order = await lab.CreateOrderAsync();
        await lab.PayAsync(order.OrderId);

        var response = await lab.CreateClient().PostAsync($"/api/orders/{order.OrderId}/refund", null);

        response.EnsureSuccessStatusCode();
        var refunded = (await response.Content.ReadFromJsonAsync<OrderActionResponse>(ProviderFactory.Json))!;
        Assert.Equal(OrderStatus.Refunded, refunded.Status);
        Assert.Equal(EventTypes.PaymentRefunded, (await lab.WithDbAsync(db => db.Events.SingleAsync(e => e.Id == refunded.EventId))).Type);
    }

    [Fact]
    public async Task Paying_twice_is_a_conflict_and_queues_nothing()
    {
        await using var lab = new ProviderFactory();
        var order = await lab.CreateOrderAsync();
        await lab.PayAsync(order.OrderId);

        var second = await lab.CreateClient().PostAsync($"/api/orders/{order.OrderId}/pay", null);

        Assert.Equal(HttpStatusCode.Conflict, second.StatusCode);
        Assert.Equal(2, await lab.WithDbAsync(db => db.Events.CountAsync()));
        Assert.Equal(2, await lab.WithDbAsync(db => db.Deliveries.CountAsync()));
    }

    [Fact]
    public async Task Refunding_an_unpaid_order_is_a_conflict()
    {
        await using var lab = new ProviderFactory();
        var order = await lab.CreateOrderAsync();

        var response = await lab.CreateClient().PostAsync($"/api/orders/{order.OrderId}/refund", null);

        Assert.Equal(HttpStatusCode.Conflict, response.StatusCode);
        Assert.Equal(OrderStatus.Created, (await lab.WithDbAsync(db => db.Orders.SingleAsync())).Status);
    }

    [Fact]
    public async Task Unknown_order_is_not_found()
    {
        await using var lab = new ProviderFactory();

        var response = await lab.CreateClient().PostAsync("/api/orders/999/pay", null);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public async Task Non_positive_amount_is_rejected(int amount)
    {
        await using var lab = new ProviderFactory();

        var response = await lab.CreateClient().PostAsJsonAsync("/api/orders", new CreateOrderRequest(amount));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Equal(0, await lab.WithDbAsync(db => db.Orders.CountAsync()));
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `dotnet test tests/WebhookLab.Provider.Tests --filter OrderEndpointsTests`
Expected: build FAILS with `The type or namespace name 'CreateOrderRequest' could not be found`.

- [ ] **Step 4: Realtime notifier**

`src/WebhookLab.Provider/Realtime/ProviderHub.cs`:
```csharp
using Microsoft.AspNetCore.SignalR;

namespace WebhookLab.Provider.Realtime;

/// <summary>Dashboard-only live feed. Webhook delivery never depends on it.</summary>
public sealed class ProviderHub : Hub;
```

`src/WebhookLab.Provider/Realtime/ProviderMessages.cs`:
```csharp
using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Realtime;

// Every message carries EventId + At so the dashboard can merge both services into one timeline.
public sealed record EventCreatedMessage(string EventId, DateTimeOffset At, string Type, int OrderId);

public sealed record AttemptStartedMessage(
    string EventId, DateTimeOffset At, int DeliveryId, int AttemptNumber, bool IsReplay, bool IsDuplicateSend);

public sealed record AttemptCompletedMessage(
    string EventId, DateTimeOffset At, int DeliveryId, int AttemptNumber, int? StatusCode, string? Error, long DurationMs);

public sealed record DeliveryStatusChangedMessage(
    string EventId, DateTimeOffset At, int DeliveryId, DeliveryStatus Status, DateTime? NextAttemptAt);
```

`src/WebhookLab.Provider/Realtime/ProviderNotifier.cs`:
```csharp
using Microsoft.AspNetCore.SignalR;
using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Realtime;

public sealed class ProviderNotifier(IHubContext<ProviderHub> hub, TimeProvider time)
{
    public Task EventCreatedAsync(WebhookEvent webhookEvent, int orderId) =>
        hub.Clients.All.SendAsync("EventCreated", new EventCreatedMessage(
            webhookEvent.Id, time.GetUtcNow(), webhookEvent.Type, orderId));

    public Task AttemptStartedAsync(Delivery delivery, int attemptNumber, bool isDuplicateSend) =>
        hub.Clients.All.SendAsync("AttemptStarted", new AttemptStartedMessage(
            delivery.EventId, time.GetUtcNow(), delivery.Id, attemptNumber, delivery.IsReplay, isDuplicateSend));

    public Task AttemptCompletedAsync(Delivery delivery, int attemptNumber, int? statusCode, string? error, long durationMs) =>
        hub.Clients.All.SendAsync("AttemptCompleted", new AttemptCompletedMessage(
            delivery.EventId, time.GetUtcNow(), delivery.Id, attemptNumber, statusCode, error, durationMs));

    public Task DeliveryStatusChangedAsync(Delivery delivery) =>
        hub.Clients.All.SendAsync("DeliveryStatusChanged", new DeliveryStatusChangedMessage(
            delivery.EventId,
            time.GetUtcNow(),
            delivery.Id,
            delivery.Status,
            delivery.Status == DeliveryStatus.Pending ? delivery.NextAttemptAt : null));
}
```

- [ ] **Step 5: Order service**

`src/WebhookLab.Provider/Orders/OrderService.cs`:
```csharp
using System.Text.Json;
using WebhookLab.Contracts;
using WebhookLab.Provider.Data;
using WebhookLab.Provider.Realtime;

namespace WebhookLab.Provider.Orders;

public enum OrderActionStatus
{
    Done,
    NotFound,
    /// <summary>The order isn't in the state this action needs (e.g. refunding an unpaid order).</summary>
    InvalidState,
}

public sealed record OrderActionResult(OrderActionStatus Status, Order? Order = null, string? EventId = null);

/// <summary>
/// Changes an order and, in the SAME transaction, records the webhook event plus a Pending delivery
/// (the outbox pattern). All three are saved or none are: an order change can't happen without its
/// webhook, and no webhook can describe a change that didn't happen. Sending is the dispatcher's job.
/// </summary>
public sealed class OrderService(ProviderDbContext db, TimeProvider time, ProviderNotifier notifier)
{
    public async Task<OrderActionResult> CreateAsync(decimal amount)
    {
        await using var transaction = await db.Database.BeginTransactionAsync();
        var order = new Order { Amount = amount };
        db.Orders.Add(order);
        await db.SaveChangesAsync(); // assigns order.Id, which the event payload needs

        var webhookEvent = AddEventWithDelivery(EventTypes.OrderCreated, order);
        await db.SaveChangesAsync();
        await transaction.CommitAsync();

        await notifier.EventCreatedAsync(webhookEvent, order.Id);
        return new OrderActionResult(OrderActionStatus.Done, order, webhookEvent.Id);
    }

    public Task<OrderActionResult> PayAsync(int orderId) =>
        TransitionAsync(orderId, from: OrderStatus.Created, to: OrderStatus.Paid, EventTypes.PaymentSucceeded);

    public Task<OrderActionResult> RefundAsync(int orderId) =>
        TransitionAsync(orderId, from: OrderStatus.Paid, to: OrderStatus.Refunded, EventTypes.PaymentRefunded);

    private async Task<OrderActionResult> TransitionAsync(int orderId, OrderStatus from, OrderStatus to, string eventType)
    {
        // Returning early without CommitAsync disposes the transaction, which rolls it back: nothing changes.
        await using var transaction = await db.Database.BeginTransactionAsync();
        var order = await db.Orders.FindAsync(orderId);
        if (order is null)
            return new OrderActionResult(OrderActionStatus.NotFound);
        if (order.Status != from)
            return new OrderActionResult(OrderActionStatus.InvalidState, order);

        order.Status = to;
        var webhookEvent = AddEventWithDelivery(eventType, order);
        await db.SaveChangesAsync();
        await transaction.CommitAsync();

        await notifier.EventCreatedAsync(webhookEvent, order.Id);
        return new OrderActionResult(OrderActionStatus.Done, order, webhookEvent.Id);
    }

    private WebhookEvent AddEventWithDelivery(string type, Order order)
    {
        var now = time.GetUtcNow();
        var envelope = new WebhookEnvelope($"evt_{Guid.NewGuid():N}", type, now, new OrderEventData(order.Id, order.Amount));
        var webhookEvent = new WebhookEvent
        {
            Id = envelope.Id,
            Type = type,
            Payload = JsonSerializer.Serialize(envelope, WebhookJson.Options),
            CreatedAt = now.UtcDateTime,
        };
        db.Events.Add(webhookEvent);
        db.Deliveries.Add(new Delivery { EventId = webhookEvent.Id, NextAttemptAt = now.UtcDateTime, CreatedAt = now.UtcDateTime });
        return webhookEvent;
    }
}
```

- [ ] **Step 6: Order endpoints**

`src/WebhookLab.Provider/Api/OrderEndpoints.cs`:
```csharp
using WebhookLab.Provider.Data;
using WebhookLab.Provider.Orders;

namespace WebhookLab.Provider.Api;

public sealed record CreateOrderRequest(decimal Amount);

public sealed record OrderActionResponse(int OrderId, OrderStatus Status, string EventId);

public static class OrderEndpoints
{
    public static IEndpointRouteBuilder MapOrderEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapPost("/api/orders", async (CreateOrderRequest request, OrderService orders) =>
        {
            if (request.Amount <= 0)
                return Results.BadRequest("Amount must be greater than zero.");

            var result = await orders.CreateAsync(request.Amount);
            return Results.Created($"/api/orders/{result.Order!.Id}", ToResponse(result));
        });

        app.MapPost("/api/orders/{id:int}/pay", async (int id, OrderService orders) =>
            ToHttpResult(await orders.PayAsync(id)));

        app.MapPost("/api/orders/{id:int}/refund", async (int id, OrderService orders) =>
            ToHttpResult(await orders.RefundAsync(id)));

        return app;
    }

    private static IResult ToHttpResult(OrderActionResult result) => result.Status switch
    {
        OrderActionStatus.Done => Results.Ok(ToResponse(result)),
        OrderActionStatus.NotFound => Results.NotFound(),
        _ => Results.Conflict($"Order is {result.Order!.Status}; that action isn't allowed now."),
    };

    private static OrderActionResponse ToResponse(OrderActionResult result) =>
        new(result.Order!.Id, result.Order.Status, result.EventId!);
}
```

- [ ] **Step 7: Register in `Program.cs`**

Add usings `WebhookLab.Provider.Orders` and `WebhookLab.Provider.Realtime`. After `builder.Services.AddSingleton<ReorderBuffer>();` add:
```csharp
builder.Services.AddScoped<OrderService>();
builder.Services.AddSingleton<ProviderNotifier>();
builder.Services.AddSignalR().AddJsonProtocol(options =>
    options.PayloadSerializerOptions.Converters.Add(new JsonStringEnumConverter()));
```
After `app.MapLabControlEndpoints();` add:
```csharp
app.MapOrderEndpoints();
app.MapHub<ProviderHub>("/hubs/provider");
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `dotnet test tests/WebhookLab.Provider.Tests`
Expected: PASS, 17 tests.

- [ ] **Step 9: Commit**

```bash
git add src/WebhookLab.Provider tests/WebhookLab.Provider.Tests
git commit -m "feat: add order actions that write event and delivery in one transaction"
```

---

### Task 4: Webhook sender (sign + POST one webhook)

**Files:**
- Create: `src/WebhookLab.Provider/Dispatch/{WebhookOptions,WebhookSender}.cs`
- Modify: `src/WebhookLab.Provider/Program.cs`
- Test: `tests/WebhookLab.Provider.Tests/{FakeReceiver,WebhookSenderTests}.cs`

**Interfaces:**
- Consumes: `WebhookEvent` (Task 1); `WebhookSignature`, `WebhookHeaders` (Contracts).
- Produces:
  - `WebhookOptions { string Secret; string TargetUrl; TimeSpan Timeout = 5 s }` bound from `Webhook`
  - `record SendResult(int? StatusCode, string? Error, long DurationMs) { bool IsSuccess }`
  - `WebhookSender` (typed `HttpClient`): `SendAsync(WebhookEvent, bool useWrongSecret, CancellationToken) : Task<SendResult>`. It never throws for delivery problems.
  - Test: `FakeReceiver : HttpMessageHandler` with `Received`, `RespondWith(params HttpStatusCode[])`, `TimeOutNext(int count = 1)`, `RefuseConnectionNext()`; `record ReceivedWebhook(Uri Url, string Id, string Timestamp, string Signature, string Body) { IsSignedWith(string secret, DateTimeOffset now) }`

- [ ] **Step 1: Fake receiver**

`tests/WebhookLab.Provider.Tests/FakeReceiver.cs`:
```csharp
using System.Collections.Concurrent;
using System.Net;
using WebhookLab.Contracts;

namespace WebhookLab.Provider.Tests;

/// <summary>
/// Stands in for the Receiver: records every webhook that arrives and answers from a script
/// (200 OK once the script runs out). Timeouts and refused connections are scripted as exceptions,
/// exactly what HttpClient throws for them.
/// </summary>
public sealed class FakeReceiver : HttpMessageHandler
{
    private readonly ConcurrentQueue<Func<HttpResponseMessage>> _script = new();
    private readonly ConcurrentQueue<ReceivedWebhook> _received = new();

    public IReadOnlyList<ReceivedWebhook> Received => _received.ToArray();

    public void RespondWith(params HttpStatusCode[] statuses)
    {
        foreach (var status in statuses)
            _script.Enqueue(() => new HttpResponseMessage(status));
    }

    public void TimeOutNext(int count = 1)
    {
        for (var i = 0; i < count; i++)
            _script.Enqueue(() => throw new TaskCanceledException("Simulated timeout"));
    }

    public void RefuseConnectionNext() =>
        _script.Enqueue(() => throw new HttpRequestException("Connection refused (simulated)"));

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var body = await request.Content!.ReadAsStringAsync(cancellationToken);
        _received.Enqueue(new ReceivedWebhook(
            request.RequestUri!,
            Header(request, WebhookHeaders.Id),
            Header(request, WebhookHeaders.Timestamp),
            Header(request, WebhookHeaders.Signature),
            body));

        return _script.TryDequeue(out var next) ? next() : new HttpResponseMessage(HttpStatusCode.OK);
    }

    private static string Header(HttpRequestMessage request, string name) => request.Headers.GetValues(name).Single();
}

public sealed record ReceivedWebhook(Uri Url, string Id, string Timestamp, string Signature, string Body)
{
    public bool IsSignedWith(string secret, DateTimeOffset now) =>
        WebhookSignature.Verify(secret, Id, Timestamp, Body, Signature, now);
}
```

- [ ] **Step 2: Write the failing tests**

`tests/WebhookLab.Provider.Tests/WebhookSenderTests.cs`:
```csharp
using System.Net;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using WebhookLab.Provider.Data;
using WebhookLab.Provider.Dispatch;

namespace WebhookLab.Provider.Tests;

public class WebhookSenderTests
{
    private const string Secret = "sender-test-secret";
    private const string TargetUrl = "http://receiver.test/webhooks";

    private readonly FakeReceiver _receiver = new();
    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 10, 8, 10, 0, 0, TimeSpan.Zero));
    private readonly WebhookEvent _event = new()
    {
        Id = "evt_123",
        Type = "order.created",
        Payload = """{"id":"evt_123","type":"order.created","createdAt":"2026-10-08T10:00:00+00:00","data":{"orderId":1,"amount":50}}""",
    };

    private WebhookSender CreateSender() => new(
        new HttpClient(_receiver),
        Options.Create(new WebhookOptions { Secret = Secret, TargetUrl = TargetUrl }),
        _time);

    [Fact]
    public async Task Sends_the_exact_payload_signed_to_the_target_url()
    {
        var result = await CreateSender().SendAsync(_event, useWrongSecret: false, CancellationToken.None);

        Assert.True(result.IsSuccess);
        Assert.Equal(200, result.StatusCode);
        var received = Assert.Single(_receiver.Received);
        Assert.Equal(new Uri(TargetUrl), received.Url);
        Assert.Equal(_event.Id, received.Id);
        Assert.Equal(_event.Payload, received.Body);
        Assert.True(received.IsSignedWith(Secret, _time.GetUtcNow()));
    }

    [Fact]
    public async Task Non_2xx_response_is_a_failure_with_its_status_code()
    {
        _receiver.RespondWith(HttpStatusCode.InternalServerError);

        var result = await CreateSender().SendAsync(_event, useWrongSecret: false, CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Equal(500, result.StatusCode);
    }

    [Fact]
    public async Task Timeout_is_a_failure_with_no_status_code()
    {
        _receiver.TimeOutNext();

        var result = await CreateSender().SendAsync(_event, useWrongSecret: false, CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Null(result.StatusCode);
        Assert.Contains("Timed out", result.Error);
    }

    [Fact]
    public async Task Refused_connection_is_a_failure_with_no_status_code()
    {
        _receiver.RefuseConnectionNext();

        var result = await CreateSender().SendAsync(_event, useWrongSecret: false, CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Null(result.StatusCode);
        Assert.Contains("Connection refused", result.Error);
    }

    [Fact]
    public async Task Wrong_secret_produces_a_signature_the_receiver_rejects()
    {
        await CreateSender().SendAsync(_event, useWrongSecret: true, CancellationToken.None);

        Assert.False(Assert.Single(_receiver.Received).IsSignedWith(Secret, _time.GetUtcNow()));
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `dotnet test tests/WebhookLab.Provider.Tests --filter WebhookSenderTests`
Expected: build FAILS with `The type or namespace name 'Dispatch' does not exist in the namespace 'WebhookLab.Provider'`.

- [ ] **Step 4: Options**

`src/WebhookLab.Provider/Dispatch/WebhookOptions.cs`:
```csharp
namespace WebhookLab.Provider.Dispatch;

public sealed class WebhookOptions
{
    public const string SectionName = "Webhook";

    /// <summary>Shared with the Receiver. Used to sign every webhook.</summary>
    public string Secret { get; set; } = "";

    /// <summary>The Receiver's webhook URL — the one address it registered with us.</summary>
    public string TargetUrl { get; set; } = "";

    /// <summary>How long to wait for the Receiver's answer before counting the attempt as failed.</summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(5);
}
```

- [ ] **Step 5: Sender**

`src/WebhookLab.Provider/Dispatch/WebhookSender.cs`:
```csharp
using System.Text;
using Microsoft.Extensions.Options;
using WebhookLab.Contracts;
using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Dispatch;

/// <param name="StatusCode">The Receiver's HTTP status, or null when no response arrived.</param>
public sealed record SendResult(int? StatusCode, string? Error, long DurationMs)
{
    public bool IsSuccess => StatusCode is >= 200 and < 300;
}

/// <summary>
/// Signs and POSTs one webhook. Delivery problems never throw: a timeout, a refused connection and
/// a 500 all come back as a SendResult, because to the dispatcher they mean the same thing —
/// "not delivered, retry later".
/// </summary>
public sealed class WebhookSender(HttpClient http, IOptions<WebhookOptions> options, TimeProvider time)
{
    public async Task<SendResult> SendAsync(WebhookEvent webhookEvent, bool useWrongSecret, CancellationToken ct)
    {
        var settings = options.Value;
        // A fresh timestamp per attempt: retries must still pass the Receiver's 5-minute freshness check.
        var timestamp = time.GetUtcNow().ToUnixTimeSeconds();
        var secret = useWrongSecret ? "not-the-shared-secret" : settings.Secret;

        using var request = new HttpRequestMessage(HttpMethod.Post, settings.TargetUrl)
        {
            Content = new StringContent(webhookEvent.Payload, Encoding.UTF8, "application/json"),
        };
        request.Headers.Add(WebhookHeaders.Id, webhookEvent.Id);
        request.Headers.Add(WebhookHeaders.Timestamp, timestamp.ToString());
        request.Headers.Add(WebhookHeaders.Signature, WebhookSignature.Sign(secret, webhookEvent.Id, timestamp, webhookEvent.Payload));

        var startedAt = time.GetTimestamp();
        try
        {
            using var response = await http.SendAsync(request, ct);
            return new SendResult((int)response.StatusCode, null, ElapsedMs(startedAt));
        }
        catch (TaskCanceledException) when (!ct.IsCancellationRequested)
        {
            // HttpClient.Timeout expired (not our own shutdown): the Receiver never answered in time.
            return new SendResult(null, $"Timed out after {settings.Timeout.TotalSeconds:0} s", ElapsedMs(startedAt));
        }
        catch (HttpRequestException ex)
        {
            return new SendResult(null, ex.Message, ElapsedMs(startedAt));
        }
    }

    private long ElapsedMs(long startedAt) => (long)time.GetElapsedTime(startedAt).TotalMilliseconds;
}
```

- [ ] **Step 6: Register in `Program.cs`**

Add `using Microsoft.Extensions.Options;` and `using WebhookLab.Provider.Dispatch;`. After `builder.Services.AddScoped<OrderService>();` add:
```csharp
builder.Services.AddOptions<WebhookOptions>()
    .Bind(builder.Configuration.GetSection(WebhookOptions.SectionName))
    .Validate(o => !string.IsNullOrWhiteSpace(o.Secret), "Webhook:Secret must be configured.")
    .Validate(o => Uri.IsWellFormedUriString(o.TargetUrl, UriKind.Absolute), "Webhook:TargetUrl must be an absolute URL.")
    .ValidateOnStart();
builder.Services.AddHttpClient<WebhookSender>((services, client) =>
    client.Timeout = services.GetRequiredService<IOptions<WebhookOptions>>().Value.Timeout);
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `dotnet test tests/WebhookLab.Provider.Tests`
Expected: PASS, 22 tests.

- [ ] **Step 8: Commit**

```bash
git add src/WebhookLab.Provider tests/WebhookLab.Provider.Tests
git commit -m "feat: add webhook sender with signing, timeout, and failure results"
```

---

### Task 5: Delivery dispatcher (retries, dead letters, chaos) + background worker

**Files:**
- Create: `src/WebhookLab.Provider/Dispatch/{DeliveryDispatcher,DeliveryDispatcherWorker}.cs`
- Modify: `src/WebhookLab.Provider/Program.cs`, `tests/WebhookLab.Provider.Tests/ProviderFactory.cs`
- Test: `tests/WebhookLab.Provider.Tests/{DeliveryDispatcherTests,DispatcherChaosTests}.cs`

**Interfaces:**
- Consumes: data model (Task 1); `ChaosSettings`, `ReorderBuffer` (Task 2); `ProviderNotifier` (Task 3); `WebhookSender`, `SendResult` (Task 4); `Backoff` (Contracts).
- Produces:
  - `DeliveryDispatcher` (scoped): `DispatchDueAsync(CancellationToken ct = default) : Task<int>`. It sends due `Pending` deliveries oldest-first and returns how many were sent.
  - `DeliveryDispatcherWorker` (`BackgroundService`, 1 s loop), registered only when `Provider:EnableBackgroundDispatch` is true.
  - Test: `ProviderFactory.Receiver` (`FakeReceiver` wired in as the sender's HTTP handler), `DispatchAsync() : Task<int>`, `GetDeliveriesAsync(string eventId) : Task<List<Delivery>>` (attempts included, ordered by id)

- [ ] **Step 1: Factory helpers**

In `ProviderFactory.cs` add usings `Microsoft.EntityFrameworkCore` and `WebhookLab.Provider.Dispatch`. Add the property below `Time`:
```csharp
    public FakeReceiver Receiver { get; } = new();
```
Inside `ConfigureTestServices(services => { ... })`, after the `TimeProvider` lines, add:
```csharp
            // Every webhook the Provider sends lands in the FakeReceiver instead of the network.
            services.AddHttpClient<WebhookSender>()
                .ConfigurePrimaryHttpMessageHandler(() => Receiver)
                .SetHandlerLifetime(Timeout.InfiniteTimeSpan);
```
And these methods:
```csharp
    /// <summary>Runs one pass of the dispatcher, like one tick of the background worker.</summary>
    public async Task<int> DispatchAsync()
    {
        using var scope = Services.CreateScope();
        return await scope.ServiceProvider.GetRequiredService<DeliveryDispatcher>().DispatchDueAsync();
    }

    public Task<List<Delivery>> GetDeliveriesAsync(string eventId) =>
        WithDbAsync(db => db.Deliveries.AsNoTracking()
            .Include(d => d.Attempts.OrderBy(a => a.Id))
            .Where(d => d.EventId == eventId)
            .OrderBy(d => d.Id)
            .ToListAsync());
```

- [ ] **Step 2: Write the failing dispatcher tests**

`tests/WebhookLab.Provider.Tests/DeliveryDispatcherTests.cs`:
```csharp
using System.Net;
using System.Text.Json;
using WebhookLab.Contracts;
using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Tests;

public class DeliveryDispatcherTests
{
    [Fact]
    public async Task Due_delivery_is_sent_signed_and_marked_succeeded()
    {
        await using var lab = new ProviderFactory();
        var order = await lab.CreateOrderAsync();

        Assert.Equal(1, await lab.DispatchAsync());

        var received = Assert.Single(lab.Receiver.Received);
        Assert.Equal(order.EventId, received.Id);
        Assert.True(received.IsSignedWith(ProviderFactory.Secret, lab.Time.GetUtcNow()));
        var delivery = Assert.Single(await lab.GetDeliveriesAsync(order.EventId));
        Assert.Equal(DeliveryStatus.Succeeded, delivery.Status);
        Assert.Equal(1, delivery.AttemptCount);
        Assert.Equal(200, Assert.Single(delivery.Attempts).StatusCode);
    }

    [Fact]
    public async Task Failed_delivery_is_retried_after_its_backoff_not_before()
    {
        await using var lab = new ProviderFactory();
        lab.Receiver.RespondWith(HttpStatusCode.InternalServerError);
        var order = await lab.CreateOrderAsync();
        var sentAt = lab.Time.GetUtcNow().UtcDateTime;

        await lab.DispatchAsync();

        var delivery = Assert.Single(await lab.GetDeliveriesAsync(order.EventId));
        Assert.Equal(DeliveryStatus.Pending, delivery.Status);
        Assert.Equal(1, delivery.AttemptCount);
        Assert.InRange(delivery.NextAttemptAt, sentAt.AddSeconds(2), sentAt.AddSeconds(2.5));

        lab.Time.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(0, await lab.DispatchAsync());

        lab.Time.Advance(TimeSpan.FromSeconds(2));
        Assert.Equal(1, await lab.DispatchAsync());
        Assert.Equal(DeliveryStatus.Succeeded, Assert.Single(await lab.GetDeliveriesAsync(order.EventId)).Status);
        Assert.Equal(2, lab.Receiver.Received.Count);
    }

    [Fact]
    public async Task Timeouts_refused_connections_and_client_errors_are_all_retried()
    {
        await using var lab = new ProviderFactory();
        lab.Receiver.TimeOutNext();
        lab.Receiver.RefuseConnectionNext();
        lab.Receiver.RespondWith(HttpStatusCode.Unauthorized);
        var order = await lab.CreateOrderAsync();

        for (var pass = 0; pass < 4; pass++)
        {
            await lab.DispatchAsync();
            lab.Time.Advance(TimeSpan.FromSeconds(20));
        }

        var delivery = Assert.Single(await lab.GetDeliveriesAsync(order.EventId));
        Assert.Equal(DeliveryStatus.Succeeded, delivery.Status);
        Assert.Equal(new int?[] { null, null, 401, 200 }, delivery.Attempts.Select(a => a.StatusCode));
        Assert.NotNull(delivery.Attempts[0].Error);
        Assert.NotNull(delivery.Attempts[1].Error);
    }

    [Fact]
    public async Task Delivery_is_dead_after_max_attempts_and_never_sent_again()
    {
        await using var lab = new ProviderFactory();
        lab.Receiver.RespondWith(Enumerable.Repeat(HttpStatusCode.InternalServerError, Backoff.MaxAttempts + 1).ToArray());
        var order = await lab.CreateOrderAsync();

        for (var pass = 0; pass < Backoff.MaxAttempts + 2; pass++)
        {
            await lab.DispatchAsync();
            lab.Time.Advance(TimeSpan.FromSeconds(20));
        }

        var delivery = Assert.Single(await lab.GetDeliveriesAsync(order.EventId));
        Assert.Equal(DeliveryStatus.Dead, delivery.Status);
        Assert.Equal(Backoff.MaxAttempts, delivery.AttemptCount);
        Assert.Equal(Backoff.MaxAttempts, lab.Receiver.Received.Count);
    }

    [Fact]
    public async Task Oldest_delivery_is_sent_first()
    {
        await using var lab = new ProviderFactory();
        var first = await lab.CreateOrderAsync();
        var second = await lab.CreateOrderAsync();

        await lab.DispatchAsync();

        Assert.Equal([first.EventId, second.EventId], lab.Receiver.Received.Select(r => r.Id));
    }

    [Fact]
    public async Task Every_attempt_resends_the_same_body_with_a_fresh_signature()
    {
        await using var lab = new ProviderFactory();
        lab.Receiver.RespondWith(HttpStatusCode.ServiceUnavailable);
        var order = await lab.CreateOrderAsync();

        await lab.DispatchAsync();
        lab.Time.Advance(TimeSpan.FromSeconds(10));
        await lab.DispatchAsync();

        var (attempt1, attempt2) = (lab.Receiver.Received[0], lab.Receiver.Received[1]);
        Assert.Equal(attempt1.Body, attempt2.Body);
        Assert.Equal(attempt1.Id, attempt2.Id);
        Assert.NotEqual(attempt1.Timestamp, attempt2.Timestamp);
        Assert.Equal(order.EventId, JsonSerializer.Deserialize<WebhookEnvelope>(attempt2.Body, WebhookJson.Options)!.Id);
    }
}
```

- [ ] **Step 3: Write the failing chaos tests**

`tests/WebhookLab.Provider.Tests/DispatcherChaosTests.cs`:
```csharp
using System.Diagnostics;
using System.Net;
using System.Text.Json;
using WebhookLab.Contracts;
using WebhookLab.Provider.Chaos;
using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Tests;

public class DispatcherChaosTests
{
    [Fact]
    public async Task DuplicateNext_sends_the_same_event_twice()
    {
        await using var lab = new ProviderFactory();
        lab.Chaos.Apply(new ChaosSnapshot(DuplicateNext: 1, DelaySeconds: 0, ReorderNext: 0, BadSignatureNext: 0));
        var order = await lab.CreateOrderAsync();

        await lab.DispatchAsync();

        Assert.Equal(2, lab.Receiver.Received.Count);
        Assert.All(lab.Receiver.Received, r => Assert.Equal(order.EventId, r.Id));
        var delivery = Assert.Single(await lab.GetDeliveriesAsync(order.EventId));
        Assert.Equal(DeliveryStatus.Succeeded, delivery.Status);
        Assert.Equal(1, delivery.AttemptCount);
        Assert.Equal([false, true], delivery.Attempts.Select(a => a.IsDuplicateSend));
    }

    [Fact]
    public async Task BadSignatureNext_signs_with_the_wrong_secret_once()
    {
        await using var lab = new ProviderFactory();
        lab.Chaos.Apply(new ChaosSnapshot(DuplicateNext: 0, DelaySeconds: 0, ReorderNext: 0, BadSignatureNext: 1));
        lab.Receiver.RespondWith(HttpStatusCode.Unauthorized); // what the real Receiver answers to a forged signature
        await lab.CreateOrderAsync();

        await lab.DispatchAsync();
        lab.Time.Advance(TimeSpan.FromSeconds(10));
        await lab.DispatchAsync();

        Assert.False(lab.Receiver.Received[0].IsSignedWith(ProviderFactory.Secret, lab.Time.GetUtcNow()));
        Assert.True(lab.Receiver.Received[1].IsSignedWith(ProviderFactory.Secret, lab.Time.GetUtcNow()));
    }

    [Fact]
    public async Task ReorderNext_holds_deliveries_until_collected_then_sends_them_newest_first()
    {
        await using var lab = new ProviderFactory();
        lab.Chaos.Apply(new ChaosSnapshot(DuplicateNext: 0, DelaySeconds: 0, ReorderNext: 2, BadSignatureNext: 0));
        var first = await lab.CreateOrderAsync();

        Assert.Equal(0, await lab.DispatchAsync());
        Assert.Empty(lab.Receiver.Received);

        var second = await lab.CreateOrderAsync();
        await lab.DispatchAsync();

        Assert.Equal([second.EventId, first.EventId], lab.Receiver.Received.Select(r => r.Id));
        Assert.False(lab.Reorder.HasHeld);
    }

    [Fact]
    public async Task Turning_reorder_off_releases_anything_still_held()
    {
        await using var lab = new ProviderFactory();
        lab.Chaos.Apply(new ChaosSnapshot(DuplicateNext: 0, DelaySeconds: 0, ReorderNext: 3, BadSignatureNext: 0));
        var order = await lab.CreateOrderAsync();
        await lab.DispatchAsync();

        lab.Chaos.Reset();
        await lab.DispatchAsync();

        Assert.Equal(order.EventId, Assert.Single(lab.Receiver.Received).Id);
    }

    [Fact]
    public async Task DelaySeconds_waits_before_sending()
    {
        await using var lab = new ProviderFactory();
        lab.Chaos.Apply(new ChaosSnapshot(DuplicateNext: 0, DelaySeconds: 1, ReorderNext: 0, BadSignatureNext: 0));
        await lab.CreateOrderAsync();

        var stopwatch = Stopwatch.StartNew();
        await lab.DispatchAsync();

        Assert.True(stopwatch.Elapsed >= TimeSpan.FromSeconds(1));
        Assert.Single(lab.Receiver.Received);
    }

    [Fact]
    public async Task Reordered_payment_and_refund_reach_the_receiver_refund_first()
    {
        await using var lab = new ProviderFactory();
        var order = await lab.CreateOrderAsync();
        await lab.DispatchAsync();
        lab.Chaos.Apply(new ChaosSnapshot(DuplicateNext: 0, DelaySeconds: 0, ReorderNext: 2, BadSignatureNext: 0));

        await lab.PayAsync(order.OrderId);
        await lab.CreateClient().PostAsync($"/api/orders/{order.OrderId}/refund", null);
        await lab.DispatchAsync();

        var types = lab.Receiver.Received.Skip(1)
            .Select(r => JsonSerializer.Deserialize<WebhookEnvelope>(r.Body, WebhookJson.Options)!.Type);
        Assert.Equal([EventTypes.PaymentRefunded, EventTypes.PaymentSucceeded], types);
    }
}
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `dotnet test tests/WebhookLab.Provider.Tests --filter "DeliveryDispatcherTests|DispatcherChaosTests"`
Expected: build FAILS with `The type or namespace name 'DeliveryDispatcher' could not be found`.

- [ ] **Step 5: Implement the dispatcher**

`src/WebhookLab.Provider/Dispatch/DeliveryDispatcher.cs`:
```csharp
using Microsoft.EntityFrameworkCore;
using WebhookLab.Contracts;
using WebhookLab.Provider.Chaos;
using WebhookLab.Provider.Data;
using WebhookLab.Provider.Realtime;

namespace WebhookLab.Provider.Dispatch;

/// <summary>
/// The outbox worker: finds deliveries that are due, sends them, records every attempt, and
/// reschedules failures with exponential backoff until the Receiver answers 2xx — or gives up
/// (Dead) after Backoff.MaxAttempts. Sending-side chaos is applied here.
/// </summary>
public sealed class DeliveryDispatcher(
    ProviderDbContext db,
    WebhookSender sender,
    ChaosSettings chaos,
    ReorderBuffer reorder,
    TimeProvider time,
    ProviderNotifier notifier)
{
    /// <summary>Sends every due delivery, oldest first. Returns how many were sent.</summary>
    public async Task<int> DispatchDueAsync(CancellationToken ct = default)
    {
        var now = time.GetUtcNow().UtcDateTime;
        var due = await db.Deliveries
            .Where(d => d.Status == DeliveryStatus.Pending && d.NextAttemptAt <= now)
            .OrderBy(d => d.Id)
            .Select(d => new { d.Id, d.AttemptCount })
            .ToListAsync(ct);

        var sent = 0;
        foreach (var delivery in due)
        {
            if (reorder.IsHeld(delivery.Id))
                continue;

            // ReorderNext chaos: hold brand-new deliveries back instead of sending them now...
            if (delivery.AttemptCount == 0 && chaos.TryConsumeReorder())
            {
                reorder.Hold(delivery.Id);
                continue;
            }

            await DeliverAsync(delivery.Id, ct);
            sent++;
        }

        // ...and once the last one is collected (or reorder was switched off), send them all newest-first.
        if (!chaos.ReorderPending && reorder.HasHeld)
        {
            foreach (var deliveryId in reorder.ReleaseReversed())
            {
                await DeliverAsync(deliveryId, ct);
                sent++;
            }
        }

        return sent;
    }

    private async Task DeliverAsync(int deliveryId, CancellationToken ct)
    {
        db.ChangeTracker.Clear();
        // POST /api/reset can delete the row between the due-list query and this load; nothing left to do.
        var delivery = await db.Deliveries.SingleOrDefaultAsync(d => d.Id == deliveryId, ct);
        if (delivery is not { Status: DeliveryStatus.Pending })
            return;
        var webhookEvent = await db.Events.AsNoTracking().SingleAsync(e => e.Id == delivery.EventId, ct);

        if (chaos.Delay > TimeSpan.Zero)
            await Task.Delay(chaos.Delay, ct);

        // No database transaction is open here: never hold one across a network call that can take seconds.
        var attemptNumber = delivery.AttemptCount + 1;
        var result = await SendAndRecordAsync(delivery, webhookEvent, attemptNumber, isDuplicateSend: false, ct);
        if (chaos.TryConsumeDuplicate())
            await SendAndRecordAsync(delivery, webhookEvent, attemptNumber, isDuplicateSend: true, ct);

        RecordOutcome(delivery, result);
        await db.SaveChangesAsync(ct);
        await notifier.DeliveryStatusChangedAsync(delivery);
    }

    private async Task<SendResult> SendAndRecordAsync(
        Delivery delivery, WebhookEvent webhookEvent, int attemptNumber, bool isDuplicateSend, CancellationToken ct)
    {
        var startedAt = time.GetUtcNow().UtcDateTime;
        await notifier.AttemptStartedAsync(delivery, attemptNumber, isDuplicateSend);

        var result = await sender.SendAsync(webhookEvent, useWrongSecret: chaos.TryConsumeBadSignature(), ct);

        db.DeliveryAttempts.Add(new DeliveryAttempt
        {
            DeliveryId = delivery.Id,
            AttemptNumber = attemptNumber,
            IsDuplicateSend = isDuplicateSend,
            StartedAt = startedAt,
            DurationMs = result.DurationMs,
            StatusCode = result.StatusCode,
            Error = result.Error,
        });
        await notifier.AttemptCompletedAsync(delivery, attemptNumber, result.StatusCode, result.Error, result.DurationMs);
        return result;
    }

    private void RecordOutcome(Delivery delivery, SendResult result)
    {
        delivery.AttemptCount++;

        if (result.IsSuccess)
            delivery.Status = DeliveryStatus.Succeeded;
        else if (delivery.AttemptCount >= Backoff.MaxAttempts)
            delivery.Status = DeliveryStatus.Dead; // a "dead letter": kept for inspection and replay
        else
            delivery.NextAttemptAt = time.GetUtcNow().UtcDateTime + Backoff.Delay(delivery.AttemptCount, Random.Shared);
    }
}
```

- [ ] **Step 6: Implement the worker**

`src/WebhookLab.Provider/Dispatch/DeliveryDispatcherWorker.cs`:
```csharp
namespace WebhookLab.Provider.Dispatch;

/// <summary>Runs the delivery dispatcher once a second for the lifetime of the app.</summary>
public sealed class DeliveryDispatcherWorker(IServiceScopeFactory scopes, ILogger<DeliveryDispatcherWorker> logger) : BackgroundService
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
                await scope.ServiceProvider.GetRequiredService<DeliveryDispatcher>().DispatchDueAsync(stoppingToken);
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                // A stray failure (e.g. a database error) must not stop the host; only shutdown ends this loop.
                logger.LogError(ex, "Delivery dispatch pass failed; trying again next tick");
            }
        }
    }
}
```

- [ ] **Step 7: Register in `Program.cs`**

After the `AddHttpClient<WebhookSender>(...)` registration add:
```csharp
builder.Services.AddScoped<DeliveryDispatcher>();
if (builder.Configuration.GetValue("Provider:EnableBackgroundDispatch", true))
    builder.Services.AddHostedService<DeliveryDispatcherWorker>();
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `dotnet test tests/WebhookLab.Provider.Tests`
Expected: PASS, 34 tests.

- [ ] **Step 9: Commit**

```bash
git add src/WebhookLab.Provider tests/WebhookLab.Provider.Tests
git commit -m "feat: add delivery dispatcher with backoff retries, dead letters, and chaos"
```

---

### Task 6: Replay, delivery lessons, end-to-end run with the Receiver

**Files:**
- Create: `src/WebhookLab.Provider/Api/EventEndpoints.cs`
- Modify: `src/WebhookLab.Provider/Program.cs`
- Test: `tests/WebhookLab.Provider.Tests/DeliveryLessonTests.cs`

**Interfaces:**
- Consumes: everything above.
- Produces: `POST /api/events/{id}/replay` → `202 Accepted` with `record ReplayResponse(int DeliveryId, string EventId)`, or `404`.

- [ ] **Step 1: Write the failing tests**

`tests/WebhookLab.Provider.Tests/DeliveryLessonTests.cs`:
```csharp
using System.Net;
using System.Net.Http.Json;
using WebhookLab.Contracts;
using WebhookLab.Provider.Api;
using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Tests;

/// <summary>The Provider-side lessons, each stated as the behavior it proves.</summary>
public class DeliveryLessonTests
{
    [Fact]
    public async Task Receiver_outage_is_retried_with_growing_backoff_then_delivered()
    {
        await using var lab = new ProviderFactory();
        lab.Receiver.RespondWith(HttpStatusCode.InternalServerError, HttpStatusCode.InternalServerError, HttpStatusCode.InternalServerError);
        var order = await lab.CreateOrderAsync();

        for (var pass = 0; pass < 4; pass++)
        {
            await lab.DispatchAsync();
            var pending = (await lab.GetDeliveriesAsync(order.EventId)).Single();
            if (pending.Status == DeliveryStatus.Pending)
                lab.Time.SetUtcNow(new DateTimeOffset(pending.NextAttemptAt, TimeSpan.Zero)); // jump exactly to the retry
        }

        var delivery = Assert.Single(await lab.GetDeliveriesAsync(order.EventId));
        Assert.Equal(DeliveryStatus.Succeeded, delivery.Status);
        var gaps = delivery.Attempts.Zip(delivery.Attempts.Skip(1), (a, b) => (b.StartedAt - a.StartedAt).TotalSeconds).ToList();
        Assert.InRange(gaps[0], 2, 2.5);
        Assert.InRange(gaps[1], 4, 4.5);
        Assert.InRange(gaps[2], 8, 8.5);
    }

    [Fact]
    public async Task Dead_letter_is_delivered_by_replay()
    {
        await using var lab = new ProviderFactory();
        lab.Receiver.RespondWith(Enumerable.Repeat(HttpStatusCode.InternalServerError, Backoff.MaxAttempts).ToArray());
        var order = await lab.CreateOrderAsync();
        for (var pass = 0; pass < Backoff.MaxAttempts; pass++)
        {
            await lab.DispatchAsync();
            lab.Time.Advance(TimeSpan.FromSeconds(20));
        }
        Assert.Equal(DeliveryStatus.Dead, (await lab.GetDeliveriesAsync(order.EventId)).Single().Status);

        var response = await lab.CreateClient().PostAsync($"/api/events/{order.EventId}/replay", null);
        await lab.DispatchAsync();

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var deliveries = await lab.GetDeliveriesAsync(order.EventId);
        Assert.Equal([DeliveryStatus.Dead, DeliveryStatus.Succeeded], deliveries.Select(d => d.Status));
        Assert.True(deliveries[1].IsReplay);
    }

    [Fact]
    public async Task Replay_resends_the_same_event_id_so_the_receiver_can_spot_it()
    {
        await using var lab = new ProviderFactory();
        var order = await lab.CreateOrderAsync();
        await lab.DispatchAsync();

        var response = await lab.CreateClient().PostAsync($"/api/events/{order.EventId}/replay", null);
        var replay = (await response.Content.ReadFromJsonAsync<ReplayResponse>(ProviderFactory.Json))!;
        await lab.DispatchAsync();

        Assert.Equal(order.EventId, replay.EventId);
        Assert.Equal(2, lab.Receiver.Received.Count);
        Assert.Equal(lab.Receiver.Received[0].Id, lab.Receiver.Received[1].Id);
        Assert.Equal(lab.Receiver.Received[0].Body, lab.Receiver.Received[1].Body);
    }

    [Fact]
    public async Task Replaying_an_unknown_event_is_not_found()
    {
        await using var lab = new ProviderFactory();

        var response = await lab.CreateClient().PostAsync("/api/events/evt_missing/replay", null);

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test tests/WebhookLab.Provider.Tests --filter DeliveryLessonTests`
Expected: build FAILS with `The type or namespace name 'ReplayResponse' could not be found`.

- [ ] **Step 3: Replay endpoint**

`src/WebhookLab.Provider/Api/EventEndpoints.cs`:
```csharp
using Microsoft.EntityFrameworkCore;
using WebhookLab.Provider.Data;
using WebhookLab.Provider.Realtime;

namespace WebhookLab.Provider.Api;

public sealed record ReplayResponse(int DeliveryId, string EventId);

public static class EventEndpoints
{
    public static IEndpointRouteBuilder MapEventEndpoints(this IEndpointRouteBuilder app)
    {
        // Replay = a brand-new delivery job for an existing event. The event id (the Receiver's
        // idempotency key) and payload stay the same, so a Receiver that already has it skips it.
        app.MapPost("/api/events/{id}/replay", async (string id, ProviderDbContext db, TimeProvider time, ProviderNotifier notifier) =>
        {
            if (!await db.Events.AnyAsync(e => e.Id == id))
                return Results.NotFound();

            var now = time.GetUtcNow().UtcDateTime;
            var delivery = new Delivery { EventId = id, IsReplay = true, NextAttemptAt = now, CreatedAt = now };
            db.Deliveries.Add(delivery);
            await db.SaveChangesAsync();

            await notifier.DeliveryStatusChangedAsync(delivery);
            // 202: queued, not sent yet — the dispatcher picks it up on its next pass.
            return Results.Accepted("/api/deliveries", new ReplayResponse(delivery.Id, id));
        });

        return app;
    }
}
```

- [ ] **Step 4: Register in `Program.cs`**

After `app.MapOrderEndpoints();` add:
```csharp
app.MapEventEndpoints();
```

- [ ] **Step 5: Run the tests**

Run: `dotnet test`
Expected: PASS. Contracts 13, Receiver 37, Provider 38, 0 failed, no warnings.

- [ ] **Step 6: End-to-end run with the real Receiver**

Terminal 1:
```bash
dotnet run --project src/WebhookLab.Receiver
```
Terminal 2:
```bash
dotnet run --project src/WebhookLab.Provider
```
Wait for `Now listening on: http://localhost:5002` and `http://localhost:5001`. Then, in terminal 3:
```bash
curl -s -X POST localhost:5001/api/orders -H 'content-type: application/json' -d '{"amount":50}'
curl -s -X POST localhost:5001/api/orders/1/pay
sleep 3
curl -s localhost:5002/api/ledger
```
Expected: order 1 `"status":"Paid"`, `"chargeCount":1`.

Receiver outage:
```bash
curl -s -X PUT localhost:5002/api/faults -H 'content-type: application/json' -d '{"failNext":2,"slowNext":0,"crashNext":0,"dedupeOff":false}'
curl -s -X POST localhost:5001/api/orders -H 'content-type: application/json' -d '{"amount":20}'
sleep 10
curl -s localhost:5001/api/deliveries
curl -s localhost:5002/api/ledger
```
Expected: the newest delivery shows attempts `500, 500, 200` and `"status":"Succeeded"`; the ledger has order 2.

Duplicate send:
```bash
curl -s -X PUT localhost:5001/api/chaos -H 'content-type: application/json' -d '{"duplicateNext":1,"delaySeconds":0,"reorderNext":0,"badSignatureNext":0}'
curl -s -X POST localhost:5001/api/orders/2/pay
sleep 3
curl -s localhost:5001/api/deliveries
curl -s localhost:5002/api/ledger
```
Expected: the payment delivery has two attempts (`isDuplicateSend` false, then true), both `200`; order 2 is `Paid` with `"chargeCount":1`.

Clean up: `curl -s -X POST localhost:5001/api/reset` and `curl -s -X POST localhost:5002/api/reset`, then stop both apps (Ctrl+C). `provider.db` and `receiver.db` are created in each project's directory and are git-ignored.

- [ ] **Step 7: Commit**

```bash
git add src/WebhookLab.Provider tests/WebhookLab.Provider.Tests
git commit -m "feat: add event replay and provider delivery lesson tests"
```
