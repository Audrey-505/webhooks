using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Time.Testing;
using WebhookLab.Contracts;
using WebhookLab.Receiver.Data;
using WebhookLab.Receiver.Faults;

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

    public FaultSettings Faults => Services.GetRequiredService<FaultSettings>();

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

    public override async ValueTask DisposeAsync()
    {
        await base.DisposeAsync();
        SqliteConnection.ClearAllPools();
        File.Delete(_databasePath);
    }
}
