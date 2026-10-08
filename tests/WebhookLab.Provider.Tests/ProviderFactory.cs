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
