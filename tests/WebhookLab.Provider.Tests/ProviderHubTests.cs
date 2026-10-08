using System.Text.Json;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.AspNetCore.SignalR.Client;

namespace WebhookLab.Provider.Tests;

/// <summary>
/// Connects a real HubConnection through the test server (long polling — the in-memory TestServer
/// has no WebSocket support) and checks the wire shapes Plan 3's Angular dashboard will code
/// against: message order, field names (camelCase), and enum values (as strings).
/// </summary>
public class ProviderHubTests
{
    [Fact]
    public async Task One_order_push_through_dispatch_arrives_in_order_with_matching_eventId()
    {
        await using var lab = new ProviderFactory();
        var received = new List<(string Name, JsonElement Payload)>();
        var sawAll = new TaskCompletionSource();

        var connection = new HubConnectionBuilder()
            .WithUrl(new Uri(lab.Server.BaseAddress, "hubs/provider"), options =>
            {
                options.HttpMessageHandlerFactory = _ => lab.Server.CreateHandler();
                options.Transports = HttpTransportType.LongPolling;
            })
            .Build();

        void Record(string name) => connection.On<JsonElement>(name, payload =>
        {
            lock (received)
            {
                received.Add((name, payload));
                if (received.Count >= 5)
                    sawAll.TrySetResult();
            }
        });

        foreach (var name in new[] { "EventCreated", "DeliveryStatusChanged", "AttemptStarted", "AttemptCompleted" })
            Record(name);

        await connection.StartAsync();
        try
        {
            var order = await lab.CreateOrderAsync();
            await lab.DispatchAsync();

            await Task.WhenAny(sawAll.Task, Task.Delay(TimeSpan.FromSeconds(5)));

            var names = received.Select(r => r.Name).ToList();
            Assert.Equal(
                ["EventCreated", "DeliveryStatusChanged", "AttemptStarted", "AttemptCompleted", "DeliveryStatusChanged"],
                names);

            Assert.All(received, r => Assert.Equal(order.EventId, r.Payload.GetProperty("eventId").GetString()));

            Assert.Equal("Pending", received[1].Payload.GetProperty("status").GetString());
            Assert.False(received[3].Payload.GetProperty("isDuplicateSend").GetBoolean());
            Assert.Equal(200, received[3].Payload.GetProperty("statusCode").GetInt32());
            Assert.Equal("Succeeded", received[4].Payload.GetProperty("status").GetString());
        }
        finally
        {
            await connection.DisposeAsync();
        }
    }
}
