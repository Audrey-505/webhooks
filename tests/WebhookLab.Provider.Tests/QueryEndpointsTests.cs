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
