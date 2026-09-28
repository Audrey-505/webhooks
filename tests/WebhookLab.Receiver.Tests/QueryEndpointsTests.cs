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

    [Fact]
    public async Task Inbox_receivedAt_is_serialized_as_UTC()
    {
        await using var lab = new ReceiverFactory();
        await lab.SendAsync(TestEvents.Created(42));

        var items = await lab.CreateClient().GetFromJsonAsync<List<JsonElement>>("/api/inbox");

        var receivedAt = items![0].GetProperty("receivedAt").GetString();
        Assert.EndsWith("Z", receivedAt);
    }
}
