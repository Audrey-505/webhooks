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
