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
