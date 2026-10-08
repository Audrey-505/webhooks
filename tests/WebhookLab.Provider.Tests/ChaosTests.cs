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
    public void DelaySeconds_above_60_is_clamped_to_60()
    {
        var chaos = new ChaosSettings();

        chaos.Apply(new ChaosSnapshot(DuplicateNext: 0, DelaySeconds: 999, ReorderNext: 0, BadSignatureNext: 0));

        Assert.Equal(60, chaos.Snapshot().DelaySeconds);
        Assert.Equal(TimeSpan.FromSeconds(60), chaos.Delay);
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
