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
