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

        app.MapPost("/api/reset", async (ProviderDbContext db, ChaosSettings chaos, ReorderBuffer reorder, ResetSignal resetSignal) =>
        {
            // Cancel any delivery in flight first: the dispatcher is linked to this signal and bails
            // out on its own, so the deletes below never race a send or a SaveChanges for that row.
            resetSignal.Trigger();

            // One transaction so a reset can't be observed half-done. Children before parents is just
            // for clarity here — SQLite's required FKs (attempts → deliveries → events) would cascade
            // the deletes either way.
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
