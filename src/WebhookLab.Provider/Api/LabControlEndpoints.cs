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

        app.MapPost("/api/reset", async (ProviderDbContext db, ChaosSettings chaos, ReorderBuffer reorder) =>
        {
            // One transaction so a reset can't be observed half-done. Children before parents:
            // SQLite enforces the foreign keys (attempts → deliveries → events).
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
