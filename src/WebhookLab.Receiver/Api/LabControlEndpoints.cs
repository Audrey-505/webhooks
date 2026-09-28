using Microsoft.EntityFrameworkCore;
using WebhookLab.Receiver.Data;
using WebhookLab.Receiver.Faults;

namespace WebhookLab.Receiver.Api;

public static class LabControlEndpoints
{
    public static IEndpointRouteBuilder MapLabControlEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/api/faults", (FaultSettings faults) => faults.Snapshot());

        app.MapPut("/api/faults", (FaultSnapshot update, FaultSettings faults) =>
        {
            faults.Apply(update);
            return faults.Snapshot();
        });

        app.MapPost("/api/reset", async (ReceiverDbContext db, FaultSettings faults) =>
        {
            await db.InboxEvents.ExecuteDeleteAsync();
            await db.LedgerOrders.ExecuteDeleteAsync();
            faults.Reset();
            return Results.NoContent();
        });

        return app;
    }
}
