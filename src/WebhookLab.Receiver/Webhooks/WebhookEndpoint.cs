using System.Data.Common;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using WebhookLab.Contracts;
using WebhookLab.Receiver.Faults;
using WebhookLab.Receiver.Realtime;

namespace WebhookLab.Receiver.Webhooks;

public static class WebhookEndpoint
{
    public static IEndpointRouteBuilder MapWebhookEndpoint(this IEndpointRouteBuilder app)
    {
        app.MapPost("/webhooks", HandleAsync);
        return app;
    }

    /// <summary>
    /// Verify → store → acknowledge. No business logic here: that happens later in InboxProcessor,
    /// so this responds fast and the Provider doesn't time out and retry needlessly.
    /// </summary>
    private static async Task<IResult> HandleAsync(
        HttpRequest request,
        InboxStore inbox,
        FaultSettings faults,
        IOptions<WebhookOptions> options,
        TimeProvider time,
        ReceiverNotifier notifier)
    {
        // Read as text and HMAC the decoded string below; production verifiers usually HMAC the raw
        // request bytes instead, which avoids any risk of re-encoding changing the signed content.
        using var reader = new StreamReader(request.Body);
        var body = await reader.ReadToEndAsync();
        var eventId = request.Headers[WebhookHeaders.Id].ToString();

        if (faults.TryConsumeFail())
        {
            await notifier.WebhookReceivedAsync(eventId, WebhookOutcome.FaultInjected);
            return Results.StatusCode(StatusCodes.Status500InternalServerError);
        }

        var isGenuine = WebhookSignature.Verify(
            options.Value.Secret,
            eventId,
            request.Headers[WebhookHeaders.Timestamp].ToString(),
            body,
            request.Headers[WebhookHeaders.Signature].ToString(),
            time.GetUtcNow());
        if (!isGenuine)
        {
            await notifier.WebhookReceivedAsync(eventId, WebhookOutcome.Rejected);
            return Results.Unauthorized();
        }

        var envelope = TryParse(body);
        if (envelope is null || envelope.Id != eventId || envelope.Data is null || string.IsNullOrEmpty(envelope.Type))
            return Results.BadRequest("Body must be a webhook envelope whose id matches the webhook-id header.");

        StoreResult result;
        try
        {
            result = await inbox.StoreAsync(envelope, body, faults.DedupeOff);
        }
        catch (Exception ex) when (ex is DbException or DbUpdateException)
        {
            // Temporary problem on our side: ask the Provider to retry later.
            return Results.StatusCode(StatusCodes.Status503ServiceUnavailable);
        }

        await notifier.WebhookReceivedAsync(eventId, result switch
        {
            StoreResult.Stored => WebhookOutcome.Stored,
            StoreResult.Duplicate => WebhookOutcome.Duplicate,
            _ => WebhookOutcome.DuplicateReprocessed,
        });

        if (faults.TryConsumeSlow())
            await Task.Delay(faults.SlowResponseDelay); // stored already; the sender just won't hear back in time

        // 200 for new AND duplicate events: the Provider only asks "did you get it?"
        return Results.Ok();
    }

    private static WebhookEnvelope? TryParse(string body)
    {
        try
        {
            return JsonSerializer.Deserialize<WebhookEnvelope>(body, WebhookJson.Options);
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
