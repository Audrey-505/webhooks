using System.Text;
using Microsoft.Extensions.Options;
using WebhookLab.Contracts;
using WebhookLab.Provider.Data;

namespace WebhookLab.Provider.Dispatch;

/// <param name="StatusCode">The Receiver's HTTP status, or null when no response arrived.</param>
public sealed record SendResult(int? StatusCode, string? Error, long DurationMs)
{
    public bool IsSuccess => StatusCode is >= 200 and < 300;
}

/// <summary>
/// Signs and POSTs one webhook. Delivery problems never throw: a timeout, a refused connection and
/// a 500 all come back as a SendResult, because to the dispatcher they mean the same thing —
/// "not delivered, retry later". The one thing that does propagate as an exception is the caller's
/// own cancellation (host shutdown, or a reset — this class can't tell them apart and doesn't try).
/// </summary>
public sealed class WebhookSender(HttpClient http, IOptions<WebhookOptions> options, TimeProvider time)
{
    public async Task<SendResult> SendAsync(WebhookEvent webhookEvent, bool useWrongSecret, CancellationToken ct)
    {
        var settings = options.Value;
        // A fresh timestamp per attempt: retries must still pass the Receiver's 5-minute freshness check.
        var timestamp = time.GetUtcNow().ToUnixTimeSeconds();
        var secret = useWrongSecret ? "not-the-shared-secret" : settings.Secret;

        using var request = new HttpRequestMessage(HttpMethod.Post, settings.TargetUrl)
        {
            Content = new StringContent(webhookEvent.Payload, Encoding.UTF8, "application/json"),
        };
        request.Headers.Add(WebhookHeaders.Id, webhookEvent.Id);
        request.Headers.Add(WebhookHeaders.Timestamp, timestamp.ToString());
        request.Headers.Add(WebhookHeaders.Signature, WebhookSignature.Sign(secret, webhookEvent.Id, timestamp, webhookEvent.Payload));

        var startedAt = time.GetTimestamp();
        try
        {
            using var response = await http.SendAsync(request, ct);
            return new SendResult((int)response.StatusCode, null, ElapsedMs(startedAt));
        }
        catch (TaskCanceledException) when (!ct.IsCancellationRequested)
        {
            // HttpClient.Timeout expired (not our own shutdown): the Receiver never answered in time.
            return new SendResult(null, $"Timed out after {settings.Timeout.TotalSeconds:0} s", ElapsedMs(startedAt));
        }
        catch (HttpRequestException ex)
        {
            return new SendResult(null, ex.Message, ElapsedMs(startedAt));
        }
    }

    private long ElapsedMs(long startedAt) => (long)time.GetElapsedTime(startedAt).TotalMilliseconds;
}
