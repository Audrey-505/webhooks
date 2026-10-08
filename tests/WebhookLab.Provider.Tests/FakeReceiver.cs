using System.Collections.Concurrent;
using System.Net;
using WebhookLab.Contracts;

namespace WebhookLab.Provider.Tests;

/// <summary>
/// Stands in for the Receiver: records every webhook that arrives and answers from a script
/// (200 OK once the script runs out). Timeouts and refused connections are scripted as exceptions,
/// exactly what HttpClient throws for them.
/// </summary>
public sealed class FakeReceiver : HttpMessageHandler
{
    private readonly ConcurrentQueue<Func<HttpResponseMessage>> _script = new();
    private readonly ConcurrentQueue<ReceivedWebhook> _received = new();

    public IReadOnlyList<ReceivedWebhook> Received => _received.ToArray();

    public void RespondWith(params HttpStatusCode[] statuses)
    {
        foreach (var status in statuses)
            _script.Enqueue(() => new HttpResponseMessage(status));
    }

    public void TimeOutNext(int count = 1)
    {
        for (var i = 0; i < count; i++)
            _script.Enqueue(() => throw new TaskCanceledException("Simulated timeout"));
    }

    public void RefuseConnectionNext() =>
        _script.Enqueue(() => throw new HttpRequestException("Connection refused (simulated)"));

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var body = await request.Content!.ReadAsStringAsync(cancellationToken);
        _received.Enqueue(new ReceivedWebhook(
            request.RequestUri!,
            Header(request, WebhookHeaders.Id),
            Header(request, WebhookHeaders.Timestamp),
            Header(request, WebhookHeaders.Signature),
            body));

        return _script.TryDequeue(out var next) ? next() : new HttpResponseMessage(HttpStatusCode.OK);
    }

    private static string Header(HttpRequestMessage request, string name) => request.Headers.GetValues(name).Single();
}

public sealed record ReceivedWebhook(Uri Url, string Id, string Timestamp, string Signature, string Body)
{
    public bool IsSignedWith(string secret, DateTimeOffset now) =>
        WebhookSignature.Verify(secret, Id, Timestamp, Body, Signature, now);
}
