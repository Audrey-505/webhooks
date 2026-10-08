using System.Net;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using WebhookLab.Provider.Data;
using WebhookLab.Provider.Dispatch;

namespace WebhookLab.Provider.Tests;

public class WebhookSenderTests
{
    private const string Secret = "sender-test-secret";
    private const string TargetUrl = "http://receiver.test/webhooks";

    private readonly FakeReceiver _receiver = new();
    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 10, 8, 10, 0, 0, TimeSpan.Zero));
    private readonly WebhookEvent _event = new()
    {
        Id = "evt_123",
        Type = "order.created",
        Payload = """{"id":"evt_123","type":"order.created","createdAt":"2026-10-08T10:00:00+00:00","data":{"orderId":1,"amount":50}}""",
    };

    private WebhookSender CreateSender() => new(
        new HttpClient(_receiver),
        Options.Create(new WebhookOptions { Secret = Secret, TargetUrl = TargetUrl }),
        _time);

    [Fact]
    public async Task Sends_the_exact_payload_signed_to_the_target_url()
    {
        var result = await CreateSender().SendAsync(_event, useWrongSecret: false, CancellationToken.None);

        Assert.True(result.IsSuccess);
        Assert.Equal(200, result.StatusCode);
        var received = Assert.Single(_receiver.Received);
        Assert.Equal(new Uri(TargetUrl), received.Url);
        Assert.Equal(_event.Id, received.Id);
        Assert.Equal(_event.Payload, received.Body);
        Assert.True(received.IsSignedWith(Secret, _time.GetUtcNow()));
    }

    [Fact]
    public async Task Non_2xx_response_is_a_failure_with_its_status_code()
    {
        _receiver.RespondWith(HttpStatusCode.InternalServerError);

        var result = await CreateSender().SendAsync(_event, useWrongSecret: false, CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Equal(500, result.StatusCode);
    }

    [Fact]
    public async Task Timeout_is_a_failure_with_no_status_code()
    {
        _receiver.TimeOutNext();

        var result = await CreateSender().SendAsync(_event, useWrongSecret: false, CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Null(result.StatusCode);
        Assert.Contains("Timed out", result.Error);
    }

    [Fact]
    public async Task Refused_connection_is_a_failure_with_no_status_code()
    {
        _receiver.RefuseConnectionNext();

        var result = await CreateSender().SendAsync(_event, useWrongSecret: false, CancellationToken.None);

        Assert.False(result.IsSuccess);
        Assert.Null(result.StatusCode);
        Assert.Contains("Connection refused", result.Error);
    }

    [Fact]
    public async Task Wrong_secret_produces_a_signature_the_receiver_rejects()
    {
        await CreateSender().SendAsync(_event, useWrongSecret: true, CancellationToken.None);

        Assert.False(Assert.Single(_receiver.Received).IsSignedWith(Secret, _time.GetUtcNow()));
    }
}
