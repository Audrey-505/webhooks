using WebhookLab.Contracts;

namespace WebhookLab.Contracts.Tests;

public class WebhookSignatureTests
{
    private const string Secret = "whsec_test";
    private const string Id = "evt_1";
    private const long Timestamp = 1_790_000_000;
    private const string Body = """{"a":1}""";
    private static readonly DateTimeOffset SignedAt = DateTimeOffset.FromUnixTimeSeconds(Timestamp);

    [Fact]
    public void Sign_matches_known_vector()
    {
        // Computed independently with: printf '%s' 'evt_1.1790000000.{"a":1}' | openssl dgst -sha256 -hmac whsec_test -binary | base64
        Assert.Equal("v1,wn9RxYh879SGYsmO/Vaiv4CLIqfB0qkd6S5loWxUSmM=", WebhookSignature.Sign(Secret, Id, Timestamp, Body));
    }

    [Fact]
    public void Verify_accepts_a_valid_signature()
    {
        var signature = WebhookSignature.Sign(Secret, Id, Timestamp, Body);

        Assert.True(WebhookSignature.Verify(Secret, Id, Timestamp.ToString(), Body, signature, SignedAt));
    }

    [Fact]
    public void Verify_rejects_a_tampered_body()
    {
        var signature = WebhookSignature.Sign(Secret, Id, Timestamp, Body);

        Assert.False(WebhookSignature.Verify(Secret, Id, Timestamp.ToString(), """{"a":2}""", signature, SignedAt));
    }

    [Fact]
    public void Verify_rejects_the_wrong_secret()
    {
        var signature = WebhookSignature.Sign("some-other-secret", Id, Timestamp, Body);

        Assert.False(WebhookSignature.Verify(Secret, Id, Timestamp.ToString(), Body, signature, SignedAt));
    }

    [Fact]
    public void Verify_rejects_a_timestamp_older_than_the_tolerance()
    {
        var signature = WebhookSignature.Sign(Secret, Id, Timestamp, Body);
        var now = SignedAt + WebhookSignature.Tolerance + TimeSpan.FromSeconds(1);

        Assert.False(WebhookSignature.Verify(Secret, Id, Timestamp.ToString(), Body, signature, now));
    }

    [Fact]
    public void Verify_rejects_a_timestamp_too_far_in_the_future()
    {
        var signature = WebhookSignature.Sign(Secret, Id, Timestamp, Body);
        var now = SignedAt - WebhookSignature.Tolerance - TimeSpan.FromSeconds(1);

        Assert.False(WebhookSignature.Verify(Secret, Id, Timestamp.ToString(), Body, signature, now));
    }

    [Theory]
    [InlineData("")]
    [InlineData("not-a-number")]
    public void Verify_rejects_a_malformed_timestamp(string timestamp)
    {
        var signature = WebhookSignature.Sign(Secret, Id, Timestamp, Body);

        Assert.False(WebhookSignature.Verify(Secret, Id, timestamp, Body, signature, SignedAt));
    }
}
