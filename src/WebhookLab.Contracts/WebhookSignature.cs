using System.Security.Cryptography;
using System.Text;

namespace WebhookLab.Contracts;

/// <summary>
/// HMAC-SHA256 webhook signatures. The sender signs "{id}.{timestamp}.{body}" with a shared
/// secret; the receiver recomputes it to prove the request is genuine and unmodified.
/// Including the timestamp lets the receiver reject old requests replayed by an attacker.
/// This follows the Standard Webhooks header/signature *format* only. Real Standard Webhooks also
/// base64-decodes a `whsec_`-prefixed secret before using it, and accepts multiple space-separated
/// signatures on one request so a sender can rotate keys without a flag day.
/// </summary>
public static class WebhookSignature
{
    public static readonly TimeSpan Tolerance = TimeSpan.FromMinutes(5);

    public static string Sign(string secret, string webhookId, long timestamp, string body)
    {
        var payload = Encoding.UTF8.GetBytes($"{webhookId}.{timestamp}.{body}");
        var hash = HMACSHA256.HashData(Encoding.UTF8.GetBytes(secret), payload);
        return "v1," + Convert.ToBase64String(hash);
    }

    public static bool Verify(string secret, string webhookId, string timestamp, string body, string signature, DateTimeOffset now)
    {
        if (!long.TryParse(timestamp, out var sentAtSeconds))
            return false;

        var nowSeconds = now.ToUnixTimeSeconds();
        var toleranceSeconds = (long)Tolerance.TotalSeconds;
        if (sentAtSeconds < nowSeconds - toleranceSeconds || sentAtSeconds > nowSeconds + toleranceSeconds)
            return false;

        var expected = Encoding.UTF8.GetBytes(Sign(secret, webhookId, sentAtSeconds, body));
        // Constant-time comparison: doesn't leak how many leading characters matched.
        return CryptographicOperations.FixedTimeEquals(expected, Encoding.UTF8.GetBytes(signature));
    }
}
