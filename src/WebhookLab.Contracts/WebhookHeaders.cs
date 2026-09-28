namespace WebhookLab.Contracts;

/// <summary>Header names from the Standard Webhooks convention (standardwebhooks.com).</summary>
public static class WebhookHeaders
{
    public const string Id = "webhook-id";
    public const string Timestamp = "webhook-timestamp";
    public const string Signature = "webhook-signature";
}
