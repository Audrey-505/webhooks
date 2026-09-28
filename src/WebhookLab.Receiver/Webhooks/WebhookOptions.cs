namespace WebhookLab.Receiver.Webhooks;

public sealed class WebhookOptions
{
    public const string SectionName = "Webhook";

    /// <summary>Shared with the Provider. Used to verify each request's signature.</summary>
    public string Secret { get; set; } = "";
}
