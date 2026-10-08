namespace WebhookLab.Provider.Dispatch;

public sealed class WebhookOptions
{
    public const string SectionName = "Webhook";

    /// <summary>Shared with the Receiver. Used to sign every webhook.</summary>
    public string Secret { get; set; } = "";

    /// <summary>The Receiver's webhook URL — the one address it registered with us.</summary>
    public string TargetUrl { get; set; } = "";

    /// <summary>How long to wait for the Receiver's answer before counting the attempt as failed.</summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(5);
}
