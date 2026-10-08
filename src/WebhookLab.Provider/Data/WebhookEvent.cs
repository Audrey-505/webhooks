namespace WebhookLab.Provider.Data;

/// <summary>Something that happened at the Provider, to be delivered to the Receiver as a webhook.</summary>
public sealed class WebhookEvent
{
    public required string Id { get; init; }
    public required string Type { get; init; }

    /// <summary>The exact JSON body. Every attempt and replay sends — and signs — this same string.</summary>
    public required string Payload { get; init; }

    public DateTime CreatedAt { get; init; }
}
