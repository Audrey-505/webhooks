namespace WebhookLab.Contracts;

/// <summary>
/// The JSON body of every webhook. <see cref="Id"/> is the idempotency key:
/// retries and replays of the same event reuse it.
/// </summary>
public sealed record WebhookEnvelope(string Id, string Type, DateTimeOffset CreatedAt, OrderEventData Data);

public sealed record OrderEventData(int OrderId, decimal Amount);
