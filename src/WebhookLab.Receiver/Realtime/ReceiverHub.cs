using Microsoft.AspNetCore.SignalR;

namespace WebhookLab.Receiver.Realtime;

/// <summary>Dashboard-only live feed. Webhook delivery never depends on it.</summary>
public sealed class ReceiverHub : Hub;
