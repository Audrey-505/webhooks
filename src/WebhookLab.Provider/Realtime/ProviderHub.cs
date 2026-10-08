using Microsoft.AspNetCore.SignalR;

namespace WebhookLab.Provider.Realtime;

/// <summary>Dashboard-only live feed. Webhook delivery never depends on it.</summary>
public sealed class ProviderHub : Hub;
