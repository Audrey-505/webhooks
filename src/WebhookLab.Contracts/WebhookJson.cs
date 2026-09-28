using System.Text.Json;

namespace WebhookLab.Contracts;

public static class WebhookJson
{
    /// <summary>camelCase JSON, matching what the Provider sends.</summary>
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);
}
