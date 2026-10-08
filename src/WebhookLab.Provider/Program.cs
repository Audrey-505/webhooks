using System.Text.Json.Serialization;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using WebhookLab.Provider.Api;
using WebhookLab.Provider.Chaos;
using WebhookLab.Provider.Data;
using WebhookLab.Provider.Dispatch;
using WebhookLab.Provider.Orders;
using WebhookLab.Provider.Realtime;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<ProviderDbContext>(options =>
    options.UseSqlite(builder.Configuration.GetConnectionString("Provider")));
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<ChaosSettings>();
builder.Services.AddSingleton<ReorderBuffer>();
builder.Services.AddScoped<OrderService>();
builder.Services.AddOptions<WebhookOptions>()
    .Bind(builder.Configuration.GetSection(WebhookOptions.SectionName))
    .Validate(o => !string.IsNullOrWhiteSpace(o.Secret), "Webhook:Secret must be configured.")
    .Validate(o => Uri.IsWellFormedUriString(o.TargetUrl, UriKind.Absolute), "Webhook:TargetUrl must be an absolute URL.")
    .ValidateOnStart();
builder.Services.AddHttpClient<WebhookSender>((services, client) =>
    client.Timeout = services.GetRequiredService<IOptions<WebhookOptions>>().Value.Timeout);
builder.Services.AddScoped<DeliveryDispatcher>();
if (builder.Configuration.GetValue("Provider:EnableBackgroundDispatch", true))
    builder.Services.AddHostedService<DeliveryDispatcherWorker>();
builder.Services.AddSingleton<ProviderNotifier>();
builder.Services.AddSignalR().AddJsonProtocol(options =>
    options.PayloadSerializerOptions.Converters.Add(new JsonStringEnumConverter()));
builder.Services.ConfigureHttpJsonOptions(options =>
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));
builder.Services.AddCors(options => options.AddDefaultPolicy(policy => policy
    .WithOrigins(builder.Configuration["Dashboard:Origin"] ?? "http://localhost:4200")
    .AllowAnyHeader()
    .AllowAnyMethod()
    .AllowCredentials()));

var app = builder.Build();

// A lab doesn't need migrations: create the schema if the database file is new. EnsureCreated never
// alters an existing provider.db, so after changing an entity's shape, delete the file and rerun.
using (var scope = app.Services.CreateScope())
    scope.ServiceProvider.GetRequiredService<ProviderDbContext>().Database.EnsureCreated();

app.UseCors();
app.MapQueryEndpoints();
app.MapLabControlEndpoints();
app.MapOrderEndpoints();
app.MapHub<ProviderHub>("/hubs/provider");

app.Run();

public partial class Program;
