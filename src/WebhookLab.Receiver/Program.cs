using System.Text.Json.Serialization;
using Microsoft.EntityFrameworkCore;
using WebhookLab.Receiver.Api;
using WebhookLab.Receiver.Data;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<ReceiverDbContext>(options =>
    options.UseSqlite(builder.Configuration.GetConnectionString("Receiver")));
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.ConfigureHttpJsonOptions(options =>
    options.SerializerOptions.Converters.Add(new JsonStringEnumConverter()));
builder.Services.AddCors(options => options.AddDefaultPolicy(policy => policy
    .WithOrigins(builder.Configuration["Dashboard:Origin"] ?? "http://localhost:4200")
    .AllowAnyHeader()
    .AllowAnyMethod()
    .AllowCredentials()));

var app = builder.Build();

// A lab doesn't need migrations: create the schema if the database file is new.
using (var scope = app.Services.CreateScope())
    scope.ServiceProvider.GetRequiredService<ReceiverDbContext>().Database.EnsureCreated();

app.UseCors();
app.MapQueryEndpoints();

app.Run();

public partial class Program;
