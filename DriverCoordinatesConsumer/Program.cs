using DriverCoordinatesConsumer;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHostedService<DriverPositionConsumer>();

var app = builder.Build();

app.MapGet("/", () => "Hello World!");

app.Run();
