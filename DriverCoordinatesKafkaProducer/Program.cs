var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHostedService<DriverPositionProducer>();
var app = builder.Build();

app.MapGet("/", () => "Hello World!");

app.Run();
