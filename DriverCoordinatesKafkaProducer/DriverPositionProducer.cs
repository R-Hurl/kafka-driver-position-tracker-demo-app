using System.Text.Json;
using Confluent.Kafka;
using DriverCoordinatesKafkaProducer.Models;

public class DriverPositionProducer : BackgroundService
{
    private readonly ILogger<DriverPositionProducer> _logger;
    private readonly IProducer<Null, string> _producer;

    public DriverPositionProducer(ILogger<DriverPositionProducer> logger)
    {
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        };

        _producer = new ProducerBuilder<Null, string>(config)
            .Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Driver position producer started.");

        var clevelandLatitude = 41.4993;
        var clevelandLongitude = -81.6944;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Simulate getting the driver's coordinates around Cleveland, Ohio
                var latitude = clevelandLatitude + (new Random().NextDouble() - 0.5) * 0.1; // +/- 0.05 degrees from Cleveland's latitude
                var longitude = clevelandLongitude + (new Random().NextDouble() - 0.5) * 0.1; // +/- 0.05 degrees from Cleveland's longitude

                var driverPosition = new DriverPosition(latitude, longitude);
                var message = JsonSerializer.Serialize(driverPosition);

                var deliveryReport = await _producer.ProduceAsync("driver-positions", new Message<Null, string> { Value = message }, stoppingToken);

                _logger.LogInformation($"Driver position message sent (value: {message}, topic: {deliveryReport.Topic}, partition: {deliveryReport.Partition}, offset: {deliveryReport.Offset})");

                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken); // Wait 1 seconds before sending the next message
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Driver position producer stopped.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred while sending the driver position message.");
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping driver position producer.");

        _producer.Dispose();

        await base.StopAsync(cancellationToken);
    }
}
