using System.Text.Json;
using Confluent.Kafka;
using DriverCoordinatesKafkaProducer.Models;

namespace DriverCoordinatesConsumer;
public class DriverPositionConsumer : BackgroundService
{
    private readonly ILogger<DriverPositionConsumer> _logger;
    private readonly IConsumer<Ignore, string> _consumer;

    public DriverPositionConsumer(ILogger<DriverPositionConsumer> logger)
    {
        _logger = logger;

        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "driver-position-consumer-group",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        _consumer = new ConsumerBuilder<Ignore, string>(config)
            .Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Driver position consumer started.");

        _consumer.Subscribe("driver-positions");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var consumeResult = _consumer.Consume(stoppingToken);

                var driverPosition = JsonSerializer.Deserialize<DriverPosition>(consumeResult.Message.Value);
                _logger.LogInformation($"Driver position message received (value: {consumeResult.Message.Value}, topic: {consumeResult.Topic}, partition: {consumeResult.Partition}, offset: {consumeResult.Offset}, latitude: {driverPosition.Latitude}, longitude: {driverPosition.Longitude})");
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Driver position consumer stopped.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred while consuming the driver position message.");
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Stopping driver position consumer.");

        _consumer.Close();
        _consumer.Dispose();

        await base.StopAsync(cancellationToken);
    }
}
