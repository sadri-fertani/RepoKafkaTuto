using Confluent.Kafka;
using Microsoft.Extensions.Options;
using POC.WorkerProducer.Options;
using System.Text.Json;

namespace POC.WorkerProducer;

public class Worker(ILogger<Worker> logger, IOptions<KafkaOptions> kafkaOptions, IHostEnvironment environment) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = kafkaOptions.Value.BootstrapServers,
            Acks = kafkaOptions.Value.Acks,
            EnableIdempotence = kafkaOptions.Value.EnableIdempotence,
            SecurityProtocol = environment.IsProduction() ? SecurityProtocol.SaslPlaintext : null,
            SaslMechanism = environment.IsProduction() ? SaslMechanism.Plain : null,
            SaslUsername = environment.IsProduction() ? kafkaOptions.Value.Username : null,
            SaslPassword = environment.IsProduction() ? kafkaOptions.Value.Password : null
        };

        var id = 0;

        while (!stoppingToken.IsCancellationRequested)
        {
            using var producer = new ProducerBuilder<string, string>(config)
                .SetKeySerializer(Serializers.Utf8)
                .SetValueSerializer(Serializers.Utf8)
                .Build();

            logger.LogInformation("Worker Producer running at: {time}", DateTimeOffset.Now);

            var evt = new { Id = ++id, Type = "OrderCreated", At = DateTimeOffset.UtcNow };

            var dr = await producer.ProduceAsync
                (
                    kafkaOptions.Value.Topic,
                    new Message<string, string>
                    {
                        Key = Guid.NewGuid().ToString(),
                        Value = JsonSerializer.Serialize(evt)
                    },
                    stoppingToken
                );

            logger.LogInformation("Produced to {TopicPartitionOffset}", dr.TopicPartitionOffset);

            await Task.Run(() => producer.Flush(TimeSpan.FromSeconds(10)));

            await Task.Delay(1000 * 30, stoppingToken);
        }
    }
}
