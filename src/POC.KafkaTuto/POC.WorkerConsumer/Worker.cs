using Confluent.Kafka;
using Microsoft.Extensions.Options;
using POC.WorkerConsumer.Options;

namespace POC.WorkerConsumer;

public class Worker(ILogger<Worker> logger, IOptions<KafkaOptions> kafkaOptions) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Delay(1000 * 2, stoppingToken);

        var config = new ConsumerConfig
        {
            BootstrapServers = kafkaOptions.Value.BootstrapServers,
            GroupId = kafkaOptions.Value.GroupId,
            AutoOffsetReset = kafkaOptions.Value.AutoOffsetReset,
            EnableAutoCommit = kafkaOptions.Value.EnableAutoCommit,
            MaxPollIntervalMs = kafkaOptions.Value.MaxPollIntervalMs,
            SessionTimeoutMs = kafkaOptions.Value.SessionTimeoutMs
        };

        logger.LogInformation("Worker Consumer running at: {time}", DateTimeOffset.Now);

        using var consumer = new ConsumerBuilder<string, string>(config)
                .SetErrorHandler((_, e) => logger.LogError("Kafka error: {Error}", e))
                .Build();

        consumer.Subscribe("orders.raw");

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var cr = consumer.Consume(stoppingToken);

                // Traitement idempotent conseill� ici (ex: upsert par cl�)
                logger.LogInformation("Order: {Key} -> {Value}", cr.Message.Key, cr.Message.Value);

                consumer.Commit(cr); // commit apr�s traitement -> at-least-once
            }
        }
        catch (OperationCanceledException)
        {
            logger.LogError("Operation cancelled");
        }
        finally
        {
            consumer.Close();
        }
    }
}
