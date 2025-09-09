using Confluent.Kafka;
using Microsoft.Extensions.Options;
using POC.WorkerConsumer.Options;

namespace POC.WorkerConsumer;

public class Worker(ILogger<Worker> logger, IOptions<KafkaOptions> kafkaOptions, IHostEnvironment environment) : BackgroundService
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
            SessionTimeoutMs = kafkaOptions.Value.SessionTimeoutMs,
            SecurityProtocol = environment.IsProduction() ? SecurityProtocol.SaslPlaintext : null,
            SaslMechanism = environment.IsProduction() ? SaslMechanism.Plain : null,
            SaslUsername = environment.IsProduction() ? kafkaOptions.Value.Username : null,
            SaslPassword = environment.IsProduction() ? kafkaOptions.Value.Password : null
        };

        logger.LogInformation("Worker Consumer running at: {time}", DateTimeOffset.Now);

        using var consumer = new ConsumerBuilder<string, string>(config)
                .SetErrorHandler((_, e) => logger.LogError("Kafka error: {Error}", e))
                .Build();

        consumer.Subscribe(kafkaOptions.Value.Topic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var cr = consumer.Consume(stoppingToken);

                // Traitement idempotent conseillé ici (ex: upsert par clé)
                logger.LogInformation("Order: {Key} -> {Value}", cr.Message.Key, cr.Message.Value);

                consumer.Commit(cr); // commit après traitement -> at-least-once
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
