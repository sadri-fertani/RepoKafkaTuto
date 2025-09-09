using Confluent.Kafka;

namespace POC.WorkerConsumer.Options;

public class KafkaOptions
{
    public required string BootstrapServers { get; set; }

    public required string GroupId { get; set; }

    public required string Topic { get; set; }

    public required AutoOffsetReset AutoOffsetReset { get; set; }

    public required bool EnableAutoCommit { get; set; }

    public required int MaxPollIntervalMs { get; set; }

    public required int SessionTimeoutMs { get; set; }

    public string? Username { get; set; }

    public string? Password { get; set; }
}
