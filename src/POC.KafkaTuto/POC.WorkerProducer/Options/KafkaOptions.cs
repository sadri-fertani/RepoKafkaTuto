using Confluent.Kafka;

namespace POC.WorkerProducer.Options;

public class KafkaOptions
{
    public required string BootstrapServers { get; set; }

    public required Acks Acks { get; set; }

    public required bool EnableIdempotence { get; set; }
}
