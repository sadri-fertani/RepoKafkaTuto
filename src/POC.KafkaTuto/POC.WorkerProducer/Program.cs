using POC.WorkerProducer;
using POC.WorkerProducer.Options;

var builder = Host.CreateApplicationBuilder(args);
builder.Services
    .AddHostedService<Worker>()
    .Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"));

var host = builder.Build();
await host.RunAsync();
