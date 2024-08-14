using Confluent.Kafka;

class Program
{
    static async Task Main(string[] args)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:29092" 
        };
        using (var producer = new ProducerBuilder<int, string>(config).Build())
        {
            await producer.ProduceAsync("test-topic", new Message<int, string> {Key = 0,Value = "Apache Kafka" });
            producer.Flush(TimeSpan.FromSeconds(10));
        }
    }
}