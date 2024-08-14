using Confluent.Kafka;
class Program
{
    static void Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            GroupId = "test-consumer-group",
            BootstrapServers = "localhost:29092",
            AutoOffsetReset = AutoOffsetReset.Earliest,
        };

        using (var consumer = new ConsumerBuilder<string, string>(config).Build())
        {
            consumer.Subscribe("test-topic");
            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => 
            {
                e.Cancel = true; 
                cts.Cancel(); 
            };

            try
            {
                while (true)
                {
                    var consumeResult = consumer.Consume(cts.Token);
                    Console.WriteLine($"Consumed message '{consumeResult.Message}' at: '{consumeResult.TopicPartitionOffset}'.");
                }
            }
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
        }
    }
}