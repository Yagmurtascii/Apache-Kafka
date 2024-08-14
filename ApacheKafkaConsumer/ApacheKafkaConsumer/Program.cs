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

            //Düzgün sonlanmasını sağlar.
            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => 
            {
                e.Cancel = true; // CTRL+C'yi yakalar ve uygulamanın kapanmasını engeller
                cts.Cancel(); // İptal sinyali gönderir
            };

            try
            {
                while (true)
                {
                    //Consume: Bu metod, tüketiciye mesajları almak için çağrılır. Kafka broker'dan mesaj almak üzere bloklanır
                    //ve mesaj geldiğinde döner.
                    //cts.Token'a bağlı bir iptal sinyali alınmazsa, tüketici mesajı başarıyla aldığında döner. 
                    var consumeResult = consumer.Consume(cts.Token);
                    Console.WriteLine($"Consumed message '{consumeResult.Message}' at: '{consumeResult.TopicPartitionOffset}'.");
                }
            }
            //İptal sinyali alınırsa OperationCanceledException
            catch (OperationCanceledException)
            {
                consumer.Close();
            }
        }
    }
}