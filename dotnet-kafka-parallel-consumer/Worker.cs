using Confluent.Kafka;
using KafkaParallelConsumer.Models;
using KafkaParallelConsumer.Services;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using static Confluent.Kafka.ConfigPropertyNames;

namespace KafkaParallelConsumer;
public class Root
{
    public int id { get; set; }
    public string productname { get; set; }
    public int quantity { get; set; }
    public int status { get; set; }
}

public sealed class Worker<TKey, TValue> : BackgroundService
{
    private readonly ILogger<Worker<TKey, TValue>> logger;
    private readonly IConsumer<TKey, TValue> consumer;
    private readonly Processor<TKey, TValue> processor;
    private readonly ChannelProvider<TKey, TValue> channelProvider;
    private readonly IEnumerable<string> topics;
    private readonly IOrderService _orderService; 
    public Worker(ILogger<Worker<TKey, TValue>> logger, IConsumer<TKey, TValue> consumer,
        Processor<TKey, TValue> processor, ChannelProvider<TKey, TValue> channelProvider, 
        IOptions<WorkerOptions> options, IOrderService orderService) 
    {
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.consumer = consumer ?? throw new ArgumentNullException(nameof(consumer));
        this.processor = processor ?? throw new ArgumentNullException(nameof(processor));
        this.channelProvider = channelProvider ?? throw new ArgumentNullException(nameof(channelProvider));

        if (options?.Value?.Topics is null) throw new ArgumentNullException(nameof(options));
        topics = options.Value.Topics;
        _orderService = orderService;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            // because consumer.Consume() blocks, Task.Yield() fixes https://github.com/dotnet/runtime/issues/36063 without creating a new thread or using Task.Run
            await Task.Yield();

            logger.LogInformation("Subscribing to {topics}", string.Join(',', topics));

            //  consumer.Subscribe("topic");
            consumer.Subscribe(new List<string>() { "topic" });
            var start = DateTime.Now;

            while (!stoppingToken.IsCancellationRequested)
            {
                var consumeResult = consumer.Consume();

                logger.LogTrace("Received message at {messageTopicPartitionOffset}: {messageValue}", consumeResult.TopicPartitionOffset, consumeResult.Message.Value);

                if (consumeResult.Message.Value != null)
                {
                    //Thread.Sleep(1000);

                    var json = Convert.ToString(consumeResult.Message.Value);

                    var tag = JsonConvert.DeserializeObject<OrderRequest>(json);
                    tag.time = DateTime.Now;
                    await _orderService.SaveOrderData(tag);

                    Console.WriteLine("row inserted");

                    // This call will guarantee that this message that will not be included in the current batch, will be included in another batch later
                    // consumer.Seek(consumeResult.TopicPartitionOffset); // IMPORTANT LINE!!!!!!!
                    consumer.Commit(consumeResult);
                }

                // Console.WriteLine("Received message at {messageTopicPartitionOffset}: {messageValue}", consumeResult.TopicPartitionOffset, consumeResult.Message.Value);

                //var channelWriter = channelProvider.GetChannelWriter(consumer, consumeResult.TopicPartition, processor.ProcessAsync, stoppingToken);

                //await channelWriter.WriteAsync(consumeResult, stoppingToken);
            }
            Console.WriteLine("Total time duration 10000 records consumed :" + (DateTime.Now - start).TotalSeconds);
        }
        catch (Exception ex)
        {
            // This call will guarantee that this message that will not be included in the current batch, will be included in another batch later
          //  _consumer.Seek(result.TopicPartitionOffset); // IMPORTANT LINE!!!!!!!
        }
    }
}
