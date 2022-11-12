using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaParallelConsumer.Models
{
    public class OrderRequest
    {
        public int id { get; set; }
        public DateTime time { get; set; } 

        public string productname { get; set; }
        public int quantity { get; set; }

        public OrderStatus status { get; set; }
    }
    public enum OrderStatus
    {
        IN_PROGRESS,
        COMPLETED,
        REJECTED
    }
}
