using KafkaParallelConsumer.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaParallelConsumer.Services
{
    public interface IOrderService
    {
        Task<int> SaveOrderData(OrderRequest orderRequest);
    }
}
