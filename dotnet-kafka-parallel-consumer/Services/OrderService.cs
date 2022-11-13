using KafkaParallelConsumer.Models;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace KafkaParallelConsumer.Services
{
    public class OrderService : IOrderService
    {
        public async Task<int> SaveOrderData(OrderRequest orderRequest)
        {
            var cs = "";
            
            using var con = new NpgsqlConnection(cs);
            con.Open();

            var sql = "INSERT INTO rcs_app.order(order_id, productname,quantity,timestamp)" +
                "VALUES(@orderid, @productname,@quantity,@timestamp)";

            using var cmd = new NpgsqlCommand(sql, con);

            cmd.Parameters.AddWithValue("orderid", orderRequest.id);
            cmd.Parameters.AddWithValue("productname", orderRequest.productname);
            cmd.Parameters.AddWithValue("quantity", orderRequest.quantity);
            cmd.Parameters.AddWithValue("timestamp", orderRequest.time);

            cmd.Prepare();

            var result = await cmd.ExecuteNonQueryAsync();

            Console.WriteLine("row inserted");

            return result;
        }
    }
}
