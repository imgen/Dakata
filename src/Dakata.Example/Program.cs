using Dakata.Example.Dal;
using System;
using System.Linq;

namespace Dakata.Example
{
    public class Program
    {
        private const string ConnectionString = "Data Source=(local);Initial Catalog=WorldWideImporters;Integrated Security=True;MultipleActiveResultSets=True";

        static void Main(string[] args)
        {
            var connection = new DapperConnection(ConnectionString, new SqlServerDbProvider());
            GetAllExample(connection);

            GetMaxValueOfColumnExample(connection);
        }

        private static void GetAllExample(DapperConnection connection)
        {
            var purchaseOrderDal = new PurchaseOrderDal(connection);

            var top100PurchaseOrders = purchaseOrderDal.GetAll(100);
            foreach (var po in top100PurchaseOrders)
            {
                Console.WriteLine($"PO's ID is {po.PurchaseOrderID}, PO's expected delivery data is {po.ExpectedDeliveryDate}");
            }

            // If limit parameter not provided, or is 0, will retrieve all records
            var allPurchaseOrders = purchaseOrderDal.GetAll().ToArray(); 
            Console.WriteLine($"There are {allPurchaseOrders.Length} purchase orders");
        }

        private static void GetMaxValueOfColumnExample(DapperConnection connection)
        {
            var purchaseOrderDal = new PurchaseOrderDal(connection);

            var latestExpectedDeliveryDate = purchaseOrderDal.GetLatestExpectedDeliveryDate();
            Console.WriteLine($"The latest expected delivery date is {latestExpectedDeliveryDate}");
        }
    }
}
