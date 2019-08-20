using Dakata.Example.Dal;
using System;
using System.Linq;
using static System.Console;

namespace Dakata.Example
{
    public partial class Examples
    {
        private static void GetExamples(DapperConnection connection)
        {
            GetAllExample(connection);
            GetMaxValueOfColumnExample(connection);
            GetMinValueOfColumnExample(connection);
            GetCountExample(connection);
        }

        private static void GetAllExample(DapperConnection connection)
        {
            var purchaseOrderDal = new PurchaseOrderDal(connection);

            var top100PurchaseOrders = purchaseOrderDal.GetAll(100);
            foreach (var po in top100PurchaseOrders)
            {
                WriteLine($"PO's ID is {po.PurchaseOrderID}, PO's expected delivery data is {po.ExpectedDeliveryDate}");
            }

            // If limit parameter not provided, or is 0, will retrieve all records
            var allPurchaseOrders = purchaseOrderDal.GetAll().ToArray();
            WriteLine("All purchase orders retrieved");
        }

        private static void GetMaxValueOfColumnExample(DapperConnection connection)
        {
            var purchaseOrderDal = new PurchaseOrderDal(connection);

            var latestExpectedDeliveryDate = purchaseOrderDal.GetLatestExpectedDeliveryDate();
            WriteLine($"The latest expected delivery date is {latestExpectedDeliveryDate}");
        }

        private static void GetMinValueOfColumnExample(DapperConnection connection)
        {
            var purchaseOrderDal = new PurchaseOrderDal(connection);

            var earliestExpectedDeliveryDate = purchaseOrderDal.GetEarliestExpectedDeliveryDate();
            WriteLine($"The earliest expected delivery date is {earliestExpectedDeliveryDate}");
        }

        private static void GetCountExample(DapperConnection connection)
        {
            var purchaseOrderDal = new PurchaseOrderDal(connection);

            var totalCountOfPurchaseOrders = purchaseOrderDal.GetCount<int>();
            WriteLine($"There are {totalCountOfPurchaseOrders} purchase orders");

            var countOfPurchaseOrdersSinceJanuary31st = purchaseOrderDal.GetCountOfPurchaseOrdersSince(DateTime.Parse("2016-01-31"));
            WriteLine($"There are {countOfPurchaseOrdersSinceJanuary31st} purchase orders since 2016-01-31");
        }
    }
}
