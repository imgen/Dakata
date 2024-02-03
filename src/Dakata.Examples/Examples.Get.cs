using Dakata.Examples.Dal;
using Dakata.Examples.Models;
using SqlKata;
using System;
using System.Linq;
using Xunit;

namespace Dakata.Examples;

public partial class Examples
{
    [Fact]
    public void GetAllExample()
    {
        var purchaseOrderDal = new PurchaseOrderDal(CreateDapperConnection());

        var top100PurchaseOrders = purchaseOrderDal.GetAll(100);
        foreach (var po in top100PurchaseOrders)
        {
            Console.WriteLine($"PO's ID is {po.Id}, PO's expected delivery data is {po.ExpectedDeliveryDate}");
        }

        // If limit parameter not provided, or is 0, will retrieve all records
        var allPurchaseOrders = purchaseOrderDal.GetAll().ToArray();
        Console.WriteLine($"All {allPurchaseOrders.Length} purchase orders retrieved");
    }

    [Fact]
    public void GetMaxValueOfColumnExample()
    {
        var purchaseOrderDal = new PurchaseOrderDal(CreateDapperConnection());

        var latestExpectedDeliveryDate = purchaseOrderDal.GetLatestExpectedDeliveryDate();
        Console.WriteLine($"The latest expected delivery date is {latestExpectedDeliveryDate}");
    }

    [Fact]
    public void GetMinValueOfColumnExample()
    {
        var purchaseOrderDal = new PurchaseOrderDal(CreateDapperConnection());

        var earliestExpectedDeliveryDate = purchaseOrderDal.GetEarliestExpectedDeliveryDate();
        Console.WriteLine($"The earliest expected delivery date is {earliestExpectedDeliveryDate}");
    }

    [Fact]
    public void GetCountExample()
    {
        var purchaseOrderDal = new PurchaseOrderDal(CreateDapperConnection());

        var totalCountOfPurchaseOrders = purchaseOrderDal.GetCount<int>();
        Console.WriteLine($"There are {totalCountOfPurchaseOrders} purchase orders");

        var countOfPurchaseOrdersSinceJanuary31St = purchaseOrderDal.GetCountOfPurchaseOrdersSince(DateTime.Parse("2016-01-31"));
        Console.WriteLine($"There are {countOfPurchaseOrdersSinceJanuary31St} purchase orders since 2016-01-31");
    }

    [Fact]
    public void OrderByExample()
    {
        var purchaseOrderDal = new PurchaseOrderDal(CreateDapperConnection());

        Query Top10PurchaseOrder() => purchaseOrderDal.NewQuery().Limit(10);

        // Below two statements shows two different ways of doing ordering
        // If multiple columns have different directions of ordering 
        // (ascending / descending), use the second way
        var top10LatestPurchaseOrdersQuery = purchaseOrderDal
            .OrderBy(
                Top10PurchaseOrder(),
                ascending: false,
                nameof(PurchaseOrder.OrderDate),
                nameof(PurchaseOrder.ExpectedDeliveryDate)
            );

        top10LatestPurchaseOrdersQuery = purchaseOrderDal
            .OrderBy(
                Top10PurchaseOrder(),
                (
                    column: nameof(PurchaseOrder.OrderDate),
                    ascending: false
                ),
                (
                    column: nameof(PurchaseOrder.ExpectedDeliveryDate),
                    ascending: true
                )
            );
        var top10LatestPurchaseOrders = purchaseOrderDal.Query(top10LatestPurchaseOrdersQuery).ToList();
        Console.WriteLine($"The top 10 latest purchase orders are retrieved");
    }
}