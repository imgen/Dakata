using Dakata.Examples.Dal;
using Dakata.Examples.Models;
using FluentAssertions;
using System;
using System.Threading.Tasks;
using Xunit;

namespace Dakata.Examples;

public partial class Examples
{
    [Fact]
    public async Task DeleteByIdAsyncExample()
    {
        var dapperConnection = CreateDapperConnection();
        var purchaseOrderDal = new PurchaseOrderDal(dapperConnection);

        var po = new PurchaseOrder
        {
            SupplierId = 2,
            ContactPersonId = 1001,
            DeliveryMethodId = 1,
            ExpectedDeliveryDate = DateTime.UtcNow.AddDays(3),
            IsOrderFinalized = true,
            LastEditedBy = 1001
        };

        var purchaseOrderId = await purchaseOrderDal.InsertAsync(po,
            columnName =>
            {
                if (columnName == nameof(PurchaseOrder.OrderDate))
                {
                    // Only the date part
                    return "CONVERT (date, SYSUTCDATETIME())";
                }
                if (columnName == nameof(PurchaseOrder.LastEditedWhen))
                {
                    return purchaseOrderDal.DbProvider.UtcNowExpression;
                }
                return null;
            }
        );
        Console.WriteLine($"The ID of just inserted PurchaseOrder is {purchaseOrderId}");
            
        var orderDate = po.OrderDate;
        Console.WriteLine($"The order date of just inserted PurchaseOrder is {orderDate}");
        var lastEditWhen = po.LastEditedWhen;
        Console.WriteLine($"The last edit time of just inserted PurchaseOrder is {lastEditWhen}");

        po = await purchaseOrderDal.GetAsync(po.Id);

        po.Should().NotBeNull("Oops, GetAsync didn't work as expected");

        /* Delete the just inserted PurchaseOrder so the side facts are the smallest */
        await purchaseOrderDal.DeleteByIdAsync(po.Id);

        po = await purchaseOrderDal.GetAsync(po.Id);
        po.Should().BeNull("Oops, DeleteByIdAsync didn't work as expected");
    }
}