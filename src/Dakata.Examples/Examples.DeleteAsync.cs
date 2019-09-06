using Dakata.Examples.Dal;
using Dakata.Examples.Models;
using FluentAssertions;
using System;
using System.Threading.Tasks;
using Xunit;

namespace Dakata.Examples
{
    public partial class Examples
    {
        [Fact]
        public async Task DeleteByIdAsyncExample()
        {
            var dapperConnection = CreateDapperConnection();
            var purchaseOrderDal = new PurchaseOrderDal(dapperConnection);

            var po = new PurchaseOrder
            {
                SupplierID = 2,
                ContactPersonID = 1001,
                DeliveryMethodID = 1,
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

            po = await purchaseOrderDal.GetAsync(po.ID);

            po.Should().NotBeNull("Oops, GetAsync didn't work as expected");

            /* Delete the just inserted PurchaseOrder so the side facts are the smallest */
            await purchaseOrderDal.DeleteByIdAsync(po.ID);

            po = await purchaseOrderDal.GetAsync(po.ID);
            po.Should().BeNull("Oops, DeleteByIdAsync didn't work as expected");
        }
    }
}
