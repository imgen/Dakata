using Dakata.Example.Dal;
using Dakata.Example.Models;
using System;
using System.Threading.Tasks;

namespace Dakata.Example
{
    public partial class Examples
    {
        private static async Task DeleteAsyncExamples(DapperConnection dapperConnection)
        {
            await DeleteByIdAsyncExample(dapperConnection);
        }

        private static async Task DeleteByIdAsyncExample(DapperConnection dapperConnection)
        {
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

            /* Delete the just inserted PurchaseOrder so the side facts are the smallest */
            await purchaseOrderDal.DeleteByIdAsync(po.PurchaseOrderID);

            po = await purchaseOrderDal.GetAsync(po.PurchaseOrderID);
            if (po != null)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.Error.WriteLine("ERROR: Oops, DeleteByIdAsync didn't work as expected");
                Console.ResetColor();
            }
        }
    }
}
