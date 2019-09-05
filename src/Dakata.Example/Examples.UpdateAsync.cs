using Dakata.Example.Dal;
using Dakata.Example.Models;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Dakata.Example
{
    public partial class Examples
    {
        private static async Task UpdateAsyncExamples(DapperConnection dapperConnection)
        {
            await UpdateAllAsyncExample(dapperConnection);
            await UpdateAsyncExample(dapperConnection);
        }

        private static async Task UpdateAllAsyncExample(DapperConnection dapperConnection)
        {
            var purchaseOrderDal = new PurchaseOrderDal(dapperConnection,
                sqlInfo =>
                {
                    Console.WriteLine(sqlInfo.Sql);
                });

            var now = DateTime.UtcNow.TruncateDateTimeToSeconds();
            var today = now.Date;
            var pos = new List<PurchaseOrder>
            {
                new PurchaseOrder
                {
                    SupplierID = 2,
                    ContactPersonID = 1001,
                    DeliveryMethodID = 1,
                    ExpectedDeliveryDate = today.AddDays(3),
                    IsOrderFinalized = true,
                    LastEditedBy = 1001,
                    LastEditedWhen = now,
                    OrderDate = today
                },
                new PurchaseOrder
                {
                    SupplierID = 3,
                    ContactPersonID = 1001,
                    DeliveryMethodID = 1,
                    ExpectedDeliveryDate = today.AddDays(4),
                    IsOrderFinalized = true,
                    LastEditedBy = 1001,
                    LastEditedWhen = now,
                    OrderDate = today
                },
                new PurchaseOrder
                {
                    SupplierID = 4,
                    ContactPersonID = 1001,
                    DeliveryMethodID = 1,
                    ExpectedDeliveryDate = today.AddDays(5),
                    IsOrderFinalized = true,
                    LastEditedBy = 1001,
                    LastEditedWhen = now,
                    OrderDate = today
                }
            };

            await purchaseOrderDal.InsertAllAsync(pos);

            var insertedPos = await purchaseOrderDal.QueryByParametersAsync(new
            {
                LastEditedWhen = now
            });

            await purchaseOrderDal.ChangeSupplier(insertedPos, 5);

            /* Delete the just inserted PurchaseOrders so the side facts are the smallest */
            await purchaseOrderDal.DeleteAllAsync(insertedPos);
        }

        private static async Task UpdateAsyncExample(DapperConnection dapperConnection)
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

            po.Comments = "Committed";
            await purchaseOrderDal.UpdateAsync(po, columnsToUpdate: new[] { nameof(PurchaseOrder.Comments) });

            /* Delete the just inserted PurchaseOrder so the side facts are the smallest */
            await purchaseOrderDal.DeleteAsync(po);
        }
    }
}
