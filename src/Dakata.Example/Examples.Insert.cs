using Dakata.Example.Dal;
using Dakata.Example.Models;
using System;
using System.Collections.Generic;

namespace Dakata.Example
{
    public partial class Examples
    {
        private static void InsertExamples(DapperConnection dapperConnection)
        {
            InsertExample(dapperConnection);
            InsertAllExample(dapperConnection);
        }

        private static void InsertExample(DapperConnection dapperConnection)
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

            DbUtils.WithTransaction(transaction =>
            {
                var purchaseOrderId = purchaseOrderDal.Insert(po,
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
                purchaseOrderDal.Delete(po);
            });
        }

        private static void InsertAllExample(DapperConnection dapperConnection)
        {
            var purchaseOrderDal = new PurchaseOrderDal(dapperConnection);

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

            DbUtils.WithTransaction(transaction =>
            {
                var batchSize = purchaseOrderDal.InsertAll(pos);

                /* Delete the just inserted PurchaseOrders so the side facts are the smallest */
                purchaseOrderDal.DeleteByParameters(new
                {
                    LastEditedWhen = now
                });
            });
        }
    }
}
