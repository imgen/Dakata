using Dakata.Example.Dal;
using Dakata.Example.Models;
using System;

namespace Dakata.Example
{
    public partial class Examples
    {
        private static void InsertExamples(DapperConnection dapperConnection)
        {
            InsertExample(dapperConnection);
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
    }
}
