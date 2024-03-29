﻿using Dakata.Examples.Dal;
using Dakata.Examples.Models;
using System;
using System.Collections.Generic;
using Xunit;

namespace Dakata.Examples;

public partial class Examples
{
    [Fact]
    public void InsertExample()
    {
        var purchaseOrderDal = new PurchaseOrderDal(CreateDapperConnection());

        var po = new PurchaseOrder
        {
            SupplierId = 2,
            ContactPersonId = 1001,
            DeliveryMethodId = 1,
            ExpectedDeliveryDate = DateTime.UtcNow.AddDays(3),
            IsOrderFinalized = true,
            LastEditedBy = 1001
        };

        DbUtils.WithTransaction(transaction =>
        {
            var purchaseOrderId = purchaseOrderDal.Insert(po,
                columnName =>
                {
                    return columnName switch
                    {
                        nameof(PurchaseOrder.OrderDate) => "CONVERT (date, SYSUTCDATETIME())", // Only the date part
                        nameof(PurchaseOrder.LastEditedWhen) => purchaseOrderDal.DbProvider.UtcNowExpression,
                        _ => null
                    };
                }
            );
            _testOutputHelper.WriteLine($"The ID of just inserted PurchaseOrder is {purchaseOrderId}");

            var orderDate = po.OrderDate;
            _testOutputHelper.WriteLine($"The order date of just inserted PurchaseOrder is {orderDate}");
            var lastEditWhen = po.LastEditedWhen;
            _testOutputHelper.WriteLine($"The last edit time of just inserted PurchaseOrder is {lastEditWhen}");

            /* Delete the just inserted PurchaseOrder so the side facts are the smallest */
            purchaseOrderDal.Delete(po);
        });
    }

    [Fact]
    public void InsertAllExample()
    {
        var purchaseOrderDal = new PurchaseOrderDal(CreateDapperConnection());

        var now = DateTime.UtcNow.TruncateDateTimeToSeconds();
        var today = now.Date;
        var pos = new List<PurchaseOrder>
        {
            new()
            {
                SupplierId = 2,
                ContactPersonId = 1001,
                DeliveryMethodId = 1,
                ExpectedDeliveryDate = today.AddDays(3),
                IsOrderFinalized = true,
                LastEditedBy = 1001,
                LastEditedWhen = now,
                OrderDate = today
            },
            new()
            {
                SupplierId = 3,
                ContactPersonId = 1001,
                DeliveryMethodId = 1,
                ExpectedDeliveryDate = today.AddDays(4),
                IsOrderFinalized = true,
                LastEditedBy = 1001,
                LastEditedWhen = now,
                OrderDate = today
            },
            new()
            {
                SupplierId = 4,
                ContactPersonId = 1001,
                DeliveryMethodId = 1,
                ExpectedDeliveryDate = today.AddDays(5),
                IsOrderFinalized = true,
                LastEditedBy = 1001,
                LastEditedWhen = now,
                OrderDate = today
            }
        };

        DbUtils.WithTransaction(transaction =>
        {
            // ReSharper disable once UnusedVariable
            var batchSize = purchaseOrderDal.InsertAll(pos);

            /* Delete the just inserted PurchaseOrders so the side facts are the smallest */
            purchaseOrderDal.DeleteByParameters(new
            {
                LastEditedWhen = now
            });
        });
    }
}