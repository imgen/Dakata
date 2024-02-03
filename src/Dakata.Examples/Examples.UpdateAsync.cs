using Dakata.Examples.Dal;
using Dakata.Examples.Models;
using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Dakata.Examples;

public partial class Examples
{
    [Fact]
    public async Task UpdateAllAsyncExample()
    {
        var purchaseOrderDal = new PurchaseOrderDal(CreateDapperConnection(),
            sqlInfo => _testOutputHelper.WriteLine(sqlInfo.Sql));

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

        await purchaseOrderDal.InsertAllAsync(pos);

        var insertedPos = (await purchaseOrderDal.QueryByParametersAsync(new
        {
            LastEditedWhen = now
        })).ToList();

        await purchaseOrderDal.ChangeSupplier(insertedPos, 5);

        /* Delete the just inserted PurchaseOrders so the side facts are the smallest */
        await purchaseOrderDal.DeleteAllAsync(insertedPos);
    }

    [Fact]
    public async Task UpdateAsyncExample()
    {
        var purchaseOrderDal = new PurchaseOrderDal(CreateDapperConnection(), sqlInfo => Console.Write(sqlInfo.Sql));

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

        const string comments = "Committed";
        po.Comments = comments;
        await purchaseOrderDal.UpdateAsync(po, columnsToUpdate: nameof(PurchaseOrder.Comments));

        po = await purchaseOrderDal.GetAsync(po.Id);
        po.Comments.Should().Be(comments, "UpdateAsync doesn't work as expected");

        /* Delete the just inserted PurchaseOrder so the side facts are the smallest */
        await purchaseOrderDal.DeleteAsync(po);
    }
}