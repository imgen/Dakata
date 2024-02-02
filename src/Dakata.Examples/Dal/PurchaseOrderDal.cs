using Dakata.Examples.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dakata.Examples.Dal
{
    public class PurchaseOrderDal : BaseDal<PurchaseOrder>
    {
        public PurchaseOrderDal(DapperConnection connection, Action<SqlInfo> logger = null) : base(connection, logger) { }

        public DateTime GetLatestExpectedDeliveryDate() =>
            GetMaxValueOfColumn<DateTime>(nameof(Entity.ExpectedDeliveryDate));

        public DateTime GetEarliestExpectedDeliveryDate() =>
            GetMinValueOfColumn<DateTime>(nameof(Entity.ExpectedDeliveryDate));

        public int GetCountOfPurchaseOrdersSince(DateTime date) =>
            GetCount<int>(NewQuery().WhereDate(nameof(Entity.OrderDate), ">=", date));

        public async Task<PurchaseOrder> GetPurchaseOrderWithLines(int id)
        {
            var keyColumnName = GetKeyColumnName();
            var query = NewQuery().Where(AddTablePrefix(keyColumnName), id);
            // Join PurchaseOrderLine.PurchaseOrderID from PurchaseOrder.ID
            IncludeList(query,
                navigationProperty: x => x.PurchaseOrderLines,
                joinProperty: x => x.PurchaseOrderId,
                baseProperty: x => x.Id
            );

            var results = await QueryAndMapDynamicAsync(query);
            return results.FirstOrDefault();
        }

        public async Task<PurchaseOrder> GetPurchaseOrderWithLinesAndPackageType(int id)
        {
            var keyColumnName = GetKeyColumnName();
            var query = NewQuery().Where(AddTablePrefix(keyColumnName), id);

            // Join PurchaseOrderLine.PurchaseOrderID from PurchaseOrder.ID
            // and then join PackageType.ID from PurchaseOrderLine.PackageTypeID
            IncludeList(query,
                navigationProperty: x => x.PurchaseOrderLines,
                joinProperty: x => x.PurchaseOrderId,
                baseProperty: x => x.Id
            ).Include<PurchaseOrderLine, PackageType, int>(query,
                selectPrefix: $"{nameof(Entity.PurchaseOrderLines)}_{nameof(PurchaseOrderLine.PackageType)}",
                joinProperty: x => x.Id,
                baseProperty: x => x.PackageTypeId
            );

            var results = await QueryAndMapDynamicAsync(query);
            return results.FirstOrDefault();
        }

        public async Task<PurchaseOrder> GetPurchaseOrderWithLinesAndPackageType2(int id)
        {
            var keyColumnName = GetKeyColumnName();
            var query = NewQuery().Where(AddTablePrefix(keyColumnName), id);
            // Join PurchaseOrderLine.PurchaseOrderID from PurchaseOrder.ID
            // and then join PackageType.ID from PurchaseOrderLine.PackageTypeID
            Include<PurchaseOrderLine>(query,
                (po, pol) => po.Id == pol.PurchaseOrderId,
                nameof(PurchaseOrder.PurchaseOrderLines)
            ).Include<PurchaseOrderLine, PackageType>(
                query,
                (pol, packageType) => pol.PackageTypeId == packageType.Id,
                selectPrefix: $"{nameof(PurchaseOrder.PurchaseOrderLines)}_{nameof(PurchaseOrderLine.PackageType)}"
            );
            var results = await QueryAndMapDynamicAsync(query);
            return results.FirstOrDefault();
        }

        public async Task<PurchaseOrder> GetPurchaseOrderWithLinesAndPackageType3(int id)
        {
            var keyColumnName = GetKeyColumnName();
            var query = NewQuery().Where(AddTablePrefix(keyColumnName), id);
            // Join PurchaseOrderLine.PurchaseOrderID from PurchaseOrder.ID
            // and then join PackageType.ID from PurchaseOrderLine.PackageTypeID
            Include(query, po => po.Id == po.PurchaseOrderLines[0].PurchaseOrderId)
                .Include<PurchaseOrderLine>(
                    query,
                    pol => pol.PackageTypeId == pol.PackageType.Id,
                    selectPrefix: nameof(PurchaseOrder.PurchaseOrderLines)
                );
            var results = await QueryAndMapDynamicAsync(query);
            return results.FirstOrDefault();
        }

        public async Task<PurchaseOrder> GetPurchaseOrderWithLinesAndPackageType4(int id)
        {
            var keyColumnName = GetKeyColumnName();
            var query = NewQuery().Where(AddTablePrefix(keyColumnName), id);
            // Join PurchaseOrderLine.PurchaseOrderID from PurchaseOrder.ID
            // and then join PackageType.Id from PurchaseOrderLine.PackageTypeId
            MultipleInclude<PurchaseOrderLine>(query,
                (po, pol) => po.PurchaseOrderLines[0].PurchaseOrderId == po.Id &&
                            pol.PackageType.Id == pol.PackageTypeId);
            var results = await QueryAndMapDynamicAsync(query);
            return results.FirstOrDefault();
        }

        public async Task ChangeSupplier(IEnumerable<PurchaseOrder> purchaseOrders, int newSupplierId)
        {
            var purchaseOrdersArray = purchaseOrders.ToArray();
            foreach (var po in purchaseOrdersArray)
            {
                po.SupplierId = newSupplierId;
            }

            await UpdateAllAsync(purchaseOrdersArray,
                columnsToUpdate: GetColumnName(x => x.SupplierId)
            );
        }
    }
}
