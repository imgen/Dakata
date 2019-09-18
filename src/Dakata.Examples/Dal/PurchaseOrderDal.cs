using Dakata.Examples.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dakata.Examples.Dal
{
    public class PurchaseOrderDal : BaseDal<PurchaseOrder>
    {
        public PurchaseOrderDal(DapperConnection connection, Action<SqlInfo> logger = null): base(connection, logger) { }

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
                includeProperty: x => x.PurchaseOrderLines,
                joinProperty: x => x.PurchaseOrderID,
                baseProperty: x => x.ID
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
                includeProperty: x => x.PurchaseOrderLines,
                joinProperty: x => x.PurchaseOrderID,
                baseProperty: x => x.ID
            ).Include<PurchaseOrderLine, PackageType, int>(query,
                selectPrefix: $"{nameof(Entity.PurchaseOrderLines)}_{nameof(PurchaseOrderLine.PackageType)}",
                joinProperty: x => x.ID,
                baseProperty: x => x.PackageTypeID
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
                (po, pol) => po.ID == pol.PurchaseOrderID,
                nameof(PurchaseOrder.PurchaseOrderLines)
            ).Include<PurchaseOrderLine, PackageType>(
                query,
                (pol, packageType) => pol.PackageTypeID == packageType.ID,
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
            Include(query, po => po.ID == po.PurchaseOrderLines.First().PurchaseOrderID)
                .Include<PurchaseOrderLine>(
                    query,
                    pol => pol.PackageTypeID == pol.PackageType.ID,
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
            // and then join PackageType.ID from PurchaseOrderLine.PackageTypeID
            Include(query, po => po.ID == po.PurchaseOrderLines.First().PurchaseOrderID)
                .DeepInclude(
                    query,
                    po => po.PurchaseOrderLines.First().PackageTypeID == 
                        po.PurchaseOrderLines.First().PackageType.ID
                );
            var results = await QueryAndMapDynamicAsync(query);
            return results.FirstOrDefault();
        }

        public async Task ChangeSupplier(IEnumerable<PurchaseOrder> purchaseOrders, int newSupplierID)
        {
            var purchaseOrdersArray = purchaseOrders.ToArray();
            foreach(var po in purchaseOrdersArray)
            {
                po.SupplierID = newSupplierID;
            }

            await UpdateAllAsync(purchaseOrders, 
                columnsToUpdate: GetColumnName(x => x.SupplierID)
            );
        }
    }
}
