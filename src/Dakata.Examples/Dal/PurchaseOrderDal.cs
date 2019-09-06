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
            query = InnerJoinTable<PurchaseOrderLine>(query,
                joinTableColumnName: nameof(PurchaseOrderLine.PurchaseOrderID),
                baseTableColumnName: GetColumnName(x => x.ID)
            );
            var purchaseOrderSelections = GetColumnSelections();
            var purchaseOrderLineSelections = GetColumnSelectionsFromEntity<PurchaseOrderLine>(
                prefix: nameof(PurchaseOrder.PurchaseOrderLines)
            );
            var allSelections = purchaseOrderSelections.Concat(purchaseOrderLineSelections).ToArray();
            query = query.Select(allSelections);

            var results = await QueryAndMapDynamicAsync(query);
            return results.FirstOrDefault();
        }

        public async Task<PurchaseOrder> GetPurchaseOrderWithLinesAndPackageType(int id)
        {
            var keyColumnName = GetKeyColumnName();
            var query = NewQuery().Where(AddTablePrefix(keyColumnName), id);

            // Join PurchaseOrderLine.PurchaseOrderID from PurchaseOrder.ID
            query = InnerJoinTable<PurchaseOrderLine>(query,
                joinTableColumnName: nameof(PurchaseOrderLine.PurchaseOrderID),
                baseTableColumnName: GetColumnName(x => x.ID)
            );

            // Join PackageType.ID from PurchaseOrderLine.PackageTypeID
            query = InnerJoinTable<PackageType>(query,
                joinTableColumnName: GetColumnName<PackageType, int>(x => x.ID),
                baseTableColumnName: nameof(PurchaseOrderLine.PackageTypeID),
                baseTableName: DbUtils.GetTableName<PurchaseOrderLine>()
            );

            var purchaseOrderSelections = GetColumnSelections();
            var purchaseOrderLineSelections = GetColumnSelectionsFromEntity<PurchaseOrderLine>(
                prefix: nameof(PurchaseOrder.PurchaseOrderLines)
            );
            var packageTypeSelections = GetColumnSelectionsFromEntity<PackageType>(
                prefix: $"{nameof(PurchaseOrder.PurchaseOrderLines)}_{nameof(PurchaseOrderLine.PackageType)}"
            );
            var allSelections = purchaseOrderSelections
                .Concat(purchaseOrderLineSelections)
                .Concat(packageTypeSelections)
                .ToArray();
            query = query.Select(allSelections);

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
