using Dakata.Example.Models;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Dakata.Example.Dal
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
            query = InnerJoinTable<PurchaseOrderLine>(query, 
                nameof(PurchaseOrderLine.PurchaseOrderID),
                GetColumnName(x => x.ID)
                );
            var purchaseOrderSelections = GetColumnSelections();
            
            var purchaseOrderLineSelections = GetColumnSelectionsFromEntity<PurchaseOrderLine>
                (nameof(PurchaseOrder.PurchaseOrderLines) + "_");
            var allSelections = purchaseOrderSelections.Concat(purchaseOrderLineSelections).ToArray();
            query = query.Select(allSelections);

            var results = await QueryAndMapDynamicAsync(query);
            return results.FirstOrDefault();
        }
    }
}
