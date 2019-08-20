using Dakata.Example.Models;
using System;

namespace Dakata.Example.Dal
{
    public class PurchaseOrderDal : BaseDal<PurchaseOrder>
    {
        public PurchaseOrderDal(DapperConnection connection): base(connection) { }

        public DateTime GetLatestExpectedDeliveryDate() => 
            GetMaxValueOfColumn<DateTime>(nameof(Entity.ExpectedDeliveryDate));

        public DateTime GetEarliestExpectedDeliveryDate() =>
            GetMinValueOfColumn<DateTime>(nameof(Entity.ExpectedDeliveryDate));

        public int GetCountOfPurchaseOrdersSince(DateTime date) => 
            GetCount<int>(NewQuery().WhereDate(nameof(Entity.OrderDate), ">=", date));
    }
}
