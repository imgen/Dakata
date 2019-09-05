using Dapper.ColumnMapper;
using Dapper.Contrib.Extensions;
using System;
using System.Collections.Generic;

namespace Dakata.Example.Models
{
    [Table("Purchasing.PurchaseOrders")]
    public class PurchaseOrder
    {
        [Key, AutoIncrement(SequenceName = "PurchaseOrderID"), ColumnMapping("PurchaseOrderID")]
        public int ID { get; set; }
        public int SupplierID { get; set; }
        public DateTime OrderDate { get; set; }
        public int DeliveryMethodID { get; set; }
        public int ContactPersonID { get; set; }
        public DateTime? ExpectedDeliveryDate { get; set; }
        public string SupplierReference { get; set; }
        public bool IsOrderFinalized { get; set; }
        public string Comments { get; set; }
        public string InternalComments { get; set; }
        public int LastEditedBy { get; set; }
        public DateTime LastEditedWhen { get; set; }

        [Computed]
        public List<PurchaseOrderLine> PurchaseOrderLines { get; set; }
    }
}
