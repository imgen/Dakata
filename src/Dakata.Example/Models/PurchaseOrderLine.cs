using Dapper.Contrib.Extensions;
using System;

namespace Dakata.Example.Models
{
    [Table("Purchasing.PurchaseOrderLines")]
    public class PurchaseOrderLine
    {
        [Key]
        public int PurchaseOrderLineID { get; set; }
        public int PurchaseOrderID { get; set; }
        public int StockItemID { get; set; }
        public int OrderedOuters { get; set; }
        public string Description { get; set; }
        public int ReceivedOuters { get; set; }
        public int PackageTypeID { get; set; }
        public object ExpectedUnitPricePerOuter { get; set; }
        public DateTime? LastReceiptDate { get; set; }
        public bool IsOrderLineFinalized { get; set; }
        public int LastEditedBy { get; set; }
        public DateTime LastEditedWhen { get; set; }
    }
}
