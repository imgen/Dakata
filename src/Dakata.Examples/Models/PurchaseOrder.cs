using Dapper.ColumnMapper;
using Dapper.Contrib.Extensions;
using System;
using System.Collections.Generic;

namespace Dakata.Examples.Models;

[Table("Purchasing.PurchaseOrders")]
public class PurchaseOrder
{
    [Key, AutoIncrement(SequenceName = "PurchaseOrderId"), ColumnMapping("PurchaseOrderId")]
    public int Id { get; set; }
    public int SupplierId { get; set; }
    public DateTime OrderDate { get; set; }
    public int DeliveryMethodId { get; set; }
    public int ContactPersonId { get; set; }
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