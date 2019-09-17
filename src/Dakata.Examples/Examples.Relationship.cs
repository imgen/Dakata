using Dakata.Examples.Dal;
using Dakata.Examples.Models;
using FluentAssertions;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Dakata.Examples
{
    public partial class Examples
    {
        [Fact]
        public async Task SimpleInclude()
        {
            var purchaseOrderDal = new PurchaseOrderDal(CreateDapperConnection());
            var keyColumnName = purchaseOrderDal.GetKeyColumnName();
            var query = purchaseOrderDal.NewQuery().Where(purchaseOrderDal.AddTablePrefix(keyColumnName), 5);
            // Join PurchaseOrderLine.PurchaseOrderID from PurchaseOrder.ID
            purchaseOrderDal.Include<PurchaseOrderLine>(query, 
                po => po.ID == po.PurchaseOrderLines.First().PurchaseOrderID);
            purchaseOrderDal.Include<PurchaseOrderLine, PackageType>(
                query, 
                pol => pol.PackageTypeID == pol.PackageType.ID,
                selectPrefix: nameof(PurchaseOrder.PurchaseOrderLines)
            );
            var results = await purchaseOrderDal.QueryAndMapDynamicAsync(query);
            var purchaseOrder = results.FirstOrDefault();
            purchaseOrder.PurchaseOrderLines.Should().NotBeNull();
            purchaseOrder.PurchaseOrderLines.ForEach(x => x.PackageType.Should().NotBeNull());
        }
    }
}
