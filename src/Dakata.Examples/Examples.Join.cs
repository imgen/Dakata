using Dakata.Examples.Dal;
using FluentAssertions;
using System;
using System.Threading.Tasks;
using Xunit;

namespace Dakata.Examples
{
    public partial class Examples
    {
        [Fact]
        public async Task SimpleOneLevelJoin()
        {
            var purchaseOrderDal = new PurchaseOrderDal(CreateDapperConnection(), sqlInfo => 
            {
                Console.WriteLine($"The sql is {sqlInfo.Sql}");
            });
            var firstPo = await purchaseOrderDal.GetFirstAsync();
            var poWithLines = await purchaseOrderDal.GetPurchaseOrderWithLines(firstPo.ID);
            poWithLines.Should().NotBeNull("GetPurchaseOrderWithLines doesn't work as expected");
            poWithLines.PurchaseOrderLines.Should().NotBeNullOrEmpty();
        }

        [Fact]
        public async Task TwoLevelJoin()
        {
            var purchaseOrderDal = new PurchaseOrderDal(CreateDapperConnection(), sqlInfo =>
            {
                Console.WriteLine($"The sql is {sqlInfo.Sql}");
            });
            var firstPo = await purchaseOrderDal.GetFirstAsync();
            var poWithLinesAndPackageType = await purchaseOrderDal.GetPurchaseOrderWithLinesAndPackageType(firstPo.ID);
            poWithLinesAndPackageType.Should().NotBeNull("GetPurchaseOrderWithLinesAndPackageType doesn't work as expected");
            poWithLinesAndPackageType.PurchaseOrderLines.Should().NotBeNullOrEmpty();
            poWithLinesAndPackageType.PurchaseOrderLines.ForEach(x => x.PackageType.Should().NotBeNull());
        }
    }
}
