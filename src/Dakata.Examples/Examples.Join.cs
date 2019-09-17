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
                _testOutputHelper.WriteLine($"The sql is {sqlInfo.Sql}");
            });
            var poWithLines = await purchaseOrderDal.GetPurchaseOrderWithLines(1);
            poWithLines.Should().NotBeNull("GetPurchaseOrderWithLines doesn't work as expected");
            poWithLines.PurchaseOrderLines.Should().NotBeNullOrEmpty();
        }

        [Fact]
        public async Task TwoLevelJoin()
        {
            var purchaseOrderDal = new PurchaseOrderDal(CreateDapperConnection(), sqlInfo =>
            {
                _testOutputHelper.WriteLine($"The sql is {sqlInfo.Sql}");
            });
            var poWithLinesAndPackageType = await purchaseOrderDal.GetPurchaseOrderWithLinesAndPackageType(1);
            poWithLinesAndPackageType.Should().NotBeNull("GetPurchaseOrderWithLinesAndPackageType doesn't work as expected");
            poWithLinesAndPackageType.PurchaseOrderLines.Should().NotBeNullOrEmpty();
            poWithLinesAndPackageType.PurchaseOrderLines.ForEach(x => x.PackageType.Should().NotBeNull());
        }
    }
}
