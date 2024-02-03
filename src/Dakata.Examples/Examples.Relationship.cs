using Dakata.Examples.Dal;
using FluentAssertions;
using SqlKata;
using System.Threading.Tasks;
using Xunit;

namespace Dakata.Examples;

public partial class Examples
{
    [Fact]
    public async Task SimpleInclude1()
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
    public async Task ManualDeepInclude()
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

    [Fact]
    public async Task ManualDeepInclude2()
    {
        var purchaseOrderDal = new PurchaseOrderDal(
            CreateDapperConnection(),
            sqlInfo => _testOutputHelper.WriteLine($"The sql is {sqlInfo.Sql}")
        );

        var purchaseOrder = await purchaseOrderDal.GetPurchaseOrderWithLinesAndPackageType2(5);
        purchaseOrder.PurchaseOrderLines.Should().NotBeNull();
        purchaseOrder.PurchaseOrderLines.ForEach(x => x.PackageType.Should().NotBeNull());

        purchaseOrder = await purchaseOrderDal.GetPurchaseOrderWithLinesAndPackageType3(5);
        purchaseOrder.PurchaseOrderLines.Should().NotBeNull();
        purchaseOrder.PurchaseOrderLines.ForEach(x => x.PackageType.Should().NotBeNull());
    }

    [Fact]
    public async Task MultipleInclude()
    {
        var purchaseOrderDal = new PurchaseOrderDal(
            CreateDapperConnection(),
            sqlInfo => _testOutputHelper.WriteLine($"The sql is {sqlInfo.Sql}")
        );

        var purchaseOrder = await purchaseOrderDal.GetPurchaseOrderWithLinesAndPackageType4(5);
        purchaseOrder.PurchaseOrderLines.Should().NotBeNull();
        purchaseOrder.PurchaseOrderLines.ForEach(x => x.PackageType.Should().NotBeNull());
    }

    [Fact]
    public void PlayWithQuery()
    {
        var query = new Query().From("Purchaing.PurchaseOrder AS po");
    }
}