using Dakata.Examples.Dal;
using Xunit;

namespace Dakata.Examples;

public partial class Examples
{
    [Fact]
    public void LoggingExamples()
    {
            var purchaseOrderDal = new PurchaseOrderDal(CreateDapperConnection(), 
                sqlInfo => 
                {
                    _testOutputHelper.WriteLine($"The SQL is {sqlInfo.Sql}");
                    foreach(var (key, value) in sqlInfo.Parameters) 
                        _testOutputHelper.WriteLine($"The parameter name is {key}, value is {value}");
                });

            purchaseOrderDal.GetAll(100);
        }
}