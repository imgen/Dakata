using Dakata.Example.Dal;
using System;
using System.Threading.Tasks;

namespace Dakata.Example
{
    public partial class Examples
    {
        private static async Task JoinExamples(DapperConnection dapperConnection)
        {
            await SimpleOneLevelJoin(dapperConnection);
        }

        private static async Task SimpleOneLevelJoin(DapperConnection dapperConnection)
        {
            var purchaseOrderDal = new PurchaseOrderDal(dapperConnection, sqlInfo => 
            {
                Console.WriteLine($"The sql is {sqlInfo.Sql}");
            });
            var firstPo = await purchaseOrderDal.GetFirstAsync();
            var poWithLines = await purchaseOrderDal.GetPurchaseOrderWithLines(firstPo.ID);
            if (poWithLines == null)
            {
                WriteError($"Purchase Order cannot be found");
            }
        }
    }
}
