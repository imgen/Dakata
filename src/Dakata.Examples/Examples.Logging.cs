using Dakata.Examples.Dal;
using System;

namespace Dakata.Examples
{
    public partial class Examples
    {
        public static void LoggingExamples(DapperConnection dapperConnection)
        {
            var purchaseOrderDal = new PurchaseOrderDal(dapperConnection, 
                sqlInfo => 
                {
                    Console.WriteLine($"The SQL is {sqlInfo.Sql}");
                    foreach(var (key, value) in sqlInfo.Parameters)
                    {
                        Console.WriteLine($"The parameter name is {key}, value is {value}");
                    }
                });

            purchaseOrderDal.GetAll(100);
        }
    }
}
