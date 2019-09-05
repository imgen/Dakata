using Dakata.Example.Models;
using Dapper.ColumnMapper;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Dakata.Example
{
    public partial class Examples
    {
        private const string ConnectionString = "Data Source=(local);Initial Catalog=WorldWideImporters;Integrated Security=True;MultipleActiveResultSets=True";

        private static void RegisterColumnMappingsAndSlapperIdentifiers()
        {
            var poType = typeof(PurchaseOrder);
            string nspace = typeof(PurchaseOrder).Namespace;

            var modelTypes = poType.Assembly.GetTypes()
                .Where(t => t.IsClass && !t.IsAbstract && t.Namespace == nspace)
                .ToArray();
            ColumnTypeMapper.RegisterForTypes(modelTypes);
            SlapperUtils.RegisterIdentifiers(modelTypes);
        }

        private static void WriteError(string message)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.Error.WriteLine($"ERROR: {message}");
            Console.ResetColor();
        }

        static async Task<int> Main(string[] args)
        {
            RegisterColumnMappingsAndSlapperIdentifiers();
            var connection = new DapperConnection(ConnectionString, new SqlServerDbProvider());

            LoggingExamples(connection);

            GetExamples(connection);

            InsertExamples(connection);

            await JoinExamples(connection);

            await DeleteAsyncExamples(connection);

            return 0;
        }
    }
}
