using Dakata.Examples.Models;
using Dakata.SqlServer;
using Dapper.ColumnMapper;
using System.Linq;
using Xunit.Abstractions;

namespace Dakata.Examples;

public partial class Examples
{
    private const string ConnectionString = "Data Source=(local);Initial Catalog=WorldWideImporters;Integrated Security=True;MultipleActiveResultSets=True";

    private readonly ITestOutputHelper _testOutputHelper;

    public Examples(ITestOutputHelper testOutputHelper) => _testOutputHelper = testOutputHelper;

    private static void RegisterColumnMappingsAndSlapperIdentifiers()
    {
        var poType = typeof(PurchaseOrder);
        var @namespace = typeof(PurchaseOrder).Namespace;

        var modelTypes = poType.Assembly.GetTypes()
            .Where(t => t.IsClass && !t.IsAbstract && t.Namespace == @namespace)
            .ToArray();
        // If ColumnMapping attribute is present, will be processed and configured with Dapper
        ColumnTypeMapper.RegisterForTypes(modelTypes);

        // Alternatively we can use Slapper.AutoMapper.Id attribute, please see
        // https://github.com/SlapperAutoMapper/Slapper.AutoMapper
        SlapperUtils.RegisterIdentifiers(modelTypes);
    }

    static Examples()
    {
        // This is the initialization you probably should do at the startup of your application
        RegisterColumnMappingsAndSlapperIdentifiers();
    }

    private DapperConnection CreateDapperConnection() => new DapperConnection(ConnectionString, new SqlServerDbProvider());
}