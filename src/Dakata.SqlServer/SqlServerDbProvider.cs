using System;
using System.Data;
using SqlKata.Compilers;
using System.Data.SqlClient;
using Dapper;
using System.Linq;

namespace Dakata.Example
{
    public class SqlServerDbProvider : IDbProvider
    {
        public DbEngines DbEngine => DbEngines.SqlServer;

        public Func<Compiler> SqlCompilerProvider => () => new SqlServerCompiler();

        public string UtcNowExpression => "SYSUTCDATETIME()";

        public int MaxParameterCount => 2100;

        public IDbConnection CreateConnection(string connectionString) => new SqlConnection(connectionString);

        public long Insert(string sql, object parameters, IDbConnection connection)
        {
            sql += ";select SCOPE_IDENTITY() id";
            var results = connection.Query<dynamic>(sql, parameters);
            dynamic first = results.FirstOrDefault();
            if (first == null)
            {
                return 0;
            }
            var id = first.id;
            return id == null ? 0 : (long)id;
        }
    }
}
