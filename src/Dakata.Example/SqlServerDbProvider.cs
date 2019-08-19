using System;
using System.Data;
using SqlKata.Compilers;
using System.Data;
using System.Data.Common;

namespace Dakata.Example
{
    public class SqlServerDbProvider : IDbProvider
    {
        public DbEngines DbEngine => DbEngines.SqlServer;

        public Func<Compiler> SqlCompilerProvider => () => new SqlServerCompiler();

        public string UtcNowExpression => throw new NotImplementedException();

        public int MaxParameterCount => 1000;

        public IDbConnection CreateConnection(string connectionString)
        {
            return new DbConnection(connectionString);
        }

        public long Insert(string sql, object parameters, IDbConnection connection)
        {
            throw new NotImplementedException();
        }
    }
}
