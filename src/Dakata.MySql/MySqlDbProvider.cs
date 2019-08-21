using System;
using System.Data;
using System.Linq;
using Dapper;
using MySql.Data.MySqlClient;
using SqlKata.Compilers;

namespace Dakata.MySql
{
    public class MySqlDbProvider : IDbProvider
    {
        public DbEngines DbEngine { get; } = DbEngines.MySql;

        public IDbConnection CreateConnection(string connectionString)
        {
            return new MySqlConnection(connectionString);
        }

        public Func<Compiler> SqlCompilerProvider => () => new MySqlCompiler();
        public string DbConnectionName { get; } = "MySqlConnection";
        public string UtcNowExpression { get; } = "NOW()";

        public long Insert(string sql, object parameters, IDbConnection connection)
        {
            sql += ";select LAST_INSERT_ID() id";
            var results = connection.Query<dynamic>(sql, parameters);
            dynamic first = results.FirstOrDefault();
            if (first == null)
            {
                return 0;
            }
            var id = first.id;
            return id == null ? 0 : (long)id;
        }

        public int MaxParameterCount { get; } = 1000;
    }
}
