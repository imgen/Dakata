using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using MySqlConnector;
using SqlKata.Compilers;

namespace Dakata.MySql;

public class MySqlDbProvider : IDbProvider
{
    public DbEngines DbEngine { get; } = DbEngines.MySql;

    public DbConnection CreateConnection(string connectionString) => new MySqlConnection(connectionString);

    public Func<Compiler> SqlCompilerProvider => () => new MySqlCompiler();
    public string UtcNowExpression { get; } = "NOW()";

    private const string SelectIdStatement = ";select LAST_INSERT_ID() id";

    public long Insert(string sql, object parameters, IDbConnection connection, string sequenceName, int? commandTimeout = null)
    {
        // Sequence name will be ignored since MySQL doesn't support that
        sql += SelectIdStatement;
        var results = connection.Query<dynamic>(sql, parameters, commandTimeout: commandTimeout);
        return GetId(results);
    }

    public async Task<long> InsertAsync(string sql, object parameters, IDbConnection connection, string sequenceName, int? commandTimeout = null)
    {
        // Sequence name will be ignored since MySQL doesn't support that
        sql += SelectIdStatement;
        var results = await connection.QueryAsync<dynamic>(sql, parameters, commandTimeout: commandTimeout);
        return GetId(results);
    }

    private static long GetId(IEnumerable<dynamic> results)
    {
        var first = results.FirstOrDefault();
        if (first == null)
        {
            return 0;
        }
        var id = first.id;
        return id == null ? 0 : (long)id;
    }

    public int MaxParameterCount { get; } = 1000;
}
