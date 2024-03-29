﻿using System;
using SqlKata.Compilers;
using Dapper;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Data.Common;
using Microsoft.Data.SqlClient;

namespace Dakata.SqlServer;

public class SqlServerDbProvider : IDbProvider
{
    public DbEngines DbEngine => DbEngines.SqlServer;

    public Func<Compiler> SqlCompilerProvider => () => new SqlServerCompiler();

    public string UtcNowExpression => "SYSUTCDATETIME()";

    public int MaxParameterCount => 2100;

    public DbConnection CreateConnection(string connectionString) => new SqlConnection(connectionString);

    public long Insert(string sql, object parameters, DbConnection connection, string sequenceName, int? commandTimeout = null)
    {
        sql = AddSelectIdStatement(sql, sequenceName);
        var results = connection.Query<dynamic>(sql, parameters, commandTimeout: commandTimeout);
        return GetId(results);
    }

    public async Task<long> InsertAsync(string sql, object parameters, DbConnection connection, string sequenceName, int? commandTimeout = null)
    {
        sql = AddSelectIdStatement(sql, sequenceName);
        var results = await connection.QueryAsync<dynamic>(sql, parameters, commandTimeout: commandTimeout);
        return GetId(results);
    }

    private static string AddSelectIdStatement(string sql, string sequenceName) =>
        sql + (
            string.IsNullOrEmpty(sequenceName) ?
                ";select SCOPE_IDENTITY() id" :
                $";select current_value AS id from sys.sequences where name = '{sequenceName}'"
        );

    private static long GetId(IEnumerable<dynamic> results)
    {
        var first = results.FirstOrDefault();
        if (first == null)
            return 0;
        var id = first.id;
        return id == null ? 0 : (long)id;
    }
}