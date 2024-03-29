﻿using System;
using System.Data.Common;
using System.Threading.Tasks;
using SqlKata.Compilers;

namespace Dakata;

public interface IDbProvider
{
    DbEngines DbEngine { get; }
    DbConnection CreateConnection(string connectionString);
    Func<Compiler> SqlCompilerProvider { get; }
    string UtcNowExpression { get; }
    long Insert(string sql, object parameters, DbConnection connection, string sequenceName, int? commandTimeout = null);
    Task<long> InsertAsync(string sql, object parameters, DbConnection connection, string sequenceName, int? commandTimeout = null);
    int MaxParameterCount { get; }
}