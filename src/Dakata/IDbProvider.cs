using System;
using System.Data;
using SqlKata.Compilers;

namespace Dakata
{
    public interface IDbProvider
    {
        void Initialize();
        DbEngines DbEngine { get; }
        IDbConnection CreateConnection(string connectionString);
        Func<Compiler> SqlCompilerProvider { get; }
        string DbConnectionName { get; }
        string UtcNowExpression { get; }
        long Insert(string sql, object parameters, IDbConnection connection);
        int MaxParameterCount { get; }
    }
}