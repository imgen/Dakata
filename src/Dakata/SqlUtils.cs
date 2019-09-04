using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Dapper;
using SqlKata;
using SqlKata.Compilers;

namespace Dakata
{
    public static class SqlUtils
    {
        public static Func<Compiler> CompilerProvider { get; set; }

        private static Compiler Compiler => CompilerProvider();
        public static string GetSqlScript(this Query query)
        {
            return Compiler.Compile(query).Sql;
        }

        public static Dictionary<string, object> GetBindings(this Query query)
        {
            return Compiler.Compile(query).NamedBindings;
        }

        public static SqlResult CompileResult(this Query query)
        {
            return Compiler.Compile(query);
        }

        public static DynamicParameters GetParametersObject(this SqlResult result)
        {
            var parameters = new DynamicParameters();
            foreach (var pair in result.NamedBindings)
            {
                parameters.Add(pair.Key, pair.Value);
            }
            return parameters;
        }

        public static T ExecuteWithSqlKataQuery<T>(this Query query, Func<string, object, T> func)
        {
            var result = query.CompileResult();
            return func(result.Sql, result.GetParametersObject());
        }

        public static async Task<T> ExecuteWithSqlKataQueryAsync<T>(this Query query, Func<string, object, Task<T>> func)
        {
            var result = query.CompileResult();
            return await func(result.Sql, result.GetParametersObject());
        }

        public static void ExecuteWithSqlKataQuery(this Query query, Action<string, object> action)
        {
            query.ExecuteWithSqlKataQuery<object>((sql, parameter) =>
            {
                action(sql, parameter);
                return null;
            });
        }

        public static async Task ExecuteWithSqlKataQueryAsync(this Query query, Func<string, object, Task> action)
        {
            await query.ExecuteWithSqlKataQueryAsync<object>(
                async (sql, parameter) =>
                {
                    await action(sql, parameter);
                    return null;
                });
        }
    }
}