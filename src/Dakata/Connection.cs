using System;
using System.Collections.Generic;
using System.Data;
using Dapper;
using Dapper.Contrib.Extensions;
using Slapper;
using SqlKata;

// ReSharper disable MemberCanBePrivate.Global

namespace Dakata
{
    public static class Connection
    {
        private static string _connectionString;
        private static IDbProvider _dbProvider;

        static Connection()
        {
            SqlMapper.AddTypeHandler(new DateTimeDapperTypeHandler());
            AutoMapper.Configuration.TypeConverters.Add(new DateTimeAutoMapperTypeConverter());
        }

        public static void Initialize(string connectionString, IDbProvider dbProvider)
        {
            _connectionString = connectionString;
            SqlUtils.CompilerProvider = dbProvider.SqlCompilerProvider;
            BaseDal.DbProvider = _dbProvider = dbProvider;
            dbProvider.Initialize();
        }

        public static long Insert<T>(T entityToInsert, IDbTransaction transaction = null,
            int? commandTimeout = null) where T : class
        {
            return Execute(conn => conn.Insert(entityToInsert, transaction, commandTimeout));
        }

        public static bool Update<T>(T entityToUpdate, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            return Execute(conn => conn.Update(entityToUpdate, transaction, commandTimeout));
        }

        public static T ExecuteScalar<T>(string sql, object param = null,
            IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            return Execute(conn => conn.ExecuteScalar<T>(sql, param, transaction, commandTimeout));
        }

        public static T ExecuteScalar<T>(Query query)
        {
            return query.ExecuteWithSqlKataQuery((sql, parameter) => ExecuteScalar<T>(sql, parameter));
        }

        public static int Execute(string sql, object param = null,
            IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            return Execute(conn => conn.Execute(sql, param, transaction, commandTimeout));
        }

        public static void Execute(Query query)
        {
            query.ExecuteWithSqlKataQuery((sql, parameter) => Execute(sql, parameter));
        }

        public static IEnumerable<T> Query<T>(string sql, object param = null, IDbTransaction transaction = null, bool buffered = true, int? commandTimeout = null, CommandType? commandType = null)
        {
            return Execute(conn => conn.Query<T>(sql, param, transaction, buffered, commandTimeout, commandType));
        }

        public static IEnumerable<T> Query<T>(Query query)
        {
            return query.ExecuteWithSqlKataQuery((sql, parameter) => Query<T>(sql, parameter));
        }

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TReturn>(string sql, Func<TFirst, TSecond, TReturn> map, object param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
        {
            return Execute(conn => conn.Query(sql, map, param, transaction, buffered, splitOn,
                    commandTimeout, commandType));
        }

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TReturn>(string sql, Func<TFirst, TSecond, TThird, TReturn> map, object param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
        {
            return Execute(conn => conn.Query(sql, map, param, transaction, buffered, splitOn,
                    commandTimeout, commandType));
        }

        public static IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TReturn> map, object param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
        {
            return Execute(conn => conn.Query(sql, map, param, transaction, buffered, splitOn,
                    commandTimeout, commandType));
        }

        public static T Get<T>(object id, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            return Execute(conn => conn.Get<T>(id, transaction, commandTimeout));
        }

        public static IEnumerable<T> GetAll<T>(IDbTransaction transaction = null, int? commandTimeout = null)
            where T : class
        {
            return Execute(conn => conn.GetAll<T>(transaction, commandTimeout));
        }

        public static bool Delete<T>(T entityToDelete, IDbTransaction transaction = null,
            int? commandTimeout = null) where T : class
        {
            return Execute(conn => conn.Delete(entityToDelete, transaction, commandTimeout));
        }

        public static T Execute<T>(Func<IDbConnection, T> func)
        {
            var connection = _dbProvider.CreateConnection(_connectionString);

            using (var conn = connection)
            {
                try
                {
                    return func(conn);
                }
                finally
                {
                    conn.Close();
                }
            }
        }
    }
}