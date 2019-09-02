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
    public  class DapperConnection
    {
        private readonly string _connectionString;
        public IDbProvider DbProvider { get; }

        public Action<SqlInfo> Logger { get; set; } = _ => { };

        public DapperConnection(string connectionString, IDbProvider dbProvider)
        {
            SqlMapper.AddTypeHandler(new DateTimeDapperTypeHandler());
            AutoMapper.Configuration.TypeConverters.Add(new DateTimeAutoMapperTypeConverter());
            _connectionString = connectionString;
            SqlUtils.CompilerProvider = dbProvider.SqlCompilerProvider;
            DbProvider = dbProvider;
        }

        private void Log(string sql, object param)
        {
            Logger(new SqlInfo(sql, param.AsDictionary()));
        }

        public  T ExecuteScalar<T>(string sql, object param = null,
            IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            Log(sql, param);
            return Execute(conn => conn.ExecuteScalar<T>(sql, param, transaction, commandTimeout));
        }

        public  T ExecuteScalar<T>(Query query)
        {
            return query.ExecuteWithSqlKataQuery((sql, parameter) => ExecuteScalar<T>(sql, parameter));
        }

        public  int Execute(string sql, object param = null,
            IDbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            Log(sql, param);
            return Execute(conn => conn.Execute(sql, param, transaction, commandTimeout));
        }

        public  void Execute(Query query)
        {
            query.ExecuteWithSqlKataQuery((sql, parameter) => Execute(sql, parameter));
        }

        public  IEnumerable<T> Query<T>(string sql, object param = null, IDbTransaction transaction = null, bool buffered = true, int? commandTimeout = null, CommandType? commandType = null)
        {
            Log(sql, param);
            return Execute(conn => conn.Query<T>(sql, param, transaction, buffered, commandTimeout, commandType));
        }

        public  IEnumerable<T> Query<T>(Query query)
        {
            return query.ExecuteWithSqlKataQuery((sql, parameter) => Query<T>(sql, parameter));
        }

        public  IEnumerable<TReturn> Query<TFirst, TSecond, TReturn>(string sql, Func<TFirst, TSecond, TReturn> map, object param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
        {
            Log(sql, param);
            return Execute(conn => conn.Query(sql, map, param, transaction, buffered, splitOn,
                    commandTimeout, commandType));
        }

        public  IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TReturn>(string sql, Func<TFirst, TSecond, TThird, TReturn> map, object param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
        {
            Log(sql, param);
            return Execute(conn => conn.Query(sql, map, param, transaction, buffered, splitOn,
                    commandTimeout, commandType));
        }

        public  IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TReturn> map, object param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
        {
            Log(sql, param);
            return Execute(conn => conn.Query(sql, map, param, transaction, buffered, splitOn,
                    commandTimeout, commandType));
        }

        public  T Get<T>(object id, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
        {
            return Execute(conn => conn.Get<T>(id, transaction, commandTimeout));
        }

        public  IEnumerable<T> GetAll<T>(IDbTransaction transaction = null, int? commandTimeout = null)
            where T : class
        {
            return Execute(conn => conn.GetAll<T>(transaction, commandTimeout));
        }

        public  bool Delete<T>(T entityToDelete, IDbTransaction transaction = null,
            int? commandTimeout = null) where T : class
        {
            return Execute(conn => conn.Delete(entityToDelete, transaction, commandTimeout));
        }

        public  T Execute<T>(Func<IDbConnection, T> func)
        {
            using (var conn = DbProvider.CreateConnection(_connectionString))
            {
                return func(conn);
            }
        }
    }
}