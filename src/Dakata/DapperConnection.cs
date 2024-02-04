using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Dapper;
using Dapper.Contrib.Extensions;
using Slapper;
using SqlKata;

// ReSharper disable MemberCanBePrivate.Global

namespace Dakata;

public class DapperConnection
{
    private readonly string _connectionString;
    public IDbProvider DbProvider { get; }

    internal Action<SqlInfo> Logger { get; set; } = _ => { };

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

    public T ExecuteScalar<T>(string sql,
        object param = null,
        IDbTransaction transaction = null,
        int? commandTimeout = null,
        CommandType? commandType = null)
    {
        Log(sql, param);
        return Execute(conn => conn.ExecuteScalar<T>(sql, param, transaction, commandTimeout, commandType));
    }

    public T ExecuteScalar<T>(Query query, int? commandTimeout = null)
    {
        return query.ExecuteWithSqlKataQuery((sql, parameter) => ExecuteScalar<T>(sql, parameter, commandTimeout: commandTimeout));
    }

    public async Task<T> ExecuteScalarAsync<T>(string sql,
        object param = null,
        IDbTransaction transaction = null,
        int? commandTimeout = null,
        CommandType? commandType = null)
    {
        Log(sql, param);
        return await ExecuteAsync(async conn => await conn.ExecuteScalarAsync<T>(sql, param, transaction, commandTimeout, commandType));
    }


    public async Task<T> ExecuteScalarAsync<T>(Query query, int? commandTimeout = null)
    {
        return await query.ExecuteWithSqlKataQuery(async (sql, parameter) => await ExecuteScalarAsync<T>(sql, parameter, commandTimeout: commandTimeout));
    }

    public int Execute(string sql,
        object param = null,
        IDbTransaction transaction = null,
        int? commandTimeout = null,
        CommandType? commandType = null)
    {
        Log(sql, param);
        return Execute(conn => conn.Execute(sql, param, transaction, commandTimeout, commandType));
    }

    public void Execute(Query query, int? commandTimeout = null)
    {
        query.ExecuteWithSqlKataQuery((sql, parameter) => Execute(sql, parameter, commandTimeout: commandTimeout));
    }

    public T Execute<T>(Func<IDbConnection, T> func)
    {
        using var conn = DbProvider.CreateConnection(_connectionString);
        conn.Open();
        return func(conn);
    }

    public T Execute<T>(Func<IDbConnection, int?, T> func, int? commandTimeout)
    {
        using var conn = DbProvider.CreateConnection(_connectionString);
        conn.Open();
        return func(conn, commandTimeout);
    }

    public async Task<int> ExecuteAsync(string sql,
        object param = null,
        IDbTransaction transaction = null,
        int? commandTimeout = null,
        CommandType? commandType = null)
    {
        Log(sql, param);
        return await ExecuteAsync(async conn => await conn.ExecuteAsync(sql, param, transaction, commandTimeout, commandType));
    }

    public async Task<T> ExecuteAsync<T>(Func<IDbConnection, Task<T>> func)
    {
        await using var conn = DbProvider.CreateConnection(_connectionString);
        await conn.OpenAsync();
        return await func(conn);
    }

    public async Task<T> ExecuteAsync<T>(Func<IDbConnection, int?, Task<T>> func, int? commandTimeout)
    {
        await using var conn = DbProvider.CreateConnection(_connectionString);
        await conn.OpenAsync();
        return await func(conn, commandTimeout);
    }

    public async Task ExecuteAsync(Query query, int? commandTimeout = null) => 
        await query.ExecuteWithSqlKataQueryAsync(async (sql, parameter) => await ExecuteAsync(sql, parameter, commandTimeout: commandTimeout));

    public IEnumerable<T> Query<T>(string sql,
        object param = null,
        IDbTransaction transaction = null,
        bool buffered = true,
        int? commandTimeout = null,
        CommandType? commandType = null)
    {
        Log(sql, param);
        return Execute(conn => conn.Query<T>(sql, param, transaction, buffered, commandTimeout, commandType));
    }

    public IEnumerable<T> Query<T>(Query query, int? commandTimeout = null) =>
        query.ExecuteWithSqlKataQuery(
            (sql, parameter) => Query<T>(sql, parameter, commandTimeout: commandTimeout));

    public IEnumerable<TReturn> Query<TFirst, TSecond, TReturn>(string sql,
        Func<TFirst, TSecond, TReturn> map,
        object param = null,
        IDbTransaction transaction = null,
        bool buffered = true,
        string splitOn = "Id",
        int? commandTimeout = null,
        CommandType? commandType = null)
    {
        Log(sql, param);
        return Execute(conn => conn.Query(sql, map, param, transaction, buffered, splitOn,
            commandTimeout, commandType));
    }

    public IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TFourth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TReturn> map, object param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
    {
        Log(sql, param);
        return Execute(conn => conn.Query(sql, map, param, transaction, buffered, splitOn,
            commandTimeout, commandType));
    }

    public IEnumerable<TReturn> Query<TFirst, TSecond, TThird, TReturn>(string sql, Func<TFirst, TSecond, TThird, TReturn> map, object param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
    {
        Log(sql, param);
        return Execute(conn => conn.Query(sql, map, param, transaction, buffered, splitOn,
            commandTimeout, commandType));
    }

    public async Task<IEnumerable<T>> QueryAsync<T>(string sql,
        object param = null,
        IDbTransaction transaction = null,
        int? commandTimeout = null,
        CommandType? commandType = null)
    {
        Log(sql, param);
        return await ExecuteAsync(
            async conn => await conn.QueryAsync<T>(
                sql, param, transaction, commandTimeout, commandType));
    }

    public async Task<IEnumerable<T>> QueryAsync<T>(Query query, int? commandTimeout = null) =>
        await query.ExecuteWithSqlKataQueryAsync(
            async (sql, parameter) => await QueryAsync<T>(sql, parameter, commandTimeout: commandTimeout));

    public async Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TReturn>(
        string sql,
        Func<TFirst, TSecond, TReturn> map,
        object param = null,
        IDbTransaction transaction = null,
        bool buffered = true,
        string splitOn = "Id",
        int? commandTimeout = null,
        CommandType? commandType = null)
    {
        Log(sql, param);
        return await ExecuteAsync(async conn => await conn.QueryAsync(sql, map, param, transaction, buffered, splitOn,
            commandTimeout, commandType));
    }

    public async Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TReturn>(
        string sql,
        Func<TFirst, TSecond, TThird, TReturn> map,
        object param = null,
        IDbTransaction transaction = null,
        bool buffered = true,
        string splitOn = "Id",
        int? commandTimeout = null,
        CommandType? commandType = null)
    {
        Log(sql, param);
        return await ExecuteAsync(async conn => await conn.QueryAsync(sql, map, param, transaction, buffered, splitOn,
            commandTimeout, commandType));
    }

    public async Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TReturn>(string sql, Func<TFirst, TSecond, TThird, TFourth, TReturn> map, object param = null, IDbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
    {
        Log(sql, param);
        return await ExecuteAsync(async conn => await conn.QueryAsync(sql, map, param, transaction, buffered, splitOn,
            commandTimeout, commandType));
    }

    public T Get<T>(object id, IDbTransaction transaction = null, int? commandTimeout = null) where T : class => 
        Execute(conn => conn.Get<T>(id, transaction, commandTimeout));

    public async Task<T> GetAsync<T>(object id, IDbTransaction transaction = null, int? commandTimeout = null) where T : class => 
        await ExecuteAsync(async conn => await conn.GetAsync<T>(id, transaction, commandTimeout));

    public IEnumerable<T> GetAll<T>(IDbTransaction transaction = null, int? commandTimeout = null)
        where T : class =>
        Execute(conn => conn.GetAll<T>(transaction, commandTimeout));

    public async Task<IEnumerable<T>> GetAllAsync<T>(
        IDbTransaction transaction = null,
        int? commandTimeout = null)
        where T : class =>
        await ExecuteAsync(async conn => await conn.GetAllAsync<T>(transaction, commandTimeout));

    public bool Delete<T>(T entityToDelete, IDbTransaction transaction = null,
        int? commandTimeout = null) where T : class =>
        Execute(conn => conn.Delete(entityToDelete, transaction, commandTimeout));

    public async Task<bool> DeleteAsync<T>(T entityToDelete, IDbTransaction transaction = null,
        int? commandTimeout = null) where T : class =>
        await ExecuteAsync(async conn => await conn.DeleteAsync(entityToDelete, transaction, commandTimeout));
}