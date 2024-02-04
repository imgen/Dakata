using SqlKata;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dakata;

public partial class BaseDal
{
    public virtual void Execute(Query query, int? commandTimeout = null) => 
        DapperConnection.Execute(query, commandTimeout);

    public virtual T Execute<T>(Func<DbConnection, int?, T> func, int? commandTimeout = null) => 
        DapperConnection.Execute(func, commandTimeout);

    public virtual void Execute(string sql, object parameters = null, int? commandTimeout = null) =>
        DapperConnection.Execute(sql, parameters, commandTimeout: commandTimeout);

    public virtual async Task ExecuteAsync(Query query, int? commandTimeout = null) =>
        await DapperConnection.ExecuteAsync(query, commandTimeout);

    public virtual async Task<T> ExecuteAsync<T>(Func<DbConnection, int?, Task<T>> func, int? commandTimeout = null) => 
        await DapperConnection.ExecuteAsync(func, commandTimeout);

        
    public virtual async Task ExecuteAsync(string sql, object parameters = null, int? commandTimeout = null) => 
        await DapperConnection.ExecuteAsync(sql, parameters, commandTimeout: commandTimeout);

    public virtual T ExecuteScalar<T>(Query query, int? commandTimeout = null) => 
        DapperConnection.ExecuteScalar<T>(query, commandTimeout: commandTimeout);
        
    public virtual T ExecuteScalar<T>(string sql, object parameters, int? commandTimeout = null) => 
        DapperConnection.ExecuteScalar<T>(sql, parameters, commandTimeout: commandTimeout);

    public virtual async Task<T> ExecuteScalarAsync<T>(Query query, int? commandTimeout = null) =>
        await DapperConnection.ExecuteScalarAsync<T>(query, commandTimeout: commandTimeout);

    public virtual async Task<T> ExecuteScalarAsync<T>(string sql, object parameters, int? commandTimeout = null) => 
        await DapperConnection.ExecuteScalarAsync<T>(sql, parameters, commandTimeout: commandTimeout);

    public virtual IEnumerable<dynamic> QueryDynamic(Query query, int? commandTimeout = null) => 
        DapperConnection.Query<dynamic>(query, commandTimeout: commandTimeout);
        
        
    public virtual IEnumerable<dynamic> QueryDynamic(string sql, object parameters, int? commandTimeout = null) => 
        DapperConnection.Query<dynamic>(sql, parameters, commandTimeout: commandTimeout);

    public virtual async Task<IEnumerable<dynamic>> QueryDynamicAsync(Query query,
        int? commandTimeout = null) => await DapperConnection.QueryAsync<dynamic>(query, commandTimeout: commandTimeout);

    public virtual async Task<IEnumerable<dynamic>> QueryDynamicAsync(string sql, object parameters, int? commandTimeout = null) => 
        await DapperConnection.QueryAsync<dynamic>(sql, parameters, commandTimeout: commandTimeout);
}

public partial class BaseDal<TEntity>
{
    public virtual IEnumerable<TEntity> Query(string sql, object parameter, int? commandTimeout = null) => 
        DapperConnection.Query<TEntity>(sql, parameter, commandTimeout: commandTimeout);

    public virtual IEnumerable<TEntity> Query(Query query, int? commandTimeout = null) =>
        DapperConnection.Query<TEntity>(query, commandTimeout: commandTimeout);

    public virtual async Task<IEnumerable<TEntity>> QueryAsync(string sql, object parameter, int? commandTimeout = null) => 
        await DapperConnection.QueryAsync<TEntity>(sql, parameter, commandTimeout: commandTimeout);

    public virtual async Task<IEnumerable<TEntity>> QueryAsync(Query query, int? commandTimeout = null) => 
        await DapperConnection.QueryAsync<TEntity>(query, commandTimeout: commandTimeout);
}