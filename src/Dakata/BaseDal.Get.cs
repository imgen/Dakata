using Dapper.Contrib.Extensions;
using Slapper;
using SqlKata;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Dakata;

public partial class BaseDal
{
    /// <summary>
    /// Gets maximum value of certain column
    /// </summary>
    /// <typeparam name="TColumn">The type of the column</typeparam>
    /// <param name="columnName">The name of the column</param>
    /// <param name="commandTimeout">T</param>
    /// <returns>The maximum value of the specified column</returns>
    public virtual TColumn GetMaxValueOfColumn<TColumn>(string columnName,
        int? commandTimeout = null)
    {
        var query = NewQuery().AsMax(columnName);
        return ExecuteScalar<TColumn>(query, commandTimeout);
    }

    /// <summary>
    /// Gets maximum value of certain column
    /// </summary>
    /// <typeparam name="TColumn">The type of the column</typeparam>
    /// <param name="columnName">The name of the column</param>
    /// <param name="commandTimeout">The command timeout</param>
    /// <returns>The maximum value of the specified column</returns>
    public virtual async Task<TColumn> GetMaxValueOfColumnAsync<TColumn>(string columnName,
        int? commandTimeout = null)
    {
        var query = NewQuery().AsMax(columnName);
        return await ExecuteScalarAsync<TColumn>(query, commandTimeout);
    }

    /// <summary>
    /// Gets minimum value of certain column
    /// </summary>
    /// <typeparam name="TColumn">The type of the column</typeparam>
    /// <param name="columnName">The name of the column</param>
    /// <param name="commandTimeout">The command timeout</param>
    /// <returns>The minimum value of the specified column</returns>
    public virtual TColumn GetMinValueOfColumn<TColumn>(string columnName,
        int? commandTimeout = null)
    {
        var query = NewQuery().AsMin(columnName);
        return ExecuteScalar<TColumn>(query, commandTimeout);
    }

    /// <summary>
    /// Gets minimum value of certain column
    /// </summary>
    /// <typeparam name="TColumn">The type of the column</typeparam>
    /// <param name="columnName">The name of the column</param>
    /// <param name="commandTimeout">The command timeout</param>
    /// <returns>The minimum value of the specified column</returns>
    public virtual async Task<TColumn> GetMinValueOfColumnAsync<TColumn>(string columnName,
        int? commandTimeout = null)
    {
        var query = NewQuery().AsMin(columnName);
        return await ExecuteScalarAsync<TColumn>(query, commandTimeout);
    }

    /// <summary>
    /// Gets the count of a table
    /// </summary>
    /// <typeparam name="TCount">The type of the count, usually int or long</typeparam>
    /// <returns>The count</returns>
    public TCount GetCount<TCount>(int? commandTimeout = null) => 
        ExecuteScalar<TCount>(NewQuery().AsCount(), commandTimeout);

    /// <summary>
    /// Gets the count of a specific query, can be as simple as a GetAll query or as complex as multiple joins with sub queries.
    /// </summary>
    /// <typeparam name="TCount">The type of the count, usually int or long</typeparam>
    /// <param name="query">The SqlKata query.</param>
    /// <param name="commandTimeout">The command timeout</param>
    /// <returns>The count of that query</returns>
    public TCount GetCount<TCount>(Query query,
        int? commandTimeout = null) =>
        ExecuteScalar<TCount>(query.AsCount(), commandTimeout);

    /// <summary>
    /// Gets the count of a table
    /// </summary>
    /// <typeparam name="TCount">The type of the count, usually int or long</typeparam>
    /// <returns>The count</returns>
    public async Task<TCount> GetCountAsync<TCount>(int? commandTimeout = null) => 
        await ExecuteScalarAsync<TCount>(NewQuery().AsCount(), commandTimeout);

    /// <summary>
    /// Gets the count of a specific query, can be as simple as a GetAll query or as complex as multiple joins with sub queries.
    /// </summary>
    /// <typeparam name="TCount">The type of the count, usually int or long</typeparam>
    /// <param name="query">The SqlKata query.</param>
    /// <param name="commandTimeout">The command timeout</param>
    /// <returns>The count of that query</returns>
    public async Task<TCount> GetCountAsync<TCount>(Query query,
        int? commandTimeout = null) =>
        await ExecuteScalarAsync<TCount>(query.AsCount(), commandTimeout);

    public Query OrderBy(Query query, bool ascending, params string[] sortColumns) => 
        ascending ? query.OrderBy(sortColumns) : query.OrderByDesc(sortColumns);

    public Query OrderBy(Query query, params (string column, bool ascending)[] orders) =>
        orders.Aggregate(
            query, 
            (q, order) => OrderBy(q, order.ascending, order.column)
        );
}

public partial class BaseDal<TEntity>
{
    private Query BuildGetByIdQuery<TKey>(TKey key)
    {
        var keyColumnName = GetKeyColumnName();
        return NewQuery().Where(keyColumnName, key);
    }

    public virtual TEntity Get<TKey>(TKey key,
        int? commandTimeout = null)
    {
        var query = BuildGetByIdQuery(key);
        return Get(query, commandTimeout);
    }

    public virtual TEntity Get(Query query,
        int? commandTimeout = null) =>
        Query(query.Limit(1), commandTimeout).FirstOrDefault();

    public virtual async Task<TEntity> GetAsync<TKey>(TKey key,
        int? commandTimeout = null)
    {
        var query = BuildGetByIdQuery(key);
        return await GetAsync(query, commandTimeout);
    }

    public virtual async Task<TEntity> GetAsync(Query query,
        int? commandTimeout = null)
    {
        var results = await QueryAsync(query.Limit(1), commandTimeout);
        return results.FirstOrDefault();
    }

    public virtual IEnumerable<TEntity> QueryByParameters(object parameters,
        int? commandTimeout = null)
    {
        var query = NewQuery().Where(parameters.AsDictionary());
        return Query(query, commandTimeout);
    }

    public virtual async Task<IEnumerable<TEntity>> QueryByParametersAsync(object parameters,
        int? commandTimeout = null)
    {
        var query = NewQuery().Where(parameters.AsDictionary());
        return await QueryAsync(query, commandTimeout);
    }

    public virtual TEntity GetWithParameters(object parameters,
        int? commandTimeout = null)
    {
        var query = NewQuery().Where(parameters.AsDictionary());
        return Get(query, commandTimeout);
    }

    public virtual async Task<TEntity> GetWithParametersAsync(object parameters,
        int? commandTimeout = null)
    {
        var query = NewQuery().Where(parameters.AsDictionary());
        return await GetAsync(query, commandTimeout);
    }

    public virtual IEnumerable<TEntity> GetAll(int limit = 0,
        int? commandTimeout = null)
    {
        return Query(NewQuery().Limit(limit), commandTimeout);
    }

    public virtual async Task<IEnumerable<TEntity>> GetAllAsync(int limit = 0,
        int? commandTimeout = null)
    {
        return await QueryAsync(NewQuery().Limit(limit), commandTimeout);
    }

    public virtual TEntity GetFirst(int? commandTimeout = null)
    {
        return Get(NewQuery(), commandTimeout);
    }

    public virtual async Task<TEntity> GetFirstAsync(int? commandTimeout = null)
    {
        return await GetAsync(NewQuery(), commandTimeout);
    }

    private Query BuildEntityKeysQuery(TEntity keyEntity)
    {
        var keyProperties = EntityType.GetPropertiesWithAttribute<ExplicitKeyAttribute>();
        var query = NewQuery();
        keyProperties.Select(x => new KeyValuePair<string, object>(GetColumnName(x), x.GetValue(keyEntity)))
            .Where(x => x.Value != null)
            .ForEach(x => query.Where(x.Key, x.Value));
        return query;
    }

    public virtual IEnumerable<TEntity> QueryByEntityKeys(TEntity keyEntity,
        int? commandTimeout = null)
    {
        var query = BuildEntityKeysQuery(keyEntity);
        return Query(query, commandTimeout);
    }

    public virtual async Task<IEnumerable<TEntity>> QueryByEntityKeysAsync(TEntity keyEntity,
        int? commandTimeout = null)
    {
        var query = BuildEntityKeysQuery(keyEntity);
        return await QueryAsync(query, commandTimeout);
    }

    public virtual IEnumerable<TEntity> QueryByColumn<TColumn>(string columnName,
        TColumn value,
        int? commandTimeout = null)
    {
        var query = NewQuery().Where(columnName, value);
        return Query(query, commandTimeout);
    }

    public virtual IEnumerable<TEntity> QueryByColumn<TColumn>(Expression<Func<TEntity, TColumn>> columnExpression,
        TColumn value,
        int? commandTimeout = null) =>
        QueryByColumn(GetColumnName(columnExpression), value, commandTimeout);

    public virtual async Task<IEnumerable<TEntity>> QueryByColumnAsync<TColumn>(string columnName,
        TColumn value,
        int? commandTimeout = null)
    {
        var query = NewQuery().Where(columnName, value);
        return await QueryAsync(query, commandTimeout);
    }

    public virtual async Task<IEnumerable<TEntity>> QueryByColumnAsync<TColumn>(Expression<Func<TEntity, TColumn>> columnExpression,
        TColumn value,
        int? commandTimeout = null)
    {
        return await QueryByColumnAsync(GetColumnName(columnExpression), value, commandTimeout);
    }

    public virtual TEntity GetByEntityKeys(TEntity keyEntity,
        int? commandTimeout = null)
    {
        var query = BuildEntityKeysQuery(keyEntity);
        return Get(query, commandTimeout);
    }

    public virtual async Task<TEntity> GetByEntityKeysAsync(TEntity keyEntity,
        int? commandTimeout = null)
    {
        var query = BuildEntityKeysQuery(keyEntity);
        return await GetAsync(query, commandTimeout);
    }

    public virtual TMaxColumn GetMaxValueOfColumn<TMaxColumn>(Expression<Func<TEntity, TMaxColumn>> columnExpression,
        int? commandTimeout = null)
    {
        return GetMaxValueOfColumn<TMaxColumn>(GetColumnName(columnExpression), commandTimeout);
    }

    public virtual async Task<TMaxColumn> GetMaxValueOfColumnAsync<TMaxColumn>(Expression<Func<TEntity, TMaxColumn>> columnExpression,
        int? commandTimeout = null)
    {
        return await GetMaxValueOfColumnAsync<TMaxColumn>(GetColumnName(columnExpression), commandTimeout);
    }

    private Query BuildRecordsWithMaxValueOfColumnQuery(string column)
    {
        var maxQuerySql = NewQuery().AsMax(column).CompileResult().Sql;
        return NewQuery().WhereRaw($"{column} = ({maxQuerySql})");
    }

    public IEnumerable<TEntity> GetRecordsWithMaxValueOfColumn(string column,
        int? commandTimeout = null)
    {
        var query = BuildRecordsWithMaxValueOfColumnQuery(column);
        return Query(query, commandTimeout);
    }

    public IEnumerable<TEntity> GetRecordsWithMaxValueOfColumn<TMember>(
        Expression<Func<TEntity, TMember>> memberExpression,
        int? commandTimeout = null) =>
        GetRecordsWithMaxValueOfColumn(GetColumnName(memberExpression), commandTimeout);

    public async Task<IEnumerable<TEntity>> GetRecordsWithMaxValueOfColumnAsync(string column,
        int? commandTimeout = null)
    {
        var query = BuildRecordsWithMaxValueOfColumnQuery(column);
        return await QueryAsync(query, commandTimeout);
    }

    public async Task<IEnumerable<TEntity>> GetRecordsWithMaxValueOfColumnAsync<TMember>(
        Expression<Func<TEntity, TMember>> memberExpression,
        int? commandTimeout = null) =>
        await GetRecordsWithMaxValueOfColumnAsync(GetColumnName(memberExpression), commandTimeout);

    public TEntity GetRecordWithMaxValueOfColumn(string column,
        int? commandTimeout = null)
    {
        var query = BuildRecordsWithMaxValueOfColumnQuery(column);
        return Get(query, commandTimeout);
    }

    public TEntity GetRecordWithMaxValueOfColumn<TMember>(
        Expression<Func<TEntity, TMember>> memberExpression,
        int? commandTimeout = null) =>
        GetRecordWithMaxValueOfColumn(GetColumnName(memberExpression), commandTimeout);

    public async Task<TEntity> GetRecordWithMaxValueOfColumnAsync(string column,
        int? commandTimeout = null)
    {
        var query = BuildRecordsWithMaxValueOfColumnQuery(column);
        return await GetAsync(query, commandTimeout);
    }

    public async Task<TEntity> GetRecordWithMaxValueOfColumnAsync<TMember>(
        Expression<Func<TEntity, TMember>> memberExpression,
        int? commandTimeout = null)
    {
        return await GetRecordWithMaxValueOfColumnAsync(GetColumnName(memberExpression), commandTimeout);
    }

    public virtual IEnumerable<TEntity> QueryByInClause(string column, IEnumerable<object> values,
        int? commandTimeout = null)
    {
        var query = NewQuery().WhereIn(column, values);
        return Query(query, commandTimeout);
    }

    public virtual IEnumerable<TEntity> QueryByInClause<TMember>(
        Expression<Func<TEntity, TMember>> memberExpression,
        IEnumerable<object> values,
        int? commandTimeout = null) =>
        QueryByInClause(GetColumnName(memberExpression), values, commandTimeout);

    public virtual async Task<IEnumerable<TEntity>> QueryByInClauseAsync(string column, 
        IEnumerable<object> values,
        int? commandTimeout = null)
    {
        var query = NewQuery().WhereIn(column, values);
        return await QueryAsync(query, commandTimeout);
    }

    public virtual async Task<IEnumerable<TEntity>> QueryByInClauseAsync<TMember>(
        Expression<Func<TEntity, TMember>> memberExpression, 
        IEnumerable<object> values,
        int? commandTimeout = null) =>
        await QueryByInClauseAsync(GetColumnName(memberExpression), values, commandTimeout);

    public virtual IEnumerable<TEntity> QueryAndMapDynamic(Query query,
        int? commandTimeout = null)
    {
        var dynamicResults = QueryDynamic(query, commandTimeout);
        return AutoMapper.MapDynamic<TEntity>(dynamicResults, keepCache: false).ToList();
    }

    public virtual async Task<IEnumerable<TEntity>> QueryAndMapDynamicAsync(Query query,
        int? commandTimeout = null)
    {
        var dynamicResults = await QueryDynamicAsync(query, commandTimeout);
        return AutoMapper.MapDynamic<TEntity>(dynamicResults, keepCache: false).ToList();
    }
}