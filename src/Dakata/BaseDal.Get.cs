using Dapper.Contrib.Extensions;
using Slapper;
using SqlKata;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Dakata
{
    public partial class BaseDal
    {
        /// <summary>
        /// Gets maximum value of certain column
        /// </summary>
        /// <typeparam name="TColumn">The type of the column</typeparam>
        /// <param name="columnName">The name of the column</param>
        /// <returns>The maximum value of the specified column</returns>
        public virtual TColumn GetMaxValueOfColumn<TColumn>(string columnName)
        {
            var query = NewQuery().AsMax(columnName);
            return ExecuteScalar<TColumn>(query);
        }

        /// <summary>
        /// Gets maximum value of certain column
        /// </summary>
        /// <typeparam name="TColumn">The type of the column</typeparam>
        /// <param name="columnName">The name of the column</param>
        /// <returns>The maximum value of the specified column</returns>
        public virtual async Task<TColumn> GetMaxValueOfColumnAsync<TColumn>(string columnName)
        {
            var query = NewQuery().AsMax(columnName);
            return await ExecuteScalarAsync<TColumn>(query);
        }

        /// <summary>
        /// Gets minimum value of certain column
        /// </summary>
        /// <typeparam name="TColumn">The type of the column</typeparam>
        /// <param name="columnName">The name of the column</param>
        /// <returns>The minimum value of the specified column</returns>
        public virtual TColumn GetMinValueOfColumn<TColumn>(string columnName)
        {
            var query = NewQuery().AsMin(columnName);
            return ExecuteScalar<TColumn>(query);
        }

        /// <summary>
        /// Gets minimum value of certain column
        /// </summary>
        /// <typeparam name="TColumn">The type of the column</typeparam>
        /// <param name="columnName">The name of the column</param>
        /// <returns>The minimum value of the specified column</returns>
        public virtual async Task<TColumn> GetMinValueOfColumnAsync<TColumn>(string columnName)
        {
            var query = NewQuery().AsMin(columnName);
            return await ExecuteScalarAsync<TColumn>(query);
        }

        /// <summary>
        /// Gets the count of a table
        /// </summary>
        /// <typeparam name="TCount">The type of the count, usually int or long</typeparam>
        /// <returns>The count</returns>
        public TCount GetCount<TCount>()
        {
            return ExecuteScalar<TCount>(NewQuery().AsCount());
        }

        /// <summary>
        /// Gets the count of a table
        /// </summary>
        /// <typeparam name="TCount">The type of the count, usually int or long</typeparam>
        /// <returns>The count</returns>
        public async Task<TCount> GetCountAsync<TCount>()
        {
            return await ExecuteScalarAsync<TCount>(NewQuery().AsCount());
        }

        /// <summary>
        /// Gets the count of a specific query, can be as simple as a GetAll query or as complex as multiple joins with sub queries.
        /// </summary>
        /// <typeparam name="TCount">The type of the count, usually int or long</typeparam>
        /// <param name="query">The SqlKata query.</param>
        /// <returns>The count of that query</returns>
        public TCount GetCount<TCount>(Query query)
        {
            return ExecuteScalar<TCount>(query.AsCount());
        }

        /// <summary>
        /// Gets the count of a specific query, can be as simple as a GetAll query or as complex as multiple joins with sub queries.
        /// </summary>
        /// <typeparam name="TCount">The type of the count, usually int or long</typeparam>
        /// <param name="query">The SqlKata query.</param>
        /// <returns>The count of that query</returns>
        public async Task<TCount> GetCountAsync<TCount>(Query query)
        {
            return await ExecuteScalarAsync<TCount>(query.AsCount());
        }

        public Query OrderBy(Query query, bool ascending, params string[] sortColumns)
        {
            return ascending ? query.OrderBy(sortColumns) : query.OrderByDesc(sortColumns);
        }

        public Query OrderBy(Query query, params (string column, bool ascending)[] orders)
        {
            return orders.Aggregate(
                query, 
                (q, order) => OrderBy(q, order.ascending, order.column)
            );
        }
    }

    public partial class BaseDal<TEntity>
    {
        private Query BuildGetByIdQuery<TKey>(TKey key)
        {
            var keyColumnName = GetKeyColumnName();
            return NewQuery().Where(keyColumnName, key);
        }

        public virtual TEntity Get<TKey>(TKey key)
        {
            var query = BuildGetByIdQuery(key);
            return Get(query);
        }

        public virtual async Task<TEntity> GetAsync<TKey>(TKey key)
        {
            var query = BuildGetByIdQuery(key);
            return await GetAsync(query);
        }

        public virtual TEntity Get(Query query)
        {
            return Query(query.Limit(1)).FirstOrDefault();
        }

        public virtual async Task<TEntity> GetAsync(Query query)
        {
            var results = await QueryAsync(query.Limit(1));
            return results.FirstOrDefault();
        }

        public virtual IEnumerable<TEntity> QueryByParameters(object parameters)
        {
            var query = NewQuery().Where(parameters.AsDictionary());
            return Query(query);
        }

        public virtual async Task<IEnumerable<TEntity>> QueryByParametersAsync(object parameters)
        {
            var query = NewQuery().Where(parameters.AsDictionary());
            return await QueryAsync(query);
        }

        public virtual TEntity GetWithParameters(object parameters)
        {
            var query = NewQuery().Where(parameters.AsDictionary());
            return Get(query);
        }

        public virtual async Task<TEntity> GetWithParametersAsync(object parameters)
        {
            var query = NewQuery().Where(parameters.AsDictionary());
            return await GetAsync(query);
        }

        public virtual IEnumerable<TEntity> GetAll(int limit = 0)
        {
            return Query(NewQuery().Limit(limit));
        }

        public virtual async Task<IEnumerable<TEntity>> GetAllAsync(int limit = 0)
        {
            return await QueryAsync(NewQuery().Limit(limit));
        }

        public virtual TEntity GetFirst()
        {
            return Get(NewQuery());
        }

        public virtual async Task<TEntity> GetFirstAsync()
        {
            return await GetAsync(NewQuery());
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

        public virtual IEnumerable<TEntity> QueryByEntityKeys(TEntity keyEntity)
        {
            var query = BuildEntityKeysQuery(keyEntity);
            return Query(query);
        }

        public virtual async Task<IEnumerable<TEntity>> QueryByEntityKeysAsync(TEntity keyEntity)
        {
            var query = BuildEntityKeysQuery(keyEntity);
            return await QueryAsync(query);
        }

        public virtual IEnumerable<TEntity> QueryByColumn<TColumn>(string columnName,
            TColumn value)
        {
            var query = NewQuery().Where(columnName, value);
            return Query(query);
        }

        public virtual async Task<IEnumerable<TEntity>> QueryByColumnAsync<TColumn>(string columnName,
            TColumn value)
        {
            var query = NewQuery().Where(columnName, value);
            return await QueryAsync(query);
        }

        public virtual IEnumerable<TEntity> QueryByColumn<TColumn>(Expression<Func<TEntity, TColumn>> columnExpression,
            TColumn value)
        {
            return QueryByColumn(GetColumnName(columnExpression), value);
        }

        public virtual async Task<IEnumerable<TEntity>> QueryByColumnAsync<TColumn>(Expression<Func<TEntity, TColumn>> columnExpression,
            TColumn value)
        {
            return await QueryByColumnAsync(GetColumnName(columnExpression), value);
        }

        public virtual TEntity GetByEntityKeys(TEntity keyEntity)
        {
            var query = BuildEntityKeysQuery(keyEntity);
            return Get(query);
        }

        public virtual async Task<TEntity> GetByEntityKeysAsync(TEntity keyEntity)
        {
            var query = BuildEntityKeysQuery(keyEntity);
            return await GetAsync(query);
        }

        public virtual TMaxColumn GetMaxValueOfColumn<TMaxColumn>(Expression<Func<TEntity, TMaxColumn>> columnExpression)
        {
            return GetMaxValueOfColumn<TMaxColumn>(GetColumnName(columnExpression));
        }

        public virtual async Task<TMaxColumn> GetMaxValueOfColumnAsync<TMaxColumn>(Expression<Func<TEntity, TMaxColumn>> columnExpression)
        {
            return await GetMaxValueOfColumnAsync<TMaxColumn>(GetColumnName(columnExpression));
        }

        private Query BuildRecordsWithMaxValueOfColumnQuery(string column)
        {
            var maxQuerySql = NewQuery().AsMax(column).CompileResult().Sql;
            return NewQuery().WhereRaw($"{column} = ({maxQuerySql})");
        }

        public IEnumerable<TEntity> GetRecordsWithMaxValueOfColumn(string column)
        {
            var query = BuildRecordsWithMaxValueOfColumnQuery(column);
            return Query(query);
        }

        public async Task<IEnumerable<TEntity>> GetRecordsWithMaxValueOfColumnAsync(string column)
        {
            var query = BuildRecordsWithMaxValueOfColumnQuery(column);
            return await QueryAsync(query);
        }

        public IEnumerable<TEntity> GetRecordsWithMaxValueOfColumn<TMember>(
            Expression<Func<TEntity, TMember>> memberExpression)
        {
            return GetRecordsWithMaxValueOfColumn(GetColumnName(memberExpression));
        }

        public async Task<IEnumerable<TEntity>> GetRecordsWithMaxValueOfColumnAsync<TMember>(
            Expression<Func<TEntity, TMember>> memberExpression)
        {
            return await GetRecordsWithMaxValueOfColumnAsync(GetColumnName(memberExpression));
        }

        public TEntity GetRecordWithMaxValueOfColumn(string column)
        {
            var query = BuildRecordsWithMaxValueOfColumnQuery(column);
            return Get(query);
        }

        public async Task<TEntity> GetRecordWithMaxValueOfColumnAsync(string column)
        {
            var query = BuildRecordsWithMaxValueOfColumnQuery(column);
            return await GetAsync(query);
        }

        public TEntity GetRecordWithMaxValueOfColumn<TMember>(
            Expression<Func<TEntity, TMember>> memberExpression)
        {
            return GetRecordWithMaxValueOfColumn(GetColumnName(memberExpression));
        }

        public async Task<TEntity> GetRecordWithMaxValueOfColumnAsync<TMember>(
            Expression<Func<TEntity, TMember>> memberExpression)
        {
            return await GetRecordWithMaxValueOfColumnAsync(GetColumnName(memberExpression));
        }

        public virtual IEnumerable<TEntity> QueryByInClause(string column, IEnumerable<object> values)
        {
            var query = NewQuery().WhereIn(column, values);
            return Query(query);
        }

        public virtual async Task<IEnumerable<TEntity>> QueryByInClauseAsync(string column, IEnumerable<object> values)
        {
            var query = NewQuery().WhereIn(column, values);
            return await QueryAsync(query);
        }

        public virtual IEnumerable<TEntity> QueryByInClause<TMember>(
            Expression<Func<TEntity, TMember>> memberExpression, IEnumerable<object> values)
        {
            return QueryByInClause(GetColumnName(memberExpression), values);
        }

        public virtual async Task<IEnumerable<TEntity>> QueryByInClauseAsync<TMember>(
            Expression<Func<TEntity, TMember>> memberExpression, IEnumerable<object> values)
        {
            return await QueryByInClauseAsync(GetColumnName(memberExpression), values);
        }

        public virtual IEnumerable<TEntity> QueryAndMapDynamic(Query query)
        {
            var dynamicResults = QueryDynamic(query);
            return AutoMapper.MapDynamic<TEntity>(dynamicResults).ToList();
        }

        public virtual async Task<IEnumerable<TEntity>> QueryAndMapDynamicAsync(Query query)
        {
            var dynamicResults = await QueryDynamicAsync(query);
            return AutoMapper.MapDynamic<TEntity>(dynamicResults, keepCache: false).ToList();
        }
    }
}
