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
        public int GetMaxBatchSize(int parameterCountOfOneRecord) =>
            MaxParameterCount / parameterCountOfOneRecord;

        protected virtual IEnumerable<dynamic> QueryDynamic(Query query) => DapperConnection.Query<dynamic>(query);

        /// <summary>
        /// Gets maximum value of certain column
        /// </summary>
        /// <typeparam name="TColumn">The type of the column</typeparam>
        /// <param name="columnName">The name of the column</param>
        /// <returns>The maximum value of the specified column</returns>
        protected virtual TColumn GetMaxValueOfColumn<TColumn>(string columnName)
        {
            var query = NewQuery().AsMax(columnName);
            return ExecuteScalar<TColumn>(query);
        }
        
        /// <summary>
        /// Gets minimum value of certain column
        /// </summary>
        /// <typeparam name="TColumn">The type of the column</typeparam>
        /// <param name="columnName">The name of the column</param>
        /// <returns>The minimum value of the specified column</returns>
        protected virtual TColumn GetMinValueOfColumn<TColumn>(string columnName)
        {
            var query = NewQuery().AsMin(columnName);
            return ExecuteScalar<TColumn>(query);
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
        /// Gets the count of a specific query, can be as simple as a GetAll query or as complex as multiple joins with sub queries.
        /// </summary>
        /// <typeparam name="TCount">The type of the count, usually int or long</typeparam>
        /// <param name="query">The SqlKata query.</param>
        /// <returns>The count of that query</returns>
        public TCount GetCount<TCount>(Query query)
        {
            return ExecuteScalar<TCount>(query.AsCount());
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
            return Query(query).FirstOrDefault();
        }

        public virtual async Task<TEntity> GetAsync<TKey>(TKey key)
        {
            var query = BuildGetByIdQuery(key);
            var results = await QueryAsync(query);
            return results.FirstOrDefault();
        }

        protected virtual TEntity Get(Query query)
        {
            return Query(query).FirstOrDefault();
        }

        protected virtual IEnumerable<TEntity> QueryByParameters(object parameters)
        {
            var query = NewQuery().Where(parameters.AsDictionary());
            return Query(query);
        }

        protected virtual TEntity GetWithParameters(object parameters)
        {
            return QueryByParameters(parameters).FirstOrDefault();
        }

        public virtual IEnumerable<TEntity> GetAll(int limit = 0)
        {
            return Query(NewQuery().Limit(limit));
        }

        public virtual TEntity GetFirst()
        {
            return GetAll().First();
        }

        public virtual IEnumerable<TEntity> Query(string sql, object parameter)
        {
            return DapperConnection.Query<TEntity>(sql, parameter);
        }

        public virtual IEnumerable<TEntity> Query(Query query)
        {
            return DapperConnection.Query<TEntity>(query);
        }

        public virtual async Task<IEnumerable<TEntity>> QueryAsync(Query query)
        {
            return await DapperConnection.QueryAsync<TEntity>(query);
        }

        protected virtual IEnumerable<TEntity> QueryByEntityKeys(TEntity keyEntity)
        {
            var keyProperties = EntityType.GetPropertiesWithAttribute<ExplicitKeyAttribute>();
            var query = NewQuery();
            keyProperties.Select(x => new KeyValuePair<string, object>(GetColumnName(x), x.GetValue(keyEntity)))
                .Where(x => x.Value != null)
                .ForEach(x => query.Where(x.Key, x.Value));
            return Query(query);
        }

        protected virtual IEnumerable<TEntity> QueryByColumn<TColumn>(Expression<Func<TEntity, TColumn>> columnExpression,
            TColumn value)
        {
            return QueryByColumn(GetColumnName(columnExpression), value);
        }

        protected virtual IEnumerable<TEntity> QueryByColumn<TColumn>(string columnName,
            TColumn value)
        {
            var query = NewQuery().Where(columnName, value);
            return Query(query);
        }

        public string GetColumnName<TProperty>(Expression<Func<TEntity, TProperty>> propExpr)
        {
            return GetColumnName(propExpr.GetFullPropertyName());
        }

        public virtual TEntity GetByEntityKeys(TEntity keyEntity)
        {
            return QueryByEntityKeys(keyEntity).FirstOrDefault();
        }

        protected virtual TMaxColumn GetMaxValueOfColumn<TMaxColumn>(Expression<Func<TEntity, TMaxColumn>> columnExpression)
        {
            return GetMaxValueOfColumn<TMaxColumn>(GetColumnName(columnExpression));
        }

        protected IEnumerable<TEntity> GetRecordsWithMaxValueOfColumn(string column)
        {
            var maxQuerySql = NewQuery().AsMax(column).CompileResult().Sql;
            var query = NewQuery().WhereRaw($"{column} = ({maxQuerySql})");
            return Query(query);
        }

        protected IEnumerable<TEntity> GetRecordsWithMaxValueOfColumn<TMember>(
            Expression<Func<TEntity, TMember>> memberExpression)
        {
            return GetRecordsWithMaxValueOfColumn(GetColumnName(memberExpression));
        }

        protected TEntity GetRecordWithMaxValueOfColumn(string column)
        {
            return GetRecordsWithMaxValueOfColumn(column).FirstOrDefault();
        }

        protected TEntity GetRecordWithMaxValueOfColumn<TMember>(
            Expression<Func<TEntity, TMember>> memberExpression)
        {
            return GetRecordsWithMaxValueOfColumn(GetColumnName(memberExpression)).FirstOrDefault();
        }

        protected virtual IEnumerable<TEntity> QueryByInClause<TMember>(
            Expression<Func<TEntity, TMember>> memberExpression, IEnumerable<object> values)
        {
            return QueryByInClause(GetColumnName(memberExpression), values);
        }

        protected virtual IEnumerable<TEntity> QueryByInClause(string column, IEnumerable<object> values)
        {
            var query = NewQuery().WhereIn(column, values);
            return Query(query);
        }

        protected virtual IEnumerable<TEntity> QueryAndMapDynamic(Query query)
        {
            var dynamicResults = QueryDynamic(query);
            return AutoMapper.MapDynamic<TEntity>(dynamicResults).ToList();
        }
    }
}
