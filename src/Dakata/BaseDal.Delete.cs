using Dapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Dakata
{
    public partial class BaseDal
    {
        public virtual void DeleteByParameters(object parameters)
        {
            var query = NewQuery().Where(parameters.AsDictionary());
            Execute(query.AsDelete());
        }

        public virtual async Task DeleteByParametersAsync(object parameters)
        {
            var query = NewQuery().Where(parameters.AsDictionary());
            await ExecuteAsync(query.AsDelete());
        }

        public virtual void DeleteByInClause(string column, IEnumerable<object> values)
        {
            var query = NewQuery().WhereIn(column, values).AsDelete();
            Execute(query);
        }

        public virtual async Task DeleteByInClauseAsync(string column, IEnumerable<object> values)
        {
            var query = NewQuery().WhereIn(column, values).AsDelete();
            await ExecuteAsync(query);
        }

        public virtual void DeleteById<TIdentity>(TIdentity identity)
        {
            var keyProperty = GetKeyProperty();
            var query = NewQuery().Where(GetColumnName(keyProperty), identity).AsDelete();
            Execute(query);
        }

        public virtual async Task DeleteByIdAsync<TIdentity>(TIdentity identity)
        {
            var keyProperty = GetKeyProperty();
            var query = NewQuery().Where(GetColumnName(keyProperty), identity).AsDelete();
            await ExecuteAsync(query);
        }

        private (string keyColumnName, object[] keyColumnValues) GetKeyColumnNameAndValues(IEnumerable<object> entities)
        {
            var keyProperty = GetKeyProperty();
            var keyColumnName = GetColumnName(keyProperty);
            var values = entities.Select(entity => keyProperty.GetValue(entity)).Distinct().ToArray();
            return (keyColumnName, values);
        }

        public virtual void DeleteByKeyColumn(IEnumerable<object> entities)
        {
            var (keyColumnName, values) = GetKeyColumnNameAndValues(entities);
            DeleteByInClause(keyColumnName, values);
        }

        public virtual async Task DeleteByKeyColumnAsync(IEnumerable<object> entities)
        {
            var (keyColumnName, values) = GetKeyColumnNameAndValues(entities);
            await DeleteByInClauseAsync(keyColumnName, values);
        }

        private string DeleteAllStatement => $"DELETE FROM {TableName}";

        public virtual void DeleteAll()
        {
            Execute(DeleteAllStatement);
        }

        public virtual async Task DeleteAllAsync()
        {
            await ExecuteAsync(DeleteAllStatement);
        }

        private string TruncateStatement => $"TRUNCATE TABLE {TableName}";

        public virtual void Truncate()
        {
            Execute(TruncateStatement);
        }

        public virtual async Task TruncateAsync()
        {
            await ExecuteAsync(TruncateStatement);
        }

        private (string[] columns,
                int batchSize, 
                IEnumerable<IEnumerable<object>> batches,
                string tempTableName) 
            PrepareParametersForDeleteAll(
                IEnumerable<object> entities,
                int batchSize = DefaultBatchSize,
                params string[] criteriaColumns
            )
        {
            if (!IsSqlServer)
            {
                throw new NotImplementedException();
            }
            criteriaColumns = !criteriaColumns.IsNullOrEmpty() ?
                criteriaColumns : GetKeyColumns();
            if (!criteriaColumns.Any())
            {
                throw new ArgumentException($"{nameof(criteriaColumns)} is empty and also no key columns");
            }

            batchSize = CalculateBatchSize(batchSize, criteriaColumns.Length);
            var tempTableName = $"{TableName}_Values".Replace(".", "_");
            var batches = entities.Batch(batchSize);

            return (criteriaColumns, batchSize, batches, tempTableName);
        }

        // Based on SO answer https://stackoverflow.com/a/36257723/915147
        protected virtual int DeleteAll(IEnumerable<object> entities,
            int batchSize = DefaultBatchSize,
            bool parallel = false,
            Func<string, string> columnValueProvider = null,
            params string[] criteriaColumns)
        {
            var (newCriteriaColumns, newBatchSize, batches, tempTableName) = 
                PrepareParametersForDeleteAll(entities, batchSize, criteriaColumns);
            if (parallel)
            {
                Parallel.ForEach(batches, batch =>
                {
                    DeleteAll(newCriteriaColumns, batch, tempTableName, columnValueProvider);
                });
            }
            else
            {
                batches.ForEach(batch =>
                {
                    DeleteAll(newCriteriaColumns, batch, tempTableName, columnValueProvider);
                });
            }

            return newBatchSize;
        }

        // Based on SO answer https://stackoverflow.com/a/36257723/915147
        protected virtual async Task<int> DeleteAllAsync(IEnumerable<object> entities,
            int batchSize = DefaultBatchSize,
            bool parallel = false,
            Func<string, string> columnValueProvider = null,
            params string[] criteriaColumns)
        {
            var (newCriteriaColumns, newBatchSize, batches, tempTableName) =
                PrepareParametersForDeleteAll(entities, batchSize, criteriaColumns);
            if (parallel)
            {
                var deleteAllTasks = batches
                    .Select(
                        batch => 
                        DeleteAllAsync(newCriteriaColumns, 
                            batch, 
                            tempTableName, 
                            columnValueProvider)
                    )
                    .ToArray();
                await Task.WhenAll(deleteAllTasks);
            }
            else
            {
                foreach(var batch in batches)
                {
                    await DeleteAllAsync(newCriteriaColumns,
                            batch,
                            tempTableName,
                            columnValueProvider);
                }
            }

            return newBatchSize;
        }

        private (string sql, DynamicParameters parameters) GetDeleteSql(object entity, Func<string, string> columnValueProvider)
        {
            var keyColumns = GetKeyColumns();

            var parameters = new DynamicParameters();
            var whereClause = keyColumns.Select(SimpleProcessColumn)
                .JoinString(" AND ");
            var sql = $"DELETE FROM {TableName} WHERE {whereClause}";

            return (sql, parameters);

            string SimpleProcessColumn(string column) =>
                ProcessColumn(column, columnValueProvider, parameters, entity);
        }

        protected virtual void DeleteByRawSql(object entity, Func<string, string> columnValueProvider)
        {
            var (sql, parameters) = GetDeleteSql(entity, columnValueProvider);
            Execute(sql, parameters);
        }

        protected virtual async Task DeleteByRawSqlAsync(object entity, Func<string, string> columnValueProvider)
        {
            var (sql, parameters) = GetDeleteSql(entity, columnValueProvider);
            await ExecuteAsync(sql, parameters);
        }

        private (string sql, DynamicParameters parameters) BuildDeleteAllQuery(string[] criteriaColumns, IEnumerable<object> batch, string tempTableName,
            Func<string, string> columnValueProvider)
        {
            var sql = $@"DELETE {TableName} FROM {TableName} INNER JOIN (VALUES ";
            var (valueClauses, parameters) = BuildValueClauses(batch, columnValueProvider, criteriaColumns);
            sql += valueClauses.JoinString(",");
            sql += $@")
 AS {tempTableName} ({criteriaColumns.JoinString(",")})
ON {criteriaColumns.Select(column => $"{AddTablePrefix(column)} = {tempTableName}.{column}").JoinString(" AND ")}";
            return (sql, parameters);
        }

        private void DeleteAll(string[] criteriaColumns, 
            IEnumerable<object> batch, 
            string tempTableName,
            Func<string, string> columnValueProvider)
        {
            var (sql, parameters) = BuildDeleteAllQuery(criteriaColumns, batch, tempTableName, columnValueProvider);
            Execute(sql, parameters);
        }

        private async Task DeleteAllAsync(string[] criteriaColumns, IEnumerable<object> batch, string tempTableName,
            Func<string, string> columnValueProvider)
        {
            var (sql, parameters) = BuildDeleteAllQuery(criteriaColumns, batch, tempTableName, columnValueProvider);
            await ExecuteAsync(sql, parameters);
        }
    }

    public partial class BaseDal<TEntity>
    {
        public virtual void Delete(TEntity entity, Func<string, string> columnValueProvider = null)
        {
            DeleteByRawSql(entity, columnValueProvider);
        }

        public virtual async Task DeleteAsync(TEntity entity, Func<string, string> columnValueProvider = null)
        {
            await DeleteByRawSqlAsync(entity, columnValueProvider);
        }

        public virtual int DeleteAll(IEnumerable<TEntity> entities,
            int batchSize = DefaultBatchSize,
            bool parallel = false,
            Func<string, string> columnValueProvider = null,
            params string[] criteriaColumns)
        {
            return base.DeleteAll(entities, batchSize, parallel, columnValueProvider, criteriaColumns);
        }

        public virtual async Task<int> DeleteAllAsync(IEnumerable<TEntity> entities,
            int batchSize = DefaultBatchSize,
            bool parallel = false,
            Func<string, string> columnValueProvider = null,
            params string[] criteriaColumns)
        {
            return await base.DeleteAllAsync(entities, batchSize, parallel, columnValueProvider, criteriaColumns);
        }

        protected virtual void DeleteByInClause<TMember>(Expression<Func<TEntity, TMember>> memberExpression, IEnumerable<object> values)
        {
            DeleteByInClause(GetColumnName(memberExpression), values);
        }
        protected virtual async Task DeleteByInClauseAsync<TMember>(Expression<Func<TEntity, TMember>> memberExpression, IEnumerable<object> values)
        {
            await DeleteByInClauseAsync(GetColumnName(memberExpression), values);
        }
    }
}
