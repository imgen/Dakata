using Dapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dakata
{
    public partial class BaseDal
    {
        public virtual void Update(object whereParameters, object values)
        {
            Update(whereParameters.AsDictionary(), values.AsDictionary());
        }

        public virtual async Task UpdateAsync(object whereParameters, object values)
        {
            await UpdateAsync(whereParameters.AsDictionary(), values.AsDictionary());
        }

        public virtual void Update(IReadOnlyDictionary<string, object> whereParameters,
            IReadOnlyDictionary<string, object> values)
        {
            var query = NewQuery().Where(whereParameters).AsUpdate(values);
            Execute(query);
        }
        public virtual async Task UpdateAsync(IReadOnlyDictionary<string, object> whereParameters,
            IReadOnlyDictionary<string, object> values)
        {
            var query = NewQuery().Where(whereParameters).AsUpdate(values);
            await ExecuteAsync(query);
        }

        protected virtual void UpdateByRawSql(object entity, Func<string, string> columnValueProvider = null)
        {
            var (sql, parameters) = BuildUpdateQuery(entity, columnValueProvider);
            Execute(sql, parameters);
            RefreshEntityFromJustInsertedOrUpdatedRecord(entity);
        }

        protected virtual async Task UpdateByRawSqlAsync(object entity, Func<string, string> columnValueProvider = null)
        {
            var (sql, parameters) = BuildUpdateQuery(entity, columnValueProvider);
            await ExecuteAsync(sql, parameters);
            RefreshEntityFromJustInsertedOrUpdatedRecord(entity);
        }

        private (string sql, DynamicParameters parameters) BuildUpdateQuery(object entity, Func<string, string> columnValueProvider)
        {
            var columnsToUpdate = GetTableColumns(ignoreAutoIncrementColumns: true,
                            ignoreKeyProperty: true);
            var keyColumns = GetKeyColumns();

            var parameters = new DynamicParameters();
            var setClause = columnsToUpdate.Select(SimpleProcessColumn)
.JoinString(",");
            var whereClause = keyColumns.Select(SimpleProcessColumn)
                .JoinString(" AND ");
            var sql = $"UPDATE {TableName} SET {setClause}";
            if (keyColumns.Any())
            {
                sql += $" WHERE {whereClause}";
            }

            return (sql, parameters);

            string SimpleProcessColumn(string column) =>
                ProcessColumn(column, columnValueProvider, parameters, entity);
        }

        private (
                string[] columnsToUpdate,
                string[] allColumns, 
                string[] keyColumns, 
                int batchSize, IEnumerable<IEnumerable<object>> batches,
                string tempTableName)
            PrepareParametersForUpdateAll(
                IEnumerable<object> entities, 
                int batchSize = DefaultBatchSize,
                params string[] columnsToUpdate)
        {
            if (!IsSqlServer)
            {
                throw new NotImplementedException();
            }
            columnsToUpdate = !columnsToUpdate.IsNullOrEmpty() ?
                columnsToUpdate : GetTableColumns(ignoreAutoIncrementColumns: false, ignoreKeyProperty: true);
            var keyColumns = GetKeyColumns();
            if (keyColumns.IsNullOrEmpty())
            {
                throw new ArgumentException("No key columns found on entity. Please specify key columns in the DAL entity");
            }
            var allColumns = keyColumns.Concat(columnsToUpdate).ToArray();
            var tempTableName = $"{TableName}_Values";

            batchSize = CalculateBatchSize(batchSize, allColumns.Length);

            var batches = entities.Batch(batchSize);

            return (columnsToUpdate, allColumns, keyColumns, batchSize, batches, tempTableName);
        }

        // Based on SE/SO answer https://dba.stackexchange.com/a/186149 and https://stackoverflow.com/a/16932591
        protected virtual int UpdateAll(
            IEnumerable<object> entities, 
            int batchSize = DefaultBatchSize,
            bool parallel = false,
            Func<string, string> columnValueProvider = null,
            params string[] columnsToUpdate)
        {
            var (newColumnsToUpdate, allColumns, keyColumns, newBatchSize, batches, tempTableName) = 
                PrepareParametersForUpdateAll(entities, batchSize, columnsToUpdate);

            if (parallel)
            {
                Parallel.ForEach(batches, batch =>
                {
                    UpdateAll(newColumnsToUpdate, tempTableName, batch, allColumns, keyColumns,
                        columnValueProvider);
                });
            }
            else
            {
                batches.ForEach(batch =>
                {
                    UpdateAll(newColumnsToUpdate, tempTableName, batch, allColumns, keyColumns,
                        columnValueProvider);
                });
            }

            return newBatchSize;
        }

        // Based on SE/SO answer https://dba.stackexchange.com/a/186149 and https://stackoverflow.com/a/16932591
        protected virtual async Task<int> UpdateAllAsync(
            IEnumerable<object> entities,
            int batchSize = DefaultBatchSize,
            bool parallel = false,
            Func<string, string> columnValueProvider = null,
            params string[] columnsToUpdate)
        {
            var (newColumnsToUpdate, allColumns, keyColumns, newBatchSize, batches, tempTableName) =
                PrepareParametersForUpdateAll(entities, batchSize, columnsToUpdate);

            if (parallel)
            {
                var updateAllTasks = batches
                    .Select(
                        batch =>
                        UpdateAllAsync(newColumnsToUpdate, 
                            tempTableName, 
                            batch, 
                            allColumns, 
                            keyColumns,
                            columnValueProvider)
                    )
                    .ToArray();
                await Task.WhenAll(updateAllTasks);
            }
            else
            {
                foreach(var batch in batches)
                {
                    await UpdateAllAsync(newColumnsToUpdate, 
                        tempTableName, 
                        batch, 
                        allColumns, 
                        keyColumns,
                        columnValueProvider);
                }
            }

            return newBatchSize;
        }

        private (string sql, DynamicParameters parameters) BuildUpdateAllQuery(string[] columnsToUpdate,
            string tempTableName,
            IEnumerable<object> batch,
            string[] allColumns,
            string[] keyColumns,
            Func<string, string> columnValueProvider
            )
        {
            var sql = $"UPDATE {TableName} SET ";
            var setClause = columnsToUpdate.Select(column =>
                    $"{column} = {tempTableName}.{column}")
                .JoinString(",");
            sql += setClause + $@" FROM {TableName} INNER JOIN (VALUES ";
            var (valueClauses, parameters) = BuildValueClauses(batch, columnValueProvider, allColumns);
            sql += valueClauses.JoinString(",");
            sql += $@")
 AS {tempTableName} ({allColumns.JoinString(",")})
ON {keyColumns.Select(column => $"{AddTablePrefix(column)} = {tempTableName}.{column}").JoinString(" AND ")}";

            return (sql, parameters);

        }

        private void UpdateAll(string[] columnsToUpdate,
            string tempTableName,
            IEnumerable<object> batch,
            string[] allColumns,
            string[] keyColumns,
            Func<string, string> columnValueProvider)
        {
            var (sql, parameters) = BuildUpdateAllQuery(columnsToUpdate, 
                tempTableName, 
                batch, 
                allColumns, 
                keyColumns, 
                columnValueProvider);
            Execute(sql, parameters);
        }

        private async Task UpdateAllAsync(string[] columnsToUpdate,
            string tempTableName,
            IEnumerable<object> batch,
            string[] allColumns,
            string[] keyColumns,
            Func<string, string> columnValueProvider)
        {
            var (sql, parameters) = BuildUpdateAllQuery(columnsToUpdate,
                tempTableName,
                batch,
                allColumns,
                keyColumns,
                columnValueProvider);
            await ExecuteAsync(sql, parameters);
        }
    }

    public partial class BaseDal<TEntity>
    {
        public virtual void Update(TEntity entity, Func<string, string> columnValueProvider = null)
        {
            UpdateByRawSql(entity, columnValueProvider);
        }

        public virtual async Task UpdateAsync(TEntity entity, Func<string, string> columnValueProvider = null)
        {
            await UpdateByRawSqlAsync(entity, columnValueProvider);
        }

        public virtual int UpdateAll(IEnumerable<TEntity> entities,
            int batchSize = DefaultBatchSize,
            bool parallel = false,
            Func<string, string> columnValueProvider = null,
            params string[] columnsToUpdate)
        {
            return base.UpdateAll(entities, batchSize, parallel, columnValueProvider, columnsToUpdate);
        }

        public virtual async Task<int> UpdateAllAsync(IEnumerable<TEntity> entities,
            int batchSize = DefaultBatchSize,
            bool parallel = false,
            Func<string, string> columnValueProvider = null,
            params string[] columnsToUpdate)
        {
            return await base.UpdateAllAsync(entities, batchSize, parallel, columnValueProvider, columnsToUpdate);
        }
    }
}
