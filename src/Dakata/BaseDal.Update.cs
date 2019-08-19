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

        protected virtual void UpdateByRawSql(object entity, Func<string, string> columnValueProvider = null)
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

            Execute(sql, parameters);

            RefreshEntityFromJustInsertedOrUpdatedRecord(entity);

            string SimpleProcessColumn(string column) =>
                ProcessColumn(column, columnValueProvider, parameters, entity);
        }

        public virtual void Update(IReadOnlyDictionary<string, object> whereParameters,
            IReadOnlyDictionary<string, object> values)
        {
            var query = NewQuery().Where(whereParameters).AsUpdate(values);
            Execute(query);
        }

        // Based on SE/SO answer https://dba.stackexchange.com/a/186149 and https://stackoverflow.com/a/16932591
        protected virtual int UpdateAll(IEnumerable<object> entities, int batchSize = DefaultBatchSize,
            bool parallel = false,
            Func<string, string> columnValueProvider = null,
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
                throw new ArgumentException("No key columns. Please specify key columns in the DAL entity");
            }
            var allColumns = keyColumns.Concat(columnsToUpdate).ToArray();
            var tempTableName = $"{TableName}_Values";

            batchSize = CalculateBatchSize(batchSize, allColumns.Length);

            if (parallel)
            {
                Parallel.ForEach(entities.Batch(batchSize), batch =>
                {
                    UpdateAll(columnsToUpdate, tempTableName, batch, allColumns, keyColumns,
                        columnValueProvider);
                });
            }
            else
            {
                entities.Batch(batchSize).ForEach(batch =>
                {
                    UpdateAll(columnsToUpdate, tempTableName, batch, allColumns, keyColumns,
                        columnValueProvider);
                });
            }

            return batchSize;
        }

        private void UpdateAll(string[] columnsToUpdate,
            string tempTableName,
            IEnumerable<object> batch,
            string[] allColumns,
            string[] keyColumns,
            Func<string, string> columnValueProvider)
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

            Execute(sql, parameters);
        }
    }

    public partial class BaseDal<TEntity>
    {
        public virtual void Update(TEntity entity, Func<string, string> columnValueProvider = null)
        {
            UpdateByRawSql(entity, columnValueProvider);
        }

        public virtual int UpdateAll(IEnumerable<TEntity> entities,
            int batchSize = DefaultBatchSize,
            bool parallel = false,
            Func<string, string> columnValueProvider = null,
            params string[] columnsToUpdate)
        {
            return base.UpdateAll(entities, batchSize, parallel, columnValueProvider, columnsToUpdate);
        }
    }
}
