using Dapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Dakata
{
    public partial class BaseDal
    {
        public virtual int InsertAll(IEnumerable<object> entities,
            int batchSize = DefaultBatchSize,
            bool parallel = false,
            Func<string, string> columnValueProvider = null,
            params string[] columns)
        {
            columns = !columns.IsNullOrEmpty() ?
                columns : GetTableColumns(ignoreAutoIncrementColumns: true, ignoreKeyProperty: false);
            var joinedColumns = columns.JoinString(",");
            batchSize = CalculateBatchSize(batchSize, columns.Length);
            var batches = entities.Batch(batchSize);
            if (parallel)
            {
                Parallel.ForEach(batches, batch =>
                {
                    InsertAll(columns, IsOracle, joinedColumns, batch, columnValueProvider);
                });
            }
            else
            {
                batches.ForEach(batch =>
                {
                    InsertAll(columns, IsOracle, joinedColumns, batch, columnValueProvider);
                });
            }

            return batchSize;
        }

        public virtual async Task<int> InsertAllAsync(IEnumerable<object> entities,
            int batchSize = DefaultBatchSize,
            bool parallel = false,
            Func<string, string> columnValueProvider = null,
            params string[] columns)
        {
            columns = !columns.IsNullOrEmpty() ?
                columns : GetTableColumns(ignoreAutoIncrementColumns: true, ignoreKeyProperty: false);
            var joinedColumns = columns.JoinString(",");
            batchSize = CalculateBatchSize(batchSize, columns.Length);
            var batches = entities.Batch(batchSize);
            if (parallel)
            {
                var insertAllTasks = batches
                    .Select(
                        batch => 
                            InsertAllAsync(columns, IsOracle, joinedColumns, batch, columnValueProvider))
                    .ToArray();

                await Task.WhenAll(insertAllTasks);
            }
            else
            {
                foreach(var batch in batches)
                {
                    await InsertAllAsync(columns, IsOracle, joinedColumns, batch, columnValueProvider);
                }
            }

            return batchSize;
        }

        private (string sql, DynamicParameters parameters) BuildInsertAllQuery(string[] columns, 
            bool isOracle,
            string joinedColumns,
            IEnumerable<object> batch,
            Func<string, string> columnValueProvider)
        {
            var sql = isOracle ? "INSERT ALL " : $"INSERT INTO {TableName} ({joinedColumns}) VALUES ";
            var (valueClauses, parameters) = BuildValueClauses(batch, columnValueProvider, columns);

            if (isOracle)
            {
                var intoClauses = valueClauses
                    .Select(x => $"INTO {TableName} ({joinedColumns}) VALUES {x}");
                sql += intoClauses.JoinString(" ");
            }
            else
            {
                sql += valueClauses.JoinString(",");
            }

            return (sql, parameters);
        }

        private void InsertAll(string[] columns,
            bool isOracle,
            string joinedColumns,
            IEnumerable<object> batch,
            Func<string, string> columnValueProvider)
        {
            var (sql, parameters) = BuildInsertAllQuery(columns, isOracle, joinedColumns, batch, columnValueProvider);
            Execute(sql, parameters);
        }

        private async Task InsertAllAsync(string[] columns,
            bool isOracle,
            string joinedColumns,
            IEnumerable<object> batch,
            Func<string, string> columnValueProvider)
        {
            var (sql, parameters) = BuildInsertAllQuery(columns, isOracle, joinedColumns, batch, columnValueProvider);
            await ExecuteAsync(sql, parameters);
        }

        public long InsertByRawSql(object entity, 
            Func<string, string> columnValueProvider, 
            int? commandTimeout = null,
            params string[] columns)
        {
            var (sql, parameters, autoIncrementAttribute, autoIncrementAttributeProperty) =
                PrepareInsertByRawSqlParameters(entity,
                    columnValueProvider,
                    columns);
            var identity = Execute((connection, timeout) =>
                                        DbProvider.Insert(sql,
                                            parameters,
                                            connection,
                                            autoIncrementAttribute?.SequenceName,
                                            timeout),
                                    commandTimeout
                                 );
            RefreshEntity(entity, autoIncrementAttributeProperty, identity);

            return identity;
        }

        public async Task<long> InsertByRawSqlAsync(object entity,
            Func<string, string> columnValueProvider,
            int? commandTimeout = null,
            params string[] columns)
        {
            var (sql, parameters, autoIncrementAttribute, autoIncrementAttributeProperty) =
                PrepareInsertByRawSqlParameters(entity,
                    columnValueProvider,
                    columns);
            var identity = await ExecuteAsync(async (connection, timeout) =>
                                            await DbProvider.InsertAsync(sql,
                                            parameters,
                                            connection,
                                            autoIncrementAttribute?.SequenceName,
                                            timeout),
                                            commandTimeout
                                 );
            RefreshEntity(entity, autoIncrementAttributeProperty, identity);

            return identity;
        }

        private void RefreshEntity(object entity, PropertyInfo autoIncrementAttributeProperty, long identity)
        {
            if (autoIncrementAttributeProperty != null)
            {
                var propertyType = autoIncrementAttributeProperty.PropertyType;
                autoIncrementAttributeProperty.SetValue(entity,
                    Convert.ChangeType(identity, propertyType));
            }

            RefreshEntityFromJustInsertedOrUpdatedRecord(entity);
        }

        private (string sql,
            DynamicParameters parameters,
            AutoIncrementAttribute autoIncrementAttribute,
            PropertyInfo autoIncrementAttributeProperty
            ) PrepareInsertByRawSqlParameters(
            object entity, 
            Func<string, string> columnValueProvider, 
            string[] columns)
        {
            columns = columns.IsNullOrEmpty() ?
                            GetTableColumns(ignoreAutoIncrementColumns: true, ignoreKeyProperty: false) :
                            columns;
            var parameters = new DynamicParameters();
            var valueClause = new List<string>(columns.Length);
            var parameterPrefix = ParameterPrefix;
            foreach (var column in columns)
            {
                var columnValue = columnValueProvider?.Invoke(column);
                if (columnValue.IsNullOrEmpty())
                {
                    var paraName = $"{column}";
                    var value = GetProperty(column)?.GetValue(entity);
                    parameters.Add(paraName, value);
                    columnValue = $"{parameterPrefix}{paraName}";
                }
                valueClause.Add(columnValue);
            }
            var autoIncrementAttributeProperty = entity.GetType().GetPropertiesWithAttribute<AutoIncrementAttribute>().FirstOrDefault();
            var autoIncrementAttribute = autoIncrementAttributeProperty?.
GetCustomAttributes(true)?.
Cast<Attribute>()?.
FirstOrDefault(x => x is AutoIncrementAttribute) as AutoIncrementAttribute;
            var sql = $"INSERT INTO {TableName} ({columns.JoinString(",")}) VALUES ({valueClause.JoinString(",")})";
            Logger(new SqlInfo(sql, parameters.AsDictionary()));
            return (sql, parameters, autoIncrementAttribute, autoIncrementAttributeProperty);
        }
    }

    public partial class BaseDal<TEntity>
    {
        public virtual int InsertAll(IEnumerable<TEntity> entities, 
            int batchSize = DefaultBatchSize,
            bool parallel = false,
            Func<string, string> columnValueProvider = null,
            params string[] columns)
        {
            return base.InsertAll(entities, batchSize, parallel, columnValueProvider, columns);
        }

        public virtual async Task<int> InsertAllAsync(IEnumerable<TEntity> entities,
            int batchSize = DefaultBatchSize,
            bool parallel = false,
            Func<string, string> columnValueProvider = null,
            params string[] columns)
        {
            return await base.InsertAllAsync(entities, batchSize, parallel, columnValueProvider, columns);
        }

        public virtual long Insert(TEntity entity, Func<string, string> columnValueProvider = null,
            int? commandTimeout = null,
            params string[] columns)
        {
            return InsertByRawSql(entity, columnValueProvider, commandTimeout, columns);
        }

        public virtual async Task<long> InsertAsync(TEntity entity, Func<string, string> columnValueProvider = null,
            int? commandTimeout = null,
            params string[] columns)
        {
            return await InsertByRawSqlAsync(entity, columnValueProvider, commandTimeout, columns);
        }
    }
}
