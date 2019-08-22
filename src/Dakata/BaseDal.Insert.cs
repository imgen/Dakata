using Dapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dakata
{
    public partial class BaseDal
    {
        protected virtual int InsertAll(IEnumerable<object> entities,
            int batchSize = DefaultBatchSize,
            bool parallel = false,
            Func<string, string> columnValueProvider = null,
            params string[] columns)
        {
            columns = !columns.IsNullOrEmpty() ?
                columns : GetTableColumns(ignoreAutoIncrementColumns: true, ignoreKeyProperty: false);
            var isOracle = IsOracle;
            var joinedColumns = columns.JoinString(",");

            batchSize = CalculateBatchSize(batchSize, columns.Length);
            if (parallel)
            {
                Parallel.ForEach(entities.Batch(batchSize), batch =>
                {
                    InsertAll(columns, isOracle, joinedColumns, batch, columnValueProvider);
                });
            }
            else
            {
                entities.Batch(batchSize).ForEach(batch =>
                {
                    InsertAll(columns, isOracle, joinedColumns, batch, columnValueProvider);
                });
            }

            return batchSize;
        }

        private void InsertAll(string[] columns,
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

            Execute(sql, parameters);
        }

        public long InsertByRawSql(object entity, Func<string, string> columnValueProvider, params string[] columns)
        {
            columns = columns.IsNullOrEmpty()? 
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
            var autoIncrementAttributeProperty =
                EntityType.GetPropertiesWithAttribute<AutoIncrementAttribute>().FirstOrDefault();
            var autoIncrementAttribute = autoIncrementAttributeProperty?.
                    GetCustomAttributes(true)?.
                    Cast<Attribute>()?.
                    FirstOrDefault(x => x is AutoIncrementAttribute) as AutoIncrementAttribute;
            var sql = $"INSERT INTO {TableName} ({columns.JoinString(",")}) VALUES ({valueClause.JoinString(",")})";
            long identity = DapperConnection.Execute(connection => DbProvider.Insert(
                sql, parameters, connection, autoIncrementAttribute?.SequenceName));
            
            if (autoIncrementAttributeProperty != null)
            {
                var propertyType = autoIncrementAttributeProperty.PropertyType;
                autoIncrementAttributeProperty.SetValue(entity,
                    Convert.ChangeType(identity, propertyType));
            }

            RefreshEntityFromJustInsertedOrUpdatedRecord(entity);

            return identity;
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

        public virtual long Insert(TEntity entity, Func<string, string> columnValueProvider = null,
            params string[] columns)
        {
            return InsertByRawSql(entity, columnValueProvider, columns);
        }
    }
}
