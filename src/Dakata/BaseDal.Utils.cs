using Dapper;
using Dapper.ColumnMapper;
using Dapper.Contrib.Extensions;
using SqlKata;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using static Dakata.DbUtils;

namespace Dakata
{
    public partial class BaseDal
    {
        public IEnumerable<string> GetColumnSelections(
            string prefix = "", string tableName = null, Type entityType = null)
        {
            prefix = prefix ?? string.Empty;
            if (!prefix.IsNullOrEmpty())
            {
                prefix = $"{prefix}_";
            }
            tableName = tableName ?? (entityType != null ? GetTableName(entityType) : TableName);
            return GetPropertyColumnMapping(entityType: entityType)
                .Select(mapping => $"{AddTablePrefix(mapping.column, tableName)} AS {prefix}{mapping.property}");
        }

        public Func<string, string> ProvideUtcNowForColumn(string columnName)
        {
            return column => columnName.Equals(column, StringComparison.InvariantCultureIgnoreCase)
                ? DbProvider.UtcNowExpression
                : null;
        }

        protected Query LeftJoinTable(Query query, string joinTableName, string joinTableColumnName, string baseTableColumnName = null, string baseTableName = null)
        {
            return JoinTable(query, query.LeftJoin, joinTableName, joinTableColumnName, baseTableColumnName, baseTableName);
        }

        protected Query JoinTable(Query query, Func<string, Func<Join, Join>, Query> joiner, string joinTableName, string joinTableColumnName, string baseTableColumnName = null, string baseTableName = null)
        {
            baseTableName = baseTableName ?? TableName;
            baseTableColumnName = baseTableColumnName ?? joinTableName + joinTableColumnName;
            return joiner(joinTableName,
                join => join.On(AddTablePrefix(joinTableColumnName, joinTableName), AddTablePrefix(baseTableColumnName, baseTableName))
            );
        }

        protected Query InnerJoinTable(Query query, string joinTableName, string joinTableColumnName, string baseTableColumnName = null, string baseTableName = null)
        {
            return JoinTable(query,
                (joinTableName2, join) => query.Join(joinTableName, join),
                joinTableName,
                joinTableColumnName,
                baseTableColumnName,
                baseTableName);
        }

        protected Query LeftJoinTable<TJoinEntity>(Query query, string joinTableColumnName, string baseTableColumnName = null, string baseTableName = null)
        {
            var joinTableName = GetTableName<TJoinEntity>();
            return LeftJoinTable(query, joinTableName, joinTableColumnName, baseTableColumnName, baseTableName);
        }

        protected Query InnerJoinTable<TJoinEntity>(Query query, string joinTableColumnName, string baseTableColumnName = null, string baseTableName = null)
        {
            var joinTableName = GetTableName<TJoinEntity>();
            return InnerJoinTable(query, joinTableName, joinTableColumnName, baseTableColumnName, baseTableName);
        }

        protected string AddTablePrefix<TTableEntity>(string columnName) =>
            AddTablePrefix(columnName, GetTableName<TTableEntity>());

        protected virtual PropertyInfo GetProperty(string columnName)
        {
            return GetMappedProperties(false, false).First(x => GetColumnName(x)
                .Equals(columnName, StringComparison.InvariantCultureIgnoreCase));
        }

        protected virtual string GetPropertyName(string columnName)
        {
            return GetProperty(columnName).Name;
        }

        protected PropertyInfo GetKeyProperty()
        {
            var entityType = EntityType;
            var keyProperty =
                entityType.GetPropertiesWithAttribute<KeyAttribute>().SingleOrDefault() ??
                entityType.GetPropertiesWithAttribute<ExplicitKeyAttribute>().SingleOrDefault();
            if (keyProperty == null) throw new ApplicationException("No single [Key] or [ExplicitKey] property");
            return keyProperty;
        }

        protected string GetKeyColumnName()
        {
            var keyProperty = GetKeyProperty();
            return GetColumnName(keyProperty);
        }

        protected PropertyInfo[] GetKeyProperties()
        {
            var entityType = EntityType;
            var keyProperties =
                entityType.GetPropertiesWithAttribute<KeyAttribute>();
            var explicitKeyProperties = entityType.GetPropertiesWithAttribute<ExplicitKeyAttribute>();
            return keyProperties.Concat(explicitKeyProperties).ToArray();
        }

        protected Query BuildQueryByProperties(PropertyInfo[] properties, object entity)
        {
            var whereDictionary = PropertiesToDictionary(properties, entity);
            return NewQuery().Where(whereDictionary);
        }

        protected virtual Query AddWhereQueryWithPrefix(Query query, string columnName, object value, bool allowNull = false, string tableName = null)
        {
            if (value is string str && str.IsNullOrEmpty())
            {
                value = null;
            }
            return value.IsNull() && !allowNull ? query : query.Where(AddTablePrefix(columnName, tableName), value);
        }

        protected virtual Query AddWhereQueryWithPrefix<TEntity>(Query query, string columnName, object value, bool allowNull = false)
        {
            return AddWhereQueryWithPrefix(query, columnName, value, allowNull, GetTableName<TEntity>());
        }

        protected string AddTablePrefix(string columnName, string tableName = null) =>
                    $"{tableName ?? TableName}.{columnName}";

        /// <summary>
        /// Creates a new SqlKata query. If no more changes made to that query, it will simply get all the records of the table
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        protected Query NewQuery(string tableName = null) => new Query(tableName ?? TableName);

        protected static string GetColumnName(PropertyInfo property)
        {
            var columnMappingAttr = property.GetCustomAttribute<ColumnMappingAttribute>();
            return columnMappingAttr == null ? property.Name : columnMappingAttr.ColumnName;
        }

        protected virtual IEnumerable<(string property, string column)> GetPropertyColumnMapping(
            bool ignoreAutoIncrementColumns = false,
            bool ignoreKeyProperty = false,
            Type entityType = null)
        {
            return GetMappedProperties(ignoreAutoIncrementColumns, ignoreKeyProperty, entityType)
                .Select(prop => (prop.Name, GetColumnName(prop)));
        }

        protected virtual string GetColumnName(string propertyName)
        {
            return GetColumnName(EntityType.GetProperty(propertyName));
        }

        protected virtual IEnumerable<PropertyInfo> GetMappedProperties(bool ignoreAutoIncrementColumns,
            bool ignoreKeyProperty,
            Type entityType = null)
        {
            entityType = entityType ?? EntityType;
            var keyProperties = GetKeyProperties().Select(x => x.Name).ToList();
            return entityType.GetProperties(BindingFlags.Instance | BindingFlags.Public)
                .Where(prop => !Attribute.IsDefined(prop, typeof(ComputedAttribute)) &&
                               (!ignoreAutoIncrementColumns ||
                                 prop.GetCustomAttribute<AutoIncrementAttribute>() == null) &&
                               (!ignoreKeyProperty || !keyProperties.Contains(prop.Name)));
        }

        public virtual string[] GetTableColumns(bool ignoreAutoIncrementColumns, bool ignoreKeyProperty)
        {
            return GetMappedProperties(ignoreAutoIncrementColumns, ignoreKeyProperty).Select(GetColumnName).ToArray();
        }

        protected string[] GetKeyColumns()
        {
            return GetKeyProperties().Select(GetColumnName).ToArray();
        }

        protected IEnumerable<string> GetColumnSelectionsFromEntity<TEntity>(
            string prefix = "")
        {
            return GetColumnSelections(prefix, entityType: typeof(TEntity));
        }

        protected static Dictionary<string, object> PropertiesToDictionary(IEnumerable<PropertyInfo> properties, object entity)
        {
            return properties.ToDictionary(GetColumnName, x => x.GetValue(entity));
        }

        private (IEnumerable<string> valueClauses, DynamicParameters parameters) BuildValueClauses(
            IEnumerable<object> entities,
            Func<string, string> columnValueProvider,
            params string[] columns)
        {
            int i = 0;
            var parameters = new DynamicParameters();
            var valueClauses = entities.Select(
                x =>
                {
                    var valueClause = new List<string>(columns.Length);
                    foreach (var column in columns)
                    {
                        var columnValue = columnValueProvider?.Invoke(column);
                        if (columnValue.IsNullOrEmpty())
                        {
                            var paraName = $"p{i}";
                            var value = GetProperty(column)?.GetValue(x);
                            parameters.Add(paraName, value);
                            columnValue = $"{ParameterPrefix}{paraName}";
                        }
                        valueClause.Add(columnValue);
                        i++;
                    }
                    return $"({valueClause.JoinString(",")})";
                }
            );

            return (valueClauses, parameters);
        }

        private int CalculateBatchSize(int batchSize, int parameterCountOfOneRecord) =>
            Math.Min(batchSize, GetMaxBatchSize(parameterCountOfOneRecord));

        private string ProcessColumn(string column, Func<string, string> columnValueProvider, DynamicParameters parameters, object entity)
        {
            var columnValue = columnValueProvider?.Invoke(column);
            if (columnValue.IsNullOrEmpty())
            {
                var paraName = $"{column}";
                var value = GetProperty(column)?.GetValue(entity);
                parameters.Add(paraName, value);
                columnValue = $"{ParameterPrefix}{paraName}";
            }

            return $"{column} = {columnValue}";
        }

        private void RefreshEntityFromJustInsertedOrUpdatedRecord(object entity)
        {
            var keyColumns = GetKeyColumns();
            if (keyColumns.IsNullOrEmpty()) return;
            var parameters = new DynamicParameters();
            var whereClause = keyColumns.Select(column => ProcessColumn(column, null, parameters, entity))
                .JoinString(" AND ");
            var sql = $"SELECT * FROM {TableName} WHERE {whereClause}";
            var results = DapperConnection.Query<dynamic>(sql, parameters);
            if (!(results.FirstOrDefault() is IDictionary<string, object> row))
            {
                return;
            }

            var mappedColumns = GetTableColumns(ignoreAutoIncrementColumns: true, ignoreKeyProperty: true);
            var selectedColumns = row.Keys;
            var columns = selectedColumns.Where(selectedColumn =>
                    mappedColumns.Any(mappedColumn =>
                        mappedColumn.Equals(selectedColumn, StringComparison.InvariantCultureIgnoreCase)
                        )
                ).ToArray();
            foreach (var column in columns)
            {
                var property = GetProperty(column);
                if (property == null) continue;
                var value = row[column];
                if (value == null) continue;
                var propertyType = property.PropertyType;
                // Handle nullable types
                propertyType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;
                value = Convert.ChangeType(value, propertyType);
                property.SetValue(entity, value);
            }
        }
    }
}
