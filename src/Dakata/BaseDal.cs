using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Dapper;
using Dapper.ColumnMapper;
using Dapper.Contrib.Extensions;
using Slapper;
using SqlKata;
using static Dakata.DbUtils;

namespace Dakata
{
    public class BaseDal
    {
        public const int DefaultBatchSize = 100;
        public virtual string TableName { get; }

        public BaseDal(string tableName)
        {
            TableName = tableName;
        }

        public static IDbProvider DbProvider { get; set; }

        protected static DbEngines DbEngine => DbProvider.DbEngine;

        protected static bool IsOracle => DbEngine == DbEngines.Oracle;
        protected static bool IsSqlServer => DbEngine == DbEngines.SqlServer;

        protected static string ParameterPrefix => IsOracle ? ":" : "@";

        protected Type EntityType;

        public static int MaxParameterCount => DbProvider.MaxParameterCount;

        public static int GetMaxBatchSize(int parameterCountOfOneRecord) =>
            MaxParameterCount / parameterCountOfOneRecord;

        private static int CalculateBatchSize(int batchSize, int parameterCountOfOneRecord) =>
            Math.Min(batchSize, GetMaxBatchSize(parameterCountOfOneRecord));

        protected virtual IEnumerable<dynamic> QueryDynamic(Query query)
        {
            return Connection.Query<dynamic>(query);
        }

        protected virtual TMaxColumn GetMaxValueOfColumn<TMaxColumn>(string columnName)
        {
            var query = NewQuery().AsMax(columnName);
            return ExecuteScalar<TMaxColumn>(query);
        }

        public virtual void DeleteAll()
        {
            Execute($"DELETE FROM {TableName}");
        }

        public virtual void Truncate()
        {
            Execute($"TRUNCATE TABLE {TableName}");
        }

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
            
            RefreshEntityFromJustInsertedOrUpdatedRow(entity);

            string SimpleProcessColumn(string column) => 
                ProcessColumn(column, columnValueProvider, parameters, entity);
        }

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

        public virtual void Update(IReadOnlyDictionary<string, object> whereParameters,
            IReadOnlyDictionary<string, object> values)
        {
            var query = NewQuery().Where(whereParameters).AsUpdate(values);
            Execute(query);
        }

        protected static Dictionary<string, object> PropertiesToDictionary(IEnumerable<PropertyInfo> properties, object entity)
        {
            return properties.ToDictionary(GetColumnName, x => x.GetValue(entity));
        }

        public virtual void DeleteByParameters(object parameters)
        {
            var query = NewQuery().Where(parameters.AsDictionary());
            Execute(query.AsDelete());
        }

        public virtual void DeleteByInClause(string column, IEnumerable<object> values)
        {
            var query = NewQuery().WhereIn(column, values).AsDelete();
            Execute(query);
        }

        public virtual void DeleteById<TIdentity>(TIdentity identity)
        {
            var keyProperty = GetKeyProperty();
            var query = NewQuery().Where(GetColumnName(keyProperty), identity).AsDelete();
            Execute(query);
        }

        public virtual void DeleteByKeyColumn(IEnumerable<object> entities)
        {
            var keyProperty = GetKeyProperty();
            var keyColumnName = GetColumnName(keyProperty);
            var values = entities.Select(entity => keyProperty.GetValue(entity)).Distinct();
            DeleteByInClause(keyColumnName, values);
        }

        public virtual void InsertAllByParams(params object[] entities)
        {
            InsertAll(entities);
        }

        protected virtual T ExecuteScalar<T>(Query query) => Connection.ExecuteScalar<T>(query);
        protected virtual T ExecuteScalar<T>(string sql, object parameters) => Connection.ExecuteScalar<T>(sql, parameters);

        protected virtual void Execute(Query query) => Connection.Execute(query);

        protected virtual void Execute(string sql, object parameters = null) => Connection.Execute(sql, parameters);

        protected virtual Query AddWhereQueryWithPrefix(Query query, string columnName, object value, bool allowNull = false, string tableName = null)
        {
            if (value is string str && str.IsNullOrEmpty())
            {
                value = null;
            }
            return value.IsNull() && !allowNull ? query : query.Where(AddTablePrefix(columnName, tableName), value);
        }

        protected virtual Query AddWhereQueryWithPrefix<TEntity>(Query query, string columnName, object value, bool allowNull = false)
            where TEntity: class
        {
            return AddWhereQueryWithPrefix(query, columnName, value, allowNull, GetTableName<TEntity>());
        }

        protected string AddTablePrefix(string columnName, string tableName = null) =>
                    $"{tableName ?? TableName}.{columnName}";

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

        public IEnumerable<string> GetColumnSelections(
            string prefix = "", string tableName = null, Type entityType = null)
        {
            prefix = prefix ?? string.Empty;
            if (!prefix.IsNullOrEmpty())
            {
                prefix = $"{prefix}_";
            }
            tableName = tableName?? (entityType != null? GetTableName(entityType) : TableName);
            return GetPropertyColumnMapping(entityType: entityType)
                .Select(mapping => $"{AddTablePrefix(mapping.column, tableName)} AS {prefix}{mapping.property}");
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

        public TCount GetCount<TCount>()
        {
            return ExecuteScalar<TCount>(NewQuery().AsCount());
        }


        public int GetCount(Query query)
        {
            return ExecuteScalar<int>(query.AsCount());
        }

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

        protected virtual int InsertAll(IEnumerable<object> entities,
            int batchSize = DefaultBatchSize,
            bool ignoreAutoIncrementColumns = true,
            bool enableMultithreading = true,
            Func<string, string> columnValueProvider = null,
            params string[] columns)
        {
            columns = !columns.IsNullOrEmpty() ?
                columns : GetTableColumns(ignoreAutoIncrementColumns, false);
            var isOracle = IsOracle;
            var joinedColumns = columns.JoinString(",");

            var batchIndex = 0;

            batchSize = CalculateBatchSize(batchSize, columns.Length);
            if (enableMultithreading)
            {
                Parallel.ForEach(entities.Batch(batchSize), batch =>
                {
                    batchIndex = InsertAll(columns, isOracle, joinedColumns, batch, columnValueProvider, batchIndex);
                });
            }
            else
            {
                entities.Batch(batchSize).ForEach(batch =>
                {
                    batchIndex = InsertAll(columns, isOracle, joinedColumns, batch, columnValueProvider, batchIndex);
                });
            }

            return batchSize;
        }

        private int InsertAll(string[] columns,
            bool isOracle,
            string joinedColumns,
            IEnumerable<object> batch,
            Func<string, string> columnValueProvider,
            int batchIndex)
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
            return batchIndex;
        }

        // Based on SE/SO answer https://dba.stackexchange.com/a/186149 and https://stackoverflow.com/a/16932591
        protected virtual int UpdateAll(IEnumerable<object> entities, int batchSize = DefaultBatchSize,
            bool enableMultithreading = true,
            Func<string, string> columnValueProvider = null,
            params string[] columnsToUpdate)
        {
            if (!IsSqlServer)
            {
                throw new NotImplementedException();
            }
            columnsToUpdate = !columnsToUpdate.IsNullOrEmpty() ?
                columnsToUpdate : GetTableColumns(false, ignoreKeyProperty: true);
            var keyColumns = GetKeyColumns();
            if (keyColumns.IsNullOrEmpty())
            {
                throw new ArgumentException("No key columns. Please specify key columns in the DAL entity");
            }
            var allColumns = keyColumns.Concat(columnsToUpdate).ToArray();
            var tempTableName = $"{TableName}_Values";

            var batchIndex = 0;

            batchSize = CalculateBatchSize(batchSize, allColumns.Length);

            if (enableMultithreading)
            {
                Parallel.ForEach(entities.Batch(batchSize), batch =>
                {
                    batchIndex = UpdateAll(columnsToUpdate, tempTableName, batch, allColumns, keyColumns, 
                        columnValueProvider, batchIndex);
                });
            }
            else
            {
                entities.Batch(batchSize).ForEach(batch =>
                {
                    batchIndex = UpdateAll(columnsToUpdate, tempTableName, batch, allColumns, keyColumns,
                        columnValueProvider,
                        batchIndex);
                });
            }

            return batchSize;
        }

        private int UpdateAll(string[] columnsToUpdate, string tempTableName, IEnumerable<object> batch,
            string[] allColumns, string[] keyColumns,
            Func<string, string> columnValueProvider,
            int batchIndex)
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
            return batchIndex;
        }

        // Based on SO answer https://stackoverflow.com/a/36257723/915147
        protected virtual int DeleteAll(IEnumerable<object> entities, int batchSize = DefaultBatchSize,
            bool enableMultithreading = true,
            Func<string, string> columnValueProvider = null,
            params string[] criteriaColumns)
        {
            if (!IsSqlServer)
            {
                throw new NotImplementedException();
            }
            criteriaColumns = !criteriaColumns.IsNullOrEmpty() ?
                criteriaColumns : GetKeyColumns();
            if (!criteriaColumns.Any())
            {
                throw new ArgumentException("criteriaColumns is empty and also no key columns");
            }

            batchSize = CalculateBatchSize(batchSize, criteriaColumns.Length);
            var tempTableName = $"{TableName}_Values";

            var batchIndex = 0;

            if (enableMultithreading)
            {
                Parallel.ForEach(entities.Batch(batchSize), batch =>
                {
                    batchIndex = DeleteAll(criteriaColumns, batch, tempTableName, columnValueProvider, batchIndex);
                });
            }
            else
            {
                entities.Batch(batchSize).ForEach(batch =>
                {
                    batchIndex = DeleteAll(criteriaColumns, batch, tempTableName, columnValueProvider, batchIndex);
                });
            }

            return batchSize;
        }

        private int DeleteAll(string[] criteriaColumns, IEnumerable<object> batch, string tempTableName,
            Func<string, string> columnValueProvider, int batchIndex)
        {
            var sql = $@"DELETE {TableName} FROM {TableName} INNER JOIN (VALUES ";
            var (valueClauses, parameters) = BuildValueClauses(batch, columnValueProvider, criteriaColumns);
            sql += valueClauses.JoinString(",");
            sql += $@")
 AS {tempTableName} ({criteriaColumns.JoinString(",")})
ON {criteriaColumns.Select(column => $"{AddTablePrefix(column)} = {tempTableName}.{column}").JoinString(" AND ")}";

            Execute(sql, parameters);
            return batchIndex;
        }

        public long InsertByRawSql(object entity, Func<string, string> columnValueProvider)
        {
            var columns = GetTableColumns(ignoreAutoIncrementColumns: true, ignoreKeyProperty: false);
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

            var sql = $"INSERT INTO {TableName} ({columns.JoinString(",")}) VALUES ({valueClause.JoinString(",")})";
            long identity = Connection.Execute(connection => DbProvider.Insert(sql, parameters, connection));
            var autoIncrementAttributeProperty =
                EntityType.GetPropertiesWithAttribute<AutoIncrementAttribute>().FirstOrDefault();
            if (autoIncrementAttributeProperty != null)
            {
                var propertyType = autoIncrementAttributeProperty.PropertyType;
                autoIncrementAttributeProperty.SetValue(entity,
                    Convert.ChangeType(identity, propertyType));
            }

            RefreshEntityFromJustInsertedOrUpdatedRow(entity);
            
            return identity;
        }

        private void RefreshEntityFromJustInsertedOrUpdatedRow(object entity)
        {
            var keyColumns = GetKeyColumns();
            if (keyColumns.IsNullOrEmpty()) return;
            var parameters = new DynamicParameters();
            var whereClause = keyColumns.Select(column => ProcessColumn(column, null, parameters, entity))
                .JoinString(" AND ");
            var sql = $"SELECT * FROM {TableName} WHERE {whereClause}";
            var results = Connection.Query<dynamic>(sql, parameters);
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

        public Query OrderBy(Query query, bool asc, params string[] sortColumns)
        {
            return asc? query.OrderBy(sortColumns) : query.OrderByDesc(sortColumns);
        }

        protected Query BuildQueryByProperties(PropertyInfo[] properties, object entity)
        {
            var whereDictionary = PropertiesToDictionary(properties, entity);
            return NewQuery().Where(whereDictionary);
        }

        protected virtual void DeleteByRawSql(object entity, Func<string, string> columnValueProvider)
        {
            var keyColumns = GetKeyColumns();

            var parameters = new DynamicParameters();
            var whereClause = keyColumns.Select(SimpleProcessColumn)
                .JoinString(" AND ");
            var sql = $"DELETE FROM {TableName} WHERE {whereClause}";

            Execute(sql, parameters);

            string SimpleProcessColumn(string column) =>
                ProcessColumn(column, columnValueProvider, parameters, entity);
        }

        public static Func<string, string> ProvideUtcNowForColumn(string columnName)
        {
            return column => columnName.Equals(column, StringComparison.InvariantCultureIgnoreCase)
                ? DbProvider.UtcNowExpression
                : null;
        }
    }

    public class BaseDal<TEntity> : BaseDal
        where TEntity: class, new()
    {
        protected static readonly TEntity Entity = new TEntity(); // To be used with nameof
        
        public BaseDal(string tableName): base(tableName)
        {
            EntityType = typeof(TEntity);
        }

        protected BaseDal() :
            this(GetTableName<TEntity>())
        {
        }

        public virtual TEntity Get<TKey>(TKey key)
        {
            var keyColumnName = GetKeyColumnName();
            var query = NewQuery().Where(keyColumnName, key);
            return Query(query).FirstOrDefault();
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
            return Connection.Query<TEntity>(sql, parameter);
        }

        protected virtual IEnumerable<TEntity> Query(Query query)
        {
            return Connection.Query<TEntity>(query);
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

        protected IEnumerable<TEntity> GetMaxItems(string column)
        {
            var maxQuerySql = NewQuery().AsMax(column).CompileResult().Sql;
            var query = NewQuery().WhereRaw($"{column} = ({maxQuerySql})");
            return Query(query);
        }

        protected IEnumerable<TEntity> GetMaxItems<TMember>(
            Expression<Func<TEntity, TMember>> memberExpression)
        {
            return GetMaxItems(GetColumnName(memberExpression));
        }

        protected TEntity GetMaxItem(string column)
        {
            return GetMaxItems(column).FirstOrDefault();
        }

        protected TEntity GetMaxItem<TMember>(
            Expression<Func<TEntity, TMember>> memberExpression)
        {
            return GetMaxItems(GetColumnName(memberExpression)).FirstOrDefault();
        }

        public virtual long Insert(TEntity entity)
        {
            return Insert(entity, columnValueProvider: null);
        }

        public virtual long Insert(TEntity entity, Func<string, string> columnValueProvider)
        {
            return InsertByRawSql(entity, columnValueProvider);
        }

        public virtual void InsertAll(IEnumerable<TEntity> entities, int batchSize = DefaultBatchSize,
            bool ignoreAutoIncrementColumns = true, bool enableMultithreading = true,
            params string[] columns)
        {
            InsertAll(entities, batchSize, ignoreAutoIncrementColumns, enableMultithreading, null, columns);
        }

        public virtual int InsertAll(IEnumerable<TEntity> entities, int batchSize,
            bool ignoreAutoIncrementColumns, bool enableMultithreading,
            Func<string, string> columnValueProvider,
            params string[] columns)
        {
            return base.InsertAll(entities, batchSize, ignoreAutoIncrementColumns, enableMultithreading, columnValueProvider: columnValueProvider, columns: columns);
        }

        public virtual void UpdateAll(IEnumerable<TEntity> entities, int batchSize = DefaultBatchSize,
            bool enableMultithreading = true,
            params string[] columnsToUpdate)
        {
            UpdateAll(entities, batchSize, enableMultithreading, columnValueProvider: null, columnsToUpdate: columnsToUpdate);
        }

        public virtual int UpdateAll(IEnumerable<TEntity> entities, int batchSize,
            bool enableMultithreading,
            Func<string, string> columnValueProvider,
            params string[] columnsToUpdate)
        {
            return base.UpdateAll(entities, batchSize, enableMultithreading, columnValueProvider, columnsToUpdate);
        }

        public virtual void DeleteAll(IEnumerable<TEntity> entities, int batchSize = DefaultBatchSize,
            bool enableMultithreading = true,
            params string[] critieraColumns)
        {
            DeleteAll(entities, batchSize, enableMultithreading, columnValueProvider: null, critieraColumns: critieraColumns);
        }

        public virtual int DeleteAll(IEnumerable<TEntity> entities, int batchSize,
            bool enableMultithreading,
            Func<string, string> columnValueProvider,
            params string[] critieraColumns)
        {
            return base.DeleteAll(entities, batchSize, enableMultithreading, columnValueProvider, critieraColumns);
        }

        public virtual void InsertAllByParams(params TEntity[] entities)
        {
            base.InsertAll(entities);
        }

        public virtual void Update(TEntity entity)
        {
            Update(entity, columnValueProvider: null);
        }

        public virtual void Update(TEntity entity, Func<string, string> columnValueProvider)
        {
            UpdateByRawSql(entity, columnValueProvider);
        }

        public virtual void Delete(TEntity entity)
        {
            Delete(entity, columnValueProvider: null);
        }

        public virtual void Delete(TEntity entity, Func<string, string> columnValueProvider)
        {
            DeleteByRawSql(entity, columnValueProvider);
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

        protected virtual void DeleteByInClause<TMember>(
            Expression<Func<TEntity, TMember>> memberExpression, IEnumerable<object> values)
        {
            DeleteByInClause(GetColumnName(memberExpression), values);
        }

        protected virtual IEnumerable<TEntity> QueryAndMapDynamic(Query query)
        {
            var dynamicResults = QueryDynamic(query);
            return AutoMapper.MapDynamic<TEntity>(dynamicResults).ToList();
        }
    }
}
