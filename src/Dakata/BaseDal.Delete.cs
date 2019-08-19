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

        public virtual void DeleteAll()
        {
            Execute($"DELETE FROM {TableName}");
        }

        public virtual void Truncate()
        {
            Execute($"TRUNCATE TABLE {TableName}");
        }

        // Based on SO answer https://stackoverflow.com/a/36257723/915147
        protected virtual int DeleteAll(IEnumerable<object> entities,
            int batchSize = DefaultBatchSize,
            bool parallel = false,
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

            if (parallel)
            {
                Parallel.ForEach(entities.Batch(batchSize), batch =>
                {
                    DeleteAll(criteriaColumns, batch, tempTableName, columnValueProvider);
                });
            }
            else
            {
                entities.Batch(batchSize).ForEach(batch =>
                {
                    DeleteAll(criteriaColumns, batch, tempTableName, columnValueProvider);
                });
            }

            return batchSize;
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

        private void DeleteAll(string[] criteriaColumns, IEnumerable<object> batch, string tempTableName,
            Func<string, string> columnValueProvider)
        {
            var sql = $@"DELETE {TableName} FROM {TableName} INNER JOIN (VALUES ";
            var (valueClauses, parameters) = BuildValueClauses(batch, columnValueProvider, criteriaColumns);
            sql += valueClauses.JoinString(",");
            sql += $@")
 AS {tempTableName} ({criteriaColumns.JoinString(",")})
ON {criteriaColumns.Select(column => $"{AddTablePrefix(column)} = {tempTableName}.{column}").JoinString(" AND ")}";

            Execute(sql, parameters);
        }
    }

    public partial class BaseDal<TEntity>
    {
        public virtual void Delete(TEntity entity, Func<string, string> columnValueProvider = null)
        {
            DeleteByRawSql(entity, columnValueProvider);
        }

        public virtual int DeleteAll(IEnumerable<TEntity> entities,
            int batchSize = DefaultBatchSize,
            bool parallel = false,
            Func<string, string> columnValueProvider = null,
            params string[] criteriaColumns)
        {
            return base.DeleteAll(entities, batchSize, parallel, columnValueProvider, criteriaColumns);
        }

        protected virtual void DeleteByInClause<TMember>(Expression<Func<TEntity, TMember>> memberExpression, IEnumerable<object> values)
        {
            DeleteByInClause(GetColumnName(memberExpression), values);
        }
    }
}
