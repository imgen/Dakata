using Dapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Dakata;

public partial class BaseDal
{
    public virtual void DeleteByParameters(object parameters, int? commandTimeout = null)
    {
        var query = NewQuery().Where(parameters.AsDictionary());
        Execute(query.AsDelete(), commandTimeout);
    }

    public virtual async Task DeleteByParametersAsync(object parameters, int? commandTimeout = null)
    {
        var query = NewQuery().Where(parameters.AsDictionary());
        await ExecuteAsync(query.AsDelete(), commandTimeout);
    }

    public virtual void DeleteByInClause(string column, IEnumerable<object> values, int? commandTimeout = null)
    {
        var query = NewQuery().WhereIn(column, values).AsDelete();
        Execute(query, commandTimeout);
    }

    public virtual async Task DeleteByInClauseAsync(string column, IEnumerable<object> values, int? commandTimeout = null)
    {
        var query = NewQuery().WhereIn(column, values).AsDelete();
        await ExecuteAsync(query, commandTimeout);
    }

    public virtual void DeleteById<TIdentity>(TIdentity identity, int? commandTimeout = null)
    {
        var keyProperty = GetKeyProperty();
        var query = NewQuery().Where(GetColumnName(keyProperty), identity).AsDelete();
        Execute(query, commandTimeout);
    }

    public virtual async Task DeleteByIdAsync<TIdentity>(TIdentity identity, int? commandTimeout = null)
    {
        var keyProperty = GetKeyProperty();
        var query = NewQuery().Where(GetColumnName(keyProperty), identity).AsDelete();
        await ExecuteAsync(query, commandTimeout);
    }

    private (string keyColumnName, object[] keyColumnValues) GetKeyColumnNameAndValues(IEnumerable<object> entities)
    {
        var keyProperty = GetKeyProperty();
        var keyColumnName = GetColumnName(keyProperty);
        var values = entities.Select(entity => keyProperty.GetValue(entity)).Distinct().ToArray();
        return (keyColumnName, values);
    }

    public virtual void DeleteByKeyColumn(IEnumerable<object> entities, int? commandTimeout = null)
    {
        var (keyColumnName, values) = GetKeyColumnNameAndValues(entities);
        DeleteByInClause(keyColumnName, values, commandTimeout);
    }

    public virtual async Task DeleteByKeyColumnAsync(IEnumerable<object> entities, int? commandTimeout = null)
    {
        var (keyColumnName, values) = GetKeyColumnNameAndValues(entities);
        await DeleteByInClauseAsync(keyColumnName, values, commandTimeout);
    }

    private string DeleteAllStatement => $"DELETE FROM {TableName}";

    public virtual void DeleteAll(int? commandTimeout = null)
    {
        Execute(DeleteAllStatement, commandTimeout);
    }

    // Based on SO answer https://stackoverflow.com/a/36257723/915147
    public virtual int DeleteAll(IEnumerable<object> entities,
        int batchSize = DefaultBatchSize,
        bool parallel = false,
        Func<string, string> columnValueProvider = null,
        int? commandTimeout = null,
        params string[] criteriaColumns)
    {
        var (newCriteriaColumns, newBatchSize, batches, tempTableName) =
            PrepareParametersForDeleteAll(entities, batchSize, criteriaColumns);
        if (parallel)
        {
            Parallel.ForEach(batches, batch =>
            {
                DeleteAll(newCriteriaColumns, batch, tempTableName, columnValueProvider, commandTimeout);
            });
        }
        else
        {
            batches.ForEach(batch =>
            {
                DeleteAll(newCriteriaColumns, batch, tempTableName, columnValueProvider, commandTimeout);
            });
        }

        return newBatchSize;
    }

    public virtual async Task DeleteAllAsync(int? commandTimeout = null)
    {
        await ExecuteAsync(DeleteAllStatement, commandTimeout);
    }

    // Based on SO answer https://stackoverflow.com/a/36257723/915147
    public virtual async Task<int> DeleteAllAsync(IEnumerable<object> entities,
        int batchSize = DefaultBatchSize,
        bool parallel = false,
        Func<string, string> columnValueProvider = null,
        int? commandTimeout = null,
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
                            columnValueProvider,
                            commandTimeout)
                )
                .ToArray();
            await Task.WhenAll(deleteAllTasks);
        }
        else
        {
            foreach (var batch in batches)
            {
                await DeleteAllAsync(newCriteriaColumns,
                    batch,
                    tempTableName,
                    columnValueProvider,
                    commandTimeout);
            }
        }

        return newBatchSize;
    }

    private string TruncateStatement => $"TRUNCATE TABLE {TableName}";

    public virtual void Truncate(int? commandTimeout = null)
    {
        Execute(TruncateStatement, commandTimeout);
    }

    public virtual async Task TruncateAsync(int? commandTimeout = null)
    {
        await ExecuteAsync(TruncateStatement, commandTimeout);
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
            throw new NotImplementedException();
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

    public virtual void DeleteByRawSql(object entity, Func<string, string> columnValueProvider, int? commandTimeout = null)
    {
        var (sql, parameters) = GetDeleteSql(entity, columnValueProvider);
        Execute(sql, parameters, commandTimeout);
    }

    public virtual async Task DeleteByRawSqlAsync(object entity, Func<string, string> columnValueProvider, int? commandTimeout = null)
    {
        var (sql, parameters) = GetDeleteSql(entity, columnValueProvider);
        await ExecuteAsync(sql, parameters, commandTimeout);
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
        Func<string, string> columnValueProvider, 
        int? commandTimeout = null)
    {
        var (sql, parameters) = BuildDeleteAllQuery(criteriaColumns, batch, tempTableName, columnValueProvider);
        Execute(sql, parameters, commandTimeout);
    }

    private async Task DeleteAllAsync(string[] criteriaColumns, 
        IEnumerable<object> batch, string tempTableName,
        Func<string, string> columnValueProvider, 
        int? commandTimeout = null)
    {
        var (sql, parameters) = BuildDeleteAllQuery(criteriaColumns, batch, tempTableName, columnValueProvider);
        await ExecuteAsync(sql, parameters, commandTimeout);
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
        int? commandTimeout = null,
        params string[] criteriaColumns)
    {
        return base.DeleteAll(entities, batchSize, parallel, columnValueProvider, commandTimeout, criteriaColumns);
    }

    public virtual async Task<int> DeleteAllAsync(IEnumerable<TEntity> entities,
        int batchSize = DefaultBatchSize,
        bool parallel = false,
        Func<string, string> columnValueProvider = null,
        int? commandTimeout = null,
        params string[] criteriaColumns)
    {
        return await base.DeleteAllAsync(entities, batchSize, parallel, columnValueProvider, commandTimeout, criteriaColumns);
    }

    public virtual void DeleteByInClause<TMember>(Expression<Func<TEntity, TMember>> memberExpression,
        IEnumerable<object> values,
        int? commandTimeout = null)
    {
        DeleteByInClause(GetColumnName(memberExpression), values, commandTimeout);
    }

    public virtual async Task DeleteByInClauseAsync<TMember>(Expression<Func<TEntity, TMember>> memberExpression,
        IEnumerable<object> values,
        int? commandTimeout = null)
    {
        await DeleteByInClauseAsync(GetColumnName(memberExpression), values, commandTimeout);
    }
}