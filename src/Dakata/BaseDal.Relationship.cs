using SqlKata;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using static Dakata.DbUtils;

namespace Dakata
{
    public partial class BaseDal
    {
        public Query JoinTable(
            Func<string, Func<Join, Join>, Query> joiner, 
            string joinTableName, string joinTableColumnName, 
            string baseTableColumnName = null, 
            string baseTableName = null)
        {
            baseTableName = baseTableName ?? TableName;
            baseTableColumnName = baseTableColumnName ?? joinTableName + joinTableColumnName;
            return joiner(joinTableName,
                join => join.On(AddTablePrefix(joinTableColumnName, joinTableName), AddTablePrefix(baseTableColumnName, baseTableName))
            );
        }

        public Query InnerJoinTable(Query query, 
            string joinTableName, 
            string joinTableColumnName, 
            string baseTableColumnName = null, 
            string baseTableName = null)
        {
            return JoinTable((joinTableName2, join) => query.Join(joinTableName, join),
                joinTableName,
                joinTableColumnName,
                baseTableColumnName,
                baseTableName);
        }

        public Query InnerJoinTable<TJoinEntity>(Query query, 
            string joinTableColumnName, 
            string baseTableColumnName = null, 
            string baseTableName = null)
        {
            var joinTableName = GetTableName<TJoinEntity>();
            return InnerJoinTable(query, joinTableName, joinTableColumnName, baseTableColumnName, baseTableName);
        }

        public Query LeftJoinTable(Query query, 
            string joinTableName, 
            string joinTableColumnName, 
            string baseTableColumnName = null, 
            string baseTableName = null)
        {
            return JoinTable(query.LeftJoin, joinTableName, joinTableColumnName, baseTableColumnName, baseTableName);
        }

        public Query LeftJoinTable<TJoinEntity>(Query query, 
            string joinTableColumnName, 
            string baseTableColumnName = null, 
            string baseTableName = null)
        {
            var joinTableName = GetTableName<TJoinEntity>();
            return LeftJoinTable(query, joinTableName, joinTableColumnName, baseTableColumnName, baseTableName);
        }

        public Query LeftInclude(Query query,
            Type joinEntityType,
            string selectPrefix,
            string joinTableColumnName,
            string baseTableColumnName = null,
            string baseTableName = null)
        {
            var joinTableName = GetTableName(joinEntityType);
            LeftJoinTable(query, joinTableName, joinTableColumnName,
                baseTableColumnName, baseTableName);
            
            var baseTableColumnSelections = 
                GetColumnSelections(tableName: baseTableName);
            var baseTableColumnNames = baseTableColumnSelections
                    .Select(s => (selection: s, column: GetColumnExpressionFromSelectClause(s)))
                    .ToArray();
            var clauses = query.Clauses;
            var columnsInQuery = clauses.OfType<Column>().ToArray();
            var columnNamesInQuery = columnsInQuery
                .Select(x => GetColumnExpressionFromSelectClause(x.Name)).ToArray();

            baseTableColumnSelections = baseTableColumnNames
                .Where(c => !columnNamesInQuery
                            .Any(c2 => 
                                c2.Equals(c.column, StringComparison.InvariantCultureIgnoreCase))
                            )
                .Select(c => c.selection)
                .ToArray();
            
            if (baseTableColumnSelections.Any())
            {
                query.Select(baseTableColumnSelections);
            }
            var joinTableColumnSelections =
                GetColumnSelections(selectPrefix,
                    joinTableName,
                    joinEntityType);
            return query.Select(joinTableColumnSelections);
        }

        public Query LeftInclude<TJoinEntity>(Query query,
            string selectPrefix,
            string joinTableColumnName,
            string baseTableColumnName = null,
            string baseTableName = null)
        {
            return LeftInclude(query, 
                typeof(TJoinEntity),
                selectPrefix,
                joinTableColumnName,
                baseTableColumnName,
                baseTableName);
        }

        public Query LeftInclude<TBaseEntity, 
            TJoinEntity, TJoinProperty>(Query query,
            string selectPrefix,
            Expression<Func<TJoinEntity, TJoinProperty>> joinProperty,
            Expression<Func<TBaseEntity, TJoinProperty>> baseProperty)
        {
            var joinTableColumnName = GetColumnName(joinProperty);
            var baseTableColumnName = GetColumnName(baseProperty);
            var baseTableName = GetTableName<TBaseEntity>();
            return LeftInclude<TJoinEntity>(query,
                selectPrefix,
                joinTableColumnName,
                baseTableColumnName,
                baseTableName);
        }

        public Query LeftInclude<TBaseEntity, 
            TJoinEntity, 
            TJoinProperty>(Query query,
            Expression<Func<TBaseEntity, TJoinEntity>> includeProperty,
            Expression<Func<TJoinEntity, TJoinProperty>> joinProperty,
            Expression<Func<TBaseEntity, TJoinProperty>> baseProperty)
        {
            var selectPrefix = includeProperty.GetFullPropertyName();
            return LeftInclude(query,
                selectPrefix,
                joinProperty,
                baseProperty);
        }

        public Query LeftIncludeList<TBaseEntity,
            TJoinEntity,
            TJoinProperty>(Query query,
            Expression<Func<TBaseEntity, List<TJoinEntity>>> includeProperty,
            Expression<Func<TJoinEntity, TJoinProperty>> joinProperty,
            Expression<Func<TBaseEntity, TJoinProperty>> baseProperty)
        {
            var selectPrefix = includeProperty.GetFullPropertyName();
            return LeftInclude(query,
                selectPrefix,
                joinProperty,
                baseProperty);
        }

        /// <summary>
        /// Left include an entity
        /// </summary>
        /// <typeparam name="TBaseEntity">The base entity type</typeparam>
        /// <typeparam name="TJoinEntity">The join entity type</typeparam>
        /// <param name="query">The query</param>
        /// <param name="selectPrefix">The select prefix</param>
        /// <param name="joinExpression">The expression to specify join properties,
        ///     should be something like 
        ///         () => baseEntity.JoinProperty = joinEntity.Poperty</param>
        /// <returns></returns>
        public Query LeftInclude<TBaseEntity, TJoinEntity>(Query query,
            string selectPrefix,
            Expression<Func<TBaseEntity, TJoinEntity, bool>> joinExpression)
        {
            return query;
        }

        /// <summary>
        /// Left include an entity
        /// </summary>
        /// <typeparam name="TBaseEntity">The base entity type</typeparam>
        /// <typeparam name="TJoinEntity">The join entity type</typeparam>
        /// <param name="query">The query</param>
        /// <param name="joinExpression">The expression to specify the include property 
        ///     and join properties, should be something like 
        ///         () => baseEntity.IncludeProperty.JoinProperty = baseEntity.Poperty</param>
        /// <returns></returns>
        public Query LeftInclude<TBaseEntity, TJoinEntity>(Query query,
            Expression<Func<TBaseEntity, bool>> joinExpression)
        {
            return query;
        }
    }

    public partial class BaseDal<TEntity>
    {
        public Query LeftInclude<TJoinEntity, TJoinProperty>(Query query,
            string selectPrefix,
            Expression<Func<TJoinEntity, TJoinProperty>> joinProperty,
            Expression<Func<TEntity, TJoinProperty>> baseProperty)
        {
            return base.LeftInclude
                (query, selectPrefix, joinProperty, baseProperty);
        }

        public Query LeftInclude<
            TJoinEntity,
            TJoinProperty>(Query query,
            Expression<Func<TEntity, TJoinEntity>> includeProperty,
            Expression<Func<TJoinEntity, TJoinProperty>> joinProperty,
            Expression<Func<TEntity, TJoinProperty>> baseProperty)
        {
            return base.LeftInclude(query,
                includeProperty,
                joinProperty,
                baseProperty);
        }

        public Query LeftIncludeList<
            TJoinEntity,
            TJoinProperty>(Query query,
            Expression<Func<TEntity, List<TJoinEntity>>> includeProperty,
            Expression<Func<TJoinEntity, TJoinProperty>> joinProperty,
            Expression<Func<TEntity, TJoinProperty>> baseProperty)
        {
            return base.LeftIncludeList(query,
                includeProperty,
                joinProperty,
                baseProperty);
        }
    }
}
