using Dapper.Contrib.Extensions;
using SqlKata;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using static Dakata.DbUtils;

namespace Dakata;

public partial class BaseDal
{
    public BaseDal JoinTable(
        Func<string, Func<Join, Join>, Query> joiner,
        string joinTableName, string joinTableColumnName,
        string baseTableColumnName = null,
        string baseTableName = null)
    {
        baseTableName ??= TableName;
        baseTableColumnName ??= joinTableName + joinTableColumnName;
        joiner(joinTableName,
            join => join.On(AddTablePrefix(joinTableColumnName, joinTableName), AddTablePrefix(baseTableColumnName, baseTableName))
        );
        return this;
    }

    public BaseDal InnerJoinTable(Query query,
        string joinTableName,
        string joinTableColumnName,
        string baseTableColumnName = null,
        string baseTableName = null)
    {
        return JoinTable((_, join) => query.Join(joinTableName, join),
            joinTableName,
            joinTableColumnName,
            baseTableColumnName,
            baseTableName);
    }

    public BaseDal InnerJoinTable<TJoinEntity>(Query query,
        string joinTableColumnName,
        string baseTableColumnName = null,
        string baseTableName = null)
    {
        var joinTableName = GetTableName<TJoinEntity>();
        return InnerJoinTable(query, joinTableName, joinTableColumnName, baseTableColumnName, baseTableName);
    }

    public BaseDal LeftJoinTable(Query query,
        string joinTableName,
        string joinTableColumnName,
        string baseTableColumnName = null,
        string baseTableName = null)
    {
        return JoinTable(query.LeftJoin, joinTableName, joinTableColumnName, baseTableColumnName, baseTableName);
    }

    public BaseDal LeftJoinTable<TJoinEntity>(Query query,
        string joinTableColumnName,
        string baseTableColumnName = null,
        string baseTableName = null)
    {
        var joinTableName = GetTableName<TJoinEntity>();
        return LeftJoinTable(query, joinTableName, joinTableColumnName, baseTableColumnName, baseTableName);
    }

    public BaseDal Include(Query query,
        Type baseEntityType,
        Type joinEntityType,
        string selectPrefix,
        string joinTableColumnName,
        string baseTableColumnName = null,
        bool useLeftJoin = false)
    {
        var baseTableName = GetTableName(baseEntityType);
        var joinTableName = GetTableName(joinEntityType);
        if (useLeftJoin)
        {
            LeftJoinTable(query,
                joinTableName, joinTableColumnName,
                baseTableColumnName, baseTableName);
        }
        else
        {
            InnerJoinTable(query,
                joinTableName, joinTableColumnName,
                baseTableColumnName, baseTableName);
        }

        var baseTableColumnSelections =
            GetColumnSelections(tableName: baseTableName, entityType: baseEntityType);
        var baseTableColumnNames = baseTableColumnSelections
            .Select(s => (selection: s, column: GetColumnExpressionFromSelectClause(s)))
            .ToArray();
        var clauses = query.Clauses;
        var columnsInQuery = clauses.OfType<Column>().ToArray();
        var columnNamesInQuery = columnsInQuery
            .Select(x => GetColumnExpressionFromSelectClause(x.Name)).ToArray();

        baseTableColumnSelections = baseTableColumnNames
            .Where(c => !Array.Exists(
                    columnNamesInQuery,
                    c2 =>
                        c2.Equals(c.column, StringComparison.InvariantCultureIgnoreCase)
                )
            )
            .Select(c => c.selection)
            .ToArray();

        if (baseTableColumnSelections.Any()) 
            query.Select(baseTableColumnSelections);
        var joinTableColumnSelections =
            GetColumnSelections(selectPrefix,
                joinTableName,
                joinEntityType);
        query.Select(joinTableColumnSelections);
        return this;
    }

    public BaseDal Include<TBaseEntity, TJoinEntity>(Query query,
        string selectPrefix,
        string joinTableColumnName,
        string baseTableColumnName = null,
        bool useLeftJoin = false) =>
        Include(query,
            typeof(TBaseEntity),
            typeof(TJoinEntity),
            selectPrefix,
            joinTableColumnName,
            baseTableColumnName,
            useLeftJoin);

    public BaseDal Include<TBaseEntity,
        TJoinEntity, TJoinProperty>(Query query,
        string selectPrefix,
        Expression<Func<TJoinEntity, TJoinProperty>> joinProperty,
        Expression<Func<TBaseEntity, TJoinProperty>> baseProperty,
        bool useLeftJoin = false)
    {
        var joinTableColumnName = GetColumnName(joinProperty);
        var baseTableColumnName = GetColumnName(baseProperty);
        return Include(query,
            baseEntityType: typeof(TBaseEntity),
            joinEntityType: typeof(TJoinEntity),
            selectPrefix,
            joinTableColumnName,
            baseTableColumnName,
            useLeftJoin);
    }

    public BaseDal Include<TBaseEntity,
        TJoinEntity,
        TJoinProperty>(Query query,
        Expression<Func<TBaseEntity, TJoinEntity>> navigationProperty,
        Expression<Func<TJoinEntity, TJoinProperty>> joinProperty,
        Expression<Func<TBaseEntity, TJoinProperty>> baseProperty,
        bool useLeftJoin = false)
    {
        var selectPrefix = navigationProperty.GetFullPropertyName();
        return Include(query,
            selectPrefix,
            joinProperty,
            baseProperty,
            useLeftJoin);
    }

    /// <summary>
    /// Include an entity
    /// </summary>
    /// <typeparam name="TBaseEntity">The base entity type</typeparam>
    /// <typeparam name="TJoinEntity">The join entity type</typeparam>
    /// <param name="query">The query</param>
    /// <param name="selectPrefix">The select prefix</param>
    /// <param name="joinExpression">The expression to specify join properties,
    ///     should be something like 
    ///         () => baseEntity.BaseJoinColumn = joinEntity.JoinColumn</param>
    /// <param name="useLeftJoin">Indicate whether to use left join</param>
    /// <returns></returns>
    public BaseDal Include<TBaseEntity, TJoinEntity>(Query query,
        Expression<Func<TBaseEntity, TJoinEntity, bool>> joinExpression,
        string selectPrefix,
        bool useLeftJoin = false)
    {
        var (leftSideProps, rightSideProps, _) = new IncludeVisitor()
            .VisitEqualExpression(joinExpression.Body as BinaryExpression);
        var baseTableColumnName = GetColumnName(leftSideProps[^1]);
        var joinTableColumnName = GetColumnName(rightSideProps[^1]);
        return Include(query,
            baseEntityType: typeof(TBaseEntity),
            joinEntityType: typeof(TJoinEntity),
            selectPrefix: selectPrefix,
            joinTableColumnName: joinTableColumnName,
            baseTableColumnName: baseTableColumnName,
            useLeftJoin: useLeftJoin);
    }

    /// <summary>
    /// Include an entity
    /// </summary>
    /// <typeparam name="TBaseEntity">The base entity type</typeparam>
    /// <param name="query">The query</param>
    /// <param name="joinExpression">The expression to specify the include property 
    ///     and join properties, should be something like 
    ///         baseEntity => baseEntity.NavigationProperty.JoinProperty = baseEntity.Poperty</param>
    /// <param name="selectPrefix">The select prefix</param>
    /// <param name="useLeftJoin">Indicate whether to use left join</param>
    /// <returns></returns>
    public BaseDal Include<TBaseEntity>(Query query,
        Expression<Func<TBaseEntity, bool>> joinExpression,
        string selectPrefix = "",
        bool useLeftJoin = false)
    {
        var equalExpressions = new MultipleIncludeVisitor()
            .GetEqualExpressions(joinExpression.Body as BinaryExpression);
        foreach (var _ in equalExpressions)
        {
            var (leftSideProps, rightSideProps, navigationProperty) = new IncludeVisitor()
                .VisitEqualExpression(joinExpression.Body as BinaryExpression);
            if (leftSideProps.Contains(navigationProperty))
            {
                // Navigate property is on the left side, swap to make sure the subsequent code works
                (leftSideProps, rightSideProps) = (rightSideProps, leftSideProps);
            }
            var lastRightSideProp = rightSideProps[^1];
            var baseTableColumnName = GetColumnName(leftSideProps[^1]);
            var joinTableColumnName = GetColumnName(lastRightSideProp);
            var prefixPropName = navigationProperty.Name;
            selectPrefix = selectPrefix.IsNullOrEmpty() ?
                prefixPropName :
                $"{selectPrefix}_{prefixPropName}";
            Include(query,
                baseEntityType: typeof(TBaseEntity),
                joinEntityType: lastRightSideProp.DeclaringType,
                selectPrefix: selectPrefix,
                joinTableColumnName: joinTableColumnName,
                baseTableColumnName: baseTableColumnName,
                useLeftJoin: useLeftJoin);
        }

        return this;
    }

    public BaseDal IncludeList<TBaseEntity, TJoinEntity, TJoinProperty>(Query query,
        Expression<Func<TBaseEntity, List<TJoinEntity>>> navigationProperty,
        Expression<Func<TJoinEntity, TJoinProperty>> joinProperty,
        Expression<Func<TBaseEntity, TJoinProperty>> baseProperty,
        bool useLeftJoin = false)
    {
        var selectPrefix = navigationProperty.GetFullPropertyName();
        return Include(query,
            selectPrefix,
            joinProperty,
            baseProperty,
            useLeftJoin);
    }

    /// <summary>
    /// Include an entity
    /// </summary>
    /// <typeparam name="TBaseEntity">The base entity type</typeparam>
    /// <typeparam name="TFirstJoinEntity">The first join entity type</typeparam>
    /// <param name="query">The query</param>
    /// <param name="joinExpression">The expression to specify the include property 
    ///     and join properties, should be something like 
    ///         baseEntity => baseEntity.NavigationProperty.JoinProperty = baseEntity.Property</param>
    /// <param name="useLeftJoin">Indicate whether to use left join</param>
    /// <returns></returns>
    public BaseDal MultipleInclude
        <TBaseEntity, TFirstJoinEntity>(
            Query query,
            Expression<Func<TBaseEntity, TFirstJoinEntity, bool>> joinExpression,
            bool useLeftJoin = false)
    {
        var visitor = new MultipleIncludeVisitor();
        var equalExpressions = visitor.GetEqualExpressions(joinExpression.Body as BinaryExpression);
        var firstEqualExpression = equalExpressions[0];
        var firstFunc = Expression.Lambda<Func<TBaseEntity, bool>>(firstEqualExpression,
            joinExpression.Parameters[0]);
        Include(query, firstFunc, useLeftJoin: useLeftJoin);
        var navigationProperty = visitor.GetNavigationProperty(firstEqualExpression);
        var secondEqualExpression = equalExpressions[1];
        var secondFunc = Expression.Lambda<Func<TFirstJoinEntity, bool>>(secondEqualExpression,
            joinExpression.Parameters[1]);
        return Include(query, secondFunc, navigationProperty.Name, useLeftJoin);
    }

    private sealed class MultipleIncludeVisitor : ExpressionVisitor
    {
        private readonly List<BinaryExpression> _equalExpressions = new();
#pragma warning disable S1450
        private PropertyInfo _navigationProperty;
#pragma warning restore S1450

        protected override Expression VisitBinary(BinaryExpression node)
        {
            if (node.NodeType == ExpressionType.Equal) 
                _equalExpressions.Add(node);
            return base.VisitBinary(node);
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Member is PropertyInfo property &&
                Attribute.IsDefined(property, typeof(ComputedAttribute))) 
                _navigationProperty = property;
            return base.VisitMember(node);
        }

        public BinaryExpression[] GetEqualExpressions(BinaryExpression binaryExpression)
        {
            if (binaryExpression.NodeType == ExpressionType.Equal)
                return [binaryExpression];
            if (binaryExpression.NodeType != ExpressionType.AndAlso)
                return [];
            _equalExpressions.Clear();
            Visit(binaryExpression);
            return [.. _equalExpressions];
        }

        public PropertyInfo GetNavigationProperty(BinaryExpression binaryExpression)
        {
            if (binaryExpression.NodeType != ExpressionType.Equal)
                return default;
            _navigationProperty = null;
            Visit(binaryExpression);
            return _navigationProperty;
        }
    }

    private sealed class IncludeVisitor : ExpressionVisitor
    {
        private readonly Stack<PropertyInfo> _leftSideProperties = new(),
            _rightSideProperties = new();
        private bool _isLeftSideParsingFinished;
#pragma warning disable S1450
        private PropertyInfo _navigationProperty;
#pragma warning restore S1450

        protected override Expression VisitMember(MemberExpression node)
        {
            var member = node.Member;
            if (member is not PropertyInfo property) 
                return base.VisitMember(node);
            var propertyNames = _isLeftSideParsingFinished ? _rightSideProperties : _leftSideProperties;
            propertyNames.Push(property);
            if (Attribute.IsDefined(property, typeof(ComputedAttribute))) 
                _navigationProperty = property;
            return base.VisitMember(node);
        }

        public (PropertyInfo[] leftSideProperties, PropertyInfo[] rightSideProperties, PropertyInfo navigationProperty)
            VisitEqualExpression(BinaryExpression node)
        {
            if (node.NodeType != ExpressionType.Equal)
                return default;
            _isLeftSideParsingFinished = false;
            _leftSideProperties.Clear();
            _rightSideProperties.Clear();
            _navigationProperty = null;
            Visit(node.Left);
            _isLeftSideParsingFinished = true;
            Visit(node.Right);
            return (_leftSideProperties.ToArray(), _rightSideProperties.ToArray(), _navigationProperty);
        }
    }
}

public partial class BaseDal<TEntity>
{
    public BaseDal<TEntity> Include<TJoinEntity, TJoinProperty>(Query query,
        string selectPrefix,
        Expression<Func<TJoinEntity, TJoinProperty>> joinProperty,
        Expression<Func<TEntity, TJoinProperty>> baseProperty,
        bool useLeftJoin = false)
    {
        base.Include
        (query,
            selectPrefix,
            joinProperty,
            baseProperty,
            useLeftJoin);
        return this;
    }

    public BaseDal<TEntity> Include<
        TJoinEntity,
        TJoinProperty>(Query query,
        Expression<Func<TEntity, TJoinEntity>> navigationProperty,
        Expression<Func<TJoinEntity, TJoinProperty>> joinProperty,
        Expression<Func<TEntity, TJoinProperty>> baseProperty,
        bool useLeftJoin = false)
    {
        base.Include(query,
            navigationProperty,
            joinProperty,
            baseProperty,
            useLeftJoin);
        return this;
    }

    public BaseDal<TEntity> Include(Query query,
        Expression<Func<TEntity, bool>> joinExpression,
        bool useLeftJoin = false)
    {
        base.Include(query, joinExpression, useLeftJoin: useLeftJoin);
        return this;
    }

    public BaseDal<TEntity> Include<TJoinEntity>(Query query,
        Expression<Func<TEntity, TJoinEntity, bool>> joinExpression,
        string selectPrefix,
        bool useLeftJoin = false)
    {
        Include<TEntity, TJoinEntity>(query, joinExpression, selectPrefix, useLeftJoin: useLeftJoin);
        return this;
    }

    public BaseDal<TEntity> IncludeList<
        TJoinEntity,
        TJoinProperty>(Query query,
        Expression<Func<TEntity, List<TJoinEntity>>> navigationProperty,
        Expression<Func<TJoinEntity, TJoinProperty>> joinProperty,
        Expression<Func<TEntity, TJoinProperty>> baseProperty,
        bool useLeftJoin = false)
    {
        base.IncludeList(query,
            navigationProperty,
            joinProperty,
            baseProperty,
            useLeftJoin);
        return this;
    }

    public BaseDal<TEntity> MultipleInclude
        <TJoinEntity>(
            Query query,
            Expression<Func<TEntity, TJoinEntity, bool>> joinExpression,
            bool useLeftJoin = false)
    {
        MultipleInclude<TEntity, TJoinEntity>(
            query,
            joinExpression,
            useLeftJoin
        );
        return this;
    }
}