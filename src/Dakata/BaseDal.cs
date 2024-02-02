using System;
using static Dakata.DbUtils;

namespace Dakata
{
    public partial class BaseDal
    {
        protected readonly DapperConnection DapperConnection;

        public const int DefaultBatchSize = 100;
        public virtual string TableName { get; }

        public Action<SqlInfo> Logger { get; set; } = _ => { };

        public BaseDal(string tableName, DapperConnection dapperConnection, 
            Action<SqlInfo> logger = null)
        {
            TableName = tableName;
            DapperConnection = dapperConnection;
            DbProvider = dapperConnection.DbProvider;
            Logger = logger?? Logger;
            dapperConnection.Logger = Logger;
        }

        public IDbProvider DbProvider { get; }

        protected DbEngines DbEngine => DbProvider.DbEngine;

        protected bool IsOracle => DbEngine == DbEngines.Oracle;
        protected bool IsSqlServer => DbEngine == DbEngines.SqlServer;

        protected string ParameterPrefix => IsOracle ? ":" : "@";

        public string UtcNowExpression => DbProvider.UtcNowExpression;

        protected Type EntityType;

        public int MaxParameterCount => DbProvider.MaxParameterCount;
    }

    public partial class BaseDal<TEntity> : BaseDal
        where TEntity: class
    {
        protected static readonly TEntity Entity = default; // To be used with nameof such as nameof(Entity.PropName), etc
        
        public BaseDal(string tableName, 
            DapperConnection dapperConnection, 
            Action<SqlInfo> logger = null): base(tableName, dapperConnection, logger)
        {
            EntityType = typeof(TEntity);
        }

        public BaseDal(DapperConnection dapperConnection, Action<SqlInfo> logger = null) :
            this(GetTableName<TEntity>(), dapperConnection, logger)
        {
        }
    }
}
