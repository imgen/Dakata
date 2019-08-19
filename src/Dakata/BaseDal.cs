using System;
using static Dakata.DbUtils;

namespace Dakata
{
    public partial class BaseDal
    {
        protected readonly DapperConnection DapperConnection;

        public const int DefaultBatchSize = 100;
        public virtual string TableName { get; }

        public BaseDal(string tableName, DapperConnection dapperConnection)
        {
            TableName = tableName;
            DapperConnection = dapperConnection;
            DbProvider = dapperConnection.DbProvider;
        }

        public IDbProvider DbProvider;

        protected DbEngines DbEngine => DbProvider.DbEngine;

        protected bool IsOracle => DbEngine == DbEngines.Oracle;
        protected bool IsSqlServer => DbEngine == DbEngines.SqlServer;

        protected string ParameterPrefix => IsOracle ? ":" : "@";

        protected Type EntityType;

        public int MaxParameterCount => DbProvider.MaxParameterCount;
    }

    public partial class BaseDal<TEntity> : BaseDal
        where TEntity: class
    {
        protected static readonly TEntity Entity = default; // To be used with nameof such as nameof(Entity.PropName), etc
        
        public BaseDal(string tableName, DapperConnection dapperConnection): base(tableName, dapperConnection)
        {
            EntityType = typeof(TEntity);
        }

        protected BaseDal(DapperConnection dapperConnection) :
            this(GetTableName<TEntity>(), dapperConnection)
        {
        }
    }
}
