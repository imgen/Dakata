using SqlKata;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace Dakata
{
    public partial class BaseDal
    {
        public virtual void Execute(Query query) => DapperConnection.Execute(query);
        public virtual async Task ExecuteAsync(Query query) => await DapperConnection.ExecuteAsync(query);

        public virtual T Execute<T>(Func<IDbConnection, T> func) => DapperConnection.Execute(func);
        public virtual async Task<T> ExecuteAsync<T>(Func<IDbConnection, Task<T>> func) => await DapperConnection.ExecuteAsync(func);

        public virtual void Execute(string sql, object parameters = null) => DapperConnection.Execute(sql, parameters);
        public virtual async Task ExecuteAsync(string sql, object parameters = null) => await DapperConnection.ExecuteAsync(sql, parameters);

        public virtual T ExecuteScalar<T>(Query query) => DapperConnection.ExecuteScalar<T>(query);
        public virtual async Task<T> ExecuteScalarAsync<T>(Query query) => await DapperConnection.ExecuteScalarAsync<T>(query);
        
        public virtual T ExecuteScalar<T>(string sql, object parameters) => DapperConnection.ExecuteScalar<T>(sql, parameters);
        public virtual async Task<T> ExecuteScalarAsync<T>(string sql, object parameters) => await DapperConnection.ExecuteScalarAsync<T>(sql, parameters);

        public virtual IEnumerable<dynamic> QueryDynamic(Query query) => DapperConnection.Query<dynamic>(query);
        public virtual async Task<IEnumerable<dynamic>> QueryDynamicAsync(Query query) => await DapperConnection.QueryAsync<dynamic>(query);
        
        public virtual IEnumerable<dynamic> QueryDynamic(string sql, object parameters) => DapperConnection.Query<dynamic>(sql, parameters);
        public virtual async Task<IEnumerable<dynamic>> QueryDynamicAsync(string sql, object parameters) => await DapperConnection.QueryAsync<dynamic>(sql, parameters);
    }

    public partial class BaseDal<TEntity>
    {
        public virtual IEnumerable<TEntity> Query(string sql, object parameter)
        {
            return DapperConnection.Query<TEntity>(sql, parameter);
        }

        public virtual async Task<IEnumerable<TEntity>> QueryAsync(string sql, object parameter)
        {
            return await DapperConnection.QueryAsync<TEntity>(sql, parameter);
        }

        public virtual IEnumerable<TEntity> Query(Query query)
        {
            return DapperConnection.Query<TEntity>(query);
        }

        public virtual async Task<IEnumerable<TEntity>> QueryAsync(Query query)
        {
            return await DapperConnection.QueryAsync<TEntity>(query);
        }
    }
}
