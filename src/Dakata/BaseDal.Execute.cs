using SqlKata;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Dakata
{
    public partial class BaseDal
    {
        protected virtual T ExecuteScalar<T>(Query query) => DapperConnection.ExecuteScalar<T>(query);
        protected virtual async Task<T> ExecuteScalarAsync<T>(Query query) => await DapperConnection.ExecuteScalarAsync<T>(query);
        protected virtual T ExecuteScalar<T>(string sql, object parameters) => DapperConnection.ExecuteScalar<T>(sql, parameters);
        protected virtual async Task<T> ExecuteScalarAsync<T>(string sql, object parameters) => await DapperConnection.ExecuteScalarAsync<T>(sql, parameters);

        protected virtual void Execute(Query query) => DapperConnection.Execute(query);
        protected virtual async Task ExecuteAsync(Query query) => await DapperConnection.ExecuteAsync(query);

        protected virtual void Execute(string sql, object parameters = null) => DapperConnection.Execute(sql, parameters);
        protected virtual async Task ExecuteAsync(string sql, object parameters = null) => await DapperConnection.ExecuteAsync(sql, parameters);

        protected virtual IEnumerable<dynamic> QueryDynamic(Query query) => DapperConnection.Query<dynamic>(query);
        protected virtual async Task<IEnumerable<dynamic>> QueryDynamicAsync(Query query) => await DapperConnection.QueryAsync<dynamic>(query);
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
