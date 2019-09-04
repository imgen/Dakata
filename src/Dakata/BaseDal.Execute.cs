using SqlKata;
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
    }
}
