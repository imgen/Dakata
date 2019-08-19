using SqlKata;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dakata
{
    public partial class BaseDal
    {
        protected virtual T ExecuteScalar<T>(Query query) => DapperConnection.ExecuteScalar<T>(query);
        protected virtual T ExecuteScalar<T>(string sql, object parameters) => DapperConnection.ExecuteScalar<T>(sql, parameters);

        protected virtual void Execute(Query query) => DapperConnection.Execute(query);

        protected virtual void Execute(string sql, object parameters = null) => DapperConnection.Execute(sql, parameters);
    }
}
