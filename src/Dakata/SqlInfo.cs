using System.Collections.Generic;

namespace Dakata
{
    public class SqlInfo
    {
        public string Sql { get;set; }
        public IDictionary<string, object> Parameters { get; set; }

        public SqlInfo(string sql, IDictionary<string, object> parameters)
        {
            Sql = sql;
            Parameters = parameters;
        }
    }
}
