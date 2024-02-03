using System.Collections.Generic;

namespace Dakata;

public class SqlInfo(string sql, IDictionary<string, object> parameters)
{
    public string Sql { get;set; } = sql;
    public IDictionary<string, object> Parameters { get; set; } = parameters;
}