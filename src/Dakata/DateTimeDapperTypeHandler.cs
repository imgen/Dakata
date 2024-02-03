using System;
using System.Data;
using Dapper;

namespace Dakata;

public class DateTimeDapperTypeHandler : SqlMapper.TypeHandler<DateTime>
{
    public override void SetValue(IDbDataParameter parameter, DateTime value) => 
        parameter.Value = value;

    public override DateTime Parse(object value) => 
        DateTime.SpecifyKind((DateTime)value, DateTimeKind.Utc);
}