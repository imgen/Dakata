using System;
using Slapper;

namespace Dakata;

public class DateTimeAutoMapperTypeConverter : AutoMapper.Configuration.ITypeConverter
{
    /// <summary>
    /// Order to execute an <see cref="T:Slapper.AutoMapper.Configuration.ITypeConverter" /> in.
    /// </summary>
    public int Order => 1;

    /// <summary>Converts the given value to the requested type.</summary>
    /// <param name="value">Value to convert.</param>
    /// <param name="type">Type the value is to be converted to.</param>
    /// <returns>Converted value.</returns>
    public object Convert(object value, Type type)
    {
        object obj = null;
        if (value == null)
        {
            return null;
        }

        if (value is DateTime time)
            obj = new DateTime(time.Ticks, DateTimeKind.Utc);
        return obj;
    }

    /// <summary>
    /// Indicates whether it can convert the given value to the requested type.
    /// </summary>
    /// <param name="value">Value to convert.</param>
    /// <param name="type">Type the value needs to be converted to.</param>
    /// <returns>Boolean response.</returns>
    public bool CanConvert(object value, Type type)
    {
        return type == typeof(DateTime) || type == typeof(DateTime?);
    }
}