using Dapper.Contrib.Extensions;
using System;

namespace Dakata.Examples.Models;

[Table("Application.Cities")]
public class City
{
    [ExplicitKey]
    public string CityName { get; set; }
    [ExplicitKey]
    public int StateProvinceId { get; set; }
    public int? LatestRecordedPopulation { get; set; }
    public int LastEditedBy { get; set; }
    public DateTime ValidFrom { get; set; }
    public DateTime ValidTo { get; set; }
}