using Dapper.ColumnMapper;
using Dapper.Contrib.Extensions;
using System;

namespace Dakata.Examples.Models;

[Table("Warehouse.PackageTypes")]
public class PackageType
{
    [Key, ColumnMapping("PackageTypeId")]
    public int Id { get; set; }
    public string PackageTypeName { get; set; }
    public int LastEditedBy { get; set; }
    public DateTime ValidFrom { get; set; }
    public DateTime ValidTo { get; set; }
}