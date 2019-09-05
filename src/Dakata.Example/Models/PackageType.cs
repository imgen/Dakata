using Dapper.ColumnMapper;
using Dapper.Contrib.Extensions;
using System;

namespace Dakata.Example.Models
{
    [Table("Warehouse.PackageTypes")]
    public class PackageType
    {
        [Key, ColumnMapping("PackageTypeID")]
        public int ID { get; set; }
        public string PackageTypeName { get; set; }
        public int LastEditedBy { get; set; }
        public DateTime ValidFrom { get; set; }
        public DateTime ValidTo { get; set; }
    }
}
