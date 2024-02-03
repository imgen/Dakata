using System;

namespace Dakata;

/// <summary>
/// Indicates that the column is auto incremented when inserting. Commonly used on Identity column in SQL Server or 
/// column with Sequence as default value. For MySQL it's AUTO INCREMENT columns
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public class AutoIncrementAttribute : Attribute
{
    public string SequenceName { get;set; }
}