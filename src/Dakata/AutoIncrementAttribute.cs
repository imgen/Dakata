using System;

namespace Dakata
{
    /// <summary>
    /// Indicates that the column is auto incremented when inserting. Commonly used on Identity column
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public class AutoIncrementAttribute : Attribute
    {
        public string SequenceName { get;set; }
    }
}