using Dapper;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Security.Cryptography;
using static Dapper.SqlMapper;

namespace Dakata
{
    public static class Extensions
    {
        public static bool IsNullable(this Type type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            return type.IsGenericType && type.GetGenericTypeDefinition() == typeof (Nullable<>);
        }

        public static string ToNullIfEmpty(this string s)
        {
            return string.IsNullOrEmpty(s) ? null : s;
        }

        public static bool IsNull(this object obj) => obj == null;

        public static bool IsNotNull(this object obj) => obj != null;

        public static bool IsNullOrEmpty<T>(this IEnumerable<T> sequence) => sequence == null || !sequence.Any();

        public static bool IsNullOrEmpty(this string str) => string.IsNullOrEmpty(str);

        public static bool IsNullOrEmptyAfterTrim(this string str) => string.IsNullOrEmpty(str?.Trim());

        public static T[] Shuffle<T>(this IEnumerable<T> sequence)
        {
            var array = sequence.ToArray();
            var provider = new RNGCryptoServiceProvider();
            int n = array.Length;
            while (n > 1)
            {
                byte[] box = new byte[1];
                do provider.GetBytes(box);
                while (!(box[0] < n * (byte.MaxValue / n)));
                int k = box[0] % n;
                n--;
                T value = array[k];
                array[k] = array[n];
                array[n] = value;
            }
            return array;
        }

        public static string ShortenString(this string str, int maxLength,
            string fill = "...", 
            bool skipMiddle = true, 
            int endCharsToKeep = 10)
        {
            if (str.Length <= maxLength)
            {
                return str;
            }
            fill = fill ?? string.Empty;
            if (!skipMiddle)
            {
                return str.Substring(0, maxLength - fill.Length) + fill;
            }
            endCharsToKeep = endCharsToKeep <= 0 ? 10 : endCharsToKeep;
            return str.Substring(0, maxLength - endCharsToKeep - fill.Length) + fill +
                   str.Substring(str.Length - endCharsToKeep, endCharsToKeep);
        }

        public static bool IsNull<T>(T? nullable) where T : struct
        {
            return nullable == null;
        }

        public static string ToCamelCase(this string str)
        {
            if (str.IsNullOrEmpty())
            {
                return str;
            }
            var chars = str.ToLower().Skip(1).Prepend(char.ToUpper(str[0])).ToArray();
            return new string(chars);
        }

        public static IEnumerable<T> Prepend<T>(this IEnumerable<T> sequence, T newItem)
            => sequence.PrependAll(newItem);

        public static IEnumerable<T> PrependAll<T>(this IEnumerable<T> sequence, params T[] newItems)
        {
            return newItems.Concat(sequence);
        }

        public static IEnumerable<T> Append<T>(this IEnumerable<T> sequence, T newItem)
            => sequence.AppendAll(newItem);

        public static IEnumerable<T> AppendAll<T>(this IEnumerable<T> sequence, params T[] newItems) => sequence.Concat(newItems);

        public static IEnumerable<T> ForEach<T>(this IEnumerable<T> sequence, Action<T> action)
        {
            var items = sequence as T[] ?? sequence.ToArray();
            foreach (var item in items)
            {
                action(item);
            }

            return items;
        }

        public static byte[] ReadAllBytes(this Stream stream)
        {
            using (var ms = new MemoryStream((int)stream.Length))
            {
                stream.CopyTo(ms);
                return ms.ToArray();
            }
        }

        public static TValue GetAttributeValue<TAttribute, TValue>(
            this Type type,
            Func<TAttribute, TValue> valueSelector)
        where TAttribute : Attribute
        {
            if (type.GetCustomAttributes<TAttribute>(true).FirstOrDefault() is TAttribute att)
            {
                return valueSelector(att);
            }
            return default(TValue);
        }

        public static IEnumerable<PropertyInfo> GetPropertiesWithAttribute<TAttr>(this Type type, Func<TAttr, bool> checker = null)
            where TAttr: Attribute
        {
            return type.GetPropertiesWithAttribute(typeof(TAttr), attr => checker?.Invoke(attr as TAttr) ?? true);
        }

        public static IEnumerable<PropertyInfo> GetPropertiesWithAttribute(this Type type, Type attrType, Func<Attribute, bool> checker = null)
        {
            return type.GetProperties()
                            .Where(x => x.GetCustomAttributes(true)
                                   .Any(attr => attr.GetType() == attrType && (checker?.Invoke(attr as Attribute)?? true))
                                  );
        }

        public static List<T> AddItem<T>(this List<T> list, T item)
        {
            list.Add(item);
            return list;
        }

        public static HashSet<T> AddItem<T>(this HashSet<T> list, T item)
        {
            list.Add(item);
            return list;
        }

        public static void Add<T>(this List<T> list, IEnumerable<T> items) => list.AddRange(items?? Enumerable.Empty<T>());

        public static string Prepend(this string str, string prefix)
        {
            return prefix + str;
        }

        public static string JoinString(this string separator, params object[] items) =>
            string.Join(separator, items);

        public static string JoinString<T>(this IEnumerable<T> items, string separator) =>
            string.Join(separator, items);
        
        public static IEnumerable<IEnumerable<TSource>> Batch<TSource>(
                  this IEnumerable<TSource> source, int size)
        {
            TSource[] bucket = null;
            var count = 0;

            foreach (var item in source)
            {
                if (bucket == null)
                    bucket = new TSource[size];

                bucket[count++] = item;
                if (count < size)
                    continue;

                yield return bucket;

                bucket = null;
                count = 0;
            }

            if (bucket != null && count > 0)
                yield return bucket.Take(count);
        }

        // code adjusted to prevent horizontal overflow
        public static string GetFullPropertyName<T, TProperty>(this Expression<Func<T, TProperty>> exp)
        {
            if (!TryFindMemberExpression(exp.Body, out var memberExp))
                return string.Empty;

            var memberNames = new Stack<string>();
            do
            {
                memberNames.Push(memberExp.Member.Name);
            }
            while (TryFindMemberExpression(memberExp.Expression, out memberExp));

            return string.Join(".", memberNames.ToArray());
        }

        // code adjusted to prevent horizontal overflow
        private static bool TryFindMemberExpression(Expression exp, out MemberExpression memberExp)
        {
            memberExp = exp as MemberExpression;
            if (memberExp != null)
            {
                // heyo! that was easy enough
                return true;
            }

            // if the compiler created an automatic conversion,
            // it'll look something like...
            // obj => Convert(obj.Property) [e.g., int -> object]
            // OR:
            // obj => ConvertChecked(obj.Property) [e.g., int -> long]
            // ...which are the cases checked in IsConversion
            if (!IsConversion(exp) || !(exp is UnaryExpression)) return false;
            memberExp = ((UnaryExpression)exp).Operand as MemberExpression;
            return memberExp != null;
        }

        private static bool IsConversion(Expression exp)
        {
            return exp.NodeType == ExpressionType.Convert ||
                   exp.NodeType == ExpressionType.ConvertChecked;
        }

        public static IEnumerable<(TSource item, int index)> WithIndices<TSource>(this IEnumerable<TSource> sequence)
        {
            return sequence.Select((item, index) => (item, index));
        }

        public static string[] GetFiles(this string path, string searchPattern, SearchOption searchOption)
        {
            var searchPatterns = searchPattern.Split('|');
            var files = searchPatterns.SelectMany(
                sp => Directory.GetFiles(path, sp, searchOption)
            ).ToList();
            files.Sort();
            return files.ToArray();
        }

        public static int? ParseInt(this string str)
        {
            if (str.IsNullOrEmpty())
            {
                return null;
            }
            return int.TryParse(str, out var result)? result : (int?)null;
        }

        public static long? ParseLong(this string str)
        {
            if (str.IsNullOrEmpty())
            {
                return null;
            }
            return long.TryParse(str, out var result) ? result : (long?)null;
        }

        public static float? ParseFloat(this string str)
        {
            if (str.IsNullOrEmpty())
            {
                return null;
            }
            return float.TryParse(str, out var result) ? result : (float?)null;
        }

        public static double? ParseDouble(this string str)
        {
            if (str.IsNullOrEmpty())
            {
                return null;
            }
            return double.TryParse(str, out var result) ? result : (double?)null;
        }

        public static decimal? ParseDecimal(this string str)
        {
            if (str.IsNullOrEmpty())
            {
                return null;
            }
            return decimal.TryParse(str, out var result) ? result : (decimal?)null;
        }

        public static string FormatString(this string str, params object[] args)
        {
            return string.Format(str, args);
        }

        public static TEnum[] GetEnumValues<TEnum>(this TEnum obj)
            where TEnum: struct 
        {
            return (TEnum[]) Enum.GetValues(typeof(TEnum));
        }

        public static DateTimeOffset RoundToHour(this DateTimeOffset dateTime)
        {
            var updated = dateTime.AddMinutes(30);
            return new DateTimeOffset(updated.Year, updated.Month, updated.Day,
                                 updated.Hour, 0, 0, dateTime.Offset);
        }

        public static DateTime ParseDateTime(this string format, string str)
        {
            return DateTime.ParseExact(str, format, null);
        }

        public static IOrderedEnumerable<TSource> OrderBy<TSource, TKey>(
            this IEnumerable<TSource> source,
            Func<TSource, TKey> keySelector, 
            bool ascending)
        {
            return ascending ? source.OrderBy(keySelector) : source.OrderByDescending(keySelector);
        }


        public static ExpandoObject ToExpando(this IDictionary<string, object> dictionary)
        {
            var expando = new ExpandoObject();
            var expandoDic = (IDictionary<string, object>)expando;

            // go through the items in the dictionary and copy over the key value pairs)
            foreach (var kvp in dictionary)
            {
                // if the value can also be turned into an ExpandoObject, then do it!
                if (kvp.Value is IDictionary<string, object> objects)
                {
                    var expandoValue = objects.ToExpando();
                    expandoDic.Add(kvp.Key, expandoValue);
                }
                else if (kvp.Value is ICollection collection)
                {
                    // iterate through the collection and convert any strin-object dictionaries
                    // along the way into expando objects
                    var itemList = new List<object>();
                    foreach (var item in collection)
                    {
                        if (item is IDictionary<string, object> objects1)
                        {
                            var expandoItem = objects1.ToExpando();
                            itemList.Add(expandoItem);
                        }
                        else
                        {
                            itemList.Add(item);
                        }
                    }

                    expandoDic.Add(kvp.Key, itemList);
                }
                else
                {
                    expandoDic.Add(kvp);
                }
            }

            return expando;
        }

        public static string AsString(this IEnumerable<char> chars)
        {
            return new string(chars.ToArray());
        }

        public static bool IsHttpUrl(this string s)
        {
            return s.StartsWith("http", StringComparison.InvariantCultureIgnoreCase);
        }

        public static string MakeFirstLetterLowercase(this string str)
        {
            return str.IsNullOrEmpty() ? str : str.Substring(0, 1).ToLowerInvariant() + str.Substring(1);
        }

        public static T ShadowClone<T>(this T obj)
            where T: class 
        {
            var memberwiseCloneMethod = obj?.GetType().GetMethod("MemberwiseClone", 
                BindingFlags.Instance | BindingFlags.NonPublic);
            return (T) memberwiseCloneMethod?.Invoke(obj, null);
        }

        public static string ReverseString(this string str)
        {
            return str.IsNullOrEmpty() ? str : new string(str.Reverse().ToArray());
        }

        public static string AddSlashIfNotEndsWith(this string str)
        {
            return str.AddSuffixIfNotEndsWith("/");
        }

        public static string AddSuffixIfNotEndsWith(this string str, string suffix)
        {
            return str.EndsWith(suffix) ? str : str + suffix;
        }

        public static T GetByKey<TKey, T>(this IDictionary<TKey, T> dictionary, TKey key)
        {
            return dictionary.ContainsKey(key) ? dictionary[key] : default;
        }

        public static HashSet<T> ToHashSet<T>(this IEnumerable<T> sequence)
        {
            var hashSet = new HashSet<T>();
            foreach (var item in sequence)
            {
                hashSet.Add(item);
            }

            return hashSet;
        }

        public static T[] ToSingleElementArray<T>(this T item)
        {
            return new[] {item};
        }

        public static List<T> MakeEmptyIfNull<T>(this List<T> list)
        {
            return list ?? new List<T>();
        }

        public static IDictionary<TKey, TValue> RemoveByKey<TKey, TValue>(
            this IDictionary<TKey, TValue> dictionary,
            TKey key)
        {
            dictionary.Remove(key);
            return dictionary;
        }

        public static IDictionary<TKey, TValue> RemoveByKeys<TKey, TValue>(
            this IDictionary<TKey, TValue> dictionary,
            params TKey[] keys)
        {
            foreach (var key in keys)
            {
                dictionary.Remove(key);
            }
            return dictionary;
        }

        public static T[] AsArray<T>(this T item) => new[] { item };

        public static List<T> AsList<T>(this T item) => new List<T> { item };

        public static IEnumerable<T> ConcatMany<T>(this IEnumerable<T> source, params IEnumerable<T>[] sequences)
        {
            return sequences.Aggregate(source, (current, sequence) => current.Concat(sequence));
        }
        
        public static T ToObject<T>(this IDictionary<string, object> source)
            where T : class, new()
        {
            var someObject = new T();
            var someObjectType = someObject.GetType();

            foreach (var item in source)
            {
                someObjectType.GetProperty(item.Key)?.SetValue(someObject, item.Value, null);
            }

            return someObject;
        }

        public static IDictionary<string, object> AsDictionary(this object source)
        {
            switch (source)
            {
                case null:
                    return new Dictionary<string, object>();
                case IReadOnlyDictionary<string, object> readOnlyDictionary:
                    return readOnlyDictionary.ToDictionary(x => x.Key, x => x.Value);
                case IDictionary<string, object> dictionary:
                    return dictionary.ToDictionary(x => x.Key, x => x.Value);
                case DynamicParameters dynamicParameters:
                    var dictionary2 = new Dictionary<string, object>();
                    var parameterLookup = dynamicParameters as IParameterLookup;
                    foreach(var name in dynamicParameters.ParameterNames)
                    {
                        dictionary2[name] = parameterLookup[name];
                    }
                    break;
            }

            var type = source.GetType();
            if (type.IsGenericType)
            {
                const string DICTIONARY_INTERFACE_NAME = "System.Collections.Generic.IDictionary`2",
                    READONLY_DICTIONARY_INTERFACE_NAME = "System.Collections.Generic.IReadOnlyDictionary`2";
                var dictionaryInterface = type.GetInterface(DICTIONARY_INTERFACE_NAME) ??
                    type.GetInterface(READONLY_DICTIONARY_INTERFACE_NAME);
                if (dictionaryInterface != null &&
                    dictionaryInterface.GetGenericArguments()[0] == typeof(string))
                {
                    dynamic stringDict = source;
                    ICollection<string> keys = stringDict.Keys;
                    return keys.ToDictionary(x => x, x => stringDict[x]);
                }
            }

            const BindingFlags BINDING_ATTR = BindingFlags.Public | BindingFlags.Instance | BindingFlags.GetProperty;
            // Exclude indexers when get the properties
            var properties = type.GetProperties(BINDING_ATTR)
                .Where(x => !x.GetIndexParameters().Any())
                .ToArray();
            var dict = properties.ToDictionary(
                property => property.Name,
                property => property.GetValue(source)
                );

            return dict;
        }
    }
}
