using System;
using System.Linq;

namespace Dakata
{
    public class SlapperUtils
    {
        public static void RegisterIdentifiers(params Type[] modelTypes)
        {
            foreach(var type in modelTypes)
            {
                var keyProperties = BaseDal.GetKeyProperties(type);
                var keyPropertyNames = keyProperties.Select(x => x.Name).ToList();
                Slapper.AutoMapper.Configuration.AddIdentifiers(type, keyPropertyNames);
            }
        }
    }
}
