using Dakata.Examples.Models;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Dakata.Examples
{
    public partial class Examples
    {
        private static async Task GetAsyncExamples(DapperConnection dapperConnection)
        {
            await GetAsyncWithCompositeKeysExampe(dapperConnection);
        }

        private static async Task GetAsyncWithCompositeKeysExampe(DapperConnection dapperConnection)
        {
            var cityDal = new BaseDal<City>(dapperConnection);
            var city = await cityDal.QueryByEntityKeysAsync(new City
            {
                CityName = "Alfred",
                StateProvinceID = 35
            });
            if (city == null)
            {
                WriteError("QueryByEntityKeysAsync doesn't behave correctly");
            }
        }
    }
}
