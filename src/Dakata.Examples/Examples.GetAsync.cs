using Dakata.Examples.Models;
using FluentAssertions;
using System.Threading.Tasks;
using Xunit;

namespace Dakata.Examples;

public partial class Examples
{
    [Fact]
    public async Task GetAsyncWithCompositeKeysExampe()
    {
        var cityDal = new BaseDal<City>(CreateDapperConnection());
        var city = await cityDal.QueryByEntityKeysAsync(new City
        {
            CityName = "Alfred",
            StateProvinceId = 35
        });
        city.Should().NotBeNull("QueryByEntityKeysAsync doesn't behave correctly");
    }
}