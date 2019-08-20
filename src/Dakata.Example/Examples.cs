namespace Dakata.Example
{
    public partial class Examples
    {
        private const string ConnectionString = "Data Source=(local);Initial Catalog=WorldWideImporters;Integrated Security=True;MultipleActiveResultSets=True";

        static void Main(string[] args)
        {
            var connection = new DapperConnection(ConnectionString, new SqlServerDbProvider());

            GetExamples(connection);
        }

        private static void GetExamples(DapperConnection connection)
        {
            GetAllExample(connection);
            GetMaxValueOfColumnExample(connection);
            GetMinValueOfColumnExample(connection);
        }
    }
}
