namespace Dakata.Example
{
    public partial class Examples
    {
        private const string ConnectionString = "Data Source=(local);Initial Catalog=WorldWideImporters;Integrated Security=True;MultipleActiveResultSets=True";

        static void Main(string[] args)
        {
            var connection = new DapperConnection(ConnectionString, new SqlServerDbProvider());

            LoggingExamples(connection);

            GetExamples(connection);

            InsertExamples(connection);
        }
    }
}
