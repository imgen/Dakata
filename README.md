# Dakata
A DAL layer implemented using `Dapper` + `SqlKata` + `Slapper`. Most features will work with any database `SqlKata` supports. But certain 
features / APIs such as `InsertAll`, `UpdateAll` will only work with `SQL Server` and `MySQL` since that's what I used in my own code. If someone can add suport for other databases such as `Postgres` or `Oracle`, please make a PR. 

## `IDbProvider` interface
`IDbProvider` interface is a simple abstraction of a database's behavior such as creating the connection, maximum parameter count, when insert, how to retrieve the inserted id, etc. The `UtcNowExpression` is used for columns that its value is an SQL expression instead of an value passed by the caller, such as `TimeCreated`, `TimeUpdated`, etc. Please see [`SqlServerDbProvider`](https://github.com/imgen/Dakata/blob/master/src/Dakata.SqlServer/SqlServerDbProvider.cs) file for reference implementation. 

I only created `IDbProvider` implementation for `SQL Server` and `MySQL`. It's very easy to implement for other databases that `SqlKata` supports. 
