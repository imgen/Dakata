# Dakata
A DAL layer implemented using [`Dapper`](https://github.com/StackExchange/Dapper) + [`SqlKata`](https://sqlkata.com/) + [`Slapper.AutoMapper`](https://github.com/SlapperAutoMapper/Slapper.AutoMapper) + [`Dapper.ColumnMapper`](https://github.com/dturkenk/Dapper.ColumnMapper). Most features will work with any database `SqlKata` supports. But certain 
features / APIs such as `InsertAll`, `UpdateAll` will only work with `SQL Server` and `MySQL` since that's what I used in my own code. If someone can add support for other databases such as `Postgres` or `Oracle`, feel free make a PR. 

## `IDbProvider` interface
`IDbProvider` interface is a simple abstraction of a database's behavior such as creating the connection, maximum parameter count, when insert, how to retrieve the inserted id, etc. The `UtcNowExpression` is used for columns that its value is an `SQL` expression instead of an value passed by the caller, such as `TimeCreated`, `TimeUpdated`, etc. Please see [`SqlServerDbProvider`](https://github.com/imgen/Dakata/blob/master/src/Dakata.SqlServer/SqlServerDbProvider.cs) file for reference implementation. 

I only created `IDbProvider` implementation for `SQL Server` and `MySQL`. It's very easy to implement for other databases that `SqlKata` supports. 

## Examples project
The examples XUnit test project uses `SQL Server` sample database `WorldWideImporters`. You can import the database by downloading the `.bak` file from below link

[WorldWideImporters Sample DB](https://github.com/microsoft/sql-server-samples/releases/tag/wide-world-importers-v1.0)

You probably need to change the connection string if you install `SQL Server` on a different machine or don't use `Windows Authentication` for authentication
