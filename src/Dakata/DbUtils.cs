using System;
using System.Data;
using System.Threading.Tasks;
using System.Transactions;
using Dapper.Contrib.Extensions;
using IsolationLevel = System.Transactions.IsolationLevel;

namespace Dakata
{
    public static class DbUtils
    {
        [ThreadStatic]
        private static IDbConnection _currentDbConnection;

        public static IDbConnection CurrentDbConnection => _currentDbConnection;

        [ThreadStatic]
        private static IDbTransaction _currentDbTransaction;

        public static IDbTransaction CurrentDbTransaction => _currentDbTransaction;

        private const TransactionScopeAsyncFlowOption EnableAsync = TransactionScopeAsyncFlowOption.Enabled;

        private static readonly TimeSpan DefaultTimeout = TimeSpan.FromMinutes(10);

        private static TransactionScope CreateTransactionScope(
            TimeSpan? timeout,
            IsolationLevel isolationLevel,
            TransactionScopeAsyncFlowOption asyncOption = TransactionScopeAsyncFlowOption.Suppress)
        {
            timeout = timeout ?? DefaultTimeout;
            var transactionOptions = new TransactionOptions
            {
                IsolationLevel = isolationLevel,
                Timeout = timeout.Value
            };
            return new TransactionScope(TransactionScopeOption.Required, transactionOptions, asyncOption);
        }
        
        public static T WithTransaction<T>(Func<TransactionScope, T> func, 
            TimeSpan? timeout = null,
            IsolationLevel isolationLevel = IsolationLevel.ReadCommitted,
            bool enableMultithreadSupportForTransaction = false)
        {
            using (var scope = CreateTransactionScope(timeout, isolationLevel,
                enableMultithreadSupportForTransaction
                    ? TransactionScopeAsyncFlowOption.Enabled
                    : TransactionScopeAsyncFlowOption.Suppress))
            {
                if (enableMultithreadSupportForTransaction)
                    TransactionInterop.GetTransmitterPropagationToken(Transaction.Current);

                var result = func(scope);
                scope.Complete();
                return result;
            }
        }

        public static void WithTransaction(Action<TransactionScope> action, 
            TimeSpan? timeout = null,
            IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
        {
            WithTransaction(scope =>
            {
                action(scope);
                return (object)null;
            }, timeout, isolationLevel);
        }

        public static async Task<T> WithTransaction<T>(Func<TransactionScope, Task<T>> func, 
            TimeSpan? timeout = null,
            IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
        {
            using (var scope = CreateTransactionScope(timeout, isolationLevel, EnableAsync))
            {
                var result = await func(scope);
                scope.Complete();
                return result;
            }
        }

        public static async Task WithTransaction(Func<TransactionScope, Task> func, 
            TimeSpan? timeout = null,
            IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
        {
            await WithTransaction(async scope =>
            {
                await func(scope);
                return (object)null;
            }, timeout, isolationLevel);
        }

        public static T WithTransaction<T>(Func<T> func, 
            TimeSpan? timeout = null,
            IsolationLevel isolationLevel = IsolationLevel.ReadCommitted,
            bool enableMultithreadSupportForTransaction = false)
        {
            using (var scope = CreateTransactionScope(timeout, isolationLevel,
                enableMultithreadSupportForTransaction
                    ? TransactionScopeAsyncFlowOption.Enabled
                    : TransactionScopeAsyncFlowOption.Suppress))
            {
                if (enableMultithreadSupportForTransaction)
                    TransactionInterop.GetTransmitterPropagationToken(Transaction.Current);

                var result = func();
                scope.Complete();
                return result;
            }
        }

        public static void WithTransaction(Action action, 
            TimeSpan? timeout = null,
            IsolationLevel isolationLevel = IsolationLevel.ReadCommitted,
            bool enableMultithreadSupportForTransaction = false)
        {
            WithTransaction(scope =>
            {
                action();
                return (object)null;
            }, timeout, isolationLevel);
        }

        public static async Task<T> WithTransaction<T>(Func<Task<T>> func, 
            TimeSpan? timeout = null,
            IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
        {
            using (var scope = CreateTransactionScope(timeout, isolationLevel, EnableAsync))
            {
                var result = await func();
                scope.Complete();
                return result;
            }
        }

        public static async Task WithTransaction(Func<Task> func, 
            TimeSpan? timeout = null,
            IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
        {
            await WithTransaction(async scope =>
            {
                await func();
                return (object)null;
            }, timeout, isolationLevel);
        }

        public static string GetTableName<TEntity>()
        {
            return GetTableName(typeof(TEntity));
        }

        public static string GetTableName(Type entityType)
        {
            return entityType.GetAttributeValue<TableAttribute, string>(x => x.Name).Replace("`", string.Empty);
        }

        public static string MakeLikeable(this object value)
        {
            return $"%{value}%";
        }

        public static string MakeEndLikeable(this object value)
        {
            return $"{value}%";
        }

        public static T WithRawTransaction<T>(Func<IDbConnection, IDbTransaction, T> func,
            Func<IDbConnection> connectionProvider, TimeSpan? timeout = null)
        {
            using (var connection = connectionProvider())
            {
                _currentDbConnection = connection;
                using (var transaction = connection.BeginTransaction())
                {
                    _currentDbTransaction = transaction;
                    try
                    {
                        return func(connection, transaction);
                    }
                    catch (Exception)
                    {
                        transaction.Rollback();
                        throw;
                    }
                }
            }
        }
    }
}
