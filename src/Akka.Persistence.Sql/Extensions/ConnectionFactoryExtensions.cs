// -----------------------------------------------------------------------
//  <copyright file="ConnectionFactoryExtensions.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Akka.Persistence.Sql.Db;
using Akka.Persistence.Sql.Query.Dao;

namespace Akka.Persistence.Sql.Extensions
{
    public static class ConnectionFactoryExtensions
    {
        public static async Task ExecuteWithTransactionAsync<TJournalPayload>(
            this AkkaPersistenceDataConnectionFactory<TJournalPayload> factory,
            IsolationLevel level,
            CancellationToken token,
            Func<AkkaDataConnection<TJournalPayload>, CancellationToken, Task> handler)
        {
            await using var connection = factory.GetConnection();
            await using var tx = await connection.BeginTransactionAsync(level, token);

            try
            {
                await handler(connection, token);
                await tx.CommitAsync(token);
            }
            catch (Exception ex1)
            {
                try
                {
                    await tx.RollbackAsync(token);
                }
                catch (Exception ex2)
                {
                    throw new AggregateException("Exception thrown when rolling back database transaction", ex2, ex1);
                }

                throw;
            }
        }

        public static async Task<T> ExecuteWithTransactionAsync<TJournalPayload,T>(
            this AkkaPersistenceDataConnectionFactory<TJournalPayload> factory,
            IsolationLevel level,
            CancellationToken token,
            Func<AkkaDataConnection<TJournalPayload>, CancellationToken, Task<T>> handler)
        {
            await using var connection = factory.GetConnection();
            await using var tx = await connection.BeginTransactionAsync(level, token);

            try
            {
                var result = await handler(connection, token);
                await tx.CommitAsync(token);
                return result;
            }
            catch (Exception ex1)
            {
                try
                {
                    await tx.RollbackAsync(token);
                }
                catch (Exception ex2)
                {
                    throw new AggregateException("Exception thrown when rolling back database transaction", ex2, ex1);
                }

                throw;
            }
        }

        internal static Task<T> ExecuteWithTransactionAsync<TJournalPayload, TState,T>(
            this DbStateHolder<TJournalPayload> factory,
            TState state,
            Func<AkkaDataConnection<TJournalPayload>, CancellationToken, TState, Task<T>> handler)
        {
            return factory.ConnectionFactory.ExecuteWithTransactionAsync(state, factory.IsolationLevel, factory.ShutdownToken, handler);
        }

        public static async Task<T> ExecuteWithTransactionAsync<TJournalPayload, TState,T>(
            this AkkaPersistenceDataConnectionFactory<TJournalPayload> factory,
            TState state,
            IsolationLevel level,
            CancellationToken token,
            Func<AkkaDataConnection<TJournalPayload>, CancellationToken, TState, Task<T>> handler)
        {
            await using var connection = factory.GetConnection();
            await using var tx = await connection.BeginTransactionAsync(level, token);

            try
            {
                var result = await handler(connection, token, state);
                await tx.CommitAsync(token);
                return result;
            }
            catch (Exception ex1)
            {
                try
                {
                    await tx.RollbackAsync(token);
                }
                catch (Exception ex2)
                {
                    throw new AggregateException("Exception thrown when rolling back database transaction", ex2, ex1);
                }

                throw;
            }
        }
    }
}
