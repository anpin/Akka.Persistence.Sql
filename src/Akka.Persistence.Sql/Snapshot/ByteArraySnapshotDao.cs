// -----------------------------------------------------------------------
//  <copyright file="ByteArraySnapshotDao.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Akka.Event;
using Akka.Persistence.Sql.Config;
using Akka.Persistence.Sql.Db;
using Akka.Persistence.Sql.Extensions;
using Akka.Serialization;
using Akka.Streams;
using Akka.Util;
using LinqToDB;

namespace Akka.Persistence.Sql.Snapshot
{
    public class ByteArraySnapshotDao<TJournalPayload> : ISnapshotDao
    {
        private readonly AkkaPersistenceDataConnectionFactory<TJournalPayload> _connectionFactory;
        private readonly ByteArrayDateTimeSnapshotSerializer<TJournalPayload> _dateTimeSerializer;
        private readonly ILoggingAdapter _logger;
        private readonly ByteArrayLongSnapshotSerializer<TJournalPayload> _longSerializer;
        private readonly IsolationLevel _readIsolationLevel;
        private readonly CancellationTokenSource _shutdownCts;
        private readonly SnapshotConfig<TJournalPayload> _snapshotConfig;
        private readonly IsolationLevel _writeIsolationLevel;

        public ByteArraySnapshotDao(
            AkkaPersistenceDataConnectionFactory<TJournalPayload> connectionFactory,
            SnapshotConfig<TJournalPayload> snapshotConfig,
            Akka.Serialization.Serialization serialization,
            IMaterializer materializer,
            ILoggingAdapter logger,
            Func<(Serializer, object), TJournalPayload> toPayload,
            Func<(Serializer, TJournalPayload, Type), object> fromPayload)

        {
            _logger = logger;
            _snapshotConfig = snapshotConfig;
            _connectionFactory = connectionFactory;

            _dateTimeSerializer = new ByteArrayDateTimeSnapshotSerializer<TJournalPayload>(serialization, _snapshotConfig, toPayload, fromPayload);
            _longSerializer = new ByteArrayLongSnapshotSerializer<TJournalPayload>(serialization, _snapshotConfig, toPayload, fromPayload);

            _writeIsolationLevel = snapshotConfig.WriteIsolationLevel;
            _readIsolationLevel = snapshotConfig.ReadIsolationLevel;

            _shutdownCts = new CancellationTokenSource();
        }

        public async Task DeleteAllSnapshotsAsync(
            string persistenceId,
            CancellationToken cancellationToken = default)
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownCts.Token);
            await _connectionFactory.ExecuteWithTransactionAsync(
                _writeIsolationLevel,
                cts.Token,
                async (connection, token) =>
                {
                    if (connection.UseDateTime)
                    {
                        await connection
                            .GetTable<DateTimeSnapshotRow<TJournalPayload>>()
                            .Where(r => r.PersistenceId == persistenceId)
                            .DeleteAsync(token);
                    }
                    else
                    {
                        await connection
                            .GetTable<LongSnapshotRow<TJournalPayload>>()
                            .Where(r => r.PersistenceId == persistenceId)
                            .DeleteAsync(token);
                    }
                });
        }

        public async Task DeleteUpToMaxSequenceNrAsync(
            string persistenceId,
            long maxSequenceNr,
            CancellationToken cancellationToken = default)
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownCts.Token);
            await _connectionFactory.ExecuteWithTransactionAsync(
                _writeIsolationLevel,
                cts.Token,
                async (connection, token) =>
                {
                    if (connection.UseDateTime)
                    {
                        await connection
                            .GetTable<DateTimeSnapshotRow<TJournalPayload>>()
                            .Where(
                                r =>
                                    r.PersistenceId == persistenceId &&
                                    r.SequenceNumber <= maxSequenceNr)
                            .DeleteAsync(token);
                    }
                    else
                    {
                        await connection
                            .GetTable<LongSnapshotRow<TJournalPayload>>()
                            .Where(
                                r =>
                                    r.PersistenceId == persistenceId &&
                                    r.SequenceNumber <= maxSequenceNr)
                            .DeleteAsync(token);
                    }
                });
        }

        public async Task DeleteUpToMaxTimestampAsync(
            string persistenceId,
            DateTime maxTimestamp,
            CancellationToken cancellationToken = default)
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownCts.Token);
            await _connectionFactory.ExecuteWithTransactionAsync(
                _writeIsolationLevel,
                cts.Token,
                async (connection, token) =>
                {
                    if (connection.UseDateTime)
                    {
                        await connection
                            .GetTable<DateTimeSnapshotRow<TJournalPayload>>()
                            .Where(
                                r =>
                                    r.PersistenceId == persistenceId &&
                                    r.Created <= maxTimestamp)
                            .DeleteAsync(token);
                    }
                    else
                    {
                        await connection
                            .GetTable<LongSnapshotRow<TJournalPayload>>()
                            .Where(
                                r =>
                                    r.PersistenceId == persistenceId &&
                                    r.Created <= maxTimestamp.Ticks)
                            .DeleteAsync(token);
                    }
                });
        }

        public async Task DeleteUpToMaxSequenceNrAndMaxTimestampAsync(
            string persistenceId,
            long maxSequenceNr,
            DateTime maxTimestamp,
            CancellationToken cancellationToken = default)
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownCts.Token);
            await _connectionFactory.ExecuteWithTransactionAsync(
                _writeIsolationLevel,
                cts.Token,
                async (connection, token) =>
                {
                    if (connection.UseDateTime)
                    {
                        await connection
                            .GetTable<DateTimeSnapshotRow<TJournalPayload>>()
                            .Where(
                                r =>
                                    r.PersistenceId == persistenceId &&
                                    r.SequenceNumber <= maxSequenceNr &&
                                    r.Created <= maxTimestamp)
                            .DeleteAsync(token);
                    }
                    else
                    {
                        await connection
                            .GetTable<LongSnapshotRow<TJournalPayload>>()
                            .Where(
                                r =>
                                    r.PersistenceId == persistenceId &&
                                    r.SequenceNumber <= maxSequenceNr &&
                                    r.Created <= maxTimestamp.Ticks)
                            .DeleteAsync(token);
                    }
                });
        }

        public async Task<Option<SelectedSnapshot>> LatestSnapshotAsync(
            string persistenceId,
            CancellationToken cancellationToken = default)
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownCts.Token);
            return await _connectionFactory.ExecuteWithTransactionAsync(
                _readIsolationLevel,
                cts.Token,
                async (connection, token) =>
                {
                    if (connection.UseDateTime)
                    {
                        var row = await connection
                            .GetTable<DateTimeSnapshotRow<TJournalPayload>>()
                            .Where(r => r.PersistenceId == persistenceId)
                            .OrderByDescending(t => t.SequenceNumber)
                            .FirstOrDefaultAsync(token);

                        return row != null
                            ? _dateTimeSerializer.Deserialize(row).Get()
                            : Option<SelectedSnapshot>.None;
                    }
                    else
                    {
                        var row = await connection
                            .GetTable<LongSnapshotRow<TJournalPayload>>()
                            .Where(r => r.PersistenceId == persistenceId)
                            .OrderByDescending(t => t.SequenceNumber)
                            .FirstOrDefaultAsync(token);

                        return row != null
                            ? _longSerializer.Deserialize(row).Get()
                            : Option<SelectedSnapshot>.None;
                    }
                });
        }

        public async Task<Option<SelectedSnapshot>> SnapshotForMaxTimestampAsync(
            string persistenceId,
            DateTime timestamp,
            CancellationToken cancellationToken = default)
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownCts.Token);
            return await _connectionFactory.ExecuteWithTransactionAsync(
                _readIsolationLevel,
                cts.Token,
                async (connection, token) =>
                {
                    if (connection.UseDateTime)
                    {
                        var row = await connection
                            .GetTable<DateTimeSnapshotRow<TJournalPayload>>()
                            .Where(
                                r =>
                                    r.PersistenceId == persistenceId &&
                                    r.Created <= timestamp)
                            .OrderByDescending(t => t.SequenceNumber)
                            .FirstOrDefaultAsync(token);

                        return row != null
                            ? _dateTimeSerializer.Deserialize(row).Get()
                            : Option<SelectedSnapshot>.None;
                    }
                    else
                    {
                        var row = await connection
                            .GetTable<LongSnapshotRow<TJournalPayload>>()
                            .Where(
                                r =>
                                    r.PersistenceId == persistenceId &&
                                    r.Created <= timestamp.Ticks)
                            .OrderByDescending(t => t.SequenceNumber)
                            .FirstOrDefaultAsync(token);

                        return row != null
                            ? _longSerializer.Deserialize(row).Get()
                            : Option<SelectedSnapshot>.None;
                    }
                });
        }

        public async Task<Option<SelectedSnapshot>> SnapshotForMaxSequenceNrAsync(
            string persistenceId,
            long sequenceNr,
            CancellationToken cancellationToken = default)
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownCts.Token);
            return await _connectionFactory.ExecuteWithTransactionAsync(
                _readIsolationLevel,
                cts.Token,
                async (connection, token) =>
                {
                    if (connection.UseDateTime)
                    {
                        var row = await connection
                            .GetTable<DateTimeSnapshotRow<TJournalPayload>>()
                            .Where(
                                r =>
                                    r.PersistenceId == persistenceId &&
                                    r.SequenceNumber <= sequenceNr)
                            .OrderByDescending(t => t.SequenceNumber)
                            .FirstOrDefaultAsync(token);

                        return row != null
                            ? _dateTimeSerializer.Deserialize(row).Get()
                            : Option<SelectedSnapshot>.None;
                    }
                    else
                    {
                        var row = await connection
                            .GetTable<LongSnapshotRow<TJournalPayload>>()
                            .Where(
                                r =>
                                    r.PersistenceId == persistenceId &&
                                    r.SequenceNumber <= sequenceNr)
                            .OrderByDescending(t => t.SequenceNumber)
                            .FirstOrDefaultAsync(token);

                        return row != null
                            ? _longSerializer.Deserialize(row).Get()
                            : Option<SelectedSnapshot>.None;
                    }
                });
        }

        public async Task<Option<SelectedSnapshot>> SnapshotForMaxSequenceNrAndMaxTimestampAsync(
            string persistenceId,
            long sequenceNr,
            DateTime timestamp,
            CancellationToken cancellationToken = default)
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownCts.Token);
            return await _connectionFactory.ExecuteWithTransactionAsync(
                _readIsolationLevel,
                cts.Token,
                async (connection, token) =>
                {
                    if (connection.UseDateTime)
                    {
                        var row = await connection
                            .GetTable<DateTimeSnapshotRow<TJournalPayload>>()
                            .Where(
                                r =>
                                    r.PersistenceId == persistenceId &&
                                    r.SequenceNumber <= sequenceNr &&
                                    r.Created <= timestamp)
                            .OrderByDescending(t => t.SequenceNumber)
                            .FirstOrDefaultAsync(token);

                        return row != null
                            ? _dateTimeSerializer.Deserialize(row).Get()
                            : Option<SelectedSnapshot>.None;
                    }
                    else
                    {
                        var row = await connection
                            .GetTable<LongSnapshotRow<TJournalPayload>>()
                            .Where(
                                r =>
                                    r.PersistenceId == persistenceId &&
                                    r.SequenceNumber <= sequenceNr &&
                                    r.Created <= timestamp.Ticks)
                            .OrderByDescending(t => t.SequenceNumber)
                            .FirstOrDefaultAsync(token);

                        return row != null
                            ? _longSerializer.Deserialize(row).Get()
                            : Option<SelectedSnapshot>.None;
                    }
                });
        }

        public async Task DeleteAsync(
            string persistenceId,
            long sequenceNr,
            DateTime timestamp,
            CancellationToken cancellationToken = default)
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownCts.Token);
            await _connectionFactory.ExecuteWithTransactionAsync(
                _writeIsolationLevel,
                cts.Token,
                async (connection, token) =>
                {
                    if (connection.UseDateTime)
                    {
                        var query = connection.GetTable<DateTimeSnapshotRow<TJournalPayload>>()
                                .Where(
                                    r =>
                                        r.PersistenceId == persistenceId &&
                                        r.SequenceNumber == sequenceNr);

                        if (timestamp > DateTime.MinValue)
                            query = query.Where(r => r.Created <= timestamp);

                        await query
                            .DeleteAsync(token);
                    }
                    else
                    {
                        var query = connection
                            .GetTable<DateTimeSnapshotRow<TJournalPayload>>()
                            .Where(
                                r =>
                                    r.PersistenceId == persistenceId &&
                                    r.SequenceNumber == sequenceNr);

                        if (timestamp > DateTime.MinValue)
                            query = query.Where((DateTimeSnapshotRow<TJournalPayload> r) => r.Created <= timestamp);

                        await query
                            .DeleteAsync(token);
                    }
                });
        }

        public async Task SaveAsync(
            SnapshotMetadata snapshotMetadata,
            object snapshot,
            CancellationToken cancellationToken = default)
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _shutdownCts.Token);
            await _connectionFactory.ExecuteWithTransactionAsync(
                _writeIsolationLevel,
                cts.Token,
                async (connection, token) =>
                {
                    if (connection.UseDateTime)
                    {
                        await connection
                            .InsertOrReplaceAsync(
                                _dateTimeSerializer
                                    .Serialize(snapshotMetadata, snapshot)
                                    .Get(),
                                token);
                    }
                    else
                    {
                        await connection
                            .InsertOrReplaceAsync(
                                _longSerializer
                                    .Serialize(snapshotMetadata, snapshot)
                                    .Get(),
                                token);
                    }
                });
        }

        public async Task InitializeTables()
        {
            await using var connection = _connectionFactory.GetConnection();
            var footer = _snapshotConfig.GenerateSnapshotFooter();
            if (connection.UseDateTime)
            {
                await connection.CreateTableAsync<DateTimeSnapshotRow<TJournalPayload>>(TableOptions.CreateIfNotExists, footer);
            }
            else
            {
                await connection.CreateTableAsync<LongSnapshotRow<TJournalPayload>>(TableOptions.CreateIfNotExists, footer);
            }
        }
    }
}
