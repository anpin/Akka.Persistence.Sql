﻿// -----------------------------------------------------------------------
//  <copyright file="BaseByteArrayJournalDao.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence.Sql.Config;
using Akka.Persistence.Sql.Db;
using Akka.Persistence.Sql.Extensions;
using Akka.Persistence.Sql.Journal.Types;
using Akka.Persistence.Sql.Serialization;
using Akka.Serialization;
using Akka.Streams;
using Akka.Streams.Dsl;
using LanguageExt;
using LinqToDB;
using LinqToDB.Data;
using static LanguageExt.Prelude;
using Akka.Persistence.Sql.Db;
namespace Akka.Persistence.Sql.Journal.Dao
{
    public abstract class BaseByteArrayJournalDao<TJournalPayload> : BaseJournalDaoWithReadMessages<TJournalPayload>, IJournalDaoWithUpdates
    {
        private readonly Flow<JournalRow<TJournalPayload>, Util.Try<ReplayCompletion>, NotUsed> _deserializeFlowMapped;
        private readonly TagMode _tagWriteMode;
        protected readonly JournalConfig<TJournalPayload> JournalConfig;

        protected readonly ILoggingAdapter Logger;
        protected readonly FlowPersistentRepresentationSerializer<JournalRow<TJournalPayload>> Serializer;

        protected readonly ISourceQueueWithComplete<WriteQueueEntry<TJournalPayload>> WriteQueue;

        protected BaseByteArrayJournalDao(
            IAdvancedScheduler scheduler,
            IMaterializer materializer,
            AkkaPersistenceDataConnectionFactory<TJournalPayload> connectionFactory,
            JournalConfig<TJournalPayload> config,
            Akka.Serialization.Serialization serializer,
            ILoggingAdapter logger,
            string selfUuid,
            CancellationToken shutdownToken,
            Func<(Serializer, object), TJournalPayload> toPayload,
            Func<(Serializer, TJournalPayload, Type), object> fromPayload)
            : base(scheduler, materializer, connectionFactory, config, shutdownToken)
        {
            Logger = logger;
            JournalConfig = config;
            Serializer = new ByteArrayJournalSerializer<TJournalPayload>(config, serializer, config.PluginConfig.TagSeparator, selfUuid, toPayload, fromPayload);
            _deserializeFlowMapped = Serializer.DeserializeFlow().Select(MessageWithBatchMapper());
            _tagWriteMode = JournalConfig.PluginConfig.TagMode;

            // Due to C# rules we have to initialize WriteQueue here
            // Keeping it here vs init function prevents accidental moving of init
            // to where variables aren't set yet.
            WriteQueue = Source
                .Queue<WriteQueueEntry<TJournalPayload>>(JournalConfig.DaoConfig.BufferSize, OverflowStrategy.DropNew)
                .BatchWeighted(
                    JournalConfig.DaoConfig.BatchSize,
                    cf => cf.Rows.Count,
                    r => new WriteQueueSet<TJournalPayload>(ImmutableList.Create(new[] { r.Tcs }), r.Rows),
                    (oldRows, newRows) =>
                        new WriteQueueSet<TJournalPayload>(
                            oldRows.Tcs.Add(newRows.Tcs),
                            oldRows.Rows.Concat(newRows.Rows)))
                .SelectAsync(
                    JournalConfig.DaoConfig.Parallelism,
                    async promisesAndRows =>
                    {
                        try
                        {
                            await WriteJournalRows(promisesAndRows.Rows);
                            foreach (var taskCompletionSource in promisesAndRows.Tcs)
                                taskCompletionSource.TrySetResult(NotUsed.Instance);
                        }
                        catch (Exception e)
                        {
                            foreach (var taskCompletionSource in promisesAndRows.Tcs)
                                taskCompletionSource.TrySetException(e);
                        }

                        return NotUsed.Instance;
                    })
                .ToMaterialized(
                    Sink.Ignore<NotUsed>(),
                    Keep.Left).Run(Materializer);
        }

        public async Task<IImmutableList<Exception>> AsyncWriteMessages(
            IEnumerable<AtomicWrite> messages,
            long timeStamp = 0)
        {
            var serializedTries = Serializer.Serialize(messages, timeStamp);

            // Fold our List of Lists into a single sequence
            var rows = Seq(FlattenListOfListsToList(serializedTries));

            // Wait for the write to go through. If Task fails, write will be captured as WriteMessagesFailure.
            await QueueWriteJournalRows(rows);

            // If we get here, we build an ImmutableList containing our rejections.
            // These will be captured as WriteMessagesRejected
            return BuildWriteRejections(serializedTries);
        }

        public async Task Delete(string persistenceId, long maxSequenceNr)
        {
            await ConnectionFactory.ExecuteWithTransactionAsync(
                WriteIsolationLevel,
                ShutdownToken,
                async (connection, token) =>
                {
                    await connection
                        .GetTable<JournalRow<TJournalPayload>>()
                        .Where(
                            r =>
                                r.PersistenceId == persistenceId &&
                                r.SequenceNumber <= maxSequenceNr)
                        .Set(r => r.Deleted, true)
                        .UpdateAsync(token);

                    var maxMarkedDeletion =
                        await MaxMarkedForDeletionMaxPersistenceIdQuery(connection, persistenceId).FirstOrDefaultAsync(token);

                    if (JournalConfig.DaoConfig.SqlCommonCompatibilityMode)
                    {
                        await connection
                            .GetTable<JournalMetaData>()
                            .InsertOrUpdateAsync(
                                insertSetter: () => new JournalMetaData
                                {
                                    PersistenceId = persistenceId,
                                    SequenceNumber = maxMarkedDeletion,
                                },
                                onDuplicateKeyUpdateSetter: jmd => new JournalMetaData(),
                                keySelector: () => new JournalMetaData
                                {
                                    PersistenceId = persistenceId,
                                    SequenceNumber = maxMarkedDeletion,
                                },
                                token: token);
                    }

                    await connection
                        .GetTable<JournalRow<TJournalPayload>>()
                        .Where(
                            r =>
                                r.PersistenceId == persistenceId &&
                                r.SequenceNumber <= maxSequenceNr &&
                                r.SequenceNumber < maxMarkedDeletion)
                        .DeleteAsync(token);

                    if (JournalConfig.DaoConfig.SqlCommonCompatibilityMode)
                    {
                        await connection
                            .GetTable<JournalMetaData>()
                            .Where(
                                r =>
                                    r.PersistenceId == persistenceId &&
                                    r.SequenceNumber < maxMarkedDeletion)
                            .DeleteAsync(token);
                    }

                    if (JournalConfig.PluginConfig.TagMode != TagMode.Csv)
                    {
                        await connection
                            .GetTable<JournalTagRow>()
                            .Where(
                                r =>
                                    r.SequenceNumber <= maxSequenceNr &&
                                    r.PersistenceId == persistenceId)
                            .DeleteAsync(token);
                    }
                });
        }

        public async Task<Done> Update(string persistenceId, long sequenceNr, object payload)
        {
            var write = new Persistent(payload, sequenceNr, persistenceId);
            var serialize = Serializer.Serialize(write);

            if (!serialize.IsSuccess)
            {
                throw new ArgumentException(
                    $"Failed to serialize {write.GetType()} for update of {persistenceId}] @ {sequenceNr}",
                    serialize.Failure.Value);
            }

            await ConnectionFactory.ExecuteWithTransactionAsync(
                WriteIsolationLevel,
                ShutdownToken,
                async (connection, token) =>
                {
                    await connection
                        .GetTable<JournalRow<TJournalPayload>>()
                        .Where(
                            r =>
                                r.PersistenceId == persistenceId &&
                                r.SequenceNumber == write.SequenceNr)
                        .Set(r => r.Message, serialize.Get().Message)
                        .UpdateAsync(token);
                });

            return Done.Instance;
        }

        public async Task<long> HighestSequenceNr(string persistenceId, long fromSequenceNr)
        {
            return await ConnectionFactory.ExecuteWithTransactionAsync(
                ReadIsolationLevel,
                ShutdownToken,
                async (connection, token) => (await connection.MaxSeqNumberForPersistenceIdQuery(JournalConfig.DaoConfig.SqlCommonCompatibilityMode,persistenceId, fromSequenceNr).MaxAsync(token))
                    .GetValueOrDefault(0));
        }

        /// <summary>
        ///     This override is greedy since it is always called
        ///     from within <see cref="BaseJournalDaoWithReadMessages.MessagesWithBatch" />
        /// </summary>
        /// <param name="persistenceId"></param>
        /// <param name="fromSequenceNr"></param>
        /// <param name="toSequenceNr"></param>
        /// <param name="max"></param>
        /// <returns></returns>
        public override async Task<Source<Util.Try<ReplayCompletion>, NotUsed>> Messages(
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr,
            long max)
        {
            return await ConnectionFactory.ExecuteWithTransactionAsync(
                ReadIsolationLevel,
                ShutdownToken,
                async (connection, token) =>
                {
                    IQueryable<JournalRow<TJournalPayload>> query = connection
                        .GetTable<JournalRow<TJournalPayload>>()
                        .Where(
                            r =>
                                r.PersistenceId == persistenceId &&
                                r.SequenceNumber >= fromSequenceNr &&
                                r.SequenceNumber <= toSequenceNr &&
                                r.Deleted == false)
                        .OrderBy(r => r.SequenceNumber);

                    if (max <= int.MaxValue)
                        query = query.Take((int)max);

                    var source = await query.ToListAsync(token);

                    return Source
                        .From(source)
                        .Via(_deserializeFlowMapped);
                });
        }

        private async Task QueueWriteJournalRows(Seq<JournalRow<TJournalPayload>> xs)
        {
            var promise = new TaskCompletionSource<NotUsed>(TaskCreationOptions.RunContinuationsAsynchronously);

            // Send promise and rows into queue. If the Queue takes it,
            // It will write the Promise state when finished writing (or failing)
            var result = await WriteQueue.OfferAsync(new WriteQueueEntry<TJournalPayload>(promise, xs));

            switch (result)
            {
                case QueueOfferResult.Enqueued:
                    break;

                case QueueOfferResult.Failure f:
                    promise.TrySetException(new Exception("Failed to write journal row batch", f.Cause));
                    break;

                case QueueOfferResult.Dropped:
                    promise.TrySetException(
                        new Exception(
                            $"Failed to enqueue journal row batch write, the queue buffer was full ({JournalConfig.DaoConfig.BufferSize} elements)"));
                    break;

                case QueueOfferResult.QueueClosed:
                    promise.TrySetException(
                        new Exception(
                            "Failed to enqueue journal row batch write, the queue was closed."));
                    break;
            }

            await promise.Task;
        }

        private async Task WriteJournalRows(Seq<JournalRow<TJournalPayload>> xs)
        {
            switch (xs.Count)
            {
                case 0:
                    break;

                // hot path:
                // If we only have one row, penalty for BulkCopy
                // Isn't worth it due to insert caching/etc.
                case 1 when _tagWriteMode == TagMode.Csv || xs.Head().TagArray.Length == 0:
                {
                    await ConnectionFactory.ExecuteWithTransactionAsync(
                        WriteIsolationLevel,
                        ShutdownToken,
                        async (connection, token) => await connection.InsertAsync(xs.Head, token));
                    break;
                }

                default:
                    await InsertMultiple(xs);
                    break;
            }
        }

        private async Task InsertMultiple(Seq<JournalRow<TJournalPayload>> xs)
        {
            await ConnectionFactory.ExecuteWithTransactionAsync(
                WriteIsolationLevel,
                ShutdownToken,
                async (connection, token) =>
                {
                    if (_tagWriteMode == TagMode.Csv)
                    {
                        await BulkInsertNoTagTableTags(connection, xs, JournalConfig.DaoConfig, token);
                    }
                    else
                    {
                        var config = JournalConfig.DaoConfig;
                        var tail = xs;
                        while (tail.Count > 0)
                        {
                            (var noTags, tail) = tail.Span(r => r.TagArray.Length == 0);
                            if (noTags.Count > 0)
                                await BulkInsertNoTagTableTags(connection, noTags, config, token);

                            (var hasTags, tail) = tail.Span(r => r.TagArray.Length > 0);
                            if (hasTags.Count > 0)
                                await InsertWithOrderingAndBulkInsertTags(connection, hasTags, config, token);
                        }
                    }
                });
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static async Task InsertWithOrderingAndBulkInsertTags(
            AkkaDataConnection<TJournalPayload> connection,
            Seq<JournalRow<TJournalPayload>> xs,
            BaseByteArrayJournalDaoConfig config,
            CancellationToken token)
        {
            var tagsToInsert = new List<JournalTagRow>(xs.Count);

            // We could not do bulk copy and retrieve the inserted ids
            // Issue: https://github.com/linq2db/linq2db/issues/2960
            // We're forced to insert the rows one by one.
            foreach (var journalRow in xs)
            {
                var dbId = await connection.InsertWithInt64IdentityAsync(journalRow, token);

                tagsToInsert.AddRange(
                    journalRow.TagArray.Select(
                        s1 => new JournalTagRow
                        {
                            OrderingId = dbId,
                            TagValue = s1,
                            PersistenceId = journalRow.PersistenceId,
                            SequenceNumber = journalRow.SequenceNumber,
                        }));
            }

            await connection
                .GetTable<JournalTagRow>()
                .BulkCopyAsync(
                    new BulkCopyOptions()
                        .WithBulkCopyType(BulkCopyType.MultipleRows)
                        .WithUseParameters(config.PreferParametersOnMultiRowInsert)
                        .WithMaxBatchSize(config.DbRoundTripTagBatchSize),
                    tagsToInsert,
                    token);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static async Task BulkInsertNoTagTableTags(
            AkkaDataConnection<TJournalPayload> connection,
            Seq<JournalRow<TJournalPayload>> xs,
            BaseByteArrayJournalDaoConfig config,
            CancellationToken token)
            => await connection
                .GetTable<JournalRow<TJournalPayload>>()
                .BulkCopyAsync(
                    new BulkCopyOptions()
                        .WithBulkCopyType(
                            xs.Count > config.MaxRowByRowSize
                                ? BulkCopyType.Default
                                : BulkCopyType.MultipleRows)
                        .WithUseParameters(config.PreferParametersOnMultiRowInsert)
                        .WithMaxBatchSize(config.DbRoundTripBatchSize),
                    xs,
                    token);

        // By using a custom flatten here, we avoid an Enumerable/LINQ allocation
        // And are able to have a little more control over default capacity of array.
        private static IEnumerable<JournalRow<TJournalPayload>> FlattenListOfListsToList(List<Util.Try<JournalRow<TJournalPayload>[]>> source)
        {
            var rows = new List<JournalRow<TJournalPayload>>(
                source.Count > 4
                    ? source.Count
                    : 4);

            foreach (var t in source)
            {
                var item = t.Success.Value;
                if (item is not null)
                    rows.AddRange(item);
            }

            return rows;
        }

        protected static ImmutableList<Exception> BuildWriteRejections(List<Util.Try<JournalRow<TJournalPayload>[]>> serializedTries)
        {
            var builderEx = new Exception[serializedTries.Count];
            for (var i = 0; i < serializedTries.Count; i++)
                builderEx[i] = serializedTries[i].Failure.Value;

            return ImmutableList.CreateRange(builderEx);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static IQueryable<long> MaxMarkedForDeletionMaxPersistenceIdQuery(
            AkkaDataConnection<TJournalPayload> connection,
            string persistenceId)
            => connection
                .GetTable<JournalRow<TJournalPayload>>()
                .Where(r => r.PersistenceId == persistenceId && r.Deleted)
                .OrderByDescending(r => r.SequenceNumber)
                .Select(r => r.SequenceNumber)
                .Take(1);


        private static Func<Util.Try<(IPersistentRepresentation, IImmutableSet<string>, long)>, Util.Try<ReplayCompletion>> MessageWithBatchMapper()
            => x => x.IsSuccess
                ? new Util.Try<ReplayCompletion>(new ReplayCompletion(x.Success.Value))
                : new Util.Try<ReplayCompletion>(x.Failure.Value);
    }
}
