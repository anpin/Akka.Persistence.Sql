// -----------------------------------------------------------------------
//  <copyright file="SqlReadJournal.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Pattern;
using Akka.Persistence.Journal;
using Akka.Persistence.Query;
using Akka.Persistence.Sql.Config;
using Akka.Persistence.Sql.Db;
using Akka.Persistence.Sql.Journal;
using Akka.Persistence.Sql.Journal.Dao;
using Akka.Persistence.Sql.Query.Dao;
using Akka.Persistence.Sql.Query.InternalProtocol;
using Akka.Persistence.Sql.Utility;
using Akka.Serialization;
using Akka.Streams;
using Akka.Streams.Dsl;
using Akka.Util;

namespace Akka.Persistence.Sql.Query
{
    public sealed class SqlReadJournal<TJournalPayload>
        : SqlReadJournal<TJournalPayload, ByteArrayReadJournalDao<TJournalPayload>>
    {
        public SqlReadJournal(
            ExtendedActorSystem system
            , Configuration.Config config
            , Func<(Serializer, object), TJournalPayload> toPayload
            , Func<(Serializer, TJournalPayload, Type), object> fromPayload)
            : base(system, config
                , (mat, readJournalConfig) =>
                    new ByteArrayReadJournalDao<TJournalPayload>(
                        scheduler: system.Scheduler.Advanced,
                        materializer: mat,
                        connectionFactory: new AkkaPersistenceDataConnectionFactory<TJournalPayload>(readJournalConfig),
                        readJournalConfig: readJournalConfig,
                        serializer: new ByteArrayJournalSerializer<TJournalPayload>(
                            journalConfig: readJournalConfig,
                            serializer: system.Serialization,
                            separator: readJournalConfig.PluginConfig.TagSeparator,
                            writerUuid: null,
                            toPayload: toPayload,
                            fromPayload: fromPayload),
                        // TODO: figure out a way to signal shutdown to the query executor here
                        default))
        {
        }
    }

    public class SqlReadJournal<TJournalPayload, TReaderDao> :
        IPersistenceIdsQuery,
        ICurrentPersistenceIdsQuery,
        IEventsByPersistenceIdQuery,
        ICurrentEventsByPersistenceIdQuery,
        IEventsByTagQuery,
        ICurrentEventsByTagQuery,
        IAllEventsQuery,
        ICurrentAllEventsQuery
        where TReaderDao : BaseByteReadArrayJournalDao<TJournalPayload>
    {
        // ReSharper disable once UnusedMember.Global
        [Obsolete(message: "Use SqlPersistence.Get(ActorSystem).DefaultConfig instead")]
        public static readonly Configuration.Config DefaultConfiguration = SqlWriteJournal<TJournalPayload>.DefaultConfiguration;

        private readonly Source<long, ICancelable> _delaySource;
        private readonly EventAdapters _eventAdapters;
        private readonly IActorRef _journalSequenceActor;
        private readonly ActorMaterializer _mat;
        private readonly ExtendedActorSystem _system;
        protected readonly ReadJournalConfig<TJournalPayload> ReadJournalConfig;
        protected readonly TReaderDao ReadJournalDao;



        public SqlReadJournal(
            ExtendedActorSystem system,
            Configuration.Config config,
            Func<IMaterializer, ReadJournalConfig<TJournalPayload>, TReaderDao> readerFactory
        )
        {
            ReadJournalConfig = new ReadJournalConfig<TJournalPayload>(config);

            var setup = system.Settings.Setup;
            var singleSetup = setup.Get<DataOptionsSetup<TJournalPayload>>();
            if (singleSetup.HasValue)
                ReadJournalConfig = singleSetup.Value.Apply(ReadJournalConfig);

            if (ReadJournalConfig.PluginId is not null)
            {
                var multiSetup = setup.Get<MultiDataOptionsSetup>();
                if (multiSetup.HasValue && multiSetup.Value.TryGetDataOptionsFor(ReadJournalConfig.PluginId, out var dataOptions))
                    ReadJournalConfig = ReadJournalConfig.WithDataOptions(dataOptions);
            }

            _eventAdapters = Persistence.Instance.Apply(system).AdaptersFor(ReadJournalConfig.WritePluginId);

            // Fix for https://github.com/akkadotnet/Akka.Persistence.Sql/issues/344
            var writeJournal = Persistence.Instance.Apply(system).JournalFor(ReadJournalConfig.WritePluginId);
            // we want to block, we want to crash if the journal is not available
            var started = writeJournal.Ask<Initialized>(IsInitialized.Instance, TimeSpan.FromSeconds(5)).Result;

            _system = system;

            _mat = Materializer.CreateSystemMaterializer(
                context: system,
                settings: ActorMaterializerSettings.Create(system),
                namePrefix: $"l2db-query-mat-{Guid.NewGuid():N}");

            ReadJournalDao = readerFactory(_mat, ReadJournalConfig);

            _journalSequenceActor = system.ActorOf(
                props: Props.Create(
                    () => new JournalSequenceActor(
                        ReadJournalDao,
                        ReadJournalConfig.JournalSequenceRetrievalConfiguration)),
                name: $"{ReadJournalConfig.TableConfig.EventJournalTable.Name}akka-persistence-sql-sequence-actor");

            _delaySource = Source.Tick(TimeSpan.FromSeconds(0), ReadJournalConfig.RefreshInterval, 0L).Take(1);
        }

        public static string Identifier => "akka.persistence.query.journal.sql";

        protected Task<MaxOrderingId> QueryUntil() =>
            _journalSequenceActor
                    .Ask<MaxOrderingId>(
                        GetMaxOrderingId.Instance,
                        ReadJournalConfig.JournalSequenceRetrievalConfiguration.AskTimeout);

        public Source<EventEnvelope, NotUsed> AllEvents(Offset offset)
            => Events(
                offset is Sequence s
                    ? s.Value
                    : 0,
                null);

        public Source<EventEnvelope, NotUsed> CurrentAllEvents(Offset offset)
            => AsyncSource<long>
                .FromEnumerable(
                    state: ReadJournalDao,
                    func: static async input => new[] { await input.MaxJournalSequenceAsync() })
                .ConcatMany(
                    maxInDb =>
                        Events(
                            offset is Sequence s
                                ? s.Value
                                : 0,
                            maxInDb));

        public Source<EventEnvelope, NotUsed> CurrentEventsByPersistenceId(
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr)
            => EventsByPersistenceIdSource(
                persistenceId: persistenceId,
                fromSequenceNr: fromSequenceNr,
                toSequenceNr: toSequenceNr,
                refreshInterval: Option<(TimeSpan, IScheduler)>.None);

        public Source<EventEnvelope, NotUsed> CurrentEventsByTag(string tag, Offset offset)
            => CurrentEventsByTag(tag, (offset as Sequence)?.Value ?? 0);

        public Source<string, NotUsed> CurrentPersistenceIds()
            => ReadJournalDao.AllPersistenceIdsSource(long.MaxValue);

        public Source<EventEnvelope, NotUsed> EventsByPersistenceId(
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr)
            => EventsByPersistenceIdSource(
                persistenceId: persistenceId,
                fromSequenceNr: fromSequenceNr,
                toSequenceNr: toSequenceNr,
                refreshInterval: Option<(TimeSpan, IScheduler)>.Create(
                    (ReadJournalConfig.RefreshInterval, _system.Scheduler)));

        public Source<EventEnvelope, NotUsed> EventsByTag(string tag, Offset offset)
            => EventsByTag(
                tag,
                offset is Sequence s
                    ? s.Value
                    : 0,
                null);

        public Source<string, NotUsed> PersistenceIds()
            => Source
                .Repeat(0L)
                .ConcatMany(
                    _ =>
                        _delaySource
                            .MapMaterializedValue(_ => NotUsed.Instance)
                            .ConcatMany(_ => CurrentPersistenceIds()))
                .StatefulSelectMany<string, string, NotUsed>(
                    () =>
                    {
                        var knownIds = ImmutableHashSet<string>.Empty;

                        IEnumerable<string> Next(string id)
                        {
                            var xs = ImmutableHashSet<string>.Empty.Add(id).Except(knownIds);
                            knownIds = knownIds.Add(id);
                            return xs;
                        }

                        return Next;
                    });

        protected IImmutableList<IPersistentRepresentation> AdaptEvents(
            IPersistentRepresentation persistentRepresentation)
            => _eventAdapters
                .Get(persistentRepresentation.Payload.GetType())
                .FromJournal(persistentRepresentation.Payload, persistentRepresentation.Manifest)
                .Events
                .Select(persistentRepresentation.WithPayload)
                .ToImmutableList();

        private Source<EventEnvelope, NotUsed> EventsByPersistenceIdSource(
            string persistenceId,
            long fromSequenceNr,
            long toSequenceNr,
            Option<(TimeSpan, IScheduler)> refreshInterval)
            => ReadJournalDao
                .MessagesWithBatch(persistenceId, fromSequenceNr, toSequenceNr, ReadJournalConfig.MaxBufferSize,
                    refreshInterval)
                .SelectAsync(1, representationAndOrdering => Task.FromResult(representationAndOrdering.Get()))
                .SelectMany(r => AdaptEvents(r.Representation).Select(_ => new { representation = r.Representation, ordering = r.Ordering, tags = r.Tags}))
                .Select(
                    r =>
                        new EventEnvelope(
                            offset: new Sequence(r.ordering),
                            persistenceId: r.representation.PersistenceId,
                            sequenceNr: r.representation.SequenceNr,
                            @event: r.representation.Payload,
                            timestamp: r.representation.Timestamp,
                            tags: r.tags));

        private Source<EventEnvelope, NotUsed> CurrentJournalEvents(long offset, long max, MaxOrderingId latestOrdering)
        {
            if (latestOrdering.Max < offset)
                return Source.Empty<EventEnvelope>();

            return ReadJournalDao
                .Events(offset, latestOrdering.Max, max)
                .SelectAsync(1, r => Task.FromResult(r.Get()))
                .SelectMany(
                    a =>
                    {
                        var (representation, tags, ordering) = a;
                        return AdaptEvents(representation)
                            .Select(
                                r =>
                                    new EventEnvelope(
                                        offset: new Sequence(ordering),
                                        persistenceId: r.PersistenceId,
                                        sequenceNr: r.SequenceNr,
                                        @event: r.Payload,
                                        timestamp: r.Timestamp,
                                        tags: tags));
                    });
        }

        private Source<EventEnvelope, NotUsed> CurrentJournalEventsByTag(
            string tag,
            long offset,
            long max,
            MaxOrderingId latestOrdering)
        {
            if (latestOrdering.Max < offset)
                return Source.Empty<EventEnvelope>();

            return ReadJournalDao
                .EventsByTag(tag, offset, latestOrdering.Max, max)
                .SelectAsync(1, r => Task.FromResult(r.Get()))
                .SelectMany(
                    a =>
                    {
                        var (representation, tags, ordering) = a;
                        return AdaptEvents(representation)
                            .Select(
                                r =>
                                    new EventEnvelope(
                                        offset: new Sequence(ordering),
                                        persistenceId: r.PersistenceId,
                                        sequenceNr: r.SequenceNr,
                                        @event: r.Payload,
                                        timestamp: r.Timestamp,
                                        tags: tags));
                    });
        }

        private Source<EventEnvelope, NotUsed> EventsByTag(string tag, long offset, long? terminateAfterOffset)
        {
            var batchSize = ReadJournalConfig.MaxBufferSize;

            return Source
                .UnfoldAsync<(long offset, FlowControlEnum flowControl), IImmutableList<EventEnvelope>>(
                    (offset, FlowControlEnum.Continue),
                    uf =>
                    {
                        async Task<Option<((long, FlowControlEnum), IImmutableList<EventEnvelope>)>> RetrieveNextBatch()
                        {
                            var queryUntil = await QueryUntil();

                            var xs = await CurrentJournalEventsByTag(tag, uf.offset, batchSize, queryUntil)
                                .RunWith(Sink.Seq<EventEnvelope>(), _mat);

                            var hasMoreEvents = xs.Count == batchSize;

                            var nextControl = FlowControlEnum.Unknown;
                            if (terminateAfterOffset.HasValue)
                            {
                                if (!hasMoreEvents && terminateAfterOffset.Value <= queryUntil.Max)
                                    nextControl = FlowControlEnum.Stop;

                                if (xs.Exists(r => r.Offset is Sequence s && s.Value >= terminateAfterOffset.Value))
                                    nextControl = FlowControlEnum.Stop;
                            }

                            if (nextControl == FlowControlEnum.Unknown)
                            {
                                nextControl = hasMoreEvents
                                    ? FlowControlEnum.Continue
                                    : FlowControlEnum.ContinueDelayed;
                            }

                            var nextStartingOffset = xs.Count == 0
                                ? Math.Max(uf.offset, queryUntil.Max)
                                : xs.Select(r => r.Offset as Sequence)
                                    .Max(t => t?.Value ?? long.MinValue);

                            return Option<((long, FlowControlEnum), IImmutableList<EventEnvelope>)>.Create(
                                ((nextStartingOffset, nextControl), xs));
                        }

                        return uf.flowControl switch
                        {
                            FlowControlEnum.Stop =>
                                Task.FromResult(
                                    Option<((long, FlowControlEnum), IImmutableList<EventEnvelope>)>.None),

                            FlowControlEnum.Continue =>
                                RetrieveNextBatch(),

                            FlowControlEnum.ContinueDelayed =>
                                FutureTimeoutSupport.After(
                                    duration: ReadJournalConfig.RefreshInterval,
                                    scheduler: _system.Scheduler,
                                    value: RetrieveNextBatch),

                            _ => Task.FromResult(
                                Option<((long, FlowControlEnum), IImmutableList<EventEnvelope>)>.None),
                        };
                    }).SelectMany(r => r);
        }

        private Source<EventEnvelope, NotUsed> CurrentEventsByTag(string tag, long offset)
            => AsyncSource<long>
                .FromEnumerable(
                    state: new { readJournalDao = ReadJournalDao },
                    func: static async input => new[] { await input.readJournalDao.MaxJournalSequenceAsync() })
                .ConcatMany(maxInDb => EventsByTag(tag, offset, maxInDb));

        private Source<EventEnvelope, NotUsed> Events(long offset, long? terminateAfterOffset)
        {
            var batchSize = ReadJournalConfig.MaxBufferSize;

            return Source
                .UnfoldAsync<(long offset, FlowControlEnum flowControl), IImmutableList<EventEnvelope>>(
                    (offset, FlowControlEnum.Continue),
                    uf =>
                    {
                        async Task<Option<((long, FlowControlEnum), IImmutableList<EventEnvelope>)>> RetrieveNextBatch()
                        {
                            var queryUntil = await QueryUntil();

                            var xs = await CurrentJournalEvents(uf.offset, batchSize, queryUntil)
                                .RunWith(Sink.Seq<EventEnvelope>(), _mat);

                            var hasMoreEvents = xs.Count == batchSize;

                            var nextControl = FlowControlEnum.Unknown;
                            if (terminateAfterOffset.HasValue)
                            {
                                if (!hasMoreEvents && terminateAfterOffset.Value <= queryUntil.Max)
                                    nextControl = FlowControlEnum.Stop;

                                if (xs.Exists(r => r.Offset is Sequence s && s.Value >= terminateAfterOffset.Value))
                                    nextControl = FlowControlEnum.Stop;
                            }

                            if (nextControl == FlowControlEnum.Unknown)
                            {
                                nextControl = hasMoreEvents
                                    ? FlowControlEnum.Continue
                                    : FlowControlEnum.ContinueDelayed;
                            }

                            var nextStartingOffset = xs.Count == 0
                                ? Math.Max(uf.offset, queryUntil.Max)
                                : xs.Select(r => r.Offset as Sequence)
                                    .Max(t => t?.Value ?? long.MinValue);

                            return Option<((long nextStartingOffset, FlowControlEnum nextControl),
                                IImmutableList<EventEnvelope>xs)>.Create(
                                ((nextStartingOffset, nextControl), xs));
                        }

                        return uf.flowControl switch
                        {
                            FlowControlEnum.Stop =>
                                Task.FromResult(Option<((long, FlowControlEnum), IImmutableList<EventEnvelope>)>.None),

                            FlowControlEnum.Continue =>
                                RetrieveNextBatch(),

                            FlowControlEnum.ContinueDelayed =>
                                FutureTimeoutSupport.After(
                                    ReadJournalConfig.RefreshInterval,
                                    _system.Scheduler,
                                    RetrieveNextBatch),

                            _ => Task.FromResult(
                                Option<((long, FlowControlEnum), IImmutableList<EventEnvelope>)>.None),
                        };
                    }).SelectMany(r => r);
        }
    }
}
