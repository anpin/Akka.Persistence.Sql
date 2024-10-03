// -----------------------------------------------------------------------
//  <copyright file="SqlSnapshotStore.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Persistence.Snapshot;
using Akka.Persistence.Sql.Config;
using Akka.Persistence.Sql.Db;
using Akka.Persistence.Sql.Utility;
using Akka.Serialization;

namespace Akka.Persistence.Sql.Snapshot;

public class SqlSnapshotStore(Configuration.Config snapshotConfig) : SqlSnapshotStore<byte[]>(snapshotConfig
    , (s) => s.Item1.ToBinary(s.Item2)
    , (s) => s.Item1.FromBinary(s.Item2, s.Item3));

public class SqlSnapshotStore<TJournalPayload> : SnapshotStore, IWithUnboundedStash
{
    // ReSharper disable once UnusedMember.Global
    [Obsolete(message: "Use SqlPersistence.Get(ActorSystem).DefaultConfig instead")]
    public static readonly Configuration.Config DefaultConfiguration =
        ConfigurationFactory.FromResource<SqlSnapshotStore<TJournalPayload>>("Akka.Persistence.Sql.snapshot.conf");

    private readonly ByteArraySnapshotDao<TJournalPayload> _dao;
    private readonly ILoggingAdapter _log;
    private readonly SnapshotConfig<TJournalPayload> _settings;

    public SqlSnapshotStore(Configuration.Config snapshotConfig,
        Func<(Serializer, object), TJournalPayload> toPayload,
        Func<(Serializer, TJournalPayload, Type), object> fromPayload)
    {
        _log = Context.GetLogger();

        var config = snapshotConfig.WithFallback(SqlPersistence<TJournalPayload>.DefaultSnapshotConfiguration);
        _settings = new SnapshotConfig<TJournalPayload>(config);

        var setup = Context.System.Settings.Setup;
        var singleSetup = setup.Get<DataOptionsSetup<TJournalPayload>>();
        if (singleSetup.HasValue)
            _settings = singleSetup.Value.Apply(_settings);
        if (_settings.PluginId is not null)
        {
            var multiSetup = setup.Get<MultiDataOptionsSetup>();
            if (multiSetup.HasValue && multiSetup.Value.TryGetDataOptionsFor(_settings.PluginId, out var dataOptions))
                _settings = _settings.WithDataOptions(dataOptions);
        }

        _dao = new ByteArraySnapshotDao<TJournalPayload>(
            connectionFactory: new AkkaPersistenceDataConnectionFactory<TJournalPayload>(_settings),
            snapshotConfig: _settings,
            serialization: Context.System.Serialization,
            materializer: Materializer.CreateSystemMaterializer((ExtendedActorSystem)Context.System),
            logger: Context.GetLogger(),
            toPayload: toPayload,
            fromPayload: fromPayload
        );
    }

    public IStash Stash { get; set; }

    protected override void PreStart()
    {
        base.PreStart();
        Initialize().PipeTo(Self);
        BecomeStacked(WaitingForInitialization);
    }

    private bool WaitingForInitialization(object message)
    {
        switch (message)
        {
            case Status.Success:
                UnbecomeStacked();
                Stash.UnstashAll();
                return true;

            case Status.Failure msg:
                _log.Error(msg.Cause, "Error during {0} initialization", Self);
                Context.Stop(Self);
                return true;

            default:
                Stash.Stash();
                return true;
        }
    }

    private async Task<Status> Initialize()
    {
        if (!_settings.AutoInitialize)
            return new Status.Success(NotUsed.Instance);

        try
        {
            await _dao.InitializeTables();
        }
        catch (Exception e)
        {
            return new Status.Failure(e);
        }

        return Status.Success.Instance;
    }

    protected override async Task<SelectedSnapshot> LoadAsync(
        string persistenceId,
        SnapshotSelectionCriteria criteria)
        => criteria.MaxSequenceNr switch
        {
            long.MaxValue when criteria.MaxTimeStamp == DateTime.MaxValue
                => (await _dao.LatestSnapshotAsync(persistenceId)).GetOrElse(null),

            long.MaxValue
                => (await _dao.SnapshotForMaxTimestampAsync(persistenceId, criteria.MaxTimeStamp)).GetOrElse(null),

            _ => criteria.MaxTimeStamp == DateTime.MaxValue
                ? (await _dao.SnapshotForMaxSequenceNrAsync(
                    persistenceId: persistenceId,
                    sequenceNr: criteria.MaxSequenceNr)).GetOrElse(null)
                : (await _dao.SnapshotForMaxSequenceNrAndMaxTimestampAsync(
                    persistenceId: persistenceId,
                    sequenceNr: criteria.MaxSequenceNr,
                    timestamp: criteria.MaxTimeStamp)).GetOrElse(null),
        };

    protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        => await _dao.SaveAsync(metadata, snapshot);

    protected override async Task DeleteAsync(SnapshotMetadata metadata)
        => await _dao.DeleteAsync(metadata.PersistenceId, metadata.SequenceNr, metadata.Timestamp);

    protected override async Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
    {
        switch (criteria.MaxSequenceNr)
        {
            case long.MaxValue when criteria.MaxTimeStamp == DateTime.MaxValue:
                await _dao.DeleteAllSnapshotsAsync(persistenceId);
                break;

            case long.MaxValue:
                await _dao.DeleteUpToMaxTimestampAsync(persistenceId, criteria.MaxTimeStamp);
                break;

            default:
            {
                if (criteria.MaxTimeStamp == DateTime.MaxValue)
                {
                    await _dao.DeleteUpToMaxSequenceNrAsync(persistenceId, criteria.MaxSequenceNr);
                }
                else
                {
                    await _dao.DeleteUpToMaxSequenceNrAndMaxTimestampAsync(
                        persistenceId: persistenceId,
                        maxSequenceNr: criteria.MaxSequenceNr,
                        maxTimestamp: criteria.MaxTimeStamp);
                }

                break;
            }
        }
    }
}
