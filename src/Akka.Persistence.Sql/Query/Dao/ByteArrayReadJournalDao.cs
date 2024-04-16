// -----------------------------------------------------------------------
//  <copyright file="ByteArrayReadJournalDao.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading;
using Akka.Actor;
using Akka.Persistence.Sql.Config;
using Akka.Persistence.Sql.Db;
using Akka.Persistence.Sql.Journal.Types;
using Akka.Persistence.Sql.Serialization;
using Akka.Streams;

namespace Akka.Persistence.Sql.Query.Dao
{
    public sealed class ByteArrayReadJournalDao<TJournalPayload> : BaseByteReadArrayJournalDao<TJournalPayload>
    {
        public ByteArrayReadJournalDao(
            IAdvancedScheduler scheduler,
            IMaterializer materializer,
            AkkaPersistenceDataConnectionFactory<TJournalPayload> connectionFactory,
            ReadJournalConfig<TJournalPayload> readJournalConfig,
            FlowPersistentRepresentationSerializer<JournalRow<TJournalPayload>> serializer,
            CancellationToken shutdownToken)
            : base(scheduler, materializer, connectionFactory, readJournalConfig, serializer, shutdownToken) { }
    }
}
