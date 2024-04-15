// -----------------------------------------------------------------------
//  <copyright file="SqlReadJournalProvider.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Actor;
using Akka.Persistence.Query;
using Akka.Serialization;

namespace Akka.Persistence.Sql.Query
{
    public class SqlReadJournalProvider<TJournalPayload> : IReadJournalProvider
    {
        private readonly Configuration.Config _config;
        private readonly ExtendedActorSystem _system;
        private readonly Func<(Serializer, object), TJournalPayload> _toPayload;
        private readonly Func<(Serializer, TJournalPayload, Type), object> _fromPayload;

        public SqlReadJournalProvider(
            ExtendedActorSystem system,
            Configuration.Config config,
            Func<(Serializer, object), TJournalPayload> toPayload,
            Func<(Serializer, TJournalPayload, Type), object> fromPayload)
        {
            _system = system;
            _toPayload = toPayload;
            _fromPayload = fromPayload;
            _config = config.WithFallback(SqlPersistence<TJournalPayload>.DefaultQueryConfiguration);
        }

        public IReadJournal GetReadJournal()
            => new SqlReadJournal<TJournalPayload>(_system, _config, _toPayload, _fromPayload);
    }
}
