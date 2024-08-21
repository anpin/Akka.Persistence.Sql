// -----------------------------------------------------------------------
//  <copyright file="SqlReadJournalProvider.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Actor;
using Akka.Persistence.Query;
using Akka.Serialization;

namespace Akka.Persistence.Sql.Query;

public class SqlReadJournalProvider(ExtendedActorSystem system, Configuration.Config config)
    : SqlReadJournalProvider<byte[]>(system, config
        , (s) => s.Item1.ToBinary(s.Item2)
        , (s) => s.Item1.FromBinary(s.Item2, s.Item3));

public class SqlReadJournalProvider<TJournalPayload>(
    ExtendedActorSystem system,
    Configuration.Config config,
    Func<(Serializer, object), TJournalPayload> toPayload,
    Func<(Serializer, TJournalPayload, Type), object> fromPayload)
    : IReadJournalProvider
{
    private readonly Configuration.Config _config = config.WithFallback(SqlPersistence<TJournalPayload>.DefaultQueryConfiguration);

    public IReadJournal GetReadJournal()
        => new SqlReadJournal<TJournalPayload>(system, _config, toPayload, fromPayload);
}

