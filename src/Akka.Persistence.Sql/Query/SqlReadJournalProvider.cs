// -----------------------------------------------------------------------
//  <copyright file="SqlReadJournalProvider.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Actor;
using Akka.Annotations;
using Akka.Persistence.Query;
using Akka.Serialization;

namespace Akka.Persistence.Sql.Query;

public sealed class SqlReadJournalProvider(ExtendedActorSystem system, Configuration.Config config)
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

    /// <summary>
    ///     Note that this is safe to do because the only place this is being called is
    ///     inside the `PersistenceQuery.ReadJournalFor{T}()` public method inside
    ///     Akka.Persistence.Query and the result of that method call is then cached
    ///     and reused for the duration of the ActorSystem lifetime.
    /// </summary>
    /// <returns>
    ///     A new instance of IReadJournal specific to this persistence plugin.
    /// </returns>
    [InternalApi]
    public IReadJournal GetReadJournal()
        => new SqlReadJournal<TJournalPayload>(system, _config, toPayload, fromPayload);

}

