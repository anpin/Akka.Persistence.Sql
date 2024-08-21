// -----------------------------------------------------------------------
//  <copyright file="WriteQueueSet.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Immutable;
using System.Threading.Tasks;
using LanguageExt;

namespace Akka.Persistence.Sql.Journal.Types
{
    public sealed class WriteQueueSet<T>
    {
        public WriteQueueSet(ImmutableList<TaskCompletionSource<NotUsed>> tcs, Seq<JournalRow<T>> rows)
        {
            Tcs = tcs;
            Rows = rows;
        }

        public Seq<JournalRow<T>> Rows { get; }

        public ImmutableList<TaskCompletionSource<NotUsed>> Tcs { get; }
    }
}
