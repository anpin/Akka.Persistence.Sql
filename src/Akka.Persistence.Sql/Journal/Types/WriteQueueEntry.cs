// -----------------------------------------------------------------------
//  <copyright file="WriteQueueEntry.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using LanguageExt;

namespace Akka.Persistence.Sql.Journal.Types
{
    public sealed class WriteQueueEntry<T>
    {
        public WriteQueueEntry(TaskCompletionSource<NotUsed> tcs, Seq<JournalRow<T>> rows)
        {
            Tcs = tcs;
            Rows = rows;
        }

        public Seq<JournalRow<T>> Rows { get; }

        public TaskCompletionSource<NotUsed> Tcs { get; }
    }
}
