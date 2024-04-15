// -----------------------------------------------------------------------
//  <copyright file="JournalRow.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

// using System.Text.Json;

namespace Akka.Persistence.Sql.Journal.Types
{
    public sealed class JournalRow<T>
    {
        public long Ordering { get; set; }

        public long Timestamp { get; set; } = 0;

        public bool Deleted { get; set; }

        public string PersistenceId { get; set; }

        public long SequenceNumber { get; set; }

        public T Message { get; set; }
        // public JsonDocument Message { get; set; }
        // public JsonDocument Message { get; set; }
        // public byte[] Message { get; set; }

        public string Tags { get; set; }

        public string Manifest { get; set; }

        public int? Identifier { get; set; }

        // ReSharper disable once InconsistentNaming
        public string[] TagArray { get; set; }

        public string WriterUuid { get; set; }

        public string EventManifest { get; set; }
    }
}
