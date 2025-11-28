// -----------------------------------------------------------------------
//  <copyright file="SnapshotTableConfiguration.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Configuration;

namespace Akka.Persistence.Sql.Config
{
    public class SnapshotTableConfiguration<TJournalPayload>: IEquatable<SnapshotTableConfiguration<TJournalPayload>>
    {
        public SnapshotTableConfiguration(Configuration.Config config)
        {
            var mappingPath = config.GetString("table-mapping");
            if (string.IsNullOrEmpty(mappingPath))
                throw new ConfigurationException(
                    "The configuration property akka.persistence.journal.sql.table-mapping is null or empty");

            var mappingConfig = config.GetConfig(mappingPath) ?? throw new ConfigurationException(
                $"The configuration path akka.persistence.journal.sql.{mappingPath} does not exist");

            if (mappingPath != "default")
                mappingConfig.WithFallback(SqlPersistence<TJournalPayload>.DefaultSnapshotMappingConfiguration);

            SchemaName = mappingConfig.GetString("schema-name");

            SnapshotTable = new SnapshotTableConfig(mappingConfig);
        }

        public SnapshotTableConfig SnapshotTable { get; }

        public string? SchemaName { get; }

        public bool Equals(SnapshotTableConfiguration<TJournalPayload>? other)
        {
            if (other is null)
                return false;

            if (ReferenceEquals(this, other))
                return true;

            return SnapshotTable.Equals(other.SnapshotTable) && SchemaName == other.SchemaName;
        }

        public override bool Equals(object? obj)
        {
            if (obj is null)
                return false;

            if (ReferenceEquals(this, obj))
                return true;

            return obj is SnapshotTableConfiguration<TJournalPayload> other && Equals(other);
        }

        public override int GetHashCode()
            => HashCode.Combine(SnapshotTable, SchemaName);
    }
}
