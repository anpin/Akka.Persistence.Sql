// -----------------------------------------------------------------------
//  <copyright file="ByteArrayLongSnapshotSerializer.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Persistence.Sql.Config;
using Akka.Serialization;
using Akka.Util;

namespace Akka.Persistence.Sql.Snapshot
{
    public class ByteArrayLongSnapshotSerializer<TJournalPayload> : ISnapshotSerializer<LongSnapshotRow<TJournalPayload>>
    {
        private readonly SnapshotConfig<TJournalPayload> _config;
        private readonly Akka.Serialization.Serialization _serialization;

        private readonly Func<(Serializer, object), TJournalPayload> _toPayload;
        private readonly Func<(Serializer, TJournalPayload, Type), object> _fromPayload;
        public ByteArrayLongSnapshotSerializer(
            Akka.Serialization.Serialization serialization,
            SnapshotConfig<TJournalPayload> config,
            Func<(Serializer, object), TJournalPayload> toPayload,
            Func<(Serializer, TJournalPayload, Type), object> fromPayload)
        {
            _serialization = serialization;
            _config = config;
            _toPayload = toPayload;
            _fromPayload = fromPayload;
        }

        public Try<LongSnapshotRow<TJournalPayload>> Serialize(SnapshotMetadata metadata, object snapshot)
            => Try<LongSnapshotRow<TJournalPayload>>.From(() => ToSnapshotEntry(metadata, snapshot));

        public Try<SelectedSnapshot> Deserialize(LongSnapshotRow<TJournalPayload> t)
            => Try<SelectedSnapshot>.From(() => ReadSnapshot(t));

        protected SelectedSnapshot ReadSnapshot(LongSnapshotRow<TJournalPayload> reader)
        {
            var metadata = new SnapshotMetadata(
                reader.PersistenceId,
                reader.SequenceNumber,
                new DateTime(reader.Created));

            var snapshot = GetSnapshot(reader);

            return new SelectedSnapshot(metadata, snapshot);
        }

        protected object GetSnapshot(LongSnapshotRow<TJournalPayload> reader)
        {
            var manifest = reader.Manifest;
            var payload = reader.Payload;

            if (reader.SerializerId is not null)
            {
                var type = Type.GetType(manifest, true);

                // TODO: hack. Replace when https://github.com/akkadotnet/akka.net/issues/3811
                return Akka.Serialization.Serialization.WithTransport(
                    system: _serialization.System,
                    state: (
                        serializer: _serialization.FindSerializerForType(type, _config.DefaultSerializer),
                        binary: payload,
                        type),
                    action: _fromPayload);
            }
            if (payload is byte[] binary)
            {
                var serializerId = reader.SerializerId.Value;
                return  _serialization.Deserialize(binary, serializerId, manifest);
            }
            throw new ArgumentOutOfRangeException(nameof(reader), reader, "failed to deserialize snapshot");
        }

        private LongSnapshotRow<TJournalPayload> ToSnapshotEntry(SnapshotMetadata metadata, object snapshot)
        {
            var snapshotType = snapshot.GetType();
            var serializer = _serialization.FindSerializerForType(snapshotType, _config.DefaultSerializer);
            var binary = Akka.Serialization.Serialization.WithTransport(
                system: _serialization.System,
                state: (serializer, snapshot),
                action: _toPayload); //state => state.serializer.ToBinary(state.snapshot));

            var manifest = serializer switch
            {
                SerializerWithStringManifest stringManifest => stringManifest.Manifest(snapshot),
                { IncludeManifest: true } => snapshotType.TypeQualifiedName(),
                _ => string.Empty,
            };

            return new LongSnapshotRow<TJournalPayload>
            {
                PersistenceId = metadata.PersistenceId,
                SequenceNumber = metadata.SequenceNr,
                Created = metadata.Timestamp.Ticks,
                Manifest = manifest,
                Payload = binary,
                SerializerId = serializer.Identifier,
            };
        }
    }
}
