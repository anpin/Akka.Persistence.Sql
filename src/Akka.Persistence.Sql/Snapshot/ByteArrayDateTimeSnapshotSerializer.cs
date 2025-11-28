// -----------------------------------------------------------------------
//  <copyright file="ByteArrayDateTimeSnapshotSerializer.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Akka.Persistence.Sql.Config;
using Akka.Serialization;
using Akka.Util;

namespace Akka.Persistence.Sql.Snapshot
{
    public class ByteArrayDateTimeSnapshotSerializer<TJournalPayload> : ISnapshotSerializer<DateTimeSnapshotRow<TJournalPayload>>
    {
        private readonly SnapshotConfig<TJournalPayload> _config;
        private readonly Akka.Serialization.Serialization _serialization;

        private readonly Func<(Serializer, object), TJournalPayload> _toPayload;
        private readonly Func<(Serializer, TJournalPayload, Type), object> _fromPayload;
        public ByteArrayDateTimeSnapshotSerializer(
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

        public Try<DateTimeSnapshotRow<TJournalPayload>> Serialize(SnapshotMetadata metadata, object snapshot)
            => Try<DateTimeSnapshotRow<TJournalPayload>>.From(() => ToSnapshotEntry(metadata, snapshot));

        public Try<SelectedSnapshot> Deserialize(DateTimeSnapshotRow<TJournalPayload> t)
            => Try<SelectedSnapshot>.From(() => ReadSnapshot(t));

        protected SelectedSnapshot ReadSnapshot(DateTimeSnapshotRow<TJournalPayload> reader)
        {
            var metadata = new SnapshotMetadata(
                reader.PersistenceId,
                reader.SequenceNumber,
                reader.Created);

            var snapshot = GetSnapshot(reader);

            return new SelectedSnapshot(metadata, snapshot);
        }

        protected object GetSnapshot(DateTimeSnapshotRow<TJournalPayload> reader)
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

        private DateTimeSnapshotRow<TJournalPayload> ToSnapshotEntry(SnapshotMetadata metadata, object snapshot)
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

            return new DateTimeSnapshotRow<TJournalPayload>
            {
                PersistenceId = metadata.PersistenceId,
                SequenceNumber = metadata.SequenceNr,
                Created = metadata.Timestamp,
                Manifest = manifest,
                Payload = binary,
                SerializerId = serializer.Identifier,
            };
        }
    }
}
