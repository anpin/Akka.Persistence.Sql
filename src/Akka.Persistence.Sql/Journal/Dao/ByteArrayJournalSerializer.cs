﻿// -----------------------------------------------------------------------
//  <copyright file="ByteArrayJournalSerializer.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Immutable;
using System.Linq;
using System.Runtime.CompilerServices;
using Akka.Actor;
using Akka.Persistence.Sql.Config;
using Akka.Persistence.Sql.Journal.Types;
using Akka.Persistence.Sql.Serialization;
using Akka.Serialization;
using Akka.Util;

namespace Akka.Persistence.Sql.Journal.Dao
{
    /// <summary>
    ///     Serializes <see cref="IPersistentRepresentation" />
    /// </summary>
    public sealed class ByteArrayJournalSerializer<TJournalPayload> : FlowPersistentRepresentationSerializer<JournalRow<TJournalPayload>>
    {
        private readonly IProviderConfig<JournalTableConfig<TJournalPayload>> _journalConfig;
        private readonly string _separator;
        private readonly string[] _separatorArray;
        private readonly Akka.Serialization.Serialization _serializer;
        private readonly TagMode _tagWriteMode;
        private readonly string? _writerUuid;
        private readonly Func<(Serializer, object), TJournalPayload> _toPayload;
        private readonly Func<(Serializer, TJournalPayload, Type), object> _fromPayload;


        public ByteArrayJournalSerializer(
            IProviderConfig<JournalTableConfig<TJournalPayload>> journalConfig,
            Akka.Serialization.Serialization serializer,
            string separator,
            string? writerUuid,
            Func<(Serializer, object), TJournalPayload> toPayload,
            Func<(Serializer, TJournalPayload, Type), object> fromPayload)
        {
            _journalConfig = journalConfig;
            _serializer = serializer;
            _separator = separator;
            _separatorArray = new[] { _separator };
            _tagWriteMode = journalConfig.PluginConfig.TagMode;
            _writerUuid = writerUuid;
            _toPayload = toPayload;
            _fromPayload = fromPayload;
        }

        /// <summary>
        ///     Concatenates a set of tags using a provided separator.
        /// </summary>
        /// <param name="tags"></param>
        /// <param name="separator"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static string StringSeparator(IImmutableSet<string> tags, string separator)
            => tags.Count == 0
                ? string.Empty
                : $"{separator}{string.Join(separator, tags)}{separator}";

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static JournalRow<TJournalPayload> CreateJournalRow(
            IImmutableSet<string> tags,
            IPersistentRepresentation representation,
            long timestamp,
            TagMode tagWriteMode,
            string separator,
            string? uuid)
            => tagWriteMode switch
            {
                TagMode.Csv => new JournalRow<TJournalPayload>
                {
                    Tags = StringSeparator(tags, separator),
                    Timestamp = representation.Timestamp == 0
                        ? timestamp
                        : representation.Timestamp,
                    WriterUuid = uuid,
                },

                TagMode.TagTable => new JournalRow<TJournalPayload>
                {
                    Tags = string.Empty,
                    TagArray = tags.ToArray(),
                    Timestamp = representation.Timestamp == 0
                        ? timestamp
                        : representation.Timestamp,
                    WriterUuid = uuid,
                },

                TagMode.Both => new JournalRow<TJournalPayload>
                {
                    Tags = StringSeparator(tags, separator),
                    TagArray = tags.ToArray(),
                    Timestamp = representation.Timestamp == 0
                        ? timestamp
                        : representation.Timestamp,
                    WriterUuid = uuid,
                },

                _ => throw new Exception($"Invalid Tag Write Mode! Was: {tagWriteMode}"),
            };

        protected override Try<JournalRow<TJournalPayload>> Serialize(
            IPersistentRepresentation persistentRepresentation,
            IImmutableSet<string> tTags,
            long timeStamp = 0)
        {
            try
            {
                // TODO: hack. Replace when https://github.com/akkadotnet/akka.net/issues/3811
                return Akka.Serialization.Serialization
                    .WithTransport(
                        system: _serializer.System,
                        state: (
                            persistentRepresentation,
                            _serializer.FindSerializerForType(persistentRepresentation.Payload.GetType(), _journalConfig.DefaultSerializer),
                            CreateJournalRow(tTags, persistentRepresentation, timeStamp, _tagWriteMode, _separator, _writerUuid)),
                        action: state =>
                        {
                            var (representation, serializer, row) = state;

                            row.Manifest = serializer switch
                            {
                                SerializerWithStringManifest stringManifest => stringManifest.Manifest(representation.Payload),
                                { IncludeManifest: true } => representation.Payload.GetType().TypeQualifiedName(),
                                _ => string.Empty,
                            };

                            row.Message = _toPayload((serializer,representation.Payload));
                            row.PersistenceId = representation.PersistenceId;
                            row.Identifier = serializer.Identifier;
                            row.SequenceNumber = representation.SequenceNr;
                            row.EventManifest = representation.Manifest;

                            return new Try<JournalRow<TJournalPayload>>(row);
                        });
            }
            catch (Exception e)
            {
                return new Try<JournalRow<TJournalPayload>>(e);
            }
        }

        protected override Try<(IPersistentRepresentation, string[], long)> Deserialize(JournalRow<TJournalPayload> t)
        {
            try
            {
                var identifierMaybe = t.Identifier;
                var tags = t.Tags?.Split(_separatorArray, StringSplitOptions.RemoveEmptyEntries);
                if (tags is null || tags.Length == 0)
                    tags = t.TagArray;

                if (identifierMaybe.HasValue && t.Message is byte[] bm)
                {
                    // TODO: hack. Replace when https://github.com/akkadotnet/akka.net/issues/3811
                    return new Try<(IPersistentRepresentation, string[], long)>(
                        (
                            new Persistent(
                                payload: _serializer.Deserialize(bm, identifierMaybe.Value, t.Manifest),
                                sequenceNr: t.SequenceNumber,
                                persistenceId: t.PersistenceId,
                                manifest: t.EventManifest ?? t.Manifest,
                                isDeleted: t.Deleted,
                                sender: ActorRefs.NoSender,
                                writerGuid: t.WriterUuid,
                                timestamp: t.Timestamp),
                            tags,
                            t.Ordering));
                }

                var type = Type.GetType(t.Manifest, true);

                // TODO: hack. Replace when https://github.com/akkadotnet/akka.net/issues/3811
                return new Try<(IPersistentRepresentation, string[], long)>(
                    (
                        new Persistent(
                            payload: Akka.Serialization.Serialization.WithTransport(
                                system: _serializer.System,
                                state: (_serializer.FindSerializerForType(type, _journalConfig.DefaultSerializer), message: t.Message, type),
                                action: _fromPayload),
                            sequenceNr: t.SequenceNumber,
                            persistenceId: t.PersistenceId,
                            manifest: t.EventManifest ?? t.Manifest,
                            isDeleted: t.Deleted,
                            sender: ActorRefs.NoSender,
                            writerGuid: t.WriterUuid,
                            timestamp: t.Timestamp),
                        tags,
                        t.Ordering));
            }
            catch (Exception e)
            {
                return new Try<(IPersistentRepresentation, string[], long)>(e);
            }
        }
    }
}
