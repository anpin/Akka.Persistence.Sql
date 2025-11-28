using System.Linq;
using Akka.Persistence.Sql.Journal.Types;
using LanguageExt;
using LinqToDB;
using LinqToDB.Data;
using static LanguageExt.Prelude;

namespace Akka.Persistence.Sql.Db;

public static class AkkaDataConnectionExtensions
{
        public static IQueryable<long?> MaxSeqNumberForPersistenceIdQuery<TJournalPayload>(
            this AkkaDataConnection<TJournalPayload> connection,
            bool sqlCommonCompatibilityMode,
            string persistenceId,
            long minSequenceNumber = 0)
        {
            if (minSequenceNumber != 0)
            {
                return sqlCommonCompatibilityMode
                    ? connection.MaxSeqForPersistenceIdQueryableCompatibilityModeWithMinId(
                        persistenceId,
                        minSequenceNumber)
                    : connection.MaxSeqForPersistenceIdQueryableNativeModeMinId(
                        persistenceId,
                        minSequenceNumber);
            }

            return sqlCommonCompatibilityMode
                ? connection.MaxSeqForPersistenceIdQueryableCompatibilityMode(persistenceId)
                : connection.MaxSeqForPersistenceIdQueryableNativeMode(persistenceId);
        }

        public static IQueryable<long?> MaxSeqForPersistenceIdQueryableNativeMode<TJournalPayload>(
            this AkkaDataConnection<TJournalPayload> connection,
            string persistenceId)
            => connection
                .GetTable<JournalRow<TJournalPayload>>()
                .Where(r => r.PersistenceId == persistenceId)
                .Select(r => (long?)r.SequenceNumber);

        public static IQueryable<long?> MaxSeqForPersistenceIdQueryableNativeModeMinId<TJournalPayload>(
            this AkkaDataConnection<TJournalPayload> connection,
            string persistenceId,
            long minSequenceNumber)
            => connection
                .GetTable<JournalRow<TJournalPayload>>()
                .Where(
                    r =>
                        r.PersistenceId == persistenceId &&
                        r.SequenceNumber > minSequenceNumber)
                .Select(r => (long?)r.SequenceNumber);

        public static IQueryable<long?> MaxSeqForPersistenceIdQueryableCompatibilityModeWithMinId<TJournalPayload>(
            this AkkaDataConnection<TJournalPayload> connection,
            string persistenceId,
            long minSequenceNumber)
            => connection
                .GetTable<JournalRow<TJournalPayload>>()
                .Where(
                    r =>
                        r.PersistenceId == persistenceId &&
                        r.SequenceNumber > minSequenceNumber)
                .Select(r => LinqToDB.Sql.Ext.Max<long?>(r.SequenceNumber).ToValue())
                .Union(
                    connection
                        .GetTable<JournalMetaData>()
                        .Where(
                            r =>
                                r.SequenceNumber > minSequenceNumber &&
                                r.PersistenceId == persistenceId)
                        .Select(r => LinqToDB.Sql.Ext.Max<long?>(r.SequenceNumber).ToValue()));

        public static IQueryable<long?> MaxSeqForPersistenceIdQueryableCompatibilityMode<TJournalPayload>(
            this AkkaDataConnection<TJournalPayload> connection,
            string persistenceId)
            => connection
                .GetTable<JournalRow<TJournalPayload>>()
                .Where(r => r.PersistenceId == persistenceId)
                .Select(r => LinqToDB.Sql.Ext.Max<long?>(r.SequenceNumber).ToValue())
                .Union(
                    connection
                        .GetTable<JournalMetaData>()
                        .Where(r => r.PersistenceId == persistenceId)
                        .Select(r => LinqToDB.Sql.Ext.Max<long?>(r.SequenceNumber).ToValue()));
}
