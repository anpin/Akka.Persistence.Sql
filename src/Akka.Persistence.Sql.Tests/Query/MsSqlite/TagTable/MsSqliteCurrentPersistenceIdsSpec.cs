﻿// -----------------------------------------------------------------------
//  <copyright file="MsSqliteCurrentPersistenceIdsSpec.cs" company="Akka.NET Project">
//      Copyright (C) 2013-2023 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

using Akka.Persistence.Sql.Config;
using Akka.Persistence.Sql.Tests.Common.Containers;
using Akka.Persistence.Sql.Tests.Common.Query;
using Akka.Persistence.Sql.Tests.Sqlite;
using Xunit;
using Xunit.Abstractions;

namespace Akka.Persistence.Sql.Tests.Query.MsSqlite.TagTable
{
    [Collection(nameof(MsSqlitePersistenceSpec))]
    public class MsSqliteCurrentPersistenceIdsSpec : BaseCurrentPersistenceIdsSpec<MsSqliteContainer>
    {
        public MsSqliteCurrentPersistenceIdsSpec(ITestOutputHelper output, MsSqliteContainer fixture)
            : base(TagMode.TagTable, output, nameof(MsSqliteCurrentPersistenceIdsSpec), fixture) { }
    }
}
