// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.importer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.leonsdb.proto.v1.AcquireLeaseRequest;
import com.leonsdb.proto.v1.AdminServiceGrpc;
import com.leonsdb.proto.v1.CollectionKind;
import com.leonsdb.proto.v1.CreateTokenRequest;
import com.leonsdb.proto.v1.DataServiceGrpc;
import com.leonsdb.proto.v1.EnsureCollectionRequest;
import com.leonsdb.proto.v1.FieldSchema;
import com.leonsdb.proto.v1.FieldType;
import com.leonsdb.proto.v1.GetRequest;
import com.leonsdb.proto.v1.LeaseServiceGrpc;
import com.leonsdb.proto.v1.Record;
import com.leonsdb.proto.v1.RecordRef;
import com.leonsdb.proto.v1.Role;
import com.leonsdb.proto.v1.ScanRequest;
import com.leonsdb.proto.v1.Schema;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Imports into the real LeonsDB server; runs with -PleonsdbServer=/path/to/leonsdb. */
class ImportRealServerTest {

    static Process server;
    static Path dataDir;
    static int port;
    static String admin;
    static ManagedChannel channel;
    static DataServiceGrpc.DataServiceBlockingStub data;

    @TempDir
    Path dir;

    @BeforeAll
    static void startServer() throws Exception {
        String binary = System.getProperty("leonsdb.server.binary", "");
        assumeTrue(!binary.isEmpty(), "set -PleonsdbServer to run against the real server");
        dataDir = Files.createTempDirectory("leonsdb-import-test");
        int wire;
        try (ServerSocket a = new ServerSocket(0); ServerSocket b = new ServerSocket(0)) {
            port = a.getLocalPort();
            wire = b.getLocalPort();
        }
        Files.writeString(dataDir.resolve("leonsdb.yml"), "listen: 127.0.0.1:" + port + "\nwire_listen: 127.0.0.1:" + wire
                + "\ndata_dir: data\nhttp:\n  listen: \"\"\n");
        server = new ProcessBuilder(binary, "-config", dataDir.resolve("leonsdb.yml").toString())
                .redirectErrorStream(true).redirectOutput(dataDir.resolve("server.log").toFile()).start();
        Path tokenFile = dataDir.resolve("data").resolve("initial-admin-token");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!Files.exists(tokenFile) || Files.readString(tokenFile).isBlank()) {
            if (System.nanoTime() > deadline || !server.isAlive()) {
                throw new IllegalStateException("server did not start: " + Files.readString(dataDir.resolve("server.log")));
            }
            Thread.sleep(50);
        }
        admin = Files.readString(tokenFile).trim();
        channel = NettyChannelBuilder.forAddress("127.0.0.1", port).usePlaintext().build();
        Metadata md = new Metadata();
        md.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + admin);
        data = DataServiceGrpc.newBlockingStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md))
                .withWaitForReady();
    }

    @AfterAll
    static void stopServer() throws Exception {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (server != null) {
            server.destroy();
            server.waitFor(10, TimeUnit.SECONDS);
        }
    }

    /** What a plugin's LeonsDB.register does: the collection and its fields. */
    static void register(String ns, String coll, Map<String, FieldType> fields) {
        Schema.Builder s = Schema.newBuilder();
        fields.forEach((n, t) -> s.addFields(FieldSchema.newBuilder().setName(n).setType(t)));
        data.withDeadlineAfter(10, TimeUnit.SECONDS).ensureCollection(EnsureCollectionRequest.newBuilder().setNamespace(ns)
                .setCollection(coll).setKind(CollectionKind.COLLECTION_KIND_PLAYER).setSchema(s).build());
    }

    static long count(String ns, String coll) {
        long n = 0;
        com.google.protobuf.ByteString token = com.google.protobuf.ByteString.EMPTY;
        do {
            var page = data.withDeadlineAfter(10, TimeUnit.SECONDS).scan(ScanRequest.newBuilder().setNamespace(ns)
                    .setCollection(coll).setPageSize(1000).setPageToken(token).build());
            n += page.getRecordsCount();
            token = page.getNextPageToken();
        } while (!token.isEmpty());
        return n;
    }

    static Record get(String ns, String coll, UUID player) {
        var b = java.nio.ByteBuffer.allocate(16).putLong(player.getMostSignificantBits()).putLong(player.getLeastSignificantBits());
        var resp = data.withDeadlineAfter(10, TimeUnit.SECONDS).get(GetRequest.newBuilder().setRef(RecordRef.newBuilder()
                .setNamespace(ns).setCollection(coll).setPlayer(com.google.protobuf.ByteString.copyFrom(b.array()))).build());
        return resp.getFound() ? resp.getRecord() : null;
    }

    private record Run(int exit, String out) {
    }

    private Run run(Path plan, String... args) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(out, true);
        String[] all = new String[args.length + 1];
        all[0] = plan.toString();
        System.arraycopy(args, 0, all, 1, args.length);
        int exit = Main.run(all, ps, ps, Map.of("LEONSDB_TOKEN", admin), Drivers.standard(ps));
        return new Run(exit, out.toString());
    }

    private Path sqlitePlan(Path db, String into, String extra) throws Exception {
        Path plan = dir.resolve("plan.yml");
        Files.writeString(plan, """
                source: { type: sqlite, url: "jdbc:sqlite:%s" }
                leonsdb: { server: 127.0.0.1:%d, token-env: LEONSDB_TOKEN }
                imports:
                  - from: { table: player_stats }
                    into: %s
                    key: uuid
                    fields:
                      kills: kills
                      coins: { from: balance }
                      name: name
                      lastSeen: { from: last_seen, as: epoch-seconds }
                %s""".formatted(db.toString().replace('\\', '/'), port, into, extra));
        return plan;
    }

    private Path statsTable(int rows, boolean withBadRow) throws Exception {
        Path db = dir.resolve("stats.db");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db)) {
            c.createStatement().execute("CREATE TABLE player_stats (uuid TEXT, kills INTEGER, balance REAL, name TEXT, last_seen INTEGER)");
            c.setAutoCommit(false);
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO player_stats VALUES (?, ?, ?, ?, ?)")) {
                for (int i = 0; i < rows; i++) {
                    ps.setString(1, new UUID(0x1234L, i).toString());
                    ps.setInt(2, i);
                    ps.setDouble(3, 100 + i);
                    ps.setString(4, "p" + i);
                    ps.setLong(5, 1_761_739_200L + i);
                    ps.addBatch();
                }
                if (withBadRow) {
                    ps.setString(1, "not-a-uuid");
                    ps.setInt(2, 1);
                    ps.setDouble(3, 1);
                    ps.setString(4, "bad");
                    ps.setLong(5, 0);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            c.commit();
        }
        return db;
    }

    private static final Map<String, FieldType> STATS = Map.of("kills", FieldType.FIELD_TYPE_INT,
            "coins", FieldType.FIELD_TYPE_LONG, "name", FieldType.FIELD_TYPE_STRING, "lastSeen", FieldType.FIELD_TYPE_TIMESTAMP);

    @Test
    void aDryRunWritesNothingAndApplyStoresExactlyWhatItShowed() throws Exception {
        register("imp1", "stats", STATS);
        Path plan = sqlitePlan(statsTable(3, false), "imp1.stats", "");

        Run dry = run(plan);
        assertEquals(0, dry.exit(), dry.out());
        assertTrue(dry.out().contains("would import:  3 records"), dry.out());
        assertTrue(dry.out().contains("nothing is written"), dry.out());
        assertTrue(dry.out().contains("00000000-0000-1234-0000-000000000001: coins=101, kills=1, lastSeen=2025-10-29T12:00:01Z, name=\"p1\""),
                dry.out());
        assertEquals(0, count("imp1", "stats"), "the dry run wrote nothing");
        assertTrue(Files.readString(dir.resolve("plan-report.txt")).contains("would import:  3 records"));

        Run apply = run(plan, "--apply");
        assertEquals(0, apply.exit(), apply.out());
        assertTrue(apply.out().contains("imported imp1.stats: 3 records"), apply.out());
        assertEquals(3, count("imp1", "stats"));
        Record r = get("imp1", "stats", new UUID(0x1234L, 2));
        assertEquals(2, r.getFieldsOrThrow("kills").getIntValue());
        assertEquals(102, r.getFieldsOrThrow("coins").getLongValue());
        assertEquals("p2", r.getFieldsOrThrow("name").getStringValue());
        assertEquals((1_761_739_200L + 2) * 1000, r.getFieldsOrThrow("lastSeen").getTimestampValue());

        Run again = run(plan, "--apply");
        assertEquals(1, again.exit());
        assertTrue(again.out().contains("already has records"), again.out());
        assertEquals(3, count("imp1", "stats"), "nothing already there was touched");
    }

    @Test
    void aMissingCollectionSaysWhatToDo() throws Exception {
        Path plan = sqlitePlan(statsTable(1, false), "nobody.stats", "");
        Run dry = run(plan);
        assertEquals(1, dry.exit());
        assertTrue(dry.out().contains("Start the plugin that stores it once with LeonsDB"), dry.out());
        assertTrue(dry.out().contains("then run the import again"), dry.out());
    }

    @Test
    void wrongFieldNamesAndSkippedRowsStopTheImport() throws Exception {
        register("imp2", "stats", STATS);
        Path db = statsTable(2, true);
        Run dry = run(sqlitePlan(db, "imp2.stats", ""));
        assertEquals(0, dry.exit(), dry.out());
        assertTrue(dry.out().contains("1 x \"not-a-uuid\" is not a UUID (row 3)"), dry.out());
        Run apply = run(sqlitePlan(db, "imp2.stats", ""), "--apply");
        assertEquals(1, apply.exit());
        assertTrue(apply.out().contains("nothing was imported, because of the skipped rows"), apply.out());
        assertEquals(0, count("imp2", "stats"));

        Run skips = run(sqlitePlan(db, "imp2.stats", ""), "--apply", "--allow-skips");
        assertEquals(0, skips.exit(), skips.out());
        assertEquals(2, count("imp2", "stats"));

        register("imp3", "stats", Map.of("kills", FieldType.FIELD_TYPE_INT));
        Run typo = run(sqlitePlan(db, "imp3.stats", ""));
        assertEquals(1, typo.exit());
        assertTrue(typo.out().contains("\"coins\" is not a field of imp3.stats"), typo.out());
    }

    @Test
    void aFailurePartWayStoresNothing() throws Exception {
        register("imp4", "stats", STATS);
        Plan plan = Plan.load(sqlitePlan(statsTable(12_000, false), "imp4.stats", ""), Map.of("LEONSDB_TOKEN", admin));
        int[] opened = {0};
        Importer.Sources real = Importer.sources(Drivers.standard(System.out));
        Importer.Sources failing = (source, from) -> {
            RowSource src = real.open(source, from);
            if (++opened[0] == 1) {
                return src; // the check reads everything
            }
            return new RowSource() { // the import breaks after 7000 rows (one batch already sent)
                @Override
                public void forEach(RowConsumer c) throws ImportException {
                    int[] n = {0};
                    src.forEach(row -> {
                        if (++n[0] > 7000) {
                            throw new ImportException("the source went away");
                        }
                        c.accept(row);
                    });
                }

                @Override
                public void close() {
                    src.close();
                }
            };
        };
        try (LeonsDBTarget t = new LeonsDBTarget(plan.leonsdb())) {
            Importer.Outcome o = new Importer(plan, t, failing, System.out).run(true, false);
            assertFalse(o.ok());
            assertTrue(o.report().contains("failed and stored nothing: the source went away"), o.report());
        }
        assertEquals(0, count("imp4", "stats"), "all or nothing");
        // And the collection is free again for a new try.
        assertEquals(0, run(sqlitePlan(dir.resolve("stats.db"), "imp4.stats", ""), "--apply").exit());
        assertEquals(12_000, count("imp4", "stats"));
    }

    @Test
    void anOnlinePlayerStopsTheImportAndNothingIsStored() throws Exception {
        register("imp5", "stats", STATS);
        Path plan = sqlitePlan(statsTable(3, false), "imp5.stats", "");
        // A Minecraft server has player 1 online (it owns the player's lease).
        var tok = AdminServiceGrpc.newBlockingStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(auth(admin)))
                .createToken(CreateTokenRequest.newBuilder().setName("mc").setRole(Role.ROLE_SERVER).build()).getSecret();
        UUID online = new UUID(0x1234L, 1);
        var b = java.nio.ByteBuffer.allocate(16).putLong(online.getMostSignificantBits()).putLong(online.getLeastSignificantBits());
        var leases = LeaseServiceGrpc.newBlockingStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(auth(tok)));
        var player = com.google.protobuf.ByteString.copyFrom(b.array());
        long epoch = leases.acquire(AcquireLeaseRequest.newBuilder().setPlayer(player)
                .setHolderId("mc-1").setHolderName("survival").build()).getEpoch();
        try {
            Run apply = run(plan, "--apply");
            assertEquals(1, apply.exit());
            assertTrue(apply.out().contains("Some of the players are online"), apply.out());
            assertEquals(0, count("imp5", "stats"));
        } finally {
            // The player leaves again, so other tests can import them.
            leases.release(com.leonsdb.proto.v1.ReleaseLeaseRequest.newBuilder().setPlayer(player).setHolderId("mc-1")
                    .setEpoch(epoch).build());
        }
    }

    @Test
    void manyRowsGoInBatches() throws Exception {
        register("imp6", "stats", STATS);
        Run apply = run(sqlitePlan(statsTable(50_000, false), "imp6.stats", ""), "--apply");
        assertEquals(0, apply.exit(), apply.out());
        assertEquals(50_000, count("imp6", "stats"));
        assertEquals(49_999, get("imp6", "stats", new UUID(0x1234L, 49_999)).getFieldsOrThrow("kills").getIntValue());
    }

    @Test
    void yamlUserdataFiles() throws Exception {
        register("imp7", "stats", Map.of("coins", FieldType.FIELD_TYPE_LONG, "name", FieldType.FIELD_TYPE_STRING));
        Path ud = Files.createDirectories(dir.resolve("Essentials/userdata"));
        UUID steve = UUID.fromString("0f3a2c1e-6b7d-4e8f-9a0b-1c2d3e4f5a6b");
        Files.writeString(ud.resolve(steve + ".yml"), "money: '1250'\nlast-account-name: Steve\n");
        Files.writeString(ud.resolve("broken-name.yml"), "money: '1'\n");
        Path plan = dir.resolve("essentials.yml");
        Files.writeString(plan, """
                source: { type: yaml, dir: Essentials }
                leonsdb: { server: 127.0.0.1:%d, token-env: LEONSDB_TOKEN }
                imports:
                  - from: { files: "userdata/*.yml" }
                    into: imp7.stats
                    key: $file
                    fields:
                      coins: money
                      name: last-account-name
                """.formatted(port));
        Run apply = run(plan, "--apply", "--allow-skips");
        assertEquals(0, apply.exit(), apply.out());
        assertTrue(apply.out().contains("\"broken-name\" is not a UUID (userdata/broken-name.yml)"), apply.out());
        assertEquals(1250, get("imp7", "stats", steve).getFieldsOrThrow("coins").getLongValue());
    }

    @Test
    void mysql() throws Exception {
        String url = System.getProperty("leonsdb.test.mysql", "");
        assumeTrue(!url.isEmpty(), "set -PmysqlUrl to run against MySQL");
        java.sql.Driver driver = Drivers.fromJar(SourcesTest.testDriver("mysql-connector-j-"), Drivers.MYSQL.driverClass());
        try (Connection c = driver.connect(url, new java.util.Properties())) {
            c.createStatement().execute("DROP TABLE IF EXISTS player_stats");
            c.createStatement().execute("CREATE TABLE player_stats (uuid BINARY(16), kills INT, balance DECIMAL(20,2), "
                    + "name VARCHAR(16), last_seen DATETIME, banned TINYINT(1))");
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO player_stats VALUES (?, ?, ?, ?, ?, ?)")) {
                for (int i = 0; i < 1000; i++) {
                    UUID u = new UUID(0x5678L, i);
                    ps.setBytes(1, java.nio.ByteBuffer.allocate(16).putLong(u.getMostSignificantBits())
                            .putLong(u.getLeastSignificantBits()).array());
                    ps.setInt(2, i);
                    ps.setBigDecimal(3, new java.math.BigDecimal(i + ".00"));
                    ps.setString(4, "p" + i);
                    ps.setTimestamp(5, java.sql.Timestamp.valueOf("2026-10-29 12:00:00"));
                    ps.setBoolean(6, i % 10 == 0);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
        register("imp8", "stats", Map.of("kills", FieldType.FIELD_TYPE_INT, "coins", FieldType.FIELD_TYPE_LONG,
                "name", FieldType.FIELD_TYPE_STRING, "lastSeen", FieldType.FIELD_TYPE_TIMESTAMP));
        Path plan = dir.resolve("mysql.yml");
        Files.writeString(plan, """
                source: { type: mysql, url: "%s", driver: "%s" }
                leonsdb: { server: 127.0.0.1:%d, token-env: LEONSDB_TOKEN }
                imports:
                  - from: { query: "SELECT * FROM player_stats WHERE banned = 0" }
                    into: imp8.stats
                    key: uuid
                    fields: { kills: kills, coins: balance, name: name, lastSeen: last_seen }
                """.formatted(url, SourcesTest.testDriver("mysql-connector-j-").toString().replace('\\', '/'), port));
        Run apply = run(plan, "--apply");
        assertEquals(0, apply.exit(), apply.out());
        assertEquals(900, count("imp8", "stats"));
        Record r = get("imp8", "stats", new UUID(0x5678L, 7));
        assertEquals(7, r.getFieldsOrThrow("coins").getLongValue());
        assertEquals(java.time.Instant.parse("2026-10-29T12:00:00Z").toEpochMilli(), r.getFieldsOrThrow("lastSeen").getTimestampValue());
        try (Connection c = driver.connect(url, new java.util.Properties())) {
            var rs = c.createStatement().executeQuery("SELECT COUNT(*) FROM player_stats");
            rs.next();
            assertEquals(1000, rs.getInt(1), "the source is unchanged");
        }
    }

    @Test
    void mongodb() throws Exception {
        String url = System.getProperty("leonsdb.test.mongo", "");
        assumeTrue(!url.isEmpty(), "set -PmongoUrl to run against MongoDB");
        try (var client = com.mongodb.client.MongoClients.create(url)) {
            var coll = client.getDatabase("survival").getCollection("players");
            coll.drop();
            java.util.List<org.bson.Document> docs = new java.util.ArrayList<>();
            for (int i = 0; i < 500; i++) {
                docs.add(new org.bson.Document("uuid", new UUID(0x9abcL, i).toString())
                        .append("stats", new org.bson.Document("kills", i).append("deaths", (long) i * 2))
                        .append("lastLogin", java.util.Date.from(java.time.Instant.parse("2026-10-29T12:00:00Z")))
                        .append("titles", java.util.List.of("Newbie", "Builder"))
                        .append("staff", i == 3));
            }
            coll.insertMany(docs);
        }
        register("imp9", "stats", Map.of("kills", FieldType.FIELD_TYPE_INT, "deaths", FieldType.FIELD_TYPE_LONG,
                "lastSeen", FieldType.FIELD_TYPE_TIMESTAMP, "titles", FieldType.FIELD_TYPE_LIST));
        Path plan = dir.resolve("mongo.yml");
        Files.writeString(plan, """
                source: { type: mongodb, url: "%s" }
                leonsdb: { server: 127.0.0.1:%d, token-env: LEONSDB_TOKEN }
                imports:
                  - from: { collection: players, filter: { staff: false } }
                    into: imp9.stats
                    key: uuid
                    fields: { kills: stats.kills, deaths: stats.deaths, lastSeen: lastLogin, titles: titles }
                """.formatted(url, port));
        Run apply = run(plan, "--apply");
        assertEquals(0, apply.exit(), apply.out());
        assertEquals(499, count("imp9", "stats"));
        Record r = get("imp9", "stats", new UUID(0x9abcL, 5));
        assertEquals(5, r.getFieldsOrThrow("kills").getIntValue());
        assertEquals(10, r.getFieldsOrThrow("deaths").getLongValue());
        assertEquals("Builder", r.getFieldsOrThrow("titles").getListValue().getValues(1).getStringValue());
    }

    private static Metadata auth(String token) {
        Metadata md = new Metadata();
        md.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + token);
        return md;
    }
}
