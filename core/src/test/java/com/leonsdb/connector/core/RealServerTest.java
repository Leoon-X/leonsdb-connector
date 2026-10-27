// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.leonsdb.connector.api.GlobalData;
import com.leonsdb.connector.api.Indexed;
import com.leonsdb.connector.api.LeonsDBException;
import com.leonsdb.connector.api.PlayerData;
import com.leonsdb.connector.core.cache.LeonsDBService;
import com.leonsdb.connector.core.cache.TestPlatform;
import com.leonsdb.connector.core.schema.Codecs;
import com.leonsdb.proto.v1.AdminServiceGrpc;
import com.leonsdb.proto.v1.CreateTokenRequest;
import com.leonsdb.proto.v1.Operation;
import com.leonsdb.proto.v1.RecordRef;
import com.leonsdb.proto.v1.Role;
import com.leonsdb.proto.v1.Value;
import io.grpc.Metadata;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Runs the connector against the real LeonsDB server binary, so the Java
 * and Go sides can never drift apart. Enabled when the build is given the
 * server binary: {@code ./gradlew test -PleonsdbServer=/path/to/leonsdb}.
 */
class RealServerTest {

    @PlayerData(namespace = "contract", collection = "stats")
    public static class Stats {
        @Indexed public int kills;
        public String title;
    }

    @PlayerData(namespace = "contract", collection = "stats")
    public static class StatsWithChangedType {
        public String kills;
    }

    @GlobalData(namespace = "contract", collection = "counters")
    public static class Counter {
        public long value;
    }

    private static Process server;
    private static Path dataDir;
    private static int port;
    private static String serverToken;

    @BeforeAll
    static void startServer() throws Exception {
        String binary = System.getProperty("leonsdb.server.binary", "");
        assumeTrue(!binary.isEmpty(), "set -PleonsdbServer to run against the real server");

        dataDir = Files.createTempDirectory("leonsdb-contract");
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        int wirePort;
        try (ServerSocket s = new ServerSocket(0)) {
            wirePort = s.getLocalPort();
        }
        Path config = dataDir.resolve("leonsdb.yml");
        // A short lease TTL keeps the crash test fast.
        Files.writeString(config, "listen: 127.0.0.1:" + port + "\nwire_listen: 127.0.0.1:" + wirePort
                + "\ndata_dir: data\nlease_ttl_ms: 2000\n");
        server = new ProcessBuilder(binary, "-config", config.toString())
                .redirectErrorStream(true)
                .redirectOutput(dataDir.resolve("server.log").toFile())
                .start();

        Path tokenFile = dataDir.resolve("data").resolve("initial-admin-token");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!Files.exists(tokenFile) || Files.readString(tokenFile).isBlank()) {
            if (System.nanoTime() > deadline || !server.isAlive()) {
                throw new IllegalStateException("server did not start: " + Files.readString(dataDir.resolve("server.log")));
            }
            Thread.sleep(50);
        }
        String admin = Files.readString(tokenFile).trim();

        var channel = NettyChannelBuilder.forAddress("127.0.0.1", port).usePlaintext().build();
        try {
            Metadata md = new Metadata();
            md.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + admin);
            var adminStub = AdminServiceGrpc.newBlockingStub(channel)
                    .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md))
                    .withWaitForReady()
                    .withDeadlineAfter(10, TimeUnit.SECONDS);
            serverToken = adminStub.createToken(CreateTokenRequest.newBuilder()
                    .setName("contract-test").setRole(Role.ROLE_SERVER).build()).getSecret();
        } finally {
            channel.shutdownNow();
        }
    }

    @AfterAll
    static void stopServer() throws IOException {
        if (server != null) {
            server.destroyForcibly();
        }
    }

    private static LeonsDBClient client() {
        return LeonsDBClient.connect(new ConnectionSettings("127.0.0.1", port, false, serverToken, Duration.ofSeconds(5)), "contract-test");
    }

    private static final List<LeonsDBService> started = new ArrayList<>();

    @AfterEach
    void stopServices() {
        started.forEach(s -> s.shutdown(Duration.ofSeconds(1)));
        started.clear();
    }

    private static LeonsDBService service(LeonsDBClient client, Class<?>... classes) {
        var s = TestPlatform.service(client, Logger.getLogger("contract"), new TestPlatform(), dataDir.resolve("recovery"));
        started.add(s);
        s.register(classes);
        return s;
    }

    @Test
    void callsUseTheWireProtocolWithTheSameResults() throws Exception {
        try (var client = client()) {
            client.fetchServerInfo().get(5, TimeUnit.SECONDS);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!client.usesWireProtocol()) {
                assertTrue(System.nanoTime() < deadline, "the wire protocol did not connect");
                Thread.sleep(20);
            }
            client.ensureCollection("contract", "wire", com.leonsdb.proto.v1.CollectionKind.COLLECTION_KIND_PLAYER,
                    com.leonsdb.proto.v1.Schema.getDefaultInstance()).get(5, TimeUnit.SECONDS);
            UUID uuid = UUID.randomUUID();
            var player = com.google.protobuf.ByteString.copyFrom(java.nio.ByteBuffer.allocate(16)
                    .putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array());
            RecordRef ref = RecordRef.newBuilder().setNamespace("contract").setCollection("wire").setPlayer(player).build();
            long epoch = client.acquireLease(player, "wire-holder", "wire", 0).get(5, TimeUnit.SECONDS).getEpoch();
            Operation inc = Operation.newBuilder().setField("kills")
                    .setIncrement(Value.newBuilder().setIntValue(5)).build();
            var resp = client.apply(ref, List.of(inc), epoch, OptionalLong.empty()).get(5, TimeUnit.SECONDS);
            assertEquals(1, resp.getVersion());
            assertEquals(5, client.get(ref).get(5, TimeUnit.SECONDS).getRecord().getFieldsOrThrow("kills").getIntValue());

            // Errors keep their reason, as over gRPC.
            var err = assertThrows(ExecutionException.class,
                    () -> client.apply(ref, List.of(inc), epoch + 1, OptionalLong.empty()).get(5, TimeUnit.SECONDS));
            assertTrue(err.getCause() instanceof RequestException re && "STALE_EPOCH".equals(re.reason()),
                    "expected STALE_EPOCH, got " + err.getCause());
            assertTrue(client.usesWireProtocol());
        }
    }

    @Test
    void subscriptionsRunOverTheWireProtocolToo() throws Exception {
        try (var listener = client(); var sender = client()) {
            listener.fetchServerInfo().get(5, TimeUnit.SECONDS);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!listener.usesWireProtocol()) {
                assertTrue(System.nanoTime() < deadline, "the wire protocol did not connect");
                Thread.sleep(20);
            }
            var received = new java.util.concurrent.LinkedBlockingQueue<com.leonsdb.proto.v1.ChannelMessage>();
            var closed = new CompletableFuture<io.grpc.Status>();
            var call = new java.util.concurrent.atomic.AtomicReference<io.grpc.stub.ClientCallStreamObserver<?>>();
            listener.subscribeOverWire(List.of("contract.wire"), new io.grpc.stub.ClientResponseObserver<com.leonsdb.proto.v1.SubscribeRequest, com.leonsdb.proto.v1.ChannelMessage>() {
                @Override
                public void beforeStart(io.grpc.stub.ClientCallStreamObserver<com.leonsdb.proto.v1.SubscribeRequest> c) {
                    call.set(c);
                }

                @Override
                public void onNext(com.leonsdb.proto.v1.ChannelMessage m) {
                    received.add(m);
                }

                @Override
                public void onError(Throwable t) {
                    closed.complete(io.grpc.Status.fromThrowable(t));
                }

                @Override
                public void onCompleted() {
                    closed.complete(io.grpc.Status.OK);
                }
            });
            eventually(() -> {
                try {
                    return sender.publish("contract.wire", "ping", "lobby").get(5, TimeUnit.SECONDS).getDelivered() == 1;
                } catch (Exception e) {
                    return false;
                }
            }, "subscribed");
            for (int i = 0; i < 100; i++) {
                sender.publish("contract.wire", "m" + i, "lobby").get(5, TimeUnit.SECONDS);
            }
            // In order, after the pings.
            int next = 0;
            while (next < 100) {
                var m = received.poll(5, TimeUnit.SECONDS);
                assertTrue(m != null, "message m" + next + " did not arrive");
                if (!m.getText().equals("ping")) {
                    assertEquals("m" + next, m.getText());
                    assertEquals("lobby", m.getSender());
                    next++;
                }
            }
            call.get().cancel("done", null);
            assertEquals(io.grpc.Status.Code.CANCELLED, closed.get(5, TimeUnit.SECONDS).getCode());
            eventually(() -> {
                try {
                    return sender.publish("contract.wire", "x", "lobby").get(5, TimeUnit.SECONDS).getDelivered() == 0;
                } catch (Exception e) {
                    return false;
                }
            }, "the cancelled subscription ended on the server");
            assertTrue(listener.usesWireProtocol());
        }
    }

    @Test
    void manySavesAtOnceAreBatchedAndEachGetsItsResult() throws Exception {
        try (var client = client()) {
            client.ensureCollection("contract", "batch", com.leonsdb.proto.v1.CollectionKind.COLLECTION_KIND_GLOBAL,
                    com.leonsdb.proto.v1.Schema.getDefaultInstance()).get(5, TimeUnit.SECONDS);
            Operation inc = Operation.newBuilder().setField("n").setIncrement(Value.newBuilder().setIntValue(1)).build();
            List<CompletableFuture<com.leonsdb.proto.v1.ApplyResponse>> saves = new ArrayList<>();
            for (int round = 1; round <= 5; round++) {
                for (int i = 0; i < 200; i++) {
                    RecordRef ref = RecordRef.newBuilder().setNamespace("contract").setCollection("batch").setId("r" + i).build();
                    // Every write expects the version it had, as the connector's saves do.
                    saves.add(client.apply(ref, List.of(inc), 0, OptionalLong.of(round - 1)));
                }
                for (var s : saves) {
                    assertEquals(round, s.get(10, TimeUnit.SECONDS).getVersion());
                }
                saves.clear();
            }
            // A save that expects the wrong version is refused on its own.
            RecordRef r0 = RecordRef.newBuilder().setNamespace("contract").setCollection("batch").setId("r0").build();
            RecordRef r1 = RecordRef.newBuilder().setNamespace("contract").setCollection("batch").setId("r1").build();
            var bad = client.apply(r0, List.of(inc), 0, OptionalLong.of(1));
            var good = client.apply(r1, List.of(inc), 0, OptionalLong.of(5));
            var err = assertThrows(ExecutionException.class, () -> bad.get(5, TimeUnit.SECONDS));
            assertTrue(err.getCause() instanceof RequestException re && re.isVersionMismatch(), "got " + err.getCause());
            assertEquals(6, good.get(5, TimeUnit.SECONDS).getVersion());
        }
    }

    @Test
    void playerDataIsMergedSavedAndReloaded() throws Exception {
        UUID uuid = UUID.randomUUID();
        try (var client = client()) {
            var svc = service(client, Stats.class);
            svc.loadPlayer(uuid);
            Stats s = svc.player(uuid).get(Stats.class);
            for (int i = 0; i < 40; i++) {
                s.kills++;
            }
            s.title = "Champion";
            svc.player(uuid).save().get(5, TimeUnit.SECONDS);
            s.kills += 2;
            svc.unloadPlayer(uuid).get(5, TimeUnit.SECONDS);
        }
        // A brand-new connector (another Minecraft server) sees the data.
        try (var client = client()) {
            var svc = service(client, Stats.class);
            svc.loadPlayer(uuid);
            Stats s = svc.player(uuid).get(Stats.class);
            assertEquals(42, s.kills);
            assertEquals("Champion", s.title);
        }
    }

    @Test
    void serverSwitchHandsOverTheLatestDataQuickly() throws Exception {
        UUID uuid = UUID.randomUUID();
        try (var lobbyClient = client(); var survivalClient = client()) {
            var lobbyPlatform = new TestPlatform();
            var lobby = TestPlatform.service(lobbyClient, Logger.getLogger("lobby"), lobbyPlatform, dataDir.resolve("r1"));
            lobby.register(Stats.class);
            var survival = service(survivalClient, Stats.class);

            lobby.loadPlayer(uuid);
            lobbyPlatform.online.add(uuid);
            lobby.player(uuid).get(Stats.class).kills = 10; // not saved yet

            // The proxy connects the player to survival while they are still
            // on the lobby. Survival must get the lobby's latest data.
            long start = System.nanoTime();
            survival.loadPlayer(uuid);
            long millis = (System.nanoTime() - start) / 1_000_000;

            assertEquals(10, survival.player(uuid).get(Stats.class).kills, "the lobby saved before handing over");
            assertTrue(millis < 1500, "handover took " + millis + " ms; it must not wait for the lease to expire");
            assertFalse(lobby.player(uuid).isLoaded());
            lobbyPlatform.online.remove(uuid); // the proxy moved the player

            // The new owner saves normally.
            survival.player(uuid).get(Stats.class).kills++;
            survival.player(uuid).save().get(5, TimeUnit.SECONDS);
            lobby.shutdown(Duration.ofSeconds(1));
            survival.shutdown(Duration.ofSeconds(1));
        }
    }

    @Test
    void aCrashedServerReleasesItsPlayersWhenTheLeaseExpires() throws Exception {
        UUID uuid = UUID.randomUUID();
        var crashingClient = client();
        var crashing = service(crashingClient, Stats.class);
        crashing.loadPlayer(uuid);
        crashingClient.abort(); // the server dies: no more renewals, no handover

        try (var survivalClient = client()) {
            var survival = service(survivalClient, Stats.class);
            long start = System.nanoTime();
            survival.loadPlayer(uuid);
            long millis = (System.nanoTime() - start) / 1_000_000;
            assertTrue(millis >= 1000 && millis < 6000, "took " + millis + " ms; expected about one lease TTL");
            assertTrue(survival.player(uuid).isLoaded());
            survival.shutdown(Duration.ofSeconds(1));
        }
    }

    @PlayerData(namespace = "contract", collection = "ladder")
    public static class Ladder {
        @Indexed public int kills;
        @Indexed public double bestTime;
        public String name;
        public int notIndexed;
    }

    @Test
    void leaderboardsRankSavedPlayerData() throws Exception {
        try (var client = client()) {
            var svc = service(client, Ladder.class);
            int[] kills = {50, 80, 80, 10, 30};
            double[] times = {12.5, 9.1, 15.0, 20.2, 9.1};
            UUID[] players = new UUID[kills.length];
            for (int i = 0; i < kills.length; i++) {
                players[i] = UUID.randomUUID();
                svc.loadPlayer(players[i]);
                Ladder l = svc.player(players[i]).get(Ladder.class);
                l.kills = kills[i];
                l.bestTime = times[i];
                l.name = "player" + i;
                svc.player(players[i]).save().get(5, TimeUnit.SECONDS);
            }

            var board = svc.leaderboard(Ladder.class, "kills", com.leonsdb.connector.api.Leaderboard.Order.HIGHEST_FIRST);
            var top = board.top(3).get(5, TimeUnit.SECONDS);
            assertEquals(3, top.size());
            assertEquals(1, top.get(0).rank());
            assertEquals(1, top.get(1).rank(), "equal values share a rank");
            assertEquals(80, top.get(0).value().intValue());
            assertEquals(3, top.get(2).rank());
            assertEquals("player0", top.get(2).data().name, "the full record comes along");
            assertEquals(players[0], top.get(2).player());

            assertEquals(5L, board.rankOf(players[3]).get(5, TimeUnit.SECONDS).getAsLong());
            assertTrue(board.rankOf(UUID.randomUUID()).get(5, TimeUnit.SECONDS).isEmpty(), "unknown players are not ranked");
            assertEquals(5L, board.size().get(5, TimeUnit.SECONDS));
            assertEquals(10, board.page(4, 10).get(5, TimeUnit.SECONDS).get(0).value().intValue());

            var fastest = svc.leaderboard(Ladder.class, "bestTime", com.leonsdb.connector.api.Leaderboard.Order.LOWEST_FIRST);
            var podium = fastest.top(3).get(5, TimeUnit.SECONDS);
            assertEquals(9.1, podium.get(0).value().doubleValue());
            assertEquals(1, podium.get(1).rank(), "two players tied for the fastest time");
            assertEquals(3, podium.get(2).rank());

            assertThrows(LeonsDBException.class, () -> svc.leaderboard(Ladder.class, "notIndexed",
                    com.leonsdb.connector.api.Leaderboard.Order.HIGHEST_FIRST));
            assertThrows(LeonsDBException.class, () -> svc.leaderboard(Ladder.class, "missing",
                    com.leonsdb.connector.api.Leaderboard.Order.HIGHEST_FIRST));
        }
    }

    @Test
    void changingAFieldTypeIsRefusedByTheServer() {
        try (var client = client()) {
            service(client, Stats.class);
            var e = assertThrows(LeonsDBException.class, () -> service(client, StatsWithChangedType.class));
            assertEquals("SCHEMA_CONFLICT", ((LeonsDBException) e.getCause()).reason(), e.getMessage());
        }
    }

    @Test
    void theServerEnforcesDeclaredTypes() {
        try (var client = client()) {
            service(client, Stats.class);
            var ref = RecordRef.newBuilder().setNamespace("contract").setCollection("stats")
                    .setPlayer(com.google.protobuf.ByteString.copyFrom(new byte[16])).build();
            var bad = Operation.newBuilder().setField("kills")
                    .setSet(Value.newBuilder().setStringValue("lots")).build();
            var e = assertThrows(ExecutionException.class,
                    () -> client.apply(ref, List.of(bad), 0, OptionalLong.empty()).get(5, TimeUnit.SECONDS));
            assertEquals("TYPE_MISMATCH", ((RequestException) e.getCause()).reason());
        }
    }

    @Test
    void concurrentGlobalUpdatesFromTwoServersAreAllKept() throws Exception {
        try (var a = client(); var b = client()) {
            var fromA = service(a, Counter.class).global(Counter.class);
            var fromB = service(b, Counter.class).global(Counter.class);
            List<CompletableFuture<Counter>> all = new ArrayList<>();
            for (int i = 0; i < 25; i++) {
                all.add(fromA.update("hits", c -> c.value++));
                all.add(fromB.update("hits", c -> c.value++));
            }
            CompletableFuture.allOf(all.toArray(new CompletableFuture<?>[0])).get(60, TimeUnit.SECONDS);
            assertEquals(50, fromA.get("hits").get(5, TimeUnit.SECONDS).orElseThrow().value);
            assertTrue(fromB.delete("hits").get(5, TimeUnit.SECONDS));
        }
    }

    @PlayerData(namespace = "contract", collection = "wallets")
    public static class Wallet {
        public long coins;
        public int kills;
    }

    @GlobalData(namespace = "contract", collection = "accounts")
    public static class Account {
        public long balance;
    }

    @Test
    void transactionsAreAtomicAcrossPlayersAndGlobalRecords() throws Exception {
        UUID alice = UUID.randomUUID();
        UUID bob = UUID.randomUUID();
        UUID carol = UUID.randomUUID(); // offline
        try (var client = client()) {
            var svc = service(client, Wallet.class, Account.class);
            svc.loadPlayer(alice);
            svc.loadPlayer(bob);
            svc.player(alice).update(Wallet.class, w -> w.coins = 1000).get(5, TimeUnit.SECONDS);
            svc.player(alice).get(Wallet.class).kills = 3; // not saved yet

            svc.transaction()
                    .update(alice, Wallet.class, w -> w.coins -= 300)
                    .update(bob, Wallet.class, w -> w.coins += 100)
                    .update(carol, Wallet.class, w -> w.coins += 100)
                    .update(Account.class, "bank", a -> a.balance += 100)
                    .commit().get(5, TimeUnit.SECONDS);
            assertEquals(700, svc.player(alice).get(Wallet.class).coins);

            // Refused as a whole: the abort in the last change undoes nothing
            // because nothing was stored.
            var refused = svc.transaction()
                    .update(alice, Wallet.class, w -> w.coins -= 1)
                    .update(Account.class, "bank", a -> {
                        throw new com.leonsdb.connector.api.TransactionAbortedException("closed");
                    })
                    .commit();
            assertThrows(ExecutionException.class, () -> refused.get(5, TimeUnit.SECONDS));
            assertEquals(700, svc.player(alice).get(Wallet.class).coins);

            svc.unloadPlayer(alice).get(5, TimeUnit.SECONDS);
            svc.unloadPlayer(bob).get(5, TimeUnit.SECONDS);
        }
        // Another Minecraft server sees exactly the committed state.
        try (var client = client()) {
            var svc = service(client, Wallet.class, Account.class);
            svc.loadPlayer(alice);
            svc.loadPlayer(bob);
            svc.loadPlayer(carol);
            assertEquals(700, svc.player(alice).get(Wallet.class).coins);
            assertEquals(3, svc.player(alice).get(Wallet.class).kills, "pending changes were saved with the transaction");
            assertEquals(100, svc.player(bob).get(Wallet.class).coins);
            assertEquals(100, svc.player(carol).get(Wallet.class).coins);
            assertEquals(100, svc.global(Account.class).get("bank").get(5, TimeUnit.SECONDS).orElseThrow().balance);
        }
    }

    @PlayerData(namespace = "contract", collection = "perks")
    public static class Perks {
        public String rank;
    }

    private static void eventually(java.util.function.BooleanSupplier ok, String what) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!ok.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for: " + what);
            }
            Thread.sleep(10);
        }
    }

    @Test
    void payingAPlayerOnAnotherServer() throws Exception {
        UUID alice = UUID.randomUUID();
        UUID bob = UUID.randomUUID();
        try (var lobbyClient = client(); var survivalClient = client()) {
            var lobby = service(lobbyClient, Wallet.class, Perks.class);
            var survival = service(survivalClient, Wallet.class); // survival does not use Perks
            lobby.loadPlayer(alice);
            survival.loadPlayer(bob);
            lobby.player(alice).update(Wallet.class, w -> w.coins = 50).get(5, TimeUnit.SECONDS);
            Wallet bobs = survival.player(bob).get(Wallet.class);
            bobs.kills = 4; // survival has not saved this yet

            lobby.transaction()
                    .update(alice, Wallet.class, w -> w.coins -= 10)
                    .update(bob, Wallet.class, w -> w.coins += 10)
                    .update(bob, Perks.class, p -> p.rank = "vip")
                    .commit().get(5, TimeUnit.SECONDS);
            assertEquals(40, lobby.player(alice).get(Wallet.class).coins);
            // Survival applied it to Bob's live data, keeping his unsaved kills.
            eventually(() -> bobs.coins == 10, "bob's coins on survival");
            assertEquals(4, bobs.kills);
            eventually(() -> survival.pendingRemoteWrites() == 0, "survival released bob");

            survival.unloadPlayer(bob).get(5, TimeUnit.SECONDS);
            lobby.unloadPlayer(alice).get(5, TimeUnit.SECONDS);
        }
        try (var client = client()) {
            var svc = service(client, Wallet.class, Perks.class);
            svc.loadPlayer(bob);
            assertEquals(10, svc.player(bob).get(Wallet.class).coins, "stored exactly once");
            assertEquals(4, svc.player(bob).get(Wallet.class).kills);
            assertEquals("vip", svc.player(bob).get(Perks.class).rank, "a record survival does not cache is written as is");
        }
    }

    @Test
    void paymentsBothWaysBetweenTwoServersKeepTheTotal() throws Exception {
        UUID alice = UUID.randomUUID();
        UUID bob = UUID.randomUUID();
        try (var lobbyClient = client(); var survivalClient = client()) {
            var lobby = service(lobbyClient, Wallet.class);
            var survival = service(survivalClient, Wallet.class);
            lobby.loadPlayer(alice);
            survival.loadPlayer(bob);
            lobby.player(alice).update(Wallet.class, w -> w.coins = 100).get(5, TimeUnit.SECONDS);
            survival.player(bob).update(Wallet.class, w -> w.coins = 100).get(5, TimeUnit.SECONDS);

            List<CompletableFuture<Void>> all = new ArrayList<>();
            for (int i = 0; i < 15; i++) {
                all.add(lobby.transaction()
                        .update(alice, Wallet.class, w -> w.coins -= 3)
                        .update(bob, Wallet.class, w -> w.coins += 3).commit());
                all.add(survival.transaction()
                        .update(bob, Wallet.class, w -> w.coins -= 2)
                        .update(alice, Wallet.class, w -> w.coins += 2).commit());
            }
            int failed = 0;
            for (var f : all) {
                try {
                    f.get(30, TimeUnit.SECONDS);
                } catch (ExecutionException e) {
                    failed++; // a refusal is fine (both sides busy); a partial payment is not
                }
            }
            Wallet a = lobby.player(alice).get(Wallet.class);
            Wallet b = survival.player(bob).get(Wallet.class);
            eventually(() -> a.coins + b.coins == 200 && lobby.pendingRemoteWrites() == 0 && survival.pendingRemoteWrites() == 0,
                    "settled; now " + a.coins + " + " + b.coins);
            assertTrue(failed < all.size(), "some payments went through");
            lobby.unloadPlayer(alice).get(5, TimeUnit.SECONDS);
            survival.unloadPlayer(bob).get(5, TimeUnit.SECONDS);
        }
        try (var client = client()) {
            var svc = service(client, Wallet.class);
            svc.loadPlayer(alice);
            svc.loadPlayer(bob);
            assertEquals(200, svc.player(alice).get(Wallet.class).coins + svc.player(bob).get(Wallet.class).coins);
        }
    }

    @Test
    void theOwnerChecksConditionsOnLiveData() throws Exception {
        UUID bob = UUID.randomUUID();
        try (var survivalClient = client(); var other = client()) {
            var survival = service(survivalClient, Wallet.class);
            survival.loadPlayer(bob);
            survival.player(bob).get(Wallet.class).coins = 70; // live, not saved yet

            var ref = RecordRef.newBuilder().setNamespace("contract").setCollection("wallets")
                    .setPlayer(com.leonsdb.connector.core.cache.TestPlatform.bytes(bob)).build();
            var take100 = Operation.newBuilder().setField("coins").setIncrement(Value.newBuilder().setLongValue(-100)).build();
            var atLeast100 = com.leonsdb.proto.v1.Condition.newBuilder().setField("coins")
                    .setComparison(com.leonsdb.proto.v1.Comparison.COMPARISON_GREATER_OR_EQUAL)
                    .setValue(Value.newBuilder().setLongValue(100)).build();
            var req = com.leonsdb.proto.v1.TransactRequest.newBuilder().addWrites(com.leonsdb.proto.v1.TransactionWrite.newBuilder()
                    .setRef(ref).addOperations(take100).addOwnerOperations(take100).addConditions(atLeast100)).build();
            var e = assertThrows(ExecutionException.class, () -> other.transact(req).get(5, TimeUnit.SECONDS));
            assertEquals("CONDITION_FAILED", ((RequestException) e.getCause()).reason(), e.getCause().getMessage());
            eventually(() -> survival.pendingRemoteWrites() == 0, "released");
            assertEquals(70, survival.player(bob).get(Wallet.class).coins);

            survival.player(bob).get(Wallet.class).coins = 130;
            other.transact(req).get(5, TimeUnit.SECONDS);
            eventually(() -> survival.player(bob).get(Wallet.class).coins == 30, "applied to the live data");
        }
    }

    private static com.leonsdb.connector.api.ChannelMessage nextNot(String skip,
            java.util.concurrent.BlockingQueue<com.leonsdb.connector.api.ChannelMessage> q) throws InterruptedException {
        while (true) {
            var m = q.poll(5, TimeUnit.SECONDS);
            if (m == null || !m.text().equals(skip)) {
                return m;
            }
        }
    }

    @Test
    void channelsCarryMessagesBetweenServers() throws Exception {
        try (var lobbyClient = client(); var survivalClient = client()) {
            var lobby = service(lobbyClient);
            var survival = service(survivalClient);
            var atLobby = new java.util.concurrent.LinkedBlockingQueue<com.leonsdb.connector.api.ChannelMessage>();
            var atSurvival = new java.util.concurrent.LinkedBlockingQueue<com.leonsdb.connector.api.ChannelMessage>();
            var lobbySub = lobby.channel("contract.chat").subscribe(atLobby::add);
            survival.channel("contract.chat").subscribe(atSurvival::add);
            survival.channel("contract.other").subscribe(atSurvival::add);
            eventually(() -> {
                try {
                    return lobby.channel("contract.chat").publish("ping").get(5, TimeUnit.SECONDS) == 2;
                } catch (Exception e) {
                    return false;
                }
            }, "both subscribed");
            atLobby.clear();
            atSurvival.clear();

            assertEquals(2, lobby.channel("contract.chat").publish("Steve: hello").get(5, TimeUnit.SECONDS));
            var m = nextNot("ping", atSurvival);
            assertEquals("Steve: hello", m.text());
            assertEquals("test-server", m.sender());
            assertEquals("contract.chat", m.channel());
            assertEquals("Steve: hello", nextNot("ping", atLobby).text(), "the sender gets its own message too");

            lobbySub.close();
            lobbySub.close();
            eventually(() -> {
                try {
                    return survival.channel("contract.chat").publish("x").get(5, TimeUnit.SECONDS) == 1;
                } catch (Exception e) {
                    return false;
                }
            }, "lobby unsubscribed");
            assertThrows(LeonsDBException.class, () -> lobby.channel("Not A Channel"));
        }
    }

    @Test
    void concurrentTransfersFromTwoServersKeepTheTotal() throws Exception {
        try (var a = client(); var b = client()) {
            var fromA = service(a, Account.class);
            var fromB = service(b, Account.class);
            fromA.global(Account.class).update("x", acc -> acc.balance = 100).get(5, TimeUnit.SECONDS);
            fromA.global(Account.class).update("y", acc -> acc.balance = 100).get(5, TimeUnit.SECONDS);
            List<CompletableFuture<Void>> all = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                String from = i % 2 == 0 ? "x" : "y";
                String to = i % 2 == 0 ? "y" : "x";
                for (var svc : List.of(fromA, fromB)) {
                    all.add(svc.transaction()
                            .update(Account.class, from, acc -> {
                                if (acc.balance < 30) {
                                    throw new com.leonsdb.connector.api.TransactionAbortedException("empty");
                                }
                                acc.balance -= 30;
                            })
                            .update(Account.class, to, acc -> acc.balance += 30)
                            .commit()
                            .exceptionally(err -> null));
                }
            }
            CompletableFuture.allOf(all.toArray(new CompletableFuture<?>[0])).get(60, TimeUnit.SECONDS);
            long x = fromA.global(Account.class).get("x").get(5, TimeUnit.SECONDS).orElseThrow().balance;
            long y = fromA.global(Account.class).get("y").get(5, TimeUnit.SECONDS).orElseThrow().balance;
            assertEquals(200, x + y, "no coins created or lost");
            assertTrue(x >= 0 && y >= 0);
        }
    }

    @PlayerData(namespace = "contract", collection = "vaults")
    public static class Vault {
        @com.leonsdb.connector.api.Transactional public long gold;
        public int kills;
    }

    @Test
    void historyAndRollbackOfAPlayerOnAnotherServer() throws Exception {
        UUID bob = UUID.randomUUID();
        try (var lobbyClient = client(); var survivalClient = client()) {
            var lobby = service(lobbyClient, Vault.class);
            var survival = service(survivalClient, Vault.class);
            survival.loadPlayer(bob);
            survival.player(bob).update(Vault.class, v -> v.gold = 100).get(5, TimeUnit.SECONDS);
            Thread.sleep(20);
            java.time.Instant before = java.time.Instant.now();
            Thread.sleep(20);
            // A dupe exploit: gold and kills change on survival.
            survival.player(bob).update(Vault.class, v -> v.gold = 5000).get(5, TimeUnit.SECONDS);
            Vault live = survival.player(bob).get(Vault.class);
            live.kills = 7;
            survival.player(bob).save().get(5, TimeUnit.SECONDS);

            // Staff on the lobby look at Bob's past, while he plays on survival.
            Vault then = lobby.history().player(bob, Vault.class, before).get(5, TimeUnit.SECONDS).orElseThrow();
            assertEquals(100, then.gold);
            assertEquals(0, then.kills);
            var changes = lobby.history().changes(bob, Vault.class, 10).get(5, TimeUnit.SECONDS);
            var gold = changes.stream().filter(c -> c.fields().containsKey("gold")).findFirst().orElseThrow();
            assertEquals(100L, gold.fields().get("gold").before());
            assertEquals(5000L, gold.fields().get("gold").after());
            assertEquals("contract-test", gold.by());
            assertTrue(gold.exact(), "gold is @Transactional: every change is kept on its own");

            var preview = lobby.history().rollbackPlayer(bob, before).preview().get(5, TimeUnit.SECONDS);
            assertFalse(preview.applied());
            assertEquals(1, preview.recordsChanged());
            assertEquals(List.of("gold", "kills"), preview.records().get(0).fields());
            assertEquals(5000, live.gold, "a preview changes nothing");

            var result = lobby.history().rollbackPlayer(bob, before).apply().get(10, TimeUnit.SECONDS);
            assertTrue(result.applied());
            assertEquals(1, result.recordsChanged());
            assertEquals(0, result.recordsFailed());
            // Survival applied it to Bob's live data.
            eventually(() -> live.gold == 100 && live.kills == 0, "bob's live data rolled back on survival");
            eventually(() -> survival.pendingRemoteWrites() == 0, "survival released bob");

            // Only the kills, on one collection: gold stays.
            live.kills = 9;
            survival.player(bob).update(Vault.class, v -> v.gold = 120).get(5, TimeUnit.SECONDS);
            var partial = lobby.history().rollback(bob, Vault.class, before).onlyFields("kills").apply().get(10, TimeUnit.SECONDS);
            assertEquals(1, partial.recordsChanged());
            eventually(() -> live.kills == 0, "kills rolled back");
            assertEquals(120, live.gold);
            survival.unloadPlayer(bob).get(5, TimeUnit.SECONDS);
        }
        try (var client = client()) {
            var svc = service(client, Vault.class);
            svc.loadPlayer(bob);
            assertEquals(120, svc.player(bob).get(Vault.class).gold, "stored as rolled back");
            assertEquals(0, svc.player(bob).get(Vault.class).kills);
        }
    }
}
