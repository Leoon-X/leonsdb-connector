// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.leonsdb.connector.api.DataNotLoadedException;
import com.leonsdb.connector.api.LeonsDBException;
import com.leonsdb.connector.api.PlayerData;
import com.leonsdb.connector.api.Transactional;
import com.leonsdb.connector.core.FakeServer;
import com.leonsdb.connector.core.LeonsDBClient;
import com.leonsdb.connector.core.schema.ClassSchema;
import com.leonsdb.connector.core.schema.Codecs;
import com.leonsdb.proto.v1.RecordRef;
import com.leonsdb.proto.v1.Value;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PlayerCacheTest {

    enum Rank { MEMBER, VIP }

    @PlayerData(namespace = "pvp", collection = "stats")
    public static class Stats {
        public int kills;
        public int level = 1;
        public double kd;
        @Transactional public long coins;
        public Rank rank = Rank.MEMBER;
        public String title;
    }

    @TempDir
    Path tmp;

    private FakeServer server;
    private LeonsDBClient client;
    private LeonsDBService service;
    private TestPlatform platform;
    private final UUID steve = UUID.randomUUID();

    @BeforeEach
    void setUp() throws Exception {
        server = new FakeServer();
        client = server.client();
        platform = new TestPlatform();
        service = TestPlatform.service(client, Logger.getLogger("test"), platform, tmp.resolve("recovery"));
        service.register(Stats.class);
    }

    @AfterEach
    void tearDown() {
        service.shutdown(Duration.ofSeconds(1));
        client.close();
        server.close();
    }

    private RecordRef ref(UUID uuid) {
        return LeonsDBService.playerRef(ClassSchema.of(Stats.class, Codecs.withDefaults()), uuid);
    }

    private Stats stats() {
        return service.player(steve).get(Stats.class);
    }

    private static void await(BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                fail("timed out waiting for: " + what);
            }
            Thread.sleep(5);
        }
    }

    private int storedInt(String field) {
        FakeServer.Stored s = server.get(ref(steve));
        return s == null || !s.fields.containsKey(field) ? 0 : s.fields.get(field).getIntValue();
    }

    @Test
    void newPlayerStartsWithDefaults() {
        service.loadPlayer(steve);
        assertEquals(0, stats().kills);
        assertEquals(1, stats().level);
        assertTrue(service.player(steve).isLoaded());
    }

    @Test
    void manyChangesAreMergedIntoOneWrite() throws Exception {
        service.loadPlayer(steve);
        for (int i = 0; i < 40; i++) {
            stats().kills++;
        }
        service.flushTick();
        await(() -> storedInt("kills") == 40, "kills saved");
        assertEquals(1, server.applies.size(), "40 kills must be one write");

        stats().kills += 2;
        service.flushTick();
        await(() -> storedInt("kills") == 42, "second save");
        var lastKills = server.applies.get(1).getOperationsList().stream()
                .filter(o -> o.getField().equals("kills")).findFirst().orElseThrow();
        assertEquals(2, lastKills.getIncrement().getIntValue(), "sent as an increment of the difference");

        int before = server.applies.size();
        service.flushTick();
        Thread.sleep(50);
        assertEquals(before, server.applies.size(), "nothing changed: nothing sent");
    }

    @Test
    void nonZeroDefaultsAreSetNotIncremented() throws Exception {
        service.loadPlayer(steve);
        stats().level++; // 1 -> 2, never stored before
        service.player(steve).save().get(5, TimeUnit.SECONDS);
        assertEquals(2, storedInt("level"));
    }

    @Test
    void lostResponseIsNeverAppliedTwice() throws Exception {
        // Existing values, so changes are sent as increments: the case where
        // applying a write twice would really double-count.
        server.put(ref(steve), Map.of("kills", Value.newBuilder().setIntValue(0).build(),
                "coins", Value.newBuilder().setLongValue(0).build()));
        service.loadPlayer(steve);
        stats().kills += 10;
        stats().coins += 500;

        // The server stores the write, but the answer never arrives.
        server.dropResponseAfterApply.set(1);
        assertThrows(ExecutionException.class, () -> service.player(steve).save().get(5, TimeUnit.SECONDS));

        stats().kills += 5;
        service.player(steve).save().get(5, TimeUnit.SECONDS);

        FakeServer.Stored s = server.get(ref(steve));
        assertEquals(15, s.fields.get("kills").getIntValue(), "kills counted exactly once");
        assertEquals(500, s.fields.get("coins").getLongValue(), "coins counted exactly once");
    }

    @Test
    void outageIsRetriedUntilSaved() throws Exception {
        service.loadPlayer(steve);
        stats().kills = 3;
        server.failNext.set(3);
        for (int i = 0; i < 3; i++) {
            service.flushTick();
            Thread.sleep(20);
        }
        assertEquals(0, storedInt("kills"), "nothing could be saved during the outage");
        service.flushTick();
        await(() -> storedInt("kills") == 3, "saved after the outage");
    }

    @Test
    void changesWhileAWriteIsInFlightAreQueuedAndMerged() throws Exception {
        service.loadPlayer(steve);
        server.holdApplies = new CountDownLatch(1);
        stats().kills = 1;
        service.flushTick(); // in flight, held by the server
        for (int i = 0; i < 10; i++) {
            stats().kills++;
            service.flushTick(); // queued, replacing older queued snapshots
        }
        server.holdApplies.countDown();
        await(() -> storedInt("kills") == 11, "all changes saved");
        assertEquals(2, server.applies.size(), "one write in flight plus one merged follow-up");
    }

    @Test
    void transactionalUpdateConfirmsDurability() throws Exception {
        service.loadPlayer(steve);
        var handle = service.player(steve);
        handle.update(Stats.class, s -> s.coins += 250).get(5, TimeUnit.SECONDS);
        assertEquals(250, server.get(ref(steve)).fields.get("coins").getLongValue());

        server.failNext.set(1);
        var failed = handle.update(Stats.class, s -> s.coins -= 100);
        assertThrows(ExecutionException.class, () -> failed.get(5, TimeUnit.SECONDS));
        assertEquals(150, stats().coins, "the change stays in memory");
        service.flushTick();
        await(() -> server.get(ref(steve)).fields.get("coins").getLongValue() == 150, "retried later");
    }

    @Test
    void loadingFailsWhenTheServerIsDownSoThePlayerIsNotLetIn() {
        server.failNext.set(10);
        assertThrows(LeonsDBException.class, () -> service.loadPlayer(steve));
        assertFalse(service.player(steve).isLoaded());
        assertThrows(DataNotLoadedException.class, this::stats);
    }

    @Test
    void existingDataIsLoaded() {
        server.put(ref(steve), Map.of("kills", Value.newBuilder().setIntValue(99).build(),
                "title", Value.newBuilder().setStringValue("Champion").build()));
        service.loadPlayer(steve);
        assertEquals(99, stats().kills);
        assertEquals("Champion", stats().title);
    }

    @Test
    void unreadableStoredValuesAreNeverOverwritten() throws Exception {
        server.put(ref(steve), Map.of("rank", Value.newBuilder().setStringValue("LEGEND").build()));
        service.loadPlayer(steve);
        assertEquals(Rank.MEMBER, stats().rank, "unknown value: default in memory");
        stats().rank = Rank.VIP;
        stats().kills = 1;
        service.player(steve).save().get(5, TimeUnit.SECONDS);
        assertEquals("LEGEND", server.get(ref(steve)).fields.get("rank").getStringValue(),
                "a value this plugin version cannot read must be left alone");
        assertEquals(1, storedInt("kills"));
    }

    @Test
    void quitSavesThenUnloads() throws Exception {
        service.loadPlayer(steve);
        stats().kills = 7;
        service.unloadPlayer(steve).get(5, TimeUnit.SECONDS);
        assertEquals(7, storedInt("kills"));
        assertFalse(service.player(steve).isLoaded());
    }

    @Test
    void quitDuringOutageKeepsRetryingAndRejoinWaitsForIt() throws Exception {
        service.loadPlayer(steve);
        stats().kills = 5;
        server.failNext.set(2);
        var unloading = service.unloadPlayer(steve);
        assertFalse(unloading.isDone());

        // The player rejoins while the save is still being retried. Loading
        // must wait for it, otherwise it would read the old value 0.
        service.loadPlayer(steve);
        assertTrue(unloading.isDone());
        assertEquals(5, stats().kills);
    }

    @Test
    void unregisteredClassesAndGlobalMisuseHaveClearErrors() {
        service.loadPlayer(steve);
        var e = assertThrows(LeonsDBException.class, () -> service.player(steve).get(String.class));
        assertTrue(e.getMessage().contains("not registered"));
        assertThrows(LeonsDBException.class, () -> service.global(Stats.class));
    }

    @Test
    void shutdownWritesRecoveryFilesWhenTheServerIsGone() throws Exception {
        service.loadPlayer(steve);
        stats().kills = 12;
        server.failNext.set(1000);
        service.shutdown(Duration.ofMillis(500));
        Path dir = tmp.resolve("recovery");
        assertTrue(Files.isDirectory(dir));
        try (var files = Files.list(dir)) {
            var text = Files.readString(files.findFirst().orElseThrow());
            assertTrue(text.contains("kills"), text);
        }
    }

    @Test
    void changingATransactionalFieldDirectlyWarnsOnce() throws Exception {
        var records = new java.util.ArrayList<java.util.logging.LogRecord>();
        Logger log = Logger.getLogger("transactional-warning");
        log.setUseParentHandlers(false);
        log.addHandler(new java.util.logging.Handler() {
            @Override public void publish(java.util.logging.LogRecord r) { records.add(r); }
            @Override public void flush() { }
            @Override public void close() { }
        });
        var svc = TestPlatform.service(client, log, new TestPlatform(), tmp);
        svc.register(Stats.class);
        svc.loadPlayer(steve);
        svc.player(steve).get(Stats.class).coins += 5;
        svc.flushTick();
        svc.player(steve).get(Stats.class).coins += 5;
        svc.flushTick();
        long warnings = records.stream().filter(r -> r.getMessage().contains("@Transactional")).count();
        assertEquals(1, warnings, "warn exactly once per class");
        await(() -> server.get(ref(steve)) != null
                && server.get(ref(steve)).fields.get("coins").getLongValue() == 10, "still saved");
        svc.shutdown(Duration.ofSeconds(1));
    }

    // --- Ownership ---------------------------------------------------------------

    private com.google.protobuf.ByteString playerBytes() {
        return ref(steve).getPlayer();
    }

    @Test
    void ownershipIsTakenOnJoinAndReleasedAfterTheQuitSave() throws Exception {
        service.loadPlayer(steve);
        assertTrue(server.ownerOf(playerBytes()) != null, "joining takes ownership");
        stats().kills = 3;
        service.unloadPlayer(steve).get(5, TimeUnit.SECONDS);
        assertEquals(3, storedInt("kills"));
        await(() -> server.ownerOf(playerBytes()) == null, "ownership released after saving");
    }

    @Test
    void joiningLoadsDataInTheSameCallAsOwnership() throws Exception {
        service.loadPlayer(steve);
        stats().kills = 4;
        service.unloadPlayer(steve).get(5, TimeUnit.SECONDS);
        int gets = server.gets.get();
        service.loadPlayer(steve);
        assertEquals(4, stats().kills);
        assertEquals(gets, server.gets.get(), "no separate read on join");

        // A server from before 0.9 ignores the load: the data is read anyway.
        service.unloadPlayer(steve).get(5, TimeUnit.SECONDS);
        server.oldServer = true;
        service.loadPlayer(steve);
        assertEquals(4, stats().kills, "an old server must not make existing players look new");
        assertTrue(server.gets.get() > gets);
    }

    @Test
    void playersAreDroppedWhenTheDatabaseWasRestored() throws Exception {
        service.loadPlayer(steve);
        platform.online.add(steve);
        Thread.sleep(200); // the lease stream is open and knows the instance id
        stats().kills = 5; // not saved yet
        int writesBefore = server.applies.size();
        // LeonsDB restarts with data restored from a backup.
        server.instanceId = "fake-instance-2";
        server.dropWatchers();
        await(() -> !service.player(steve).isLoaded(), "the player was let go");
        await(() -> platform.disconnected.containsKey(steve), "and disconnected");
        assertTrue(platform.disconnected.get(steve).contains("restored"), platform.disconnected.get(steve));
        service.flushTick();
        Thread.sleep(300);
        assertEquals(writesBefore, server.applies.size(), "the old cached data was not written over the restored data");
        // Joining again loads the restored data.
        service.loadPlayer(steve);
        assertTrue(service.player(steve).isLoaded());
    }

    @Test
    void aQuickRejoinCannotOvertakeTheRelease() throws Exception {
        service.loadPlayer(steve);
        long first = server.epochOf(playerBytes());
        server.holdReleases = new java.util.concurrent.CountDownLatch(1);
        var left = service.unloadPlayer(steve);
        Thread.sleep(100);
        // Saved, but the release has not reached LeonsDB yet: the player has
        // not left yet, so a rejoin waits instead of being granted the old
        // epoch that the late release would then end.
        assertFalse(left.isDone(), "leaving completes only after the release");
        var rejoin = java.util.concurrent.CompletableFuture.runAsync(() -> service.loadPlayer(steve));
        Thread.sleep(100);
        assertFalse(rejoin.isDone(), "the rejoin waits for the release");
        server.holdReleases.countDown();
        left.get(5, TimeUnit.SECONDS);
        rejoin.get(5, TimeUnit.SECONDS);
        assertTrue(server.epochOf(playerBytes()) > first, "the rejoin got a new epoch");
        assertTrue(server.ownerOf(playerBytes()) != null, "and owns the player");
        stats().kills = 1;
        service.unloadPlayer(steve).get(5, TimeUnit.SECONDS);
        assertEquals(1, storedInt("kills"), "its saves are accepted");
    }

    @Test
    void joinIsRefusedWhileAnotherServerOwnsThePlayer() {
        server.takeOver(playerBytes(), false);
        var e = assertThrows(LeonsDBException.class, () -> service.loadPlayer(steve));
        assertTrue(e.getMessage().contains("owned by"), e.getMessage());
        assertFalse(service.player(steve).isLoaded());
    }

    @Test
    void handoverSavesFirstThenReleasesAndDisconnectsIfStillHere() throws Exception {
        service.loadPlayer(steve);
        platform.online.add(steve);
        stats().kills = 9; // not saved yet: the handover must save it

        server.requestHandover(playerBytes());
        await(() -> server.ownerOf(playerBytes()) == null, "released after handover");
        assertEquals(9, storedInt("kills"), "data saved before releasing");
        assertFalse(service.player(steve).isLoaded());

        // Still on this server after the grace period: they logged in elsewhere.
        await(() -> platform.disconnected.containsKey(steve), "disconnected after the grace period");
    }

    @Test
    void handoverDoesNotDisconnectAPlayerWhoAlreadyLeft() throws Exception {
        service.loadPlayer(steve);
        server.requestHandover(playerBytes()); // player is not online here (normal switch)
        await(() -> server.ownerOf(playerBytes()) == null, "released");
        Thread.sleep(400);
        assertFalse(platform.disconnected.containsKey(steve));
    }

    @Test
    void lostOwnershipStopsSavingAndDisconnects() throws Exception {
        service.loadPlayer(steve);
        platform.online.add(steve);
        server.takeOver(playerBytes(), true);
        await(() -> platform.disconnected.containsKey(steve), "disconnected after losing ownership");
        assertFalse(service.player(steve).isLoaded());
        int writes = server.applies.size();
        service.flushTick();
        Thread.sleep(100);
        assertEquals(writes, server.applies.size(), "nothing is written after ownership is lost");
    }

    @Test
    void aRefusedWriteRevealsLostOwnershipEvenWithoutNotice() throws Exception {
        service.loadPlayer(steve);
        platform.online.add(steve);
        server.takeOver(playerBytes(), false); // no event: only the refused write tells us
        stats().kills = 50;
        service.flushTick();
        await(() -> platform.disconnected.containsKey(steve), "disconnected after a refused write");
        assertEquals(0, storedInt("kills"), "the stale write was refused");
        assertFalse(service.player(steve).isLoaded());
    }

    @Test
    void shutdownReleasesOwnership() {
        service.loadPlayer(steve);
        service.shutdown(Duration.ofSeconds(5));
        assertNull(server.ownerOf(playerBytes()), "players can join elsewhere right after a restart");
    }

    @Test
    void mainThreadIsRequiredForUpdates() throws Exception {
        var offThread = TestPlatform.service(client, Logger.getLogger("test"), new TestPlatform(false), tmp);
        offThread.register(Stats.class);
        offThread.loadPlayer(steve);
        assertThrows(IllegalStateException.class,
                () -> offThread.player(steve).update(Stats.class, s -> s.kills++));
        assertNull(server.get(ref(steve)));
        offThread.shutdown(Duration.ofSeconds(1));
    }
}
