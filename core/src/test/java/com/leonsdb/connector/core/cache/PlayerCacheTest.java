// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
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
import java.util.concurrent.CompletableFuture;
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

    @PlayerData(namespace = "pvp", collection = "wallet")
    public static class Wallet {
        @Transactional public long coins = 100;
        public int visits;
    }

    @Test
    void aNewPlayersTransactionalDefaultsDoNotWarn() throws Exception {
        var records = new java.util.concurrent.CopyOnWriteArrayList<java.util.logging.LogRecord>();
        Logger log = Logger.getLogger("transactional-defaults");
        log.setUseParentHandlers(false);
        log.addHandler(new java.util.logging.Handler() {
            @Override public void publish(java.util.logging.LogRecord r) { records.add(r); }
            @Override public void flush() { }
            @Override public void close() { }
        });
        var svc = TestPlatform.service(client, log, new TestPlatform(), tmp);
        svc.register(Wallet.class);
        svc.loadPlayer(steve);
        svc.player(steve).get(Wallet.class).visits++;
        svc.flushTick();
        svc.player(steve).update(Wallet.class, w -> w.coins += 5).get(5, TimeUnit.SECONDS);
        svc.flushTick();
        svc.player(steve).save().get(5, TimeUnit.SECONDS);
        assertEquals(0, records.stream().filter(r -> r.getMessage().contains("@Transactional")).count(),
                "the default 100 and a change by update() are not direct changes");

        svc.player(steve).get(Wallet.class).coins -= 1;
        svc.flushTick();
        assertEquals(1, records.stream().filter(r -> r.getMessage().contains("@Transactional")).count(),
                "a direct change still warns");
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

        server.nextOwnerOnRelease = "other-server";
        server.requestHandover(playerBytes());
        await(() -> "other-server".equals(server.ownerOf(playerBytes())), "released after handover");
        assertEquals(9, storedInt("kills"), "data saved before releasing");
        assertFalse(service.ownsPlayer(steve));

        // Still on this server after the grace period, and the other server
        // keeps the player: they logged in elsewhere.
        await(() -> platform.disconnected.containsKey(steve), "disconnected after the grace period");
        assertFalse(service.ownsPlayer(steve), "never taken from the other server");
    }

    @Test
    void aPlayerWhoseSwitchFailedIsTakenBackWithEveryChange() throws Exception {
        server.put(ref(steve), Map.of("kills", Value.newBuilder().setIntValue(5).build(),
                "level", Value.newBuilder().setIntValue(1).build()));
        service.loadPlayer(steve);
        platform.online.add(steve);
        Stats mine = stats(); // a plugin keeps the object
        mine.kills = 9;

        server.nextOwnerOnRelease = "other-server"; // the server the player switches to
        server.requestHandover(playerBytes());
        await(() -> "other-server".equals(server.ownerOf(playerBytes())), "handed over");
        assertEquals(9, storedInt("kills"));

        // The other server saves a change; meanwhile the player gets a kill here.
        server.put(ref(steve), Map.of("level", Value.newBuilder().setIntValue(4).build()));
        mine.kills++;
        Thread.sleep(150);
        assertFalse(service.ownsPlayer(steve), "never taken while the other server has the player");

        // The login there fails: it lets go, and the player is still here.
        server.letGo(playerBytes());
        await(() -> service.ownsPlayer(steve), "taken back");
        assertSame(mine, stats(), "plugins keep the same object");
        assertEquals(10, mine.kills, "the kill made here meanwhile is kept");
        assertEquals(4, mine.level, "and the other server's change is merged in");
        service.player(steve).save().get(5, TimeUnit.SECONDS);
        assertEquals(10, storedInt("kills"));
        assertEquals(4, storedInt("level"));

        Thread.sleep(400); // past the grace period
        assertFalse(platform.disconnected.containsKey(steve), "never disconnected");
        service.unloadPlayer(steve).get(5, TimeUnit.SECONDS);
        assertNull(server.ownerOf(playerBytes()), "released normally when they leave");
    }

    @Test
    void takingBackStopsWhenTheSwitchWentThrough() throws Exception {
        service.loadPlayer(steve);
        platform.online.add(steve);
        server.nextOwnerOnRelease = "other-server";
        server.requestHandover(playerBytes());
        await(() -> server.takeBacks.get() > 0, "waiting to take the player back");

        platform.online.remove(steve); // the proxy moved them on
        service.unloadPlayer(steve);
        Thread.sleep(300);
        int tries = server.takeBacks.get();
        server.letGo(playerBytes()); // later, they leave the other server
        Thread.sleep(300);
        assertEquals(tries, server.takeBacks.get(), "stopped trying");
        assertNull(server.ownerOf(playerBytes()), "and never took the player");
    }

    @Test
    void aPlayerTakenBackJustAfterLeavingIsSavedAndReleased() throws Exception {
        service.loadPlayer(steve);
        platform.online.add(steve);
        Stats mine = stats();
        server.holdTakeBacks = new CountDownLatch(1);
        server.nextOwnerOnRelease = "other-server";
        server.requestHandover(playerBytes());
        await(() -> server.takeBacks.get() > 0, "taking back");

        // The switch fails, but the player leaves before the take-back is answered.
        mine.kills = 3;
        server.letGo(playerBytes());
        platform.online.remove(steve);
        service.unloadPlayer(steve);
        server.holdTakeBacks.countDown();

        await(() -> server.ownerOf(playerBytes()) == null && storedInt("kills") == 3, "saved and released");
        Thread.sleep(200);
        assertNull(server.ownerOf(playerBytes()), "not kept");
        assertFalse(service.player(steve).isLoaded());
    }

    @Test
    void withALeonsDBThatCannotTakeBackThePlayerIsDisconnectedAsBefore() throws Exception {
        server.noTakeBack = true;
        service.loadPlayer(steve);
        platform.online.add(steve);
        server.requestHandover(playerBytes());
        await(() -> platform.disconnected.containsKey(steve), "disconnected after the grace period");
        assertFalse(service.ownsPlayer(steve));
    }

    // --- Changes after a handover (quit events on a switch) -------------------

    /** The player switches to "other-server", which keeps them; they are still here. */
    private Stats handOver() throws Exception {
        server.put(ref(steve), Map.of("kills", Value.newBuilder().setIntValue(10).build(),
                "title", Value.newBuilder().setStringValue("Newbie").build()));
        service.loadPlayer(steve);
        platform.online.add(steve);
        server.ownersApplyOwnerOps = true;
        server.nextOwnerOnRelease = "other-server";
        server.requestHandover(playerBytes());
        await(() -> "other-server".equals(server.ownerOf(playerBytes())), "handed over");
        return stats();
    }

    /** The proxy moved the player on; the quit event runs here. */
    private CompletableFuture<Void> quit(Runnable quitHandler) {
        platform.online.remove(steve);
        quitHandler.run();
        return service.unloadPlayer(steve);
    }

    @Test
    void changesInTheQuitEventFollowThePlayer() throws Exception {
        handOver();
        assertTrue(service.player(steve).isLoaded(), "still usable until the player leaves");
        server.put(ref(steve), Map.of("kills", Value.newBuilder().setIntValue(13).build())); // the other server: +3
        int transacts = server.transacts.size();

        quit(() -> {
            Stats s = service.player(steve).get(Stats.class); // a plugin's quit handler
            s.kills += 2;
            s.title = "Veteran";
        }).get(5, TimeUnit.SECONDS);

        assertEquals(15, storedInt("kills"), "both servers' kills count");
        assertEquals("Veteran", server.get(ref(steve)).fields.get("title").getStringValue());
        var late = server.transacts.get(transacts).getWrites(0);
        var kills = late.getOwnerOperationsList().stream().filter(o -> o.getField().equals("kills")).findFirst().orElseThrow();
        assertEquals(2, kills.getIncrement().getIntValue(), "sent to the owner as a difference");
        assertFalse(service.player(steve).isLoaded(), "gone once they left");
        assertThrows(DataNotLoadedException.class, this::stats);
    }

    @Test
    void updateAndSaveAfterTheHandoverCompleteOnceTheChangeIsStored() throws Exception {
        handOver();
        int applies = server.applies.size();
        var updated = service.player(steve).update(Stats.class, s -> s.coins += 5);
        var saved = service.player(steve).save();
        Thread.sleep(100);
        assertFalse(updated.isDone(), "the player is still here: nothing to send yet");
        assertEquals(applies, server.applies.size(), "nothing written with the old ownership");
        quit(() -> { }).get(5, TimeUnit.SECONDS);
        updated.get(5, TimeUnit.SECONDS);
        saved.get(5, TimeUnit.SECONDS);
        assertEquals(5, server.get(ref(steve)).fields.get("coins").getLongValue());
        assertFalse(platform.disconnected.containsKey(steve), "no write was refused, nobody was disconnected");
    }

    @Test
    void aTakeBackAnsweredAfterTheLateChangesWereSentNeverSavesThemAgain() throws Exception {
        server.holdTakeBacks = new CountDownLatch(1);
        handOver();
        await(() -> server.takeBacks.get() > 0, "a take-back is waiting");
        quit(() -> stats().kills += 2).get(5, TimeUnit.SECONDS);
        assertEquals(12, storedInt("kills"));

        // Later the other server lets go, and the waiting take-back gets the player.
        server.letGo(playerBytes());
        server.holdTakeBacks.countDown();
        Thread.sleep(300);
        assertEquals(12, storedInt("kills"), "the kills were not added a second time");
        assertNull(server.ownerOf(playerBytes()), "let go again");
        assertFalse(service.player(steve).isLoaded());
    }

    @Test
    void aTakeBackAnsweredWhenThePlayerCameBackHereNeverRestoresOldData() throws Exception {
        server.holdTakeBacks = new CountDownLatch(1);
        handOver();
        await(() -> server.takeBacks.get() > 0, "a take-back is waiting");
        quit(() -> { }).get(5, TimeUnit.SECONDS); // the switch went through

        // Later the player plays elsewhere and then comes back here.
        server.put(ref(steve), Map.of("kills", Value.newBuilder().setIntValue(20).build()));
        server.letGo(playerBytes());
        service.loadPlayer(steve);
        platform.online.add(steve);
        Stats fresh = stats();
        assertEquals(20, fresh.kills);

        server.holdTakeBacks.countDown(); // the old take-back is answered only now
        Thread.sleep(300);
        assertSame(fresh, stats(), "the data from before the switch did not come back");
        fresh.kills++;
        service.player(steve).save().get(5, TimeUnit.SECONDS);
        assertEquals(21, storedInt("kills"));
        assertTrue(service.ownsPlayer(steve));
    }

    @Test
    void anEarlierSwitchsWaitNeverDisconnectsAPlayerInALaterSwitch() throws Exception {
        // The grace period here is 200 ms (TestPlatform).
        service.loadPlayer(steve);
        platform.online.add(steve);
        server.nextOwnerOnRelease = "other-server";
        server.requestHandover(playerBytes()); // first switch, which fails at once
        await(() -> "other-server".equals(server.ownerOf(playerBytes())), "handed over");
        server.letGo(playerBytes());
        await(() -> service.ownsPlayer(steve), "taken back");

        Thread.sleep(80);
        server.nextOwnerOnRelease = "other-server";
        server.requestHandover(playerBytes()); // second switch, slower to fail
        await(() -> "other-server".equals(server.ownerOf(playerBytes())), "handed over again");
        Thread.sleep(160); // the first switch's wait has ended meanwhile
        assertFalse(platform.disconnected.containsKey(steve), "not disconnected by the first switch's wait");
        server.letGo(playerBytes());
        await(() -> service.ownsPlayer(steve), "taken back again");
        Thread.sleep(300);
        assertFalse(platform.disconnected.containsKey(steve), "never disconnected");
    }

    @Test
    void nothingIsSentWhenNothingChangedAfterTheHandover() throws Exception {
        handOver();
        int transacts = server.transacts.size();
        quit(() -> service.player(steve).get(Stats.class)).get(5, TimeUnit.SECONDS);
        assertEquals(transacts, server.transacts.size());
    }

    @Test
    void lateChangesAreRetriedUntilStored() throws Exception {
        handOver();
        server.failNext.set(3);
        quit(() -> stats().kills++).get(15, TimeUnit.SECONDS);
        assertEquals(11, storedInt("kills"));
    }

    @Test
    void handoverListenersChangeTheDataTheNewServerLoads() throws Exception {
        var calls = new java.util.concurrent.CopyOnWriteArrayList<String>();
        var failing = service.onHandover(p -> {
            throw new IllegalStateException("a broken plugin");
        });
        var listener = service.onHandover(p -> {
            calls.add(Thread.currentThread().getName());
            service.player(p).get(Stats.class).title = "Traveller";
        });
        service.loadPlayer(steve);
        platform.online.add(steve);
        server.nextOwnerOnRelease = "other-server";
        server.requestHandover(playerBytes());
        await(() -> "other-server".equals(server.ownerOf(playerBytes())), "handed over");
        assertEquals("Traveller", server.get(ref(steve)).fields.get("title").getStringValue(),
                "saved with the handover, before the other server got the player");
        assertEquals(1, calls.size());

        listener.close();
        failing.close();
        listener.close(); // twice is fine
        server.letGo(playerBytes());
        await(() -> service.ownsPlayer(steve), "taken back");
        server.nextOwnerOnRelease = "other-server";
        server.requestHandover(playerBytes());
        await(() -> "other-server".equals(server.ownerOf(playerBytes())), "handed over again");
        assertEquals(1, calls.size(), "closed listeners are not called");
    }

    @Test
    void handoverListenersAreNotCalledWhenAPlayerSimplyLeaves() throws Exception {
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        service.onHandover(p -> calls.incrementAndGet());
        service.loadPlayer(steve);
        service.unloadPlayer(steve).get(5, TimeUnit.SECONDS);
        assertEquals(0, calls.get());
    }

    @Test
    void lateChangesAreWrittenToRecoveryFilesIfLeonsDBIsGoneAtShutdown() throws Exception {
        var svc = TestPlatform.service(client, Logger.getLogger("late-recovery"), platform, tmp.resolve("late"));
        svc.register(Stats.class);
        server.ownersApplyOwnerOps = true;
        svc.loadPlayer(steve);
        platform.online.add(steve);
        server.nextOwnerOnRelease = "other-server";
        server.requestHandover(playerBytes());
        await(() -> "other-server".equals(server.ownerOf(playerBytes())), "handed over");
        svc.player(steve).get(Stats.class).title = "Unsaved";
        server.failNext.set(1000);
        svc.shutdown(Duration.ofMillis(500));
        try (var files = Files.list(tmp.resolve("late"))) {
            String text = Files.readString(files.findFirst().orElseThrow());
            assertTrue(text.contains("Unsaved"), text);
        }
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
