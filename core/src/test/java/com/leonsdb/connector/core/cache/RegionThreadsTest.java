// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.leonsdb.connector.api.PlayerData;
import com.leonsdb.connector.core.FakeServer;
import com.leonsdb.connector.core.LeonsDBClient;
import com.leonsdb.connector.core.schema.ClassSchema;
import com.leonsdb.connector.core.schema.Codecs;
import com.leonsdb.proto.v1.RecordRef;
import com.leonsdb.proto.v1.Value;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The connector on Folia: every player in a region (a thread) of their own.
 * A player's data may only be read and changed on their thread, which the
 * guard field checks on every read.
 */
class RegionThreadsTest {

    @PlayerData(namespace = "eco", collection = "wallets")
    public static class Wallet {
        public long coins;
        public int kills;
        public RegionTestPlatform.Guard guard;
    }

    @TempDir
    Path tmp;

    private FakeServer server;
    private LeonsDBClient client;
    private RegionTestPlatform platform;
    private LeonsDBService service;
    private Codecs codecs;
    private final UUID alice = UUID.randomUUID();
    private final UUID bob = UUID.randomUUID();

    @BeforeEach
    void setUp() throws Exception {
        server = new FakeServer();
        client = server.client();
        platform = new RegionTestPlatform();
        codecs = Codecs.withDefaults();
        platform.registerGuard(codecs);
        service = new LeonsDBService(client, codecs, Logger.getLogger("test"), platform,
                new LeonsDBService.Settings("region-server", Duration.ofSeconds(5), Duration.ofSeconds(5),
                        Duration.ofMillis(200), tmp.resolve("recovery")));
        service.start();
        service.register(Wallet.class);
    }

    @AfterEach
    void tearDown() throws Exception {
        // As on a real shutdown: the regions have stopped when the plugin is disabled.
        platform.leave(alice);
        platform.leave(bob);
        platform.onGlobal(() -> {
            service.shutdown(Duration.ofSeconds(1));
            return null;
        });
        client.close();
        server.close();
        platform.close();
        assertEquals(List.of(), platform.violations, "player data touched on the wrong thread");
    }

    /** The player joins (loads off any game thread) and enters a region of their own. */
    private void join(UUID uuid, long coins) throws Exception {
        service.loadPlayer(uuid);
        platform.enter(uuid);
        platform.onRegion(uuid, () -> {
            Wallet w = service.player(uuid).get(Wallet.class);
            w.guard = new RegionTestPlatform.Guard(uuid);
            w.coins = coins;
            return service.player(uuid).save();
        }).get(5, TimeUnit.SECONDS);
    }

    private long coins(UUID uuid) throws Exception {
        return platform.onRegion(uuid, () -> service.player(uuid).get(Wallet.class).coins);
    }

    private long stored(UUID uuid) {
        FakeServer.Stored s = server.get(ref(uuid));
        Value v = s == null ? null : s.fields.get("coins");
        return v == null ? 0 : v.getLongValue();
    }

    private RecordRef ref(UUID uuid) {
        return LeonsDBService.playerRef(ClassSchema.of(Wallet.class, codecs), uuid);
    }

    private CompletableFuture<Void> pay(UUID from, UUID to, long amount) {
        return service.transaction()
                .update(from, Wallet.class, w -> w.coins -= amount)
                .update(to, Wallet.class, w -> w.coins += amount)
                .commit();
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

    @Test
    void theSaveLoopSavesEachPlayerOnTheirOwnThread() throws Exception {
        join(alice, 10);
        join(bob, 20);
        platform.onRegion(alice, () -> service.player(alice).get(Wallet.class).coins = 11);
        platform.onRegion(bob, () -> service.player(bob).get(Wallet.class).coins = 21);

        platform.onGlobal(() -> {
            service.flushTick();
            return null;
        });
        await(() -> stored(alice) == 11 && stored(bob) == 21, "both saved");
    }

    @Test
    void aTransactionBetweenPlayersInDifferentRegions() throws Exception {
        join(alice, 100);
        join(bob, 0);

        AtomicBoolean completedOnMain = new AtomicBoolean();
        CompletableFuture<Void> paid = platform.onRegion(alice, () -> pay(alice, bob, 30)
                .whenComplete((ok, err) -> completedOnMain.set(platform.isMainThread())));
        paid.get(5, TimeUnit.SECONDS);

        assertEquals(70, coins(alice));
        assertEquals(30, coins(bob));
        assertEquals(70, stored(alice));
        assertEquals(30, stored(bob));
        assertTrue(completedOnMain.get(), "the plugin hears of it on the global region thread");
    }

    @Test
    void manyTransactionsAndTheGameRunningAtOnceLoseNothing() throws Exception {
        join(alice, 1000);
        join(bob, 1000);

        // Payments both ways while each region keeps changing its player.
        List<CompletableFuture<Void>> payments = new java.util.ArrayList<>();
        for (int i = 0; i < 20; i++) {
            payments.add(pay(alice, bob, 3));
            payments.add(pay(bob, alice, 1));
            platform.onRegion(alice, () -> service.player(alice).get(Wallet.class).kills++);
            platform.onRegion(bob, () -> service.player(bob).get(Wallet.class).kills++);
            if (i % 5 == 0) {
                platform.onGlobal(() -> {
                    service.flushTick();
                    return null;
                });
            }
        }
        CompletableFuture.allOf(payments.toArray(new CompletableFuture<?>[0])).get(10, TimeUnit.SECONDS);

        assertEquals(1000 - 60 + 20, coins(alice));
        assertEquals(1000 + 60 - 20, coins(bob));
        platform.onRegion(alice, () -> service.player(alice).save()).get(5, TimeUnit.SECONDS);
        platform.onRegion(bob, () -> service.player(bob).save()).get(5, TimeUnit.SECONDS);
        assertEquals(960, stored(alice));
        assertEquals(1040, stored(bob));
    }

    @Test
    void leavingInTheMiddleOfATransactionKeepsIt() throws Exception {
        join(alice, 100);
        join(bob, 0);

        server.holdTransacts = new CountDownLatch(1);
        CompletableFuture<Void> paid = pay(alice, bob, 60);
        Thread.sleep(100); // the transaction is sent and held
        // Alice quits; the quit event runs on her region thread.
        CompletableFuture<Void> left = platform.onRegion(alice, () -> service.unloadPlayer(alice));
        Thread.sleep(50);
        assertFalse(left.isDone(), "the final save waits for the transaction");
        server.holdTransacts.countDown();
        paid.get(5, TimeUnit.SECONDS);
        left.get(5, TimeUnit.SECONDS);
        platform.leave(alice);

        assertEquals(40, stored(alice), "the final save did not undo the payment");
        assertEquals(60, stored(bob));
        assertEquals(null, server.ownerOf(ref(alice).getPlayer()), "released after saving");
    }

    @Test
    void aTransactionStartingAfterThePlayerLeftDoesNotUseTheirCache() throws Exception {
        join(alice, 100);
        join(bob, 0);
        platform.onRegion(alice, () -> service.unloadPlayer(alice)).get(5, TimeUnit.SECONDS);
        platform.leave(alice);

        // Alice is offline now: her record is read and written by version.
        pay(alice, bob, 10).get(5, TimeUnit.SECONDS);
        assertEquals(90, stored(alice));
        assertEquals(10, coins(bob));
    }

    @Test
    void anExpectedSwitchSavesTheChangesAtOnceAndKeepsThePlayer() throws Exception {
        join(alice, 5);
        platform.onRegion(alice, () -> service.player(alice).get(Wallet.class).coins = 42);
        assertEquals(5, stored(alice), "not saved yet");

        server.expectSwitch(ref(alice).getPlayer());
        await(() -> stored(alice) == 42, "saved on the hint, without waiting for the save loop");
        assertNotNull(server.ownerOf(ref(alice).getPlayer()), "still owned here: it was only a hint");

        // Changes after the hint are still saved as usual.
        platform.onRegion(alice, () -> service.player(alice).get(Wallet.class).coins = 43);
        platform.onGlobal(() -> {
            service.flushTick();
            return null;
        });
        await(() -> stored(alice) == 43, "saved after the hint");
    }

    @Test
    void updatesOnlyOnThePlayersThread() throws Exception {
        join(alice, 1);
        var err = assertThrows(java.util.concurrent.ExecutionException.class,
                () -> platform.onGlobal(() -> service.player(alice).update(Wallet.class, w -> w.coins++)));
        assertInstanceOf(IllegalStateException.class, err.getCause(), "the global region thread may not change a player's data");
        platform.onRegion(alice, () -> service.player(alice).update(Wallet.class, w -> w.coins++)).get(5, TimeUnit.SECONDS);
        assertEquals(2, stored(alice));
    }

    @Test
    void aHandoverSavesAndDisconnectsOnThePlayersThread() throws Exception {
        join(alice, 7);
        platform.onRegion(alice, () -> service.player(alice).get(Wallet.class).coins = 8);
        server.nextOwnerOnRelease = "other-server"; // and keeps the player: they logged in there
        server.requestHandover(ref(alice).getPlayer());
        await(() -> "other-server".equals(server.ownerOf(ref(alice).getPlayer())), "handed over");
        assertEquals(8, stored(alice));
        server.joinedThere(ref(alice).getPlayer());
        await(() -> platform.disconnected().containsKey(alice), "disconnected once they joined there");
    }

    @Test
    void aPlayerWhoseSwitchFailedIsTakenBackOnTheirThread() throws Exception {
        join(alice, 7);
        Wallet kept = platform.onRegion(alice, () -> service.player(alice).get(Wallet.class));
        server.nextOwnerOnRelease = "other-server";
        server.requestHandover(ref(alice).getPlayer());
        await(() -> "other-server".equals(server.ownerOf(ref(alice).getPlayer())), "handed over");
        platform.onRegion(alice, () -> kept.kills++);

        server.letGo(ref(alice).getPlayer()); // the switch failed
        await(() -> service.ownsPlayer(alice), "taken back");
        assertEquals(1, (int) platform.onRegion(alice, () -> service.player(alice).get(Wallet.class).kills));
        assertEquals(7, coins(alice));
        assertFalse(platform.disconnected().containsKey(alice));
    }

    @Test
    void changesInTheQuitEventAfterAHandoverAreTakenOnThePlayersThread() throws Exception {
        join(alice, 7);
        server.ownersApplyOwnerOps = true;
        server.nextOwnerOnRelease = "other-server";
        server.requestHandover(ref(alice).getPlayer());
        await(() -> "other-server".equals(server.ownerOf(ref(alice).getPlayer())), "handed over");

        // The quit event, on the player's thread.
        platform.onRegion(alice, () -> {
            service.player(alice).get(Wallet.class).coins += 3;
            return service.unloadPlayer(alice);
        }).get(5, TimeUnit.SECONDS);
        platform.leave(alice);
        assertEquals(10, stored(alice));
    }
}
