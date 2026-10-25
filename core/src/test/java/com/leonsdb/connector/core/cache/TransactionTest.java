// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.leonsdb.connector.api.GlobalData;
import com.leonsdb.connector.api.LeonsDBException;
import com.leonsdb.connector.api.PlayerData;
import com.leonsdb.connector.api.TransactionAbortedException;
import com.leonsdb.connector.core.FakeServer;
import com.leonsdb.connector.core.LeonsDBClient;
import com.leonsdb.connector.core.schema.ClassSchema;
import com.leonsdb.connector.core.schema.Codecs;
import com.leonsdb.proto.v1.RecordRef;
import com.leonsdb.proto.v1.Value;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TransactionTest {

    @PlayerData(namespace = "eco", collection = "wallets")
    public static class Wallet {
        public long coins;
        public int kills;
        public String title;
    }

    @GlobalData(namespace = "eco", collection = "banks")
    public static class Bank {
        public long balance;
    }

    @TempDir
    Path tmp;

    private FakeServer server;
    private LeonsDBClient client;
    private LeonsDBService service;
    private final UUID alice = UUID.randomUUID();
    private final UUID bob = UUID.randomUUID();

    @BeforeEach
    void setUp() throws Exception {
        server = new FakeServer();
        client = server.client();
        service = TestPlatform.service(client, Logger.getLogger("test"), new TestPlatform(), tmp.resolve("recovery"));
        service.register(Wallet.class, Bank.class);
    }

    @AfterEach
    void tearDown() {
        service.shutdown(Duration.ofSeconds(1));
        client.close();
        server.close();
    }

    private static RecordRef ref(UUID uuid) {
        return LeonsDBService.playerRef(ClassSchema.of(Wallet.class, Codecs.withDefaults()), uuid);
    }

    private static RecordRef bank(String id) {
        return RecordRef.newBuilder().setNamespace("eco").setCollection("banks").setId(id).build();
    }

    private Wallet wallet(UUID uuid) {
        return service.player(uuid).get(Wallet.class);
    }

    private long stored(RecordRef ref, String field) {
        FakeServer.Stored s = server.get(ref);
        if (s == null || !s.fields.containsKey(field)) {
            return 0;
        }
        Value v = s.fields.get(field);
        return v.getKindCase() == Value.KindCase.INT_VALUE ? v.getIntValue() : v.getLongValue();
    }

    private void give(UUID uuid, long coins) throws Exception {
        service.player(uuid).update(Wallet.class, w -> w.coins = coins).get(5, TimeUnit.SECONDS);
    }

    private CompletableFuture<Void> pay(UUID from, UUID to, long amount) {
        return service.transaction()
                .update(from, Wallet.class, w -> {
                    if (w.coins < amount) {
                        throw new TransactionAbortedException("Not enough coins");
                    }
                    w.coins -= amount;
                })
                .update(to, Wallet.class, w -> w.coins += amount)
                .commit();
    }

    private static Throwable failure(CompletableFuture<?> f) throws Exception {
        try {
            f.get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            return e.getCause();
        }
        fail("expected the transaction to fail");
        return null;
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
    void payingBetweenOnlinePlayersChangesBothAtOnce() throws Exception {
        service.loadPlayer(alice);
        service.loadPlayer(bob);
        give(alice, 150);
        int appliesBefore = server.applies.size();

        pay(alice, bob, 100).get(5, TimeUnit.SECONDS);
        assertEquals(50, wallet(alice).coins);
        assertEquals(100, wallet(bob).coins);
        assertEquals(50, stored(ref(alice), "coins"));
        assertEquals(100, stored(ref(bob), "coins"));
        assertEquals(1, server.transacts.size(), "one atomic request");
        assertEquals(appliesBefore, server.applies.size(), "no separate writes");

        // The cache knows what the server has: the next save sends nothing.
        service.flushTick();
        Thread.sleep(50);
        assertEquals(appliesBefore, server.applies.size(), "nothing left to save, nothing reverted");
    }

    @Test
    void abortingChangesNothing() throws Exception {
        service.loadPlayer(alice);
        service.loadPlayer(bob);
        give(alice, 30);

        Throwable err = failure(pay(alice, bob, 100));
        assertInstanceOf(TransactionAbortedException.class, err);
        assertEquals("Not enough coins", err.getMessage());
        assertEquals(30, wallet(alice).coins);
        assertEquals(0, wallet(bob).coins);
        assertTrue(server.transacts.isEmpty(), "nothing was sent");

        // The objects are free again afterwards.
        service.player(alice).update(Wallet.class, w -> w.coins += 1).get(5, TimeUnit.SECONDS);
        assertEquals(31, stored(ref(alice), "coins"));
    }

    @Test
    void unsavedChangesAreStoredWithTheTransaction() throws Exception {
        service.loadPlayer(alice);
        service.loadPlayer(bob);
        give(alice, 100);
        wallet(alice).kills += 5; // not saved yet
        wallet(alice).title = "Rich";

        pay(alice, bob, 40).get(5, TimeUnit.SECONDS);
        assertEquals(5, stored(ref(alice), "kills"), "pending changes are part of the same atomic write");
        assertEquals(60, stored(ref(alice), "coins"));
        assertEquals("Rich", server.get(ref(alice)).fields.get("title").getStringValue());
    }

    @Test
    void changesMadeWhileTheTransactionRunsAreKept() throws Exception {
        service.loadPlayer(alice);
        service.loadPlayer(bob);
        give(alice, 100);

        server.holdTransacts = new CountDownLatch(1);
        CompletableFuture<Void> paid = pay(alice, bob, 40);
        await(() -> !server.transacts.isEmpty() || server.holdTransacts.getCount() == 1, "transaction sent");
        Thread.sleep(50);
        // The game keeps running: Alice earns coins and gets a kill.
        wallet(alice).coins += 7;
        wallet(alice).kills++;
        service.flushTick(); // queued behind the transaction
        server.holdTransacts.countDown();
        paid.get(5, TimeUnit.SECONDS);

        assertEquals(67, wallet(alice).coins, "both the payment and the earnings count");
        service.player(alice).save().get(5, TimeUnit.SECONDS);
        assertEquals(67, stored(ref(alice), "coins"));
        assertEquals(1, stored(ref(alice), "kills"));
        assertEquals(40, stored(ref(bob), "coins"));
    }

    @Test
    void doubleSpendingIsImpossible() throws Exception {
        service.loadPlayer(alice);
        service.loadPlayer(bob);
        give(alice, 150);

        // Two payments at the same moment, like a double click.
        CompletableFuture<Void> first = pay(alice, bob, 100);
        CompletableFuture<Void> second = pay(alice, bob, 100);
        int ok = 0;
        int refused = 0;
        for (CompletableFuture<Void> f : java.util.List.of(first, second)) {
            try {
                f.get(5, TimeUnit.SECONDS);
                ok++;
            } catch (ExecutionException e) {
                assertInstanceOf(TransactionAbortedException.class, e.getCause());
                refused++;
            }
        }
        assertEquals(1, ok);
        assertEquals(1, refused);
        assertEquals(50, wallet(alice).coins);
        assertEquals(50, stored(ref(alice), "coins"));
        assertEquals(100, stored(ref(bob), "coins"));
    }

    @Test
    void offlinePlayersAndGlobalRecords() throws Exception {
        service.loadPlayer(alice);
        give(alice, 500);
        server.put(bank("red"), Map.of("balance", Value.newBuilder().setLongValue(1000).build()));

        // Bob is offline: his record is read and written by version.
        service.transaction()
                .update(alice, Wallet.class, w -> w.coins -= 200)
                .update(bob, Wallet.class, w -> w.coins += 100)
                .update(Bank.class, "red", b -> b.balance += 100)
                .commit().get(5, TimeUnit.SECONDS);
        assertEquals(300, wallet(alice).coins);
        assertEquals(100, stored(ref(bob), "coins"));
        assertEquals(1100, stored(bank("red"), "balance"));
        assertEquals(0, server.transacts.get(0).getWrites(1).getEpoch(), "offline players are written outside any lease");
    }

    @Test
    void playerOnAnotherServerIsRefused() throws Exception {
        service.loadPlayer(alice);
        give(alice, 500);
        server.takeOver(ref(bob).getPlayer(), false); // Bob plays on another server

        Throwable err = failure(pay(alice, bob, 100));
        assertEquals("PLAYER_ONLINE", ((LeonsDBException) err).reason());
        assertEquals(500, wallet(alice).coins);
        assertEquals(500, stored(ref(alice), "coins"));
        assertNull(server.get(ref(bob)));
    }

    @Test
    void lostAnswerIsRetriedButAppliedOnce() throws Exception {
        service.loadPlayer(alice);
        service.loadPlayer(bob);
        give(alice, 100);

        server.dropResponseAfterApply.set(1);
        pay(alice, bob, 30).get(10, TimeUnit.SECONDS);
        assertEquals(2, server.transacts.size(), "sent again after the lost answer");
        assertEquals(server.transacts.get(0).getTransactionId(), server.transacts.get(1).getTransactionId(),
                "the retry is recognisable as the same transaction");
        assertEquals(70, stored(ref(alice), "coins"));
        assertEquals(30, stored(ref(bob), "coins"));
        assertEquals(70, wallet(alice).coins);
    }

    @Test
    void aShortOutageBeforeSendingIsWaitedOut() throws Exception {
        server.failNext.set(2); // the reads fail: LeonsDB is briefly unreachable
        service.transaction()
                .update(Bank.class, "red", b -> b.balance += 5)
                .update(bob, Wallet.class, w -> w.coins += 5)
                .commit().get(10, TimeUnit.SECONDS);
        assertEquals(5, stored(bank("red"), "balance"));
        assertEquals(5, stored(ref(bob), "coins"));
        assertEquals(1, server.transacts.size());
    }

    @Test
    void conflictWithAnotherServerReRunsTheChange() throws Exception {
        server.put(bank("red"), Map.of("balance", Value.newBuilder().setLongValue(10).build()));
        AtomicInteger runs = new AtomicInteger();
        service.transaction()
                .update(Bank.class, "red", b -> {
                    if (runs.incrementAndGet() == 1) {
                        // Another server deposits between our read and write.
                        server.put(bank("red"), Map.of("balance", Value.newBuilder().setLongValue(b.balance + 5).build()));
                    }
                    b.balance += 1;
                })
                .update(Bank.class, "blue", b -> b.balance += 1)
                .commit().get(5, TimeUnit.SECONDS);
        assertEquals(2, runs.get());
        assertEquals(16, stored(bank("red"), "balance"), "neither deposit is lost");
        assertEquals(1, stored(bank("blue"), "balance"), "the other record was written once");
    }

    @Test
    void leavingWaitsForTheTransaction() throws Exception {
        service.loadPlayer(alice);
        service.loadPlayer(bob);
        give(alice, 100);

        server.holdTransacts = new CountDownLatch(1);
        CompletableFuture<Void> paid = pay(alice, bob, 60);
        Thread.sleep(50);
        CompletableFuture<Void> left = service.unloadPlayer(alice);
        Thread.sleep(50);
        assertTrue(!left.isDone(), "the final save waits for the transaction");
        server.holdTransacts.countDown();
        paid.get(5, TimeUnit.SECONDS);
        left.get(5, TimeUnit.SECONDS);
        assertEquals(40, stored(ref(alice), "coins"), "the final save did not undo the payment");
        assertNull(server.ownerOf(ref(alice).getPlayer()), "released after saving");
    }

    @Test
    void misuseIsRejected() {
        var tx = service.transaction().update(alice, Wallet.class, w -> w.coins++);
        assertThrows(IllegalArgumentException.class, () -> tx.update(alice, Wallet.class, w -> w.kills++));
        assertThrows(LeonsDBException.class, () -> tx.update(Wallet.class, "x", w -> { }));
        assertThrows(LeonsDBException.class, () -> tx.update(alice, Bank.class, b -> { }));
        tx.commit();
        assertThrows(IllegalStateException.class, tx::commit);
        assertThrows(IllegalStateException.class, () -> tx.update(bob, Wallet.class, w -> { }));
    }

    @Test
    void mergeAddsNumbersAndOtherwiseTakesTheTransactionsValue() {
        Value i5 = Value.newBuilder().setIntValue(5).build();
        Value i8 = Value.newBuilder().setIntValue(8).build();
        Value i10 = Value.newBuilder().setIntValue(10).build();
        assertEquals(13, TransactionImpl.merge(i5, i8, i10).getIntValue(), "10 + (8 - 5)");
        assertEquals(8, TransactionImpl.merge(i5, i8, i5).getIntValue());
        Value max = Value.newBuilder().setIntValue(Integer.MAX_VALUE).build();
        assertEquals(i8, TransactionImpl.merge(i5, i8, max), "overflow: the transaction's value");
        Value a = Value.newBuilder().setStringValue("a").build();
        Value b = Value.newBuilder().setStringValue("b").build();
        Value c = Value.newBuilder().setStringValue("c").build();
        assertEquals(b, TransactionImpl.merge(a, b, c));
    }
}
