// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Changes several records together: every change is stored, or none is.
 * Even if the Minecraft server, LeonsDB or the network fails halfway, no
 * transaction is ever stored in part, and none is stored twice.
 *
 * <pre>{@code
 * LeonsDB.transaction()
 *     .update(alice, Stats.class, s -> {
 *         if (s.coins < 100) throw new TransactionAbortedException("Not enough coins");
 *         s.coins -= 100;
 *     })
 *     .update(bob, Stats.class, s -> s.coins += 100)
 *     .commit()
 *     .thenRun(() -> sender.sendMessage("Paid!"))
 *     .exceptionally(err -> { sender.sendMessage("Payment failed: " + err.getMessage()); return null; });
 * }</pre>
 *
 * <p>Changes run on a background thread, on copies of the data. They may
 * run more than once (when another server changed a record in the
 * meantime), so they must only modify the object they are given and must
 * not call the Minecraft server's API. The cached data of online players
 * only changes once the transaction is safely stored, on the main thread,
 * just before the returned future completes.
 *
 * <p>Two transactions on the same online player run one after the other,
 * so the second sees the result of the first: checks like "has enough
 * coins" cannot be fooled by double clicks.
 *
 * <p>A player can be part of a transaction when they are online on this
 * server or offline. A player who is online on another server cannot (that
 * server owns their data while they play); the commit then fails with
 * reason {@code "PLAYER_ONLINE"}.
 */
public interface Transaction {

    /**
     * Adds a change to a player's data.
     *
     * @param player the player
     * @param type   a registered class annotated with {@link PlayerData}
     * @param change modifies the data; may throw {@link TransactionAbortedException}
     * @param <T>    the data type
     * @return this transaction
     */
    <T> Transaction update(UUID player, Class<T> type, Consumer<? super T> change);

    /**
     * Adds a change to a global record, creating it if it does not exist.
     *
     * @param type   a registered class annotated with {@link GlobalData}
     * @param id     the record's key
     * @param change modifies the record; may throw {@link TransactionAbortedException}
     * @param <T>    the record type
     * @return this transaction
     */
    <T> Transaction update(Class<T> type, String id, Consumer<? super T> change);

    /**
     * Runs the changes and stores them atomically. Never blocks; call it
     * from any thread. A transaction can be committed once.
     *
     * @return completes, on the main thread, once everything is durably
     *         stored and cached data is updated. Fails with the
     *         {@link TransactionAbortedException} a change threw, or with a
     *         {@link LeonsDBException} if the transaction was refused, in
     *         which case nothing was changed. If LeonsDB could not be reached
     *         in time it fails with reason {@code "NOT_CONFIRMED"}: the
     *         outcome is then not known yet, and the connector keeps trying
     *         in the background until it is (never applying it twice).
     */
    CompletableFuture<Void> commit();
}
