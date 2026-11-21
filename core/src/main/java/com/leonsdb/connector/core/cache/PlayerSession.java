// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The cached data of one player this server owns: online, or whose data is
 * still being saved after they left.
 */
final class PlayerSession {

    final UUID uuid;
    final long loadedAtMillis = System.currentTimeMillis();
    final Map<Class<?>, CachedObject<?>> objects = new ConcurrentHashMap<>();
    private final long epoch;

    /** Set when the player leaves or is handed over; completes once saved. */
    volatile CompletableFuture<Void> unloading;

    /** Set when another server took the player; nothing may be saved any more. */
    volatile boolean lost;

    /**
     * What was saved when the player left or was handed over, per cached
     * object: if a server switch fails, the player is taken back and what
     * changed since then is merged in.
     */
    volatile Map<CachedObject<?>, Map<String, com.leonsdb.proto.v1.Value>> finals;

    /** Set when the player left while their handover was still being saved. */
    volatile boolean left;

    /** The player's name, if the platform told it; for LeonsDB's player directory. */
    volatile String name;

    /** Set when the player is handed over to another server rather than leaving. */
    volatile boolean handover;

    /** True while handover listeners run: they may still change the data. */
    volatile boolean handingOver;

    /** Completes once what changed after the handover is stored; see LeonsDBService. */
    final CompletableFuture<Void> lateChanges = new CompletableFuture<>();

    PlayerSession(UUID uuid, long epoch) {
        this.uuid = uuid;
        this.epoch = epoch;
    }

    // Transactions in progress, and what waits for them to end.
    private int transactions;
    private List<Runnable> whenNoTransactions = new ArrayList<>();

    /** @return the ownership epoch every write of this session carries */
    long epoch() {
        return epoch;
    }

    boolean isUnloading() {
        return unloading != null;
    }

    /** @return whether a transaction involving this player is in progress */
    synchronized boolean hasTransactions() {
        return transactions > 0;
    }

    /**
     * A transaction involving this player starts, unless the player is
     * leaving or was lost. Atomic with {@link #startUnloading}: on Folia a
     * transaction and the player's departure run on different threads, and
     * a transaction that began after the final snapshot was taken would be
     * undone by saving it.
     *
     * @return false if the player is leaving or lost; nothing began
     */
    synchronized boolean beginTransaction() {
        if (unloading != null || lost) {
            return false;
        }
        transactions++;
        return true;
    }

    /**
     * Marks the player as leaving, unless they already are. From then on no
     * transaction begins; {@code save} runs once none is in progress: right
     * away on this thread if none is, else when the last one ends.
     *
     * @param done completes when the player is saved and released
     * @param save takes the final snapshot and saves it
     * @return the leaving already in progress, or null if this call started it
     */
    CompletableFuture<Void> startUnloading(CompletableFuture<Void> done, Runnable save) {
        synchronized (this) {
            if (unloading != null) {
                return unloading;
            }
            unloading = done;
            if (transactions > 0) {
                whenNoTransactions.add(save);
                return null;
            }
        }
        save.run();
        return null;
    }

    /** A transaction ended; its changes are in the cached objects. */
    void endTransaction() {
        List<Runnable> run = java.util.Collections.emptyList();
        synchronized (this) {
            if (--transactions == 0) {
                run = whenNoTransactions;
                whenNoTransactions = new ArrayList<>();
            }
        }
        run.forEach(Runnable::run);
    }

    Collection<CachedObject<?>> all() {
        return objects.values();
    }
}
