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

    /** A transaction involving this player started. */
    synchronized void beginTransaction() {
        transactions++;
    }

    /** A transaction ended; its changes are in the cached objects. */
    void endTransaction() {
        List<Runnable> run = List.of();
        synchronized (this) {
            if (--transactions == 0) {
                run = whenNoTransactions;
                whenNoTransactions = new ArrayList<>();
            }
        }
        run.forEach(Runnable::run);
    }

    /**
     * Runs a task once no transaction involves this player any more.
     *
     * @param task the task
     * @return false if none is in progress; the task was then not queued
     */
    synchronized boolean whenNoTransactions(Runnable task) {
        if (transactions == 0) {
            return false;
        }
        whenNoTransactions.add(task);
        return true;
    }

    Collection<CachedObject<?>> all() {
        return objects.values();
    }
}
