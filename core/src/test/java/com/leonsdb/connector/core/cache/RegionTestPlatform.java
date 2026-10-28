// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.cache;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A stand-in for Folia: a global region thread, and a thread of its own for
 * each player in the world, as if every player stood in a different region.
 * {@link #violations} lists every time a player's data was read on another
 * thread (see {@link Guard}).
 */
public final class RegionTestPlatform implements Platform {

    /**
     * A field type that notes on which thread it is read. The live data
     * object holding one is read (snapshotted, merged) only through it, so
     * a read on the wrong thread shows up in {@link #violations}. Copies
     * (made by decoding, as transactions make them) may be read anywhere.
     *
     * @param owner the player whose data holds it
     * @param live  false for a copy
     */
    public record Guard(UUID owner, boolean live) {

        /**
         * The guard of a player's live data.
         *
         * @param owner the player
         */
        public Guard(UUID owner) {
            this(owner, true);
        }
    }

    public final List<String> violations = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService global = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "global"));
    private final Map<UUID, ScheduledExecutorService> regions = new ConcurrentHashMap<>();
    private final Map<UUID, String> disconnected = new ConcurrentHashMap<>();

    /**
     * Registers the guard codec.
     *
     * @param codecs the codecs to add it to
     */
    public void registerGuard(com.leonsdb.connector.core.schema.Codecs codecs) {
        codecs.register(Guard.class, new com.leonsdb.connector.core.schema.TypeCodec<>() {
            @Override
            public com.leonsdb.proto.v1.FieldType fieldType() {
                return com.leonsdb.proto.v1.FieldType.FIELD_TYPE_STRING;
            }

            @Override
            public com.leonsdb.proto.v1.Value encode(Guard g) {
                if (g.live() && !isPlayerThread(g.owner())) {
                    violations.add("data of " + g.owner() + " read on " + Thread.currentThread().getName());
                }
                return com.leonsdb.proto.v1.Value.newBuilder().setStringValue(g.owner().toString()).build();
            }

            @Override
            public Guard decode(com.leonsdb.proto.v1.Value v) {
                return new Guard(UUID.fromString(v.getStringValue()), false);
            }
        });
    }

    /**
     * The player enters the world, in a region of their own.
     *
     * @param player the player
     */
    public void enter(UUID player) {
        regions.computeIfAbsent(player, p -> Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "region-" + p)));
    }

    /**
     * The player leaves the world.
     *
     * @param player the player
     */
    public void leave(UUID player) {
        var region = regions.remove(player);
        if (region != null) {
            region.shutdown();
        }
    }

    /**
     * Runs a task on the player's region thread and waits for it.
     *
     * @param player the player
     * @param task   the task
     * @param <T>    its result type
     * @return its result
     * @throws Exception if the task failed
     */
    public <T> T onRegion(UUID player, Callable<T> task) throws Exception {
        return regions.get(player).submit(task).get(5, TimeUnit.SECONDS);
    }

    /**
     * Runs a task on the global region thread and waits for it.
     *
     * @param task the task
     * @param <T>  its result type
     * @return its result
     * @throws Exception if the task failed
     */
    public <T> T onGlobal(Callable<T> task) throws Exception {
        return global.submit(task).get(5, TimeUnit.SECONDS);
    }

    /** @return players disconnected, with the message */
    public Map<UUID, String> disconnected() {
        return disconnected;
    }

    @Override
    public boolean isMainThread() {
        return Thread.currentThread().getName().equals("global");
    }

    @Override
    public void runOnMainThread(Runnable task, Duration delay) {
        global.schedule(task, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean isPlayerThread(UUID player) {
        return regions.containsKey(player) ? Thread.currentThread().getName().equals("region-" + player) : isMainThread();
    }

    @Override
    public void runForPlayer(UUID player, Runnable task, Duration delay) {
        var region = regions.get(player);
        if (region == null) {
            runOnMainThread(task, delay);
            return;
        }
        try {
            region.schedule(task, delay.toMillis(), TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            runOnMainThread(task, delay); // left meanwhile
        }
    }

    @Override
    public boolean isOnline(UUID player) {
        return regions.containsKey(player);
    }

    @Override
    public void disconnect(UUID player, String message) {
        if (!isPlayerThread(player)) {
            violations.add("disconnect of " + player + " on " + Thread.currentThread().getName());
        }
        disconnected.put(player, message);
    }

    /** Stops every thread. */
    public void close() {
        regions.values().forEach(ScheduledExecutorService::shutdownNow);
        global.shutdownNow();
    }
}
