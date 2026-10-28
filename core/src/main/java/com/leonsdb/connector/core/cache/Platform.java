// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.cache;

import java.time.Duration;
import java.util.UUID;

/**
 * What the connector needs from the Minecraft server software it runs on.
 * Implemented once per platform (Paper, which also covers Folia).
 *
 * <p>Threads: on Paper one main thread runs the game. On Folia each region
 * of the world has its own thread, and a player's data may only be touched
 * on the thread of the region the player is in. The connector touches a
 * player's cached data only through {@link #isPlayerThread} and
 * {@link #runForPlayer}; everything else uses the main thread (Folia's
 * global region thread).
 */
public interface Platform {

    /**
     * Whether the caller is on the main thread (Folia: the global region
     * thread).
     *
     * @return true on the main thread
     */
    boolean isMainThread();

    /**
     * Runs a task on the main thread (Folia: the global region thread)
     * after a delay.
     *
     * @param task  the task
     * @param delay how long to wait; zero for as soon as possible
     */
    void runOnMainThread(Runnable task, Duration delay);

    /**
     * Whether the caller may touch the player's data: on Paper the main
     * thread, on Folia the thread of the region the player is in.
     *
     * @param player the player
     * @return true if the caller may touch their data
     */
    default boolean isPlayerThread(UUID player) {
        return isMainThread();
    }

    /**
     * Runs a task on the thread that may touch the player's data, after a
     * delay. If the player is not in the world (they left, or have not
     * finished joining), no other thread touches their data, and the task
     * runs on the main thread.
     *
     * @param player the player
     * @param task   the task
     * @param delay  how long to wait; zero for as soon as possible
     */
    default void runForPlayer(UUID player, Runnable task, Duration delay) {
        runOnMainThread(task, delay);
    }

    /**
     * Whether a player is currently connected to this server. Any thread.
     *
     * @param player the player
     * @return true if online here
     */
    boolean isOnline(UUID player);

    /**
     * Disconnects a player whose data is now owned by another server.
     * Called on the player's thread ({@link #runForPlayer}).
     *
     * @param player  the player
     * @param message why, shown to the player
     */
    void disconnect(UUID player, String message);
}
