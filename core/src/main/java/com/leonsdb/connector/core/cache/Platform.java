// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.cache;

import java.time.Duration;
import java.util.UUID;

/**
 * What the connector needs from the Minecraft server software it runs on.
 * Implemented once per platform (Paper today; Folia and Velocity later).
 */
public interface Platform {

    /**
     * Whether the caller is on the thread that may touch player data.
     *
     * @return true on the main thread
     */
    boolean isMainThread();

    /**
     * Runs a task on the main thread after a delay.
     *
     * @param task  the task
     * @param delay how long to wait; zero for as soon as possible
     */
    void runOnMainThread(Runnable task, Duration delay);

    /**
     * Whether a player is currently connected to this server. Main thread.
     *
     * @param player the player
     * @return true if online here
     */
    boolean isOnline(UUID player);

    /**
     * Disconnects a player whose data is now owned by another server.
     * Called on the main thread.
     *
     * @param player  the player
     * @param message why, shown to the player
     */
    void disconnect(UUID player, String message);
}
