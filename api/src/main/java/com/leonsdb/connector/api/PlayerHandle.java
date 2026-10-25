// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * The data of one online player.
 *
 * <p>Objects returned by {@link #get(Class)} are the cached copies: change
 * their fields directly on the main thread and LeonsDB saves the changes in
 * the background, merged, about once per second, when the player leaves and
 * when the server stops. If the Minecraft server crashes, changes from the
 * last moment may not have been sent yet.
 *
 * <p>For changes that must never be lost, such as money, use
 * {@link #update(Class, Consumer)}: it saves immediately and tells you when
 * the change is safely stored.
 */
public interface PlayerHandle {

    /**
     * The player this handle belongs to.
     *
     * @return the player's UUID
     */
    UUID uuid();

    /**
     * Returns whether this player's data is loaded.
     *
     * @return true while the player is online and their data is in memory
     */
    boolean isLoaded();

    /**
     * Returns the cached data object. Instant: no network access. Call it on
     * the main thread and change its fields there.
     *
     * @param type a registered class annotated with {@link PlayerData}
     * @param <T>  the data type
     * @return the cached object; always the same instance while the player is online
     * @throws DataNotLoadedException if the player's data is not loaded
     *                                (the player is offline or still joining)
     */
    <T> T get(Class<T> type);

    /**
     * Changes the cached object and saves it right away. The change is
     * applied immediately on the calling thread (call it on the main
     * thread); the save happens in the background.
     *
     * <pre>{@code
     * handle.update(Stats.class, s -> s.coins -= price)
     *       .thenRun(() -> player.sendMessage("Purchase complete"));
     * }</pre>
     *
     * @param type   a registered class annotated with {@link PlayerData}
     * @param change the change to make
     * @param <T>    the data type
     * @return completes when the change is durably stored; fails if it could
     *         not be confirmed yet (LeonsDB keeps retrying in the background)
     */
    <T> CompletableFuture<Void> update(Class<T> type, Consumer<? super T> change);

    /**
     * Saves all pending changes of this player now. Call it on the main
     * thread.
     *
     * @return completes when everything is durably stored
     */
    CompletableFuture<Void> save();
}
