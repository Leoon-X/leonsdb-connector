// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.util.concurrent.CompletableFuture;

/**
 * A rollback, from {@link History}. Preview it to see what it would change,
 * then apply it.
 *
 * <p>A rollback is an ordinary change: it shows in the history and can be
 * rolled back itself. Players who are online anywhere in the network are
 * rolled back live, by the server they play on. Each record is changed
 * atomically; if something fails, applying the same rollback again is safe.
 * Every change made since the rollback's time is undone, including
 * legitimate ones: preview first.
 */
public interface Rollback {

    /**
     * Only sets these fields back and keeps the others as they are now (for
     * example only the kit, not the coins earned since).
     *
     * @param fields field names of your class
     * @return this rollback
     */
    Rollback onlyFields(String... fields);

    /**
     * Shows what applying would change, without changing anything.
     *
     * @return the would-be changes; the future completes on the main thread
     */
    CompletableFuture<RollbackResult> preview();

    /**
     * Applies the rollback.
     *
     * @return what was changed; the future completes on the main thread
     */
    CompletableFuture<RollbackResult> apply();
}
