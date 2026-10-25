// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * A collection of records that do not belong to one player, keyed by any
 * string: clans, bans, server settings. Several servers may change the same
 * record at the same time; LeonsDB makes sure no change is lost.
 *
 * <p>All methods are asynchronous. Never call {@code join()} or
 * {@code get()} on the returned futures from the main thread.
 *
 * @param <T> the record type
 */
public interface GlobalCollection<T> {

    /**
     * Reads a record.
     *
     * @param id the record's key, 1 to 512 bytes of text
     * @return the record, or empty if it does not exist
     */
    CompletableFuture<Optional<T>> get(String id);

    /**
     * Reads a record (or starts from a new, empty one), applies a change and
     * saves it. If another server changed the same record in the meantime,
     * the change is applied again on the newest version, so it may run more
     * than once: it must only modify the object it is given.
     *
     * <pre>{@code
     * LeonsDB.global(Clan.class).update("red", clan -> clan.members.add(name));
     * }</pre>
     *
     * @param id     the record's key
     * @param change modifies the record; runs on a background thread
     * @return the record as stored
     */
    CompletableFuture<T> update(String id, Consumer<? super T> change);

    /**
     * Deletes a record.
     *
     * @param id the record's key
     * @return true if the record existed
     */
    CompletableFuture<Boolean> delete(String id);
}
