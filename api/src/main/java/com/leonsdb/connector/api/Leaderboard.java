// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * A ranking of the records of a collection by one {@link Indexed} field,
 * kept sorted by the database on every write, so reading it is instant.
 *
 * <pre>{@code
 * Leaderboard<Stats> kills = LeonsDB.leaderboard(Stats.class, "kills");
 * kills.top(10).thenAccept(top -> top.forEach(r ->
 *         Bukkit.getLogger().info("#" + r.rank() + " " + r.player() + ": " + r.value())));
 * }</pre>
 *
 * <p>Equal values share a rank (1, 2, 2, 4). Records without a value for the
 * field are not ranked. Players' changes reach the leaderboard when they are
 * saved, normally within a second.
 *
 * <p>All methods are asynchronous; never wait for them on the main thread.
 * Right after a new {@code @Indexed} field is added to a collection that
 * already holds data, the index is built in the background and requests
 * fail until it is ready.
 *
 * @param <T> the record type
 */
public interface Leaderboard<T> {

    /** Sort order. */
    enum Order {
        /** The highest value is rank 1 (kills, coins, playtime). */
        HIGHEST_FIRST,
        /** The lowest value is rank 1 (fastest times). */
        LOWEST_FIRST
    }

    /**
     * The best entries.
     *
     * @param limit how many, 1 to 1000
     * @return the entries, best first
     */
    CompletableFuture<List<Ranking<T>>> top(int limit);

    /**
     * A page of the leaderboard, for example offset 10, limit 10 for ranks
     * 11 to 20.
     *
     * @param offset how many entries to skip
     * @param limit  how many to return, 1 to 1000
     * @return the entries, best first
     */
    CompletableFuture<List<Ranking<T>>> page(int offset, int limit);

    /**
     * A player's rank (player collections).
     *
     * @param player the player
     * @return the rank, or empty if the player is not ranked
     */
    CompletableFuture<OptionalLong> rankOf(UUID player);

    /**
     * A record's rank (global collections).
     *
     * @param id the record's key
     * @return the rank, or empty if the record is not ranked
     */
    CompletableFuture<OptionalLong> rankOf(String id);

    /**
     * How many records are ranked.
     *
     * @return the number of ranked records
     */
    CompletableFuture<Long> size();
}
