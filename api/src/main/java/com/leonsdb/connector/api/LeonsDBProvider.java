// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.util.UUID;

/**
 * The implementation behind {@link LeonsDB}. Provided by the LeonsDB plugin
 * for each platform; plugins never implement it.
 */
public interface LeonsDBProvider {

    /**
     * See {@link LeonsDB#register(Class[])}.
     *
     * @param classes data classes
     */
    void register(Class<?>... classes);

    /**
     * See {@link LeonsDB#player(UUID)}.
     *
     * @param player the player's UUID
     * @return the handle
     */
    PlayerHandle player(UUID player);

    /**
     * See {@link LeonsDB#global(Class)}.
     *
     * @param type the record class
     * @param <T>  the record type
     * @return the collection
     */
    <T> GlobalCollection<T> global(Class<T> type);

    /**
     * See {@link LeonsDB#leaderboard(Class, String, Leaderboard.Order)}.
     *
     * @param type  the record class
     * @param field an indexed field
     * @param order the sort order
     * @param <T>   the record type
     * @return the leaderboard
     */
    <T> Leaderboard<T> leaderboard(Class<T> type, String field, Leaderboard.Order order);

    /**
     * See {@link LeonsDB#channel(String)}.
     *
     * @param name the channel
     * @return the channel
     */
    Channel channel(String name);

    /**
     * See {@link LeonsDB#transaction()}.
     *
     * @return a new transaction
     */
    Transaction transaction();

    /**
     * See {@link LeonsDB#history()}.
     *
     * @return the history
     */
    History history();
}
