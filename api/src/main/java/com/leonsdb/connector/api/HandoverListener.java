// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.util.UUID;

/**
 * Told right before a player's data is handed to another server, when the
 * player switches servers. Register one with {@link LeonsDB#onHandover}.
 *
 * <p>On a switch the player's data goes to the new server before the old
 * server's quit event fires (the proxy only lets the player go once the new
 * server has them). Changes made in the quit event still count: they follow
 * the player and reach the new server a moment after the player joined it.
 * Change data here instead if the new server must have it from the first
 * moment, for example something its join event reads.
 */
@FunctionalInterface
public interface HandoverListener {

    /**
     * The player's data is about to be saved and handed over. Called on the
     * player's thread (Paper: the main thread; Folia: their region's);
     * changes made here are part of what the new server loads.
     *
     * @param player the player
     */
    void beforeHandover(UUID player);
}
