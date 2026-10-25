// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

/**
 * Thrown when a player's data is requested while it is not in memory: the
 * player is offline, still joining, or the class was never registered.
 */
public final class DataNotLoadedException extends LeonsDBException {

    /**
     * Creates the exception.
     *
     * @param message what is not loaded and why
     */
    public DataNotLoadedException(String message) {
        super(message);
    }
}
