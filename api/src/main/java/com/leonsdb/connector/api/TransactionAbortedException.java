// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

/**
 * Thrown by a change in a {@link Transaction} to cancel it. Nothing is
 * stored, and the transaction's future fails with this exception, so the
 * message can be shown to the player.
 */
public class TransactionAbortedException extends LeonsDBException {

    /**
     * Cancels the transaction.
     *
     * @param message why, for example "Not enough coins"
     */
    public TransactionAbortedException(String message) {
        super(message, "ABORTED_BY_PLUGIN", null);
    }
}
