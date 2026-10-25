// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

/**
 * Base class for errors reported by LeonsDB.
 */
public class LeonsDBException extends RuntimeException {

    private final String reason;

    /**
     * Creates an exception with a message.
     *
     * @param message what went wrong
     */
    public LeonsDBException(String message) {
        this(message, null, null);
    }

    /**
     * Creates an exception with a message and a cause.
     *
     * @param message what went wrong
     * @param cause   the underlying error
     */
    public LeonsDBException(String message, Throwable cause) {
        this(message, null, cause);
    }

    /**
     * Creates an exception with a machine-readable reason.
     *
     * @param message what went wrong
     * @param reason  a stable reason such as {@code "STALE_EPOCH"}, or null
     * @param cause   the underlying error, or null
     */
    public LeonsDBException(String message, String reason, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    /**
     * A stable machine-readable reason reported by the server, such as
     * {@code "VERSION_MISMATCH"} or {@code "TYPE_MISMATCH"}.
     *
     * @return the reason, or null if there is none
     */
    public String reason() {
        return reason;
    }
}
