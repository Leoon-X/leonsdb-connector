// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.importer;

/** An import cannot go on; the message says why and what to do. */
public final class ImportException extends Exception {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param message what went wrong and what to do
     */
    public ImportException(String message) {
        super(message);
    }

    /**
     * Creates the exception.
     *
     * @param message what went wrong and what to do
     * @param cause   the underlying error
     */
    public ImportException(String message, Throwable cause) {
        super(message, cause);
    }
}
