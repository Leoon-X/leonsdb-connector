// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

/** Something registered with LeonsDB, such as a {@link HandoverListener}. */
public interface Registration extends AutoCloseable {

    /** Unregisters it. Safe to call more than once. */
    @Override
    void close();
}
