// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

/** A subscription to a {@link Channel}. */
public interface ChannelSubscription extends AutoCloseable {

    /** Stops receiving messages. Safe to call more than once. */
    @Override
    void close();
}
