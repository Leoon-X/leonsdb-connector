// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core;

import com.leonsdb.connector.api.LeonsDBException;
import com.leonsdb.connector.api.ServerInfo;

/**
 * Thrown when the server does not speak the connector's protocol version.
 * The fix is always to update either the server or the connector.
 */
public final class IncompatibleServerException extends LeonsDBException {

    private final transient ServerInfo serverInfo;

    IncompatibleServerException(ServerInfo info, int clientProtocol) {
        super(String.format(
                "LeonsDB server %s supports protocol %d-%d but this connector uses protocol %d; "
                        + "update the %s",
                info.serverVersion(), info.minProtocolVersion(), info.maxProtocolVersion(), clientProtocol,
                clientProtocol > info.maxProtocolVersion() ? "LeonsDB server" : "connector"));
        this.serverInfo = info;
    }

    /**
     * Returns what the server reported about itself.
     *
     * @return the server info
     */
    public ServerInfo serverInfo() {
        return serverInfo;
    }
}
