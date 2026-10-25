// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.util.Objects;

/**
 * Information about the LeonsDB server the connector is talking to.
 *
 * @param serverVersion      semantic version of the server, for example "0.1.0"
 * @param buildCommit        git commit the server was built from
 * @param minProtocolVersion oldest protocol version the server accepts
 * @param maxProtocolVersion newest protocol version the server understands
 * @param instanceId         identity of the server's data directory; if it
 *                           changes between connections, the connector is
 *                           talking to a different database
 */
public record ServerInfo(
        String serverVersion,
        String buildCommit,
        int minProtocolVersion,
        int maxProtocolVersion,
        String instanceId) {

    /** Validates the fields. */
    public ServerInfo {
        Objects.requireNonNull(serverVersion, "serverVersion");
        Objects.requireNonNull(buildCommit, "buildCommit");
        Objects.requireNonNull(instanceId, "instanceId");
    }

    /**
     * Returns whether the server accepts the given protocol version.
     *
     * @param protocolVersion the client's protocol version
     * @return true if the server supports it
     */
    public boolean supports(int protocolVersion) {
        return protocolVersion >= minProtocolVersion && protocolVersion <= maxProtocolVersion;
    }
}
