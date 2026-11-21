// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.util.Objects;

/**
 * Information about the LeonsDB server the connector is talking to.
 */
public final class ServerInfo {

    private final String serverVersion;
    private final String buildCommit;
    private final int minProtocolVersion;
    private final int maxProtocolVersion;
    private final String instanceId;

    /**
     * Creates the information, checking it.
     *
     * @param serverVersion      semantic version of the server, for example "0.1.0"
     * @param buildCommit        git commit the server was built from
     * @param minProtocolVersion oldest protocol version the server accepts
     * @param maxProtocolVersion newest protocol version the server understands
     * @param instanceId         identity of the server's data directory; if it
     *                           changes between connections, the connector is
     *                           talking to a different database
     */
    public ServerInfo(String serverVersion, String buildCommit, int minProtocolVersion, int maxProtocolVersion,
                      String instanceId) {
        this.serverVersion = Objects.requireNonNull(serverVersion, "serverVersion");
        this.buildCommit = Objects.requireNonNull(buildCommit, "buildCommit");
        this.minProtocolVersion = minProtocolVersion;
        this.maxProtocolVersion = maxProtocolVersion;
        this.instanceId = Objects.requireNonNull(instanceId, "instanceId");
    }

    /** @return semantic version of the server, for example "0.1.0" */
    public String serverVersion() {
        return serverVersion;
    }

    /** @return git commit the server was built from */
    public String buildCommit() {
        return buildCommit;
    }

    /** @return oldest protocol version the server accepts */
    public int minProtocolVersion() {
        return minProtocolVersion;
    }

    /** @return newest protocol version the server understands */
    public int maxProtocolVersion() {
        return maxProtocolVersion;
    }

    /**
     * @return identity of the server's data directory; if it changes between
     *         connections, the connector is talking to a different database
     */
    public String instanceId() {
        return instanceId;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ServerInfo)) {
            return false;
        }
        ServerInfo s = (ServerInfo) o;
        return minProtocolVersion == s.minProtocolVersion && maxProtocolVersion == s.maxProtocolVersion
                && serverVersion.equals(s.serverVersion) && buildCommit.equals(s.buildCommit)
                && instanceId.equals(s.instanceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverVersion, buildCommit, minProtocolVersion, maxProtocolVersion, instanceId);
    }

    @Override
    public String toString() {
        return "ServerInfo[serverVersion=" + serverVersion + ", buildCommit=" + buildCommit + ", minProtocolVersion="
                + minProtocolVersion + ", maxProtocolVersion=" + maxProtocolVersion + ", instanceId=" + instanceId + "]";
    }
}
