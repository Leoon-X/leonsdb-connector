// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core;

import java.time.Duration;
import java.util.Objects;

/**
 * Where and how to connect to a LeonsDB server.
 */
public final class ConnectionSettings {

    /** The default LeonsDB port. */
    public static final int DEFAULT_PORT = 7420;

    private final String host;
    private final int port;
    private final boolean tls;
    private final String token;
    private final Duration requestTimeout;

    /**
     * Creates and checks the settings.
     *
     * @param host           server hostname or IP address
     * @param port           server port, 7420 by default
     * @param tls            whether to use TLS; required for anything but localhost
     * @param token          access token ("ldb_..."), or null for public calls only
     * @param requestTimeout how long a single request may take before it fails
     */
    public ConnectionSettings(String host, int port, boolean tls, String token, Duration requestTimeout) {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(requestTimeout, "requestTimeout");
        if (host.trim().isEmpty()) {
            throw new IllegalArgumentException("host must not be empty");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be between 1 and 65535, got " + port);
        }
        if (requestTimeout.isNegative() || requestTimeout.isZero()) {
            throw new IllegalArgumentException("requestTimeout must be positive");
        }
        this.host = host;
        this.port = port;
        this.tls = tls;
        this.token = token == null || token.trim().isEmpty() ? null : token;
        this.requestTimeout = requestTimeout;
    }

    /**
     * Settings for a server on this machine without TLS.
     *
     * @param token access token
     * @return localhost settings with default port and timeout
     */
    public static ConnectionSettings localhost(String token) {
        return new ConnectionSettings("127.0.0.1", DEFAULT_PORT, false, token, Duration.ofSeconds(5));
    }

    /** @return server hostname or IP address */
    public String host() {
        return host;
    }

    /** @return server port */
    public int port() {
        return port;
    }

    /** @return whether to use TLS */
    public boolean tls() {
        return tls;
    }

    /** @return access token, or null for public calls only */
    public String token() {
        return token;
    }

    /** @return how long a single request may take before it fails */
    public Duration requestTimeout() {
        return requestTimeout;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectionSettings)) {
            return false;
        }
        ConnectionSettings s = (ConnectionSettings) o;
        return port == s.port && tls == s.tls && host.equals(s.host) && Objects.equals(token, s.token)
                && requestTimeout.equals(s.requestTimeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, tls, token, requestTimeout);
    }

    /** Never prints the token. */
    @Override
    public String toString() {
        return "ConnectionSettings[" + host + ":" + port + (tls ? ", tls" : "") + (token != null ? ", token set" : "") + "]";
    }
}
