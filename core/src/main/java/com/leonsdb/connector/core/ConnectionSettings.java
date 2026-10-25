// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core;

import java.time.Duration;
import java.util.Objects;

/**
 * Where and how to connect to a LeonsDB server.
 *
 * @param host           server hostname or IP address
 * @param port           server port, 7420 by default
 * @param tls            whether to use TLS; required for anything but localhost
 * @param token          access token ("ldb_..."), or null for public calls only
 * @param requestTimeout how long a single request may take before it fails
 */
public record ConnectionSettings(String host, int port, boolean tls, String token, Duration requestTimeout) {

    /** The default LeonsDB port. */
    public static final int DEFAULT_PORT = 7420;

    /** Validates the settings. */
    public ConnectionSettings {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(requestTimeout, "requestTimeout");
        if (host.isBlank()) {
            throw new IllegalArgumentException("host must not be empty");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be between 1 and 65535, got " + port);
        }
        if (requestTimeout.isNegative() || requestTimeout.isZero()) {
            throw new IllegalArgumentException("requestTimeout must be positive");
        }
        if (token != null && token.isBlank()) {
            token = null;
        }
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

    /** Never prints the token. */
    @Override
    public String toString() {
        return "ConnectionSettings[" + host + ":" + port + (tls ? ", tls" : "") + (token != null ? ", token set" : "") + "]";
    }
}
