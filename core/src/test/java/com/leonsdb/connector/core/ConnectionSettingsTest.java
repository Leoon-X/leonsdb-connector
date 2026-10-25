// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class ConnectionSettingsTest {

    @Test
    void localhostDefaults() {
        var s = ConnectionSettings.localhost(null);
        assertEquals("127.0.0.1", s.host());
        assertEquals(ConnectionSettings.DEFAULT_PORT, s.port());
    }

    @Test
    void rejectsInvalidValues() {
        var timeout = Duration.ofSeconds(1);
        assertThrows(IllegalArgumentException.class, () -> new ConnectionSettings(" ", 7420, false, null, timeout));
        assertThrows(IllegalArgumentException.class, () -> new ConnectionSettings("h", 0, false, null, timeout));
        assertThrows(IllegalArgumentException.class, () -> new ConnectionSettings("h", 65536, false, null, timeout));
        assertThrows(IllegalArgumentException.class, () -> new ConnectionSettings("h", 7420, false, null, Duration.ZERO));
        assertThrows(NullPointerException.class, () -> new ConnectionSettings(null, 7420, false, null, timeout));
    }
}
