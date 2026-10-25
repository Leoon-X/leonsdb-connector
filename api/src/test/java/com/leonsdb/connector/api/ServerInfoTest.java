// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ServerInfoTest {

    @Test
    void supportsProtocolRangeInclusive() {
        var info = new ServerInfo("1.0.0", "abc", 2, 4, "id");
        assertFalse(info.supports(1));
        assertTrue(info.supports(2));
        assertTrue(info.supports(3));
        assertTrue(info.supports(4));
        assertFalse(info.supports(5));
    }
}
