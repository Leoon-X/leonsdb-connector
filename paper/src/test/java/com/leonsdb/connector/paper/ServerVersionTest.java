// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.paper;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ServerVersionTest {

    @Test
    void readsBukkitVersions() {
        assertArrayEquals(new int[] {1, 8, 8}, ServerVersion.parse("1.8.8-R0.1-SNAPSHOT"));
        assertArrayEquals(new int[] {1, 12, 2}, ServerVersion.parse("1.12.2-R0.1-SNAPSHOT"));
        assertArrayEquals(new int[] {1, 21, 11}, ServerVersion.parse("1.21.11-R0.1-SNAPSHOT"));
        assertArrayEquals(new int[] {1, 20, 0}, ServerVersion.parse("1.20-R0.1-SNAPSHOT"));
        assertArrayEquals(new int[] {26, 1, 0}, ServerVersion.parse("26.1-R0.1-SNAPSHOT"));
        assertArrayEquals(new int[] {0, 0, 0}, ServerVersion.parse("unknown"));
        assertArrayEquals(new int[] {0, 0, 0}, ServerVersion.parse(null));
    }

    @Test
    void dataVersionsOfReleasesThatDoNotReportTheirs() {
        assertEquals(ServerVersion.DATA_VERSION_1_8, ServerVersion.knownDataVersion(1, 8, 8));
        assertEquals(169, ServerVersion.knownDataVersion(1, 9, 0));
        assertEquals(184, ServerVersion.knownDataVersion(1, 9, 4));
        assertEquals(512, ServerVersion.knownDataVersion(1, 10, 2));
        assertEquals(922, ServerVersion.knownDataVersion(1, 11, 2));
        assertEquals(1343, ServerVersion.knownDataVersion(1, 12, 2));
        assertEquals(1631, ServerVersion.knownDataVersion(1, 13, 2));
    }

    @Test
    void dataVersionsGrowWithTheRelease() {
        int last = 0;
        int[][] releases = {{8, 8}, {9, 0}, {9, 1}, {9, 2}, {9, 3}, {9, 4}, {10, 0}, {10, 1}, {10, 2},
                {11, 0}, {11, 1}, {11, 2}, {12, 0}, {12, 1}, {12, 2}, {13, 0}, {13, 1}, {13, 2}, {14, 0}};
        for (int[] r : releases) {
            int v = ServerVersion.knownDataVersion(1, r[0], r[1]);
            assertTrue(v > last, "1." + r[0] + "." + r[1] + " should come after the release before it");
            last = v;
        }
        assertTrue(ServerVersion.knownDataVersion(26, 1, 0) > last);
    }

    @Test
    void flatteningCameWith113() {
        assertTrue(new ServerVersion("x", 1, 12, 2, 1343, false, false).beforeFlattening());
        assertTrue(new ServerVersion("x", 1, 8, 8, 99, false, false).beforeFlattening());
        assertFalse(new ServerVersion("x", 1, 13, 0, 1519, false, false).beforeFlattening());
        assertFalse(new ServerVersion("x", 26, 1, 0, 5000, false, true).beforeFlattening());
    }

    @Test
    void javaVersion() {
        assertTrue(ServerVersion.javaFeatureVersion() >= 8);
    }
}
