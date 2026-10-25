// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class NamesTest {

    @ParameterizedTest
    @ValueSource(strings = {"a", "practicepvp", "player_stats", "kills_2v2", "a234567890123456789012345678901234567890123456789012345678901234"})
    void acceptsValidNames(String name) {
        assertTrue(Names.isValid(name), name);
        assertEquals(name, Names.require("namespace", name));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "Stats", "2fast", "_hidden", "has-dash", "has space", "émoji", "a.b",
            "a2345678901234567890123456789012345678901234567890123456789012345"})
    void rejectsInvalidNames(String name) {
        assertFalse(Names.isValid(name), name);
        assertThrows(IllegalArgumentException.class, () -> Names.require("collection", name));
    }

    @Test
    void rejectsNull() {
        assertFalse(Names.isValid(null));
        assertThrows(IllegalArgumentException.class, () -> Names.require("field", null));
    }

    @Test
    void errorMessageExplainsTheProblem() {
        var e = assertThrows(IllegalArgumentException.class, () -> Names.require("namespace", "PracticePvP"));
        assertTrue(e.getMessage().contains("lowercase"), e.getMessage());
        assertTrue(e.getMessage().contains("namespace"), e.getMessage());
    }
}
