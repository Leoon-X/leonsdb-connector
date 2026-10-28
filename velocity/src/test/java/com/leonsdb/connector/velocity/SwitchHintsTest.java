// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.velocity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class SwitchHintsTest {

    private final List<String> sent = new ArrayList<>();
    private final List<String> warnings = new ArrayList<>();
    private final List<String> infos = new ArrayList<>();
    private final AtomicLong now = new AtomicLong(1_000_000);
    private CompletableFuture<Boolean> answer = new CompletableFuture<>();
    private final SwitchHints hints = new SwitchHints((uuid, target) -> {
        sent.add(uuid + "->" + target);
        return answer;
    }, warnings::add, infos::add, now::get);

    private final UUID steve = UUID.randomUUID();

    @Test
    void aSwitchAndAFirstJoinAreHinted() {
        hints.beforeConnect(steve, "lobby", "survival");
        hints.beforeConnect(steve, null, "lobby"); // joining the network
        assertEquals(List.of(steve + "->survival", steve + "->lobby"), sent);
    }

    @Test
    void theFirstDeliveredHintIsConfirmedOnce() {
        hints.beforeConnect(steve, "lobby", "survival");
        hints.beforeConnect(steve, "survival", "lobby");
        answer.complete(true);
        hints.beforeConnect(steve, "lobby", "survival");
        assertEquals(1, infos.size(), infos.toString());
        assertTrue(infos.get(0).contains("Switch hints work"), infos.get(0));
        assertEquals(List.of(), warnings);
    }

    @Test
    void aHintToAPlayerNobodyOwnsIsNotAConfirmation() {
        answer.complete(false); // a first join: nobody had the player
        hints.beforeConnect(steve, null, "lobby");
        assertEquals(List.of(), infos);
    }

    @Test
    void reconnectingToTheSameServerIsNoSwitch() {
        hints.beforeConnect(steve, "lobby", "lobby");
        assertEquals(List.of(), sent);
    }

    @Test
    void aHintIsNeverWaitedFor() {
        // LeonsDB never answers: the connection goes ahead all the same.
        long start = System.nanoTime();
        for (int i = 0; i < 1000; i++) {
            hints.beforeConnect(UUID.randomUUID(), "lobby", "survival");
        }
        assertTrue(System.nanoTime() - start < 1_000_000_000L, "hints must not block");
        assertEquals(1000, sent.size());
    }

    @Test
    void anOldServerTurnsHintsOffOnce() {
        answer.completeExceptionally(Status.UNIMPLEMENTED.asRuntimeException());
        hints.beforeConnect(steve, "lobby", "survival");
        hints.beforeConnect(steve, "survival", "lobby");
        assertTrue(hints.disabled());
        assertEquals(1, sent.size(), "nothing more is sent");
        assertEquals(1, warnings.size());
        assertTrue(warnings.get(0).contains("older than this plugin"), warnings.get(0));
    }

    @Test
    void aRefusedTokenTurnsHintsOffAndSaysHowToFixIt() {
        answer.completeExceptionally(Status.PERMISSION_DENIED.withDescription("token \"x\" may not call").asRuntimeException());
        hints.beforeConnect(steve, "lobby", "survival");
        assertTrue(hints.disabled());
        assertTrue(warnings.get(0).contains("-role proxy"), warnings.get(0));
    }

    @Test
    void anUnreachableServerKeepsTryingAndWarnsAtMostOnceAMinute() {
        answer.completeExceptionally(Status.UNAVAILABLE.withDescription("connection refused").asRuntimeException());
        for (int i = 0; i < 5; i++) {
            hints.beforeConnect(steve, "lobby", "survival");
        }
        assertFalse(hints.disabled());
        assertEquals(5, sent.size(), "still sent: LeonsDB may be back any moment");
        assertEquals(1, warnings.size());
        now.addAndGet(SwitchHints.WARN_INTERVAL_MILLIS);
        hints.beforeConnect(steve, "lobby", "survival");
        assertEquals(2, warnings.size());
    }

    @Test
    void aSenderThatThrowsDoesNotBreakTheConnection() {
        var throwing = new SwitchHints((u, t) -> {
            throw new IllegalStateException("channel closed");
        }, warnings::add, infos::add, now::get);
        throwing.beforeConnect(steve, "lobby", "survival");
        assertEquals(1, warnings.size());
    }

    @Test
    void settingsAreReadByTheirPath() {
        Map<String, Object> config = Map.of("hints", false, "server", Map.of("host", "db.internal", "port", 7420));
        assertEquals("db.internal", LeonsDBVelocity.get(config, "server.host", "127.0.0.1"));
        assertEquals(7420, LeonsDBVelocity.get(config, "server.port", 1));
        assertEquals(false, LeonsDBVelocity.get(config, "hints", true));
        assertEquals("x", LeonsDBVelocity.get(config, "server.token", "x"));
        assertEquals("x", LeonsDBVelocity.get(config, "missing.deeper", "x"));
    }
}
