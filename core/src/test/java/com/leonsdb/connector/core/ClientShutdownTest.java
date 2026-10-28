// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * A plugin's classes are gone once Paper has disabled it, so no thread of
 * the client may still run after close(): it would fail to load a class.
 */
class ClientShutdownTest {

    private static List<String> clientThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .filter(Thread::isAlive)
                .map(Thread::getName)
                .filter(n -> n.startsWith("leonsdb-grpc") || n.startsWith("grpc-nio-worker"))
                .sorted()
                .toList();
    }

    @Test
    void noThreadOutlivesClose() throws Exception {
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort(); // nothing listens there afterwards
        }
        System.setProperty("leonsdb.wire", "false");
        try {
            var client = LeonsDBClient.connect(new ConnectionSettings("127.0.0.1", port, false, "token",
                    Duration.ofMillis(500)), "test");
            try {
                client.fetchServerInfo().get(5, TimeUnit.SECONDS);
            } catch (Exception expected) {
                // nobody there: the connection attempt is what matters
            }
            assertFalse(clientThreads().isEmpty(), "the client has threads while open");
            client.close();
            assertEquals(List.of(), clientThreads(), "threads still running after close()");

            // A call made after close() (a late retry) fails; it does not throw.
            var late = client.fetchServerInfo();
            var e = org.junit.jupiter.api.Assertions.assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> late.get(5, TimeUnit.SECONDS));
            assertFalse(e.getCause() instanceof java.util.concurrent.RejectedExecutionException, e.toString());
        } finally {
            System.clearProperty("leonsdb.wire");
        }
    }
}
