// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.leonsdb.connector.api.GlobalData;
import com.leonsdb.connector.core.FakeServer;
import com.leonsdb.connector.core.LeonsDBClient;
import com.leonsdb.connector.core.schema.Codecs;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GlobalCollectionTest {

    @GlobalData(namespace = "pvp", collection = "clans")
    public static class Clan {
        public String leader;
        public int wins;
        public List<String> members = new ArrayList<>();
    }

    @TempDir
    Path tmp;

    private LeonsDBService serviceFor(LeonsDBClient client) {
        var s = TestPlatform.service(client, Logger.getLogger("test"), new TestPlatform(), tmp);
        s.register(Clan.class);
        return s;
    }

    @Test
    void createReadUpdateDelete() throws Exception {
        try (var server = new FakeServer(); var client = server.client()) {
            var clans = serviceFor(client).global(Clan.class);
            assertTrue(clans.get("red").get(5, TimeUnit.SECONDS).isEmpty());

            var created = clans.update("red", c -> {
                c.leader = "Steve";
                c.members.add("Steve");
            }).get(5, TimeUnit.SECONDS);
            assertEquals("Steve", created.leader);

            var read = clans.get("red").get(5, TimeUnit.SECONDS).orElseThrow();
            assertEquals(List.of("Steve"), read.members);

            assertTrue(clans.delete("red").get(5, TimeUnit.SECONDS));
            assertFalse(clans.delete("red").get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void concurrentUpdatesFromSeveralServersAreNeverLost() throws Exception {
        try (var server = new FakeServer(); var a = server.client(); var b = server.client()) {
            var fromA = serviceFor(a).global(Clan.class);
            var fromB = serviceFor(b).global(Clan.class);
            List<CompletableFuture<Clan>> all = new ArrayList<>();
            for (int i = 0; i < 25; i++) {
                all.add(fromA.update("red", c -> c.wins++));
                all.add(fromB.update("red", c -> c.wins++));
            }
            CompletableFuture.allOf(all.toArray(new CompletableFuture<?>[0])).get(30, TimeUnit.SECONDS);
            assertEquals(50, fromA.get("red").get(5, TimeUnit.SECONDS).orElseThrow().wins);
        }
    }
}
