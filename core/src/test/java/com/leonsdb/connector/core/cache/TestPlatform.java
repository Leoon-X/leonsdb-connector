// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.cache;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** A stand-in for Paper: tasks run on a timer thread that counts as main. */
public final class TestPlatform implements Platform {

    /**
     * Creates and starts a service for tests.
     *
     * @param client   connection
     * @param log      logger
     * @param platform platform
     * @param dir      recovery directory
     * @return a started service
     */
    public static LeonsDBService service(com.leonsdb.connector.core.LeonsDBClient client, java.util.logging.Logger log,
                                         Platform platform, java.nio.file.Path dir) {
        var s = new LeonsDBService(client, com.leonsdb.connector.core.schema.Codecs.withDefaults(), log, platform,
                new LeonsDBService.Settings("test-server", Duration.ofSeconds(5), Duration.ofSeconds(5),
                        Duration.ofMillis(200), dir));
        s.start();
        return s;
    }

    /**
     * A UUID as the 16 bytes the protocol uses.
     *
     * @param uuid the UUID
     * @return its bytes
     */
    public static com.google.protobuf.ByteString bytes(UUID uuid) {
        return LeaseKeeper.bytes(uuid);
    }

    public final Set<UUID> online = ConcurrentHashMap.newKeySet();
    public final Map<UUID, String> disconnected = new ConcurrentHashMap<>();
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
    private final boolean mainThread;

    public TestPlatform() {
        this(true);
    }

    public TestPlatform(boolean mainThread) {
        this.mainThread = mainThread;
    }

    @Override
    public boolean isMainThread() {
        return mainThread;
    }

    @Override
    public void runOnMainThread(Runnable task, Duration delay) {
        timer.schedule(task, delay.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean isOnline(UUID player) {
        return online.contains(player);
    }

    @Override
    public void disconnect(UUID player, String message) {
        online.remove(player);
        disconnected.put(player, message);
    }
}
