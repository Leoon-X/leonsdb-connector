// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.paper;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.logging.Logger;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.plugin.IllegalPluginAccessException;
import org.bukkit.plugin.Plugin;

/**
 * The connector's view of a server without region schedulers: Bukkit and
 * Spigot of any version from 1.8, and Paper before 1.19.4. One main thread
 * runs the game and touches all players' data, through the Bukkit
 * scheduler. From 1.14, players' data is saved for server switches from the
 * idle-time task queue when it works (see {@link IdleQueue}), as on Paper.
 */
final class BukkitPlatform implements ServerPlatform {

    private final Plugin plugin;
    // The idle-time task queue, once checked; null means the scheduler.
    private volatile Executor idle;
    private volatile String how = "Bukkit scheduler";

    BukkitPlatform(Plugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public void start() {
        Logger log = plugin.getLogger();
        String scheduler = "Player data is saved for server switches at the next tick (scheduler): ";
        if (!Boolean.parseBoolean(System.getProperty("leonsdb.idleQueue", "true"))) {
            log.info(scheduler + "turned off with -Dleonsdb.idleQueue=false.");
            return;
        }
        IdleQueue.Lookup found = IdleQueue.find(Bukkit.getServer(), false);
        if (found.executor() == null) {
            log.info(scheduler + found.reason() + ".");
            return;
        }
        IdleQueue.check(found.executor(), Bukkit::isPrimaryThread, IdleQueue.CHECK_TIMEOUT).thenAccept(works -> {
            if (works) {
                idle = found.executor();
                how = "Bukkit scheduler, and the idle-time task queue for server switches";
                log.info("Player data is saved for server switches as soon as the main thread is idle "
                        + "(the server's idle-time task queue).");
            } else {
                log.info(scheduler + "the server's idle-time task queue did not run a test task on the main thread.");
            }
        });
    }

    @Override
    public void repeat(Runnable task, long periodTicks) {
        Bukkit.getScheduler().runTaskTimer(plugin, task, periodTicks, periodTicks);
    }

    @Override
    public String describe() {
        return how;
    }

    @Override
    public boolean isMainThread() {
        return Bukkit.isPrimaryThread();
    }

    @Override
    public void runOnMainThread(Runnable task, Duration delay) {
        long ticks = ticks(delay);
        try {
            if (ticks == 0) {
                Bukkit.getScheduler().runTask(plugin, task);
            } else {
                Bukkit.getScheduler().runTaskLater(plugin, task, ticks);
            }
        } catch (IllegalPluginAccessException e) {
            // The plugin is being disabled; shutdown saves everything anyway.
        }
    }

    @Override
    public boolean isPlayerThread(UUID uuid) {
        return Bukkit.isPrimaryThread();
    }

    @Override
    public void runForPlayer(UUID uuid, Runnable task, Duration delay) {
        Executor queue = idle;
        if (queue != null && delay.isZero() && !Bukkit.isPrimaryThread() && Bukkit.getPlayer(uuid) != null) {
            // Runs as soon as the main thread is idle instead of at the next tick.
            IdleQueue.execute(queue, task, Bukkit::isPrimaryThread, t -> runOnMainThread(t, Duration.ZERO));
            return;
        }
        runOnMainThread(task, delay);
    }

    @Override
    public boolean isOnline(UUID player) {
        return Bukkit.getPlayer(player) != null;
    }

    @Override
    public void disconnect(UUID uuid, String message) {
        Player player = Bukkit.getPlayer(uuid);
        if (player != null) {
            player.kickPlayer(message);
        }
    }

    private static long ticks(Duration delay) {
        return Math.max(0, (delay.toMillis() + 49) / 50);
    }
}
