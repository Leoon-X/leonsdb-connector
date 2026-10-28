// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.paper;

import com.leonsdb.connector.core.cache.Platform;
import java.time.Duration;
import java.util.UUID;
import net.kyori.adventure.text.Component;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.plugin.IllegalPluginAccessException;
import org.bukkit.plugin.Plugin;

/**
 * The connector's view of a Paper or Folia server. It uses the region
 * schedulers, which Paper has too: there they all run on the main thread,
 * so Paper behaves as it always did, and on Folia each player's data is
 * touched on the thread of the region the player is in.
 */
final class PaperPlatform implements Platform {

    private final Plugin plugin;
    // Experiment: Paper's own task queue, which the main thread works through
    // as soon as it is idle between ticks (as it does for incoming packets).
    private final java.util.concurrent.Executor idle = idleExecutor();

    PaperPlatform(Plugin plugin) {
        this.plugin = plugin;
        if (idle != null) {
            plugin.getLogger().info("EXPERIMENT: running player tasks on the server's idle-time queue");
        }
    }

    private static java.util.concurrent.Executor idleExecutor() {
        if (!Boolean.getBoolean("leonsdb.experiment.idle")) {
            return null;
        }
        try {
            Class.forName("io.papermc.paper.threadedregions.RegionizedServer");
            return null; // Folia
        } catch (ClassNotFoundException e) {
            // Paper
        }
        try {
            Object server = Bukkit.getServer().getClass().getMethod("getServer").invoke(Bukkit.getServer());
            return server instanceof java.util.concurrent.Executor ex ? ex : null;
        } catch (ReflectiveOperationException e) {
            return null;
        }
    }

    @Override
    public boolean isMainThread() {
        return Bukkit.isGlobalTickThread();
    }

    @Override
    public void runOnMainThread(Runnable task, Duration delay) {
        long ticks = ticks(delay);
        try {
            if (ticks == 0) {
                Bukkit.getGlobalRegionScheduler().run(plugin, t -> task.run());
            } else {
                Bukkit.getGlobalRegionScheduler().runDelayed(plugin, t -> task.run(), ticks);
            }
        } catch (IllegalPluginAccessException e) {
            // The plugin is being disabled; shutdown saves everything anyway.
        }
    }

    @Override
    public boolean isPlayerThread(UUID uuid) {
        Player player = Bukkit.getPlayer(uuid);
        return player == null ? Bukkit.isGlobalTickThread() : Bukkit.isOwnedByCurrentRegion(player);
    }

    @Override
    public void runForPlayer(UUID uuid, Runnable task, Duration delay) {
        if (idle != null && delay.isZero()) {
            idle.execute(task);
            return;
        }
        Player player = Bukkit.getPlayer(uuid);
        if (player == null) {
            runOnMainThread(task, delay);
            return;
        }
        try {
            // If the player leaves before it runs, it runs on the main
            // thread instead: nothing else touches their data then.
            boolean scheduled = player.getScheduler().execute(plugin, task,
                    () -> runOnMainThread(task, Duration.ZERO), Math.max(1, ticks(delay)));
            if (!scheduled) {
                runOnMainThread(task, delay);
            }
        } catch (IllegalPluginAccessException e) {
            // The plugin is being disabled; shutdown saves everything anyway.
        }
    }

    @Override
    public boolean isOnline(UUID player) {
        return Bukkit.getPlayer(player) != null;
    }

    @Override
    public void disconnect(UUID uuid, String message) {
        Player player = Bukkit.getPlayer(uuid);
        if (player != null) {
            player.kick(Component.text(message));
        }
    }

    private static long ticks(Duration delay) {
        return Math.max(0, (delay.toMillis() + 49) / 50);
    }
}
