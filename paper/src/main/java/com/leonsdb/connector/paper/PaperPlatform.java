// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.paper;

import com.leonsdb.connector.core.cache.Platform;
import java.time.Duration;
import java.util.UUID;
import net.kyori.adventure.text.Component;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.plugin.Plugin;
import org.bukkit.plugin.IllegalPluginAccessException;

/** The connector's view of a Paper server. */
final class PaperPlatform implements Platform {

    private final Plugin plugin;

    PaperPlatform(Plugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public boolean isMainThread() {
        return Bukkit.isPrimaryThread();
    }

    @Override
    public void runOnMainThread(Runnable task, Duration delay) {
        long ticks = Math.max(0, (delay.toMillis() + 49) / 50);
        try {
            Bukkit.getScheduler().runTaskLater(plugin, task, ticks);
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
}
