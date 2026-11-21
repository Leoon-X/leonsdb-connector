// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.paper.modern;

import com.leonsdb.connector.paper.IdleQueue;
import com.leonsdb.connector.paper.ServerPlatform;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.logging.Logger;
import net.kyori.adventure.text.Component;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.plugin.IllegalPluginAccessException;
import org.bukkit.plugin.Plugin;

/**
 * The connector's view of a Paper or Folia server with region schedulers
 * (1.19.4 and later). Paper has them too: there they all run on the main
 * thread, so Paper behaves as it always did, and on Folia each player's data
 * is touched on the thread of the region the player is in.
 *
 * <p>Built for Java 17 against the Folia API, and loaded by
 * {@link ServerPlatform#create} only on servers that have region schedulers.
 * Internal to the plugin.
 */
public final class RegionPlatform implements ServerPlatform {

    private final Plugin plugin;
    // Paper's idle-time task queue, once checked; null means the scheduler.
    private volatile Executor idle;
    private volatile String how;
    // Bukkit.isGlobalTickThread: Folia has it, and Paper from late 1.21 builds.
    // On Paper without it, the global region is the main thread.
    private final boolean globalTickCheck;

    /**
     * Creates the platform; fails if this server has no region schedulers.
     *
     * @param plugin the plugin
     */
    public RegionPlatform(Plugin plugin) {
        this.plugin = plugin;
        this.how = isFolia() ? "Folia region schedulers" : "Paper region schedulers";
        Bukkit.getGlobalRegionScheduler(); // fails here, not later, where there are none
        boolean check;
        try {
            Bukkit.class.getMethod("isGlobalTickThread");
            check = true;
        } catch (NoSuchMethodException e) {
            check = false;
        }
        this.globalTickCheck = check;
    }

    @Override
    public void repeat(Runnable task, long periodTicks) {
        // The global region scheduler is the main thread on Paper; on Folia,
        // flushTick saves each player on the thread of their region.
        Bukkit.getGlobalRegionScheduler().runAtFixedRate(plugin, t -> task.run(), periodTicks, periodTicks);
    }

    @Override
    public String describe() {
        return how;
    }

    /**
     * Starts using Paper's idle-time task queue for players' data if it works
     * here, and logs once which way is used. Until the check is done (when
     * the server starts ticking), and if it fails, the scheduler is used.
     */
    @Override
    public void start() {
        Logger log = plugin.getLogger();
        String scheduler = "Player data is saved for server switches at the next tick (scheduler): ";
        if (!Boolean.parseBoolean(System.getProperty("leonsdb.idleQueue", "true"))) {
            log.info(scheduler + "turned off with -Dleonsdb.idleQueue=false.");
            return;
        }
        IdleQueue.Lookup found = IdleQueue.find(Bukkit.getServer(), isFolia());
        if (found.executor() == null) {
            log.info(scheduler + found.reason() + ".");
            return;
        }
        IdleQueue.check(found.executor(), Bukkit::isPrimaryThread, IdleQueue.CHECK_TIMEOUT).thenAccept(works -> {
            if (works) {
                idle = found.executor();
                how = "Paper region schedulers, and the idle-time task queue for server switches";
                log.info("Player data is saved for server switches as soon as the main thread is idle "
                        + "(Paper's idle-time task queue).");
            } else {
                log.info(scheduler + "Paper's idle-time task queue did not run a test task on the main thread.");
            }
        });
    }

    private static boolean isFolia() {
        try {
            Class.forName("io.papermc.paper.threadedregions.RegionizedServer");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    @Override
    public boolean isMainThread() {
        return globalTickThread();
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
        return player == null ? globalTickThread() : Bukkit.isOwnedByCurrentRegion(player);
    }

    @Override
    public void runForPlayer(UUID uuid, Runnable task, Duration delay) {
        Player player = Bukkit.getPlayer(uuid);
        if (player == null) {
            runOnMainThread(task, delay);
            return;
        }
        Executor queue = idle;
        if (queue != null && delay.isZero() && !Bukkit.isPrimaryThread()) {
            // Paper: the player's thread is the main thread, which runs this
            // as soon as it is idle instead of at the next tick.
            IdleQueue.execute(queue, task, Bukkit::isPrimaryThread, t -> runOnMainThread(t, Duration.ZERO));
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

    private boolean globalTickThread() {
        return globalTickCheck ? Bukkit.isGlobalTickThread() : Bukkit.isPrimaryThread();
    }
}
