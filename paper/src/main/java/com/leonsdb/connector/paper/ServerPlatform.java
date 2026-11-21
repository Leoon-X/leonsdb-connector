// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.paper;

import com.leonsdb.connector.core.cache.Platform;
import java.util.logging.Level;
import org.bukkit.plugin.Plugin;

/**
 * The connector's view of the server it runs on, with what the plugin needs
 * on top of {@link Platform}. There are two: one for servers with region
 * schedulers (Paper and Folia 1.19.4 and later), built for Java 17, and one
 * for every other server (Bukkit and Spigot of any version, older Paper),
 * which uses the Bukkit scheduler.
 *
 * <p>Internal to the plugin.
 */
public interface ServerPlatform extends Platform {

    /** The class of the platform for region schedulers, loaded only where they exist. */
    String REGION_PLATFORM = "com.leonsdb.connector.paper.modern.RegionPlatform";

    /**
     * Starts what needs the server to be running, such as the check of the
     * idle-time task queue. Called once, from onEnable.
     */
    void start();

    /**
     * Runs a task on the main thread (Folia: the global region thread) at a
     * fixed rate, until the plugin is disabled.
     *
     * @param task        the task
     * @param periodTicks ticks between runs, and before the first
     */
    void repeat(Runnable task, long periodTicks);

    /**
     * How players' data is scheduled here, for the startup log.
     *
     * @return for example "Bukkit scheduler"
     */
    String describe();

    /**
     * The platform for this server.
     *
     * @param plugin  the plugin
     * @param version the server
     * @return the platform
     * @throws IllegalStateException on a Folia server whose region schedulers
     *                               cannot be used: the Bukkit scheduler does
     *                               not work there
     */
    static ServerPlatform create(Plugin plugin, ServerVersion version) {
        if (version.hasRegionSchedulers()) {
            try {
                Class<?> type = Class.forName(REGION_PLATFORM, true, ServerPlatform.class.getClassLoader());
                return (ServerPlatform) type.getConstructor(Plugin.class).newInstance(plugin);
            } catch (ReflectiveOperationException | LinkageError | RuntimeException e) {
                if (version.isFolia()) {
                    throw new IllegalStateException("this Folia server's region schedulers cannot be used: " + e, e);
                }
                plugin.getLogger().log(Level.WARNING, "Paper's region schedulers cannot be used here; "
                        + "using the Bukkit scheduler instead", e);
            }
        } else if (version.isFolia()) {
            throw new IllegalStateException("this Folia server has no region schedulers this plugin knows");
        }
        return new BukkitPlatform(plugin);
    }
}
