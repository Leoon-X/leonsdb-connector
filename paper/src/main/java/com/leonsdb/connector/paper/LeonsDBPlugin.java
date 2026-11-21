// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.paper;

import com.leonsdb.connector.api.LeonsDB;
import com.leonsdb.connector.api.LeonsDBException;
import com.leonsdb.connector.api.ServerInfo;
import com.leonsdb.connector.core.ConnectionSettings;
import com.leonsdb.connector.core.LeonsDBClient;
import com.leonsdb.connector.core.cache.LeonsDBService;
import com.leonsdb.connector.core.schema.Codecs;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import org.bukkit.Bukkit;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.event.Event;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.AsyncPlayerPreLoginEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerLoginEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.plugin.java.JavaPlugin;

/**
 * The LeonsDB plugin for Bukkit, Spigot, Paper and Folia, Minecraft 1.8 and
 * later. It connects to LeonsDB, keeps online players' data in memory and
 * saves it in the background. Other plugins use it through {@link LeonsDB}.
 *
 * <p>It is built against the Bukkit 1.8.8 API, so it loads on every server;
 * what newer servers have is found when it starts (see {@link ServerVersion},
 * {@link ServerPlatform} and {@link Items}).
 */
public final class LeonsDBPlugin extends JavaPlugin implements Listener {

    /** Logins that never reached the server are cleaned up after this long. */
    private static final long ABANDONED_LOGIN_MILLIS = TimeUnit.SECONDS.toMillis(60);

    private static final String CLOSE_EVENT = "com.destroystokyo.paper.event.player.PlayerConnectionCloseEvent";

    private LeonsDBClient client;
    private LeonsDBService service;
    // Per player, their connections between the login checks and the end.
    private final Map<UUID, Integer> connections = new ConcurrentHashMap<>();
    // Whether the server tells when a connection ends (Paper); without it,
    // connections are counted until the player quits or the login is abandoned.
    private volatile boolean closeEvent;

    @Override
    public void onEnable() {
        saveDefaultConfig();
        FileConfiguration config = getConfig();

        ConnectionSettings settings;
        try {
            settings = new ConnectionSettings(
                    config.getString("server.host", "127.0.0.1"),
                    config.getInt("server.port", ConnectionSettings.DEFAULT_PORT),
                    config.getBoolean("server.tls", false),
                    config.getString("server.token", ""),
                    Duration.ofMillis(config.getLong("server.request-timeout-ms", 5000)));
        } catch (IllegalArgumentException e) {
            getLogger().severe("Invalid LeonsDB settings in config.yml: " + e.getMessage());
            getServer().getPluginManager().disablePlugin(this);
            return;
        }
        if (settings.token() == null) {
            getLogger().severe("No access token configured. Create one with 'leonsdb token create -name <server> "
                    + "-role server' and put it in plugins/LeonsDB/config.yml under server.token.");
            getServer().getPluginManager().disablePlugin(this);
            return;
        }

        ServerVersion version = ServerVersion.detect();
        ServerPlatform platform;
        try {
            platform = ServerPlatform.create(this, version);
        } catch (IllegalStateException e) {
            getLogger().log(Level.SEVERE, "LeonsDB cannot run on " + version + ": " + e.getMessage(), e);
            getServer().getPluginManager().disablePlugin(this);
            return;
        }
        Items items = Items.forServer(version, getLogger());

        client = LeonsDBClient.connect(settings, "leonsdb-paper/" + getDescription().getVersion());
        try {
            ServerInfo info = client.fetchServerInfo().get(settings.requestTimeout().toMillis() + 1000, TimeUnit.MILLISECONDS);
            getLogger().info("Connected to LeonsDB " + info.serverVersion() + " at " + settings.host() + ":" + settings.port());
        } catch (Exception e) {
            // Keep going: the connection is retried automatically, and players
            // are refused (not given empty data) while LeonsDB is unreachable.
            getLogger().log(Level.SEVERE, "Could not reach LeonsDB at " + settings.host() + ":" + settings.port()
                    + " yet. Players cannot join until it is reachable. Cause: " + rootMessage(e));
        }

        Codecs codecs = Codecs.withDefaults();
        MinecraftCodecs.registerAll(codecs, items);
        String serverName = config.getString("server-name", "");
        if (serverName == null || serverName.trim().isEmpty()) {
            serverName = "paper-" + Bukkit.getPort();
        }
        platform.start();
        service = new LeonsDBService(client, codecs, getLogger(), platform, new LeonsDBService.Settings(
                serverName,
                Duration.ofMillis(config.getLong("load-timeout-ms", 5000)),
                Duration.ofMillis(config.getLong("lease-wait-ms", 10000)),
                Duration.ofMillis(config.getLong("handover-grace-ms", 35000)),
                getDataFolder().toPath().resolve("recovery")));
        service.start();
        LeonsDB.install(service);

        long flushTicks = Math.max(1, config.getLong("save-interval-ticks", 20));
        platform.repeat(service::flushTick, flushTicks);
        platform.repeat(this::cleanUpAbandonedLogins, 20 * 30);
        getServer().getPluginManager().registerEvents(this, this);
        closeEvent = listenForConnectionClose();
        getLogger().info("Running on " + version + ": " + platform.describe() + "; items stored as " + items.describe()
                + (closeEvent ? "" : "; logins that end before joining are released after "
                + TimeUnit.MILLISECONDS.toSeconds(ABANDONED_LOGIN_MILLIS) + " s"));
    }

    @Override
    public void onDisable() {
        if (service != null) {
            getLogger().info("Saving all player data...");
            service.shutdown(Duration.ofSeconds(10));
            LeonsDB.install(null);
            service = null;
        }
        if (client != null) {
            client.close();
            client = null;
        }
    }

    /**
     * Loads the player's data before they join. If it cannot be loaded the
     * player is turned away: letting them play with empty data would
     * overwrite their real data when it is saved.
     */
    @EventHandler(priority = EventPriority.HIGHEST)
    public void onPreLogin(AsyncPlayerPreLoginEvent event) {
        if (event.getLoginResult() != AsyncPlayerPreLoginEvent.Result.ALLOWED) {
            return; // another plugin already refused this login
        }
        try {
            service.loadPlayer(event.getUniqueId(), event.getName());
        } catch (LeonsDBException e) {
            getLogger().warning("Refused login of " + event.getName() + ": " + rootMessage(e));
            event.disallow(AsyncPlayerPreLoginEvent.Result.KICK_OTHER,
                    "Your data could not be loaded right now. Please try again in a moment.");
        }
    }

    /**
     * Counts the player's connections that got past the login checks. If
     * another plugin refused the login after the data was loaded, the data
     * is released now: no later event would.
     */
    @EventHandler(priority = EventPriority.MONITOR)
    public void onPreLoginResult(AsyncPlayerPreLoginEvent event) {
        UUID uuid = event.getUniqueId();
        if (event.getLoginResult() == AsyncPlayerPreLoginEvent.Result.ALLOWED) {
            connections.merge(uuid, 1, Integer::sum);
        } else if (Bukkit.getPlayer(uuid) == null && !connections.containsKey(uuid)) {
            service.unloadPlayer(uuid);
        }
    }

    /**
     * Listens for Paper's PlayerConnectionCloseEvent where the server has it
     * (Paper 1.12 and later); elsewhere a login that ends before the player
     * joins is released by {@link #cleanUpAbandonedLogins}.
     *
     * @return whether the event is listened to
     */
    private boolean listenForConnectionClose() {
        Class<? extends Event> type;
        Method uuid;
        try {
            type = Class.forName(CLOSE_EVENT).asSubclass(Event.class);
            uuid = type.getMethod("getPlayerUniqueId");
        } catch (ReflectiveOperationException | ClassCastException | LinkageError e) {
            return false;
        }
        getServer().getPluginManager().registerEvent(type, this, EventPriority.MONITOR, (listener, event) -> {
            if (!type.isInstance(event)) {
                return;
            }
            try {
                onConnectionClose((UUID) uuid.invoke(event));
            } catch (ReflectiveOperationException e) {
                getLogger().log(Level.WARNING, "Could not read a closed connection's player", e);
            }
        }, this);
        return true;
    }

    /**
     * A connection ended. If it was the player's last one and they never
     * reached the world (a server switch that failed, or a client that gave
     * up while logging in), their data is saved and released now, so the
     * server they are still on can take them back right away.
     */
    private void onConnectionClose(UUID uuid) {
        Integer left = connections.computeIfPresent(uuid, (u, n) -> n > 1 ? n - 1 : null);
        if (left == null && Bukkit.getPlayer(uuid) == null) {
            service.unloadPlayer(uuid); // nothing to do after a normal quit, which saved already
        }
    }

    /** If a later check refuses the login, the loaded data is released. */
    @EventHandler(priority = EventPriority.MONITOR)
    public void onLogin(PlayerLoginEvent event) {
        if (event.getResult() != PlayerLoginEvent.Result.ALLOWED) {
            service.unloadPlayer(event.getPlayer().getUniqueId());
        }
    }

    /**
     * The player is in the game: the server they came from may now treat
     * them as logged in here (see LeonsDBService#playerJoined).
     */
    @EventHandler(priority = EventPriority.MONITOR)
    public void onJoin(PlayerJoinEvent event) {
        service.playerJoined(event.getPlayer().getUniqueId());
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onQuit(PlayerQuitEvent event) {
        UUID uuid = event.getPlayer().getUniqueId();
        service.unloadPlayer(uuid);
        if (!closeEvent) {
            connections.remove(uuid);
        }
    }

    /**
     * A login can be abandoned after the data was loaded (the client
     * disconnects mid-login) without a quit event. Release such data.
     */
    private void cleanUpAbandonedLogins() {
        long now = System.currentTimeMillis();
        service.loadedPlayers().forEach((UUID uuid, Long loadedAt) -> {
            if (now - loadedAt > ABANDONED_LOGIN_MILLIS && Bukkit.getPlayer(uuid) == null) {
                service.unloadPlayer(uuid);
                if (!closeEvent) {
                    connections.remove(uuid);
                }
            }
        });
    }

    private static String rootMessage(Throwable t) {
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return t.getMessage();
    }
}
