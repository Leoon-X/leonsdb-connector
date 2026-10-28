// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.velocity;

import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import com.leonsdb.connector.core.ConnectionSettings;
import com.leonsdb.connector.core.LeonsDBClient;
import com.velocitypowered.api.event.Subscribe;
import com.velocitypowered.api.event.player.ServerPreConnectEvent;
import com.velocitypowered.api.event.proxy.ProxyInitializeEvent;
import com.velocitypowered.api.event.proxy.ProxyShutdownEvent;
import com.velocitypowered.api.plugin.annotation.DataDirectory;
import com.velocitypowered.api.proxy.server.RegisteredServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.yaml.snakeyaml.Yaml;

/**
 * The LeonsDB plugin for Velocity. Just before the proxy moves a player to
 * another server, it tells LeonsDB, so the server the player is on saves
 * them at once and the switch waits less. It holds no player data and needs
 * only a proxy token, which can do nothing else.
 */
public final class LeonsDBVelocity {

    private final Logger log;
    private final Path dataDirectory;
    private LeonsDBClient client;
    private SwitchHints hints;

    /**
     * Created by Velocity.
     *
     * @param log           the plugin's logger
     * @param dataDirectory plugins/leonsdb
     */
    @Inject
    public LeonsDBVelocity(Logger log, @DataDirectory Path dataDirectory) {
        this.log = log;
        this.dataDirectory = dataDirectory;
    }

    /**
     * Reads the settings and connects.
     *
     * @param event the event
     */
    @Subscribe
    public void onInitialize(ProxyInitializeEvent event) {
        Map<String, Object> config;
        try {
            config = loadConfig();
        } catch (IOException | RuntimeException e) {
            log.error("Could not read plugins/leonsdb/config.yml; LeonsDB hints are off: {}", e.getMessage());
            return;
        }
        if (!Boolean.parseBoolean(String.valueOf(get(config, "hints", true)))) {
            log.info("LeonsDB switch hints are turned off in config.yml.");
            return;
        }
        ConnectionSettings settings;
        try {
            settings = new ConnectionSettings(
                    String.valueOf(get(config, "server.host", "127.0.0.1")),
                    Integer.parseInt(String.valueOf(get(config, "server.port", ConnectionSettings.DEFAULT_PORT))),
                    Boolean.parseBoolean(String.valueOf(get(config, "server.tls", false))),
                    String.valueOf(get(config, "server.token", "")),
                    Duration.ofMillis(Long.parseLong(String.valueOf(get(config, "server.request-timeout-ms", 2000)))));
        } catch (IllegalArgumentException e) {
            log.error("Invalid LeonsDB settings in config.yml; hints are off: {}", e.getMessage());
            return;
        }
        if (settings.token() == null) {
            log.warn("No access token in plugins/leonsdb/config.yml; LeonsDB hints are off. Create one on the "
                    + "LeonsDB machine with: leonsdb token create -name <this proxy> -role proxy");
            return;
        }
        client = LeonsDBClient.connect(settings, "leonsdb-velocity");
        hints = new SwitchHints((uuid, target) -> client.expect(bytes(uuid), target), log::warn, log::info,
                System::currentTimeMillis);
        client.fetchServerInfo().orTimeout(settings.requestTimeout().toMillis() + 1000, TimeUnit.MILLISECONDS)
                .whenComplete((info, err) -> {
                    if (err != null) {
                        log.warn("Could not reach LeonsDB at {}:{} yet; players switch as before until it is "
                                + "reachable. Cause: {}", settings.host(), settings.port(), err.getMessage());
                    } else {
                        log.info("Sending switch hints to LeonsDB {} at {}:{}", info.serverVersion(), settings.host(),
                                settings.port());
                    }
                });
    }

    /**
     * The proxy is about to connect a player to a server: tell LeonsDB.
     * Runs last, so a plugin that redirects or refuses the connection is
     * taken into account; it never delays the connection.
     *
     * @param event the event
     */
    @Subscribe(priority = Short.MIN_VALUE)
    public void onPreConnect(ServerPreConnectEvent event) {
        SwitchHints h = hints;
        if (h == null || !event.getResult().isAllowed()) {
            return;
        }
        RegisteredServer target = event.getResult().getServer().orElse(null);
        if (target == null) {
            return;
        }
        String current = event.getPlayer().getCurrentServer().map(s -> s.getServerInfo().getName()).orElse(null);
        h.beforeConnect(event.getPlayer().getUniqueId(), current, target.getServerInfo().getName());
    }

    /**
     * Disconnects from LeonsDB.
     *
     * @param event the event
     */
    @Subscribe
    public void onShutdown(ProxyShutdownEvent event) {
        hints = null;
        if (client != null) {
            client.close();
            client = null;
        }
    }

    private Map<String, Object> loadConfig() throws IOException {
        Path file = dataDirectory.resolve("config.yml");
        if (!Files.exists(file)) {
            Files.createDirectories(dataDirectory);
            try (InputStream in = getClass().getResourceAsStream("/config.yml")) {
                Files.copy(in, file);
            }
        }
        try (Reader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            Map<String, Object> loaded = new Yaml().load(r);
            return loaded == null ? Map.of() : loaded;
        }
    }

    /** A setting by its dotted path, such as "server.host". */
    @SuppressWarnings("unchecked")
    static Object get(Map<String, Object> config, String path, Object fallback) {
        Object node = config;
        for (String key : path.split("[.]")) {
            if (!(node instanceof Map<?, ?> map) || !map.containsKey(key)) {
                return fallback;
            }
            node = ((Map<String, Object>) map).get(key);
        }
        return node == null ? fallback : node;
    }

    private static ByteString bytes(UUID uuid) {
        ByteBuffer b = ByteBuffer.allocate(16);
        b.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
        return ByteString.copyFrom(b.array());
    }
}
