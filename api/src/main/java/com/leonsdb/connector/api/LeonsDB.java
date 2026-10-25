// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.util.Objects;
import java.util.UUID;

/**
 * Entry point for plugins.
 *
 * <pre>{@code
 * // In onEnable (your plugin.yml must declare: depend: [LeonsDB])
 * LeonsDB.register(Stats.class);
 *
 * // Anywhere on the main thread
 * Stats stats = LeonsDB.player(player.getUniqueId()).get(Stats.class);
 * stats.kills++; // saved automatically within about a second
 * }</pre>
 *
 * <p>Player data is loaded before a player joins and kept in memory while
 * they are online, so {@link PlayerHandle#get(Class)} never touches the
 * network. Changes are detected, merged and saved in the background.
 */
public final class LeonsDB {

    private static volatile LeonsDBProvider provider;

    private LeonsDB() {
    }

    /**
     * Registers data classes and declares their fields to the database.
     * Call this from your plugin's {@code onEnable}. It waits for the
     * database, so do not call it while players are online.
     *
     * @param classes classes annotated with {@link PlayerData} or {@link GlobalData}
     * @throws LeonsDBException if a class is invalid, the database cannot be
     *                          reached, or a field's type conflicts with what
     *                          is already stored
     */
    public static void register(Class<?>... classes) {
        provider().register(classes);
    }

    /**
     * Returns the data of an online player.
     *
     * @param player the player's UUID
     * @return a handle to the player's data
     */
    public static PlayerHandle player(UUID player) {
        return provider().player(Objects.requireNonNull(player, "player"));
    }

    /**
     * Returns a global collection, for data that does not belong to one
     * player (clans, bans, settings).
     *
     * @param type a class annotated with {@link GlobalData}
     * @param <T>  the record type
     * @return the collection
     */
    public static <T> GlobalCollection<T> global(Class<T> type) {
        return provider().global(Objects.requireNonNull(type, "type"));
    }

    /**
     * Returns a leaderboard of a collection, highest value first.
     *
     * @param type  a registered data class
     * @param field the name of an {@link Indexed} field
     * @param <T>   the record type
     * @return the leaderboard
     */
    public static <T> Leaderboard<T> leaderboard(Class<T> type, String field) {
        return leaderboard(type, field, Leaderboard.Order.HIGHEST_FIRST);
    }

    /**
     * Returns a leaderboard of a collection.
     *
     * @param type  a registered data class
     * @param field the name of an {@link Indexed} field
     * @param order which values rank first
     * @param <T>   the record type
     * @return the leaderboard
     */
    public static <T> Leaderboard<T> leaderboard(Class<T> type, String field, Leaderboard.Order order) {
        return provider().leaderboard(Objects.requireNonNull(type, "type"), Objects.requireNonNull(field, "field"),
                Objects.requireNonNull(order, "order"));
    }

    /**
     * Starts a transaction: changes to several players and global records
     * that are stored together, or not at all. See {@link Transaction}.
     *
     * @return a new, empty transaction
     */
    public static Transaction transaction() {
        return provider().transaction();
    }

    /**
     * Returns a message channel shared by every server of the network. See
     * {@link Channel}.
     *
     * @param name {@code <namespace>.<name>}, lowercase, for example
     *             {@code "network.chat"}
     * @return the channel
     */
    public static Channel channel(String name) {
        return provider().channel(Objects.requireNonNull(name, "name"));
    }

    /**
     * Returns the past of your data: records as they were at a time, their
     * changes, and rollbacks.
     *
     * @return the history
     */
    public static History history() {
        return provider().history();
    }

    /**
     * Returns whether LeonsDB is running and ready.
     *
     * @return true if the LeonsDB plugin is enabled
     */
    public static boolean isAvailable() {
        return provider != null;
    }

    /**
     * Installs the implementation. Called by the LeonsDB plugin; plugins
     * using LeonsDB never need this.
     *
     * @param implementation the implementation, or null to uninstall
     */
    public static void install(LeonsDBProvider implementation) {
        provider = implementation;
    }

    private static LeonsDBProvider provider() {
        LeonsDBProvider p = provider;
        if (p == null) {
            throw new LeonsDBException(
                    "LeonsDB is not running. Make sure the LeonsDB plugin is installed and "
                            + "add 'depend: [LeonsDB]' to your plugin.yml");
        }
        return p;
    }
}
