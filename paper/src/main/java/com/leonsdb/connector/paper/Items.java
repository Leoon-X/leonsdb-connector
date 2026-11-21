// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.paper;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bukkit.Material;
import org.bukkit.inventory.ItemStack;

/**
 * How this server stores items, and which stored items it can load. One
 * network may mix Minecraft versions and server software, so every item is
 * stored with the data version of the server that saved it, in one of three
 * formats, told apart by their first bytes:
 *
 * <ul>
 *   <li>NBT, gzip-compressed: written by Paper 1.16+ ({@link PaperItemFormat})
 *       and by every server on 1.8 to 1.12 ({@link LegacyNbtItemFormat});
 *       loaded by both.</li>
 *   <li>Bukkit's YAML: written by servers that have neither (Spigot 1.13+,
 *       Paper 1.13 to 1.15); loaded everywhere.</li>
 * </ul>
 *
 * <p>An item saved by a newer Minecraft version than this server's, or in a
 * format this server cannot read, is never loaded: older servers cannot
 * turn it back into an older item without losing parts of it. Loading fails
 * with {@link IllegalArgumentException}, and the connector then leaves that
 * field as it is stored: the plugin sees its default value on this server,
 * and this server never saves over it. The item stays intact for the
 * servers that can load it.
 */
final class Items {

    private static final Set<String> AIR = new HashSet<>(Arrays.asList("AIR", "CAVE_AIR", "VOID_AIR", "LEGACY_AIR"));

    private final int dataVersion;
    private final ItemFormat writer;
    private final ItemFormat nbt;      // reads NBT here, or null
    private final ItemFormat yaml;

    Items(int dataVersion, ItemFormat writer, ItemFormat nbt, ItemFormat yaml) {
        this.dataVersion = dataVersion;
        this.writer = writer;
        this.nbt = nbt;
        this.yaml = yaml;
    }

    /**
     * Chooses the formats for this server. Each candidate must write and
     * read back a test item first; one that fails is not used.
     *
     * @param version the server
     * @param log     where to say what is used
     * @return the item storage
     */
    static Items forServer(ServerVersion version, Logger log) {
        ItemFormat yaml = new YamlItemFormat();
        ItemFormat nbt = PaperItemFormat.find();
        if (nbt == null && version.beforeFlattening()) {
            try {
                nbt = LegacyNbtItemFormat.find(version);
            } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
                log.log(Level.WARNING, "Items cannot be stored as NBT on this server; using Bukkit's YAML", e);
            }
        }
        if (nbt != null && !works(nbt, log)) {
            nbt = null;
        }
        return new Items(version.dataVersion(), nbt != null ? nbt : yaml, nbt, yaml);
    }

    private static boolean works(ItemFormat format, Logger log) {
        try {
            ItemStack test = new ItemStack(Material.STONE, 3);
            ItemStack back = format.read(format.write(test));
            if (back != null && back.getType() == Material.STONE && back.getAmount() == 3) {
                return true;
            }
            log.warning("Items stored as " + format.name() + " did not load back the same here; using Bukkit's YAML");
        } catch (Exception | LinkageError e) {
            log.log(Level.WARNING, "Items cannot be stored as " + format.name() + " here; using Bukkit's YAML", e);
        }
        return false;
    }

    /** @return the data version recorded with the items this server saves */
    int dataVersion() {
        return dataVersion;
    }

    /** @return how this server writes items, for the startup log */
    String describe() {
        return writer.name() + (nbt == null ? " (items stored as NBT by other servers cannot be loaded here)" : "");
    }

    /**
     * @param item an item, or null
     * @return whether it is no item: null, air, or none of it
     */
    static boolean isEmpty(ItemStack item) {
        return item == null || item.getAmount() <= 0 || AIR.contains(item.getType().name());
    }

    /**
     * @return no item: Paper's {@code ItemStack.empty()} where it exists, air elsewhere
     */
    static ItemStack empty() {
        if (EMPTY != null) {
            try {
                return (ItemStack) EMPTY.invoke(null);
            } catch (ReflectiveOperationException | RuntimeException e) {
                // fall through to air
            }
        }
        return new ItemStack(Material.AIR);
    }

    private static final Method EMPTY = emptyMethod();

    private static Method emptyMethod() {
        try {
            return ItemStack.class.getMethod("empty");
        } catch (NoSuchMethodException | LinkageError e) {
            return null;
        }
    }

    /**
     * Turns an item that is not empty into bytes.
     *
     * @param item the item
     * @return its bytes
     * @throws IllegalArgumentException if it cannot be stored
     */
    byte[] write(ItemStack item) {
        try {
            return writer.write(item);
        } catch (Exception e) {
            throw new IllegalArgumentException("the item could not be stored as " + writer.name() + ": " + e, e);
        }
    }

    /**
     * Loads a stored item.
     *
     * @param itemDataVersion the data version it was saved with
     * @param data            its bytes, not empty
     * @return the item
     * @throws IllegalArgumentException if this server must not or cannot load it
     */
    ItemStack read(int itemDataVersion, byte[] data) {
        if (itemDataVersion > dataVersion) {
            throw new IllegalArgumentException("the item was saved by a newer Minecraft version (data version "
                    + itemDataVersion + ", this server " + dataVersion + ") and is left as it is");
        }
        ItemFormat format;
        if (isGzip(data)) {
            if (nbt == null) {
                throw new IllegalArgumentException("the item is stored as NBT, which this server cannot load "
                        + "(Paper can), and is left as it is");
            }
            format = nbt;
        } else if (YamlItemFormat.wrote(data)) {
            format = yaml;
        } else {
            throw new IllegalArgumentException("the item is stored in an unknown format and is left as it is");
        }
        ItemStack item;
        try {
            item = format.read(data);
        } catch (Exception e) {
            throw new IllegalArgumentException("the item could not be loaded from " + format.name() + ": " + e, e);
        }
        if (item == null) {
            throw new IllegalArgumentException("the item could not be loaded from " + format.name());
        }
        return item;
    }

    static boolean isGzip(byte[] data) {
        return data.length >= 2 && (data[0] & 0xff) == 0x1f && (data[1] & 0xff) == 0x8b;
    }
}
