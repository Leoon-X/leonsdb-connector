// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.paper;

import java.nio.charset.StandardCharsets;
import org.bukkit.configuration.InvalidConfigurationException;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.inventory.ItemStack;

/**
 * Bukkit's own item serialization, as YAML: what every Bukkit server since
 * 1.8 can read and write. Used where neither Paper's format nor NBT is
 * available (Spigot 1.13 and later). From 1.13 Bukkit records the data
 * version in it, and loading an item saved by an older version converts it.
 *
 * <p>The text starts with a YAML comment that marks it, so it is still plain
 * YAML: {@code #leonsdb-bukkit-item v1}.
 */
final class YamlItemFormat implements ItemFormat {

    static final String MARK = "#leonsdb-bukkit-item v1\n";
    private static final byte[] MARK_BYTES = MARK.getBytes(StandardCharsets.UTF_8);

    /**
     * @param data an item's bytes
     * @return whether this format wrote them
     */
    static boolean wrote(byte[] data) {
        if (data.length < MARK_BYTES.length) {
            return false;
        }
        for (int i = 0; i < MARK_BYTES.length; i++) {
            if (data[i] != MARK_BYTES[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String name() {
        return "Bukkit YAML";
    }

    @Override
    public byte[] write(ItemStack item) {
        YamlConfiguration yaml = new YamlConfiguration();
        yaml.set("item", item);
        return (MARK + yaml.saveToString()).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public ItemStack read(byte[] data) throws InvalidConfigurationException {
        YamlConfiguration yaml = new YamlConfiguration();
        yaml.loadFromString(new String(data, StandardCharsets.UTF_8));
        ItemStack item = yaml.getItemStack("item");
        if (item == null) {
            throw new InvalidConfigurationException("no item in the stored YAML");
        }
        return item;
    }
}
