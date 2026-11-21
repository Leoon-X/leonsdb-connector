// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.paper;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.bukkit.Material;
import org.bukkit.inventory.ItemStack;
import org.junit.jupiter.api.Test;

class ItemsTest {

    private static final byte[] GZIP = {0x1f, (byte) 0x8b, 8, 0, 1, 2, 3};

    /** Remembers what it was asked to read and answers with a fixed item. */
    static class Fake implements ItemFormat {
        final String name;
        final ItemStack answer;
        final List<byte[]> read = new ArrayList<>();

        Fake(String name, ItemStack answer) {
            this.name = name;
            this.answer = answer;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public byte[] write(ItemStack item) {
            return GZIP;
        }

        @Override
        public ItemStack read(byte[] data) {
            read.add(data);
            return answer;
        }
    }

    private final ItemStack stone = new ItemStack(Material.STONE, 3);
    private final Fake nbt = new Fake("NBT", stone);
    private final Fake yaml = new Fake("YAML", stone);

    @Test
    void anItemFromANewerVersionIsNeverLoaded() {
        Items items = new Items(1343, nbt, nbt, yaml);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> items.read(1344, GZIP));
        assertTrue(e.getMessage().contains("newer Minecraft version"), e.getMessage());
        assertTrue(nbt.read.isEmpty(), "the item must not even be parsed");
    }

    @Test
    void itemsFromThisVersionOrOlderAreLoadedByTheirFormat() {
        Items items = new Items(4000, nbt, nbt, yaml);
        assertSame(stone, items.read(4000, GZIP));
        assertSame(stone, items.read(99, GZIP));
        assertEquals(2, nbt.read.size());
        byte[] text = (YamlItemFormat.MARK + "item: x\n").getBytes(StandardCharsets.UTF_8);
        assertSame(stone, items.read(1343, text));
        assertArrayEquals(text, yaml.read.get(0));
    }

    @Test
    void nbtIsLeftAloneWhereItCannotBeLoaded() {
        Items items = new Items(4000, yaml, null, yaml);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> items.read(1343, GZIP));
        assertTrue(e.getMessage().contains("left as it is"), e.getMessage());
        assertTrue(yaml.read.isEmpty());
    }

    @Test
    void unknownFormatsAreLeftAlone() {
        Items items = new Items(4000, nbt, nbt, yaml);
        assertThrows(IllegalArgumentException.class, () -> items.read(1, new byte[] {(byte) 0xac, (byte) 0xed, 0, 5}));
        assertThrows(IllegalArgumentException.class, () -> items.read(1, "item: x".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void aFormatThatFailsGivesAnIllegalArgument() {
        ItemFormat broken = new Fake("NBT", null) {
            @Override
            public ItemStack read(byte[] data) {
                throw new IllegalStateException("corrupt");
            }
        };
        Items items = new Items(4000, broken, broken, yaml);
        assertThrows(IllegalArgumentException.class, () -> items.read(1, GZIP));
        Items nothing = new Items(4000, nbt, new Fake("NBT", null), yaml);
        assertThrows(IllegalArgumentException.class, () -> nothing.read(1, GZIP));
    }

    @Test
    void empty() {
        assertTrue(Items.isEmpty(null));
        assertTrue(Items.isEmpty(new ItemStack(Material.AIR)));
        assertTrue(Items.isEmpty(new ItemStack(Material.STONE, 0)));
        assertFalse(Items.isEmpty(stone));
    }

    @Test
    void formatsAreToldApartByTheirFirstBytes() {
        assertTrue(Items.isGzip(GZIP));
        assertFalse(Items.isGzip(new byte[] {0x1f}));
        assertFalse(Items.isGzip(YamlItemFormat.MARK.getBytes(StandardCharsets.UTF_8)));
        assertTrue(YamlItemFormat.wrote(YamlItemFormat.MARK.getBytes(StandardCharsets.UTF_8)));
        assertFalse(YamlItemFormat.wrote("#leonsdb".getBytes(StandardCharsets.UTF_8)));
        assertFalse(YamlItemFormat.wrote(GZIP));
    }

    // Writing and reading real items needs a running server (Bukkit's item
    // factory): every format is tested on real servers from 1.8.8 to the
    // newest by phase-v.sh in the LeonsDB repository.
}
