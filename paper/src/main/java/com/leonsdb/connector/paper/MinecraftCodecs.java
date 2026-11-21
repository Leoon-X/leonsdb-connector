// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.paper;

import com.google.protobuf.ByteString;
import com.leonsdb.connector.core.schema.Codecs;
import com.leonsdb.connector.core.schema.TypeCodec;
import com.leonsdb.proto.v1.FieldType;
import com.leonsdb.proto.v1.Inventory;
import com.leonsdb.proto.v1.MinecraftItem;
import com.leonsdb.proto.v1.Value;
import java.util.Map;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.inventory.ItemStack;

/**
 * Storage for Minecraft types: items, inventories and locations.
 *
 * <p>Items are stored by {@link Items}, with the data version of the server
 * that saved them, so they move between servers of different Minecraft
 * versions. Air and empty stacks are stored as "no item". An inventory
 * holding one item this server cannot load is not loaded at all, and left
 * as it is stored (see {@link Items}).
 */
final class MinecraftCodecs {

    private MinecraftCodecs() {
    }

    static void registerAll(Codecs codecs, Items items) {
        codecs.register(ItemStack.class, new TypeCodec<ItemStack>() {
            @Override
            public FieldType fieldType() {
                return FieldType.FIELD_TYPE_ITEM;
            }

            @Override
            public Value encode(ItemStack item) {
                return Value.newBuilder().setItemValue(encodeItem(items, item)).build();
            }

            @Override
            public ItemStack decode(Value value) {
                if (value.getKindCase() != Value.KindCase.ITEM_VALUE) {
                    throw new IllegalArgumentException("expected an item but found " + value.getKindCase());
                }
                return decodeItem(items, value.getItemValue());
            }
        });

        codecs.register(ItemStack[].class, new TypeCodec<ItemStack[]>() {
            @Override
            public FieldType fieldType() {
                return FieldType.FIELD_TYPE_INVENTORY;
            }

            @Override
            public Value encode(ItemStack[] slots) {
                Inventory.Builder inv = Inventory.newBuilder().setSize(slots.length);
                for (int slot = 0; slot < slots.length; slot++) {
                    ItemStack item = slots[slot];
                    if (!Items.isEmpty(item)) {
                        inv.putSlots(slot, encodeItem(items, item));
                    }
                }
                return Value.newBuilder().setInventoryValue(inv).build();
            }

            @Override
            public ItemStack[] decode(Value value) {
                if (value.getKindCase() != Value.KindCase.INVENTORY_VALUE) {
                    throw new IllegalArgumentException("expected an inventory but found " + value.getKindCase());
                }
                Inventory inv = value.getInventoryValue();
                ItemStack[] slots = new ItemStack[inv.getSize()];
                for (Map.Entry<Integer, MinecraftItem> e : inv.getSlotsMap().entrySet()) {
                    try {
                        slots[e.getKey()] = decodeItem(items, e.getValue());
                    } catch (IllegalArgumentException ex) {
                        throw new IllegalArgumentException("slot " + e.getKey() + ": " + ex.getMessage(), ex);
                    }
                }
                return slots;
            }
        });

        codecs.register(Location.class, new TypeCodec<Location>() {
            @Override
            public FieldType fieldType() {
                return FieldType.FIELD_TYPE_LOCATION;
            }

            @Override
            public Value encode(Location loc) {
                World world = loc.getWorld();
                return Value.newBuilder().setLocationValue(com.leonsdb.proto.v1.Location.newBuilder()
                        .setWorld(world == null ? "" : world.getName())
                        .setX(loc.getX()).setY(loc.getY()).setZ(loc.getZ())
                        .setYaw(loc.getYaw()).setPitch(loc.getPitch())).build();
            }

            @Override
            public Location decode(Value value) {
                if (value.getKindCase() != Value.KindCase.LOCATION_VALUE) {
                    throw new IllegalArgumentException("expected a location but found " + value.getKindCase());
                }
                com.leonsdb.proto.v1.Location l = value.getLocationValue();
                // The world may not be loaded on this server; the location
                // then has no world, but its coordinates are kept.
                World world = l.getWorld().isEmpty() ? null : Bukkit.getWorld(l.getWorld());
                return new Location(world, l.getX(), l.getY(), l.getZ(), l.getYaw(), l.getPitch());
            }
        });
    }

    private static MinecraftItem encodeItem(Items items, ItemStack item) {
        MinecraftItem.Builder b = MinecraftItem.newBuilder().setDataVersion(items.dataVersion());
        if (!Items.isEmpty(item)) {
            b.setData(ByteString.copyFrom(items.write(item)));
        }
        return b.build();
    }

    private static ItemStack decodeItem(Items items, MinecraftItem item) {
        if (item.getData().isEmpty()) {
            return Items.empty();
        }
        return items.read(item.getDataVersion(), item.getData().toByteArray());
    }
}
