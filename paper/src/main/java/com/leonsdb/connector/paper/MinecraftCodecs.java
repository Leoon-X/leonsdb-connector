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
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.inventory.ItemStack;

/**
 * Storage for Minecraft types.
 *
 * <p>Items use Paper's own binary format ({@code serializeAsBytes}), which
 * records the Minecraft data version. Loading an item saved by an older
 * Minecraft version runs it through Mojang's data fixers, so items survive
 * server upgrades unchanged. Air and empty stacks are stored as "no item".
 */
final class MinecraftCodecs {

    private MinecraftCodecs() {
    }

    @SuppressWarnings("deprecation") // getDataVersion has no replacement yet
    static void registerAll(Codecs codecs) {
        int dataVersion = Bukkit.getUnsafe().getDataVersion();

        codecs.register(ItemStack.class, new TypeCodec<>() {
            @Override
            public FieldType fieldType() {
                return FieldType.FIELD_TYPE_ITEM;
            }

            @Override
            public Value encode(ItemStack item) {
                return Value.newBuilder().setItemValue(encodeItem(item, dataVersion)).build();
            }

            @Override
            public ItemStack decode(Value value) {
                if (value.getKindCase() != Value.KindCase.ITEM_VALUE) {
                    throw new IllegalArgumentException("expected an item but found " + value.getKindCase());
                }
                return decodeItem(value.getItemValue());
            }
        });

        codecs.register(ItemStack[].class, new TypeCodec<>() {
            @Override
            public FieldType fieldType() {
                return FieldType.FIELD_TYPE_INVENTORY;
            }

            @Override
            public Value encode(ItemStack[] items) {
                Inventory.Builder inv = Inventory.newBuilder().setSize(items.length);
                for (int slot = 0; slot < items.length; slot++) {
                    ItemStack item = items[slot];
                    if (item != null && !item.isEmpty()) {
                        inv.putSlots(slot, encodeItem(item, dataVersion));
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
                ItemStack[] items = new ItemStack[inv.getSize()];
                inv.getSlotsMap().forEach((slot, item) -> items[slot] = decodeItem(item));
                return items;
            }
        });

        codecs.register(Location.class, new TypeCodec<>() {
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
                var l = value.getLocationValue();
                // The world may not be loaded on this server; the location
                // then has no world, but its coordinates are kept.
                World world = l.getWorld().isEmpty() ? null : Bukkit.getWorld(l.getWorld());
                return new Location(world, l.getX(), l.getY(), l.getZ(), l.getYaw(), l.getPitch());
            }
        });
    }

    private static MinecraftItem encodeItem(ItemStack item, int dataVersion) {
        if (item.isEmpty()) {
            return MinecraftItem.newBuilder().setDataVersion(dataVersion).build();
        }
        return MinecraftItem.newBuilder()
                .setData(ByteString.copyFrom(item.serializeAsBytes()))
                .setDataVersion(dataVersion)
                .build();
    }

    private static ItemStack decodeItem(MinecraftItem item) {
        if (item.getData().isEmpty()) {
            return ItemStack.empty();
        }
        return ItemStack.deserializeBytes(item.getData().toByteArray());
    }
}
