// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.paper;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.bukkit.inventory.ItemStack;

/**
 * Paper's own item format ({@code ItemStack#serializeAsBytes}): the item's
 * NBT, gzip-compressed, with its data version. Loading an item saved by an
 * older Minecraft version runs it through Mojang's data fixers, so items
 * survive upgrades unchanged; that includes items saved as NBT on 1.8 to 1.12
 * by {@link LegacyNbtItemFormat}.
 */
final class PaperItemFormat implements ItemFormat {

    private final Method serialize;
    private final Method deserialize;

    private PaperItemFormat(Method serialize, Method deserialize) {
        this.serialize = serialize;
        this.deserialize = deserialize;
    }

    /**
     * @return the format, or null on a server without it (Spigot, Paper before 1.16)
     */
    static PaperItemFormat find() {
        try {
            return new PaperItemFormat(ItemStack.class.getMethod("serializeAsBytes"),
                    ItemStack.class.getMethod("deserializeBytes", byte[].class));
        } catch (NoSuchMethodException | LinkageError e) {
            return null;
        }
    }

    @Override
    public String name() {
        return "Paper NBT";
    }

    @Override
    public byte[] write(ItemStack item) throws Exception {
        return (byte[]) invoke(serialize, item);
    }

    @Override
    public ItemStack read(byte[] data) throws Exception {
        return (ItemStack) invoke(deserialize, null, (Object) data);
    }

    private static Object invoke(Method m, Object target, Object... args) throws Exception {
        try {
            return m.invoke(target, args);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            throw cause instanceof Exception ? (Exception) cause : e;
        }
    }
}
