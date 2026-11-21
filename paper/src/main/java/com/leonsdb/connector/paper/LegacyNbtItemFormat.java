// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.paper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.bukkit.Bukkit;
import org.bukkit.inventory.ItemStack;

/**
 * Items as Minecraft's own NBT on 1.8 to 1.12, read and written through the
 * server's CraftBukkit classes: gzip-compressed, the same format Paper uses,
 * so Paper servers on newer versions load these items through Mojang's data
 * fixers. On 1.9 to 1.12 the item's data version is added to it; 1.8 has
 * none. Unlike Bukkit's YAML, NBT keeps every tag of the item, including
 * the ones other plugins add.
 *
 * <p>Minecraft 1.9 to 1.12 read items from older versions as they are,
 * without Mojang's fixers, as they do for items in a plugin's own files.
 */
final class LegacyNbtItemFormat implements ItemFormat {

    private final int dataVersion;
    private final Method asNmsCopy;
    private final Method asBukkitCopy;
    private final Constructor<?> newCompound;
    private final Method save;
    private final Method setInt;
    private final Method load;          // static, or null when loadConstructor is used
    private final Constructor<?> loadConstructor;
    private final Method readCompressed;
    private final Method writeCompressed;

    private LegacyNbtItemFormat(int dataVersion, Method asNmsCopy, Method asBukkitCopy, Constructor<?> newCompound,
                                Method save, Method setInt, Method load, Constructor<?> loadConstructor,
                                Method readCompressed, Method writeCompressed) {
        this.dataVersion = dataVersion;
        this.asNmsCopy = asNmsCopy;
        this.asBukkitCopy = asBukkitCopy;
        this.newCompound = newCompound;
        this.save = save;
        this.setInt = setInt;
        this.load = load;
        this.loadConstructor = loadConstructor;
        this.readCompressed = readCompressed;
        this.writeCompressed = writeCompressed;
    }

    /**
     * Looks up the server classes.
     *
     * @param version the server
     * @return the format
     * @throws ReflectiveOperationException if this server does not have them
     */
    static LegacyNbtItemFormat find(ServerVersion version) throws ReflectiveOperationException {
        // org.bukkit.craftbukkit.v1_8_R3.CraftServer -> v1_8_R3
        String craft = Bukkit.getServer().getClass().getPackage().getName();
        String revision = craft.substring(craft.lastIndexOf('.') + 1);
        ClassLoader loader = Bukkit.getServer().getClass().getClassLoader();
        Class<?> craftItem = Class.forName(craft + ".inventory.CraftItemStack", true, loader);
        Class<?> compound = Class.forName("net.minecraft.server." + revision + ".NBTTagCompound", true, loader);
        Class<?> streams = Class.forName("net.minecraft.server." + revision + ".NBTCompressedStreamTools", true, loader);

        Method asNmsCopy = craftItem.getMethod("asNMSCopy", ItemStack.class);
        Class<?> nmsItem = asNmsCopy.getReturnType();
        Method asBukkitCopy = craftItem.getMethod("asBukkitCopy", nmsItem);
        Method save = nmsItem.getMethod("save", compound);
        Method setInt = compound.getMethod("setInt", String.class, int.class);

        Method load = null;
        Constructor<?> loadConstructor = null;
        try {
            load = nmsItem.getMethod("createStack", compound); // 1.8 to 1.10
        } catch (NoSuchMethodException e) {
            loadConstructor = nmsItem.getConstructor(compound); // 1.11 and 1.12
        }

        Method readCompressed = null;
        Method writeCompressed = null;
        for (Method m : streams.getMethods()) {
            if (!Modifier.isStatic(m.getModifiers())) {
                continue;
            }
            Class<?>[] p = m.getParameterTypes();
            if (p.length == 1 && p[0] == InputStream.class && m.getReturnType() == compound) {
                readCompressed = m;
            } else if (p.length == 2 && p[0] == compound && p[1] == OutputStream.class && m.getReturnType() == void.class) {
                writeCompressed = m;
            }
        }
        if (readCompressed == null || writeCompressed == null) {
            throw new NoSuchMethodException(streams.getName() + " has no compressed NBT read and write methods");
        }
        return new LegacyNbtItemFormat(version.dataVersion(), asNmsCopy, asBukkitCopy, compound.getConstructor(),
                save, setInt, load, loadConstructor, readCompressed, writeCompressed);
    }

    @Override
    public String name() {
        return "NBT";
    }

    @Override
    public byte[] write(ItemStack item) throws Exception {
        Object nms = call(asNmsCopy, null, item);
        if (nms == null) {
            throw new IllegalArgumentException("the server could not copy the item");
        }
        Object tag = call(save, nms, newCompound.newInstance());
        if (dataVersion > ServerVersion.DATA_VERSION_1_8) {
            call(setInt, tag, "DataVersion", dataVersion);
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        call(writeCompressed, null, tag, out);
        return out.toByteArray();
    }

    @Override
    public ItemStack read(byte[] data) throws Exception {
        Object tag = call(readCompressed, null, new ByteArrayInputStream(data));
        Object nms = load != null ? call(load, null, tag) : construct(tag);
        if (nms == null) {
            throw new IllegalArgumentException("the server could not read the item");
        }
        return (ItemStack) call(asBukkitCopy, null, nms);
    }

    private Object construct(Object tag) throws Exception {
        try {
            return loadConstructor.newInstance(tag);
        } catch (InvocationTargetException e) {
            throw unwrap(e);
        }
    }

    private static Object call(Method m, Object target, Object... args) throws Exception {
        try {
            return m.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw unwrap(e);
        }
    }

    private static Exception unwrap(InvocationTargetException e) {
        Throwable cause = e.getCause();
        return cause instanceof Exception ? (Exception) cause : e;
    }
}
