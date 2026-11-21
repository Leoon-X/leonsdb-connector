// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.paper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.bukkit.Bukkit;

/**
 * The server the plugin runs on: its Minecraft version, its data version and
 * what it has. Detected once at startup.
 *
 * <p>Internal to the plugin.
 */
public final class ServerVersion {

    /**
     * The data version recorded for items from Minecraft 1.8, which has
     * none. Any number below 1.9's (169) would do.
     */
    static final int DATA_VERSION_1_8 = 99;

    private static final Pattern VERSION = Pattern.compile("^(\\d+)\\.(\\d+)(?:\\.(\\d+))?");

    private final String name;
    private final int major;
    private final int minor;
    private final int patch;
    private final int dataVersion;
    private final boolean folia;
    private final boolean regionSchedulers;

    ServerVersion(String name, int major, int minor, int patch, int dataVersion, boolean folia, boolean regionSchedulers) {
        this.name = name;
        this.major = major;
        this.minor = minor;
        this.patch = patch;
        this.dataVersion = dataVersion;
        this.folia = folia;
        this.regionSchedulers = regionSchedulers;
    }

    /**
     * Detects the running server.
     *
     * @return the server
     */
    public static ServerVersion detect() {
        String bukkitVersion = Bukkit.getBukkitVersion();
        int[] v = parse(bukkitVersion);
        Integer reported = reportedDataVersion();
        int dataVersion = reported != null ? reported : knownDataVersion(v[0], v[1], v[2]);
        boolean folia = classExists("io.papermc.paper.threadedregions.RegionizedServer");
        // Everything the platform for region schedulers calls, so it is never
        // chosen on a build that has only part of it.
        boolean regions = classExists("io.papermc.paper.threadedregions.scheduler.GlobalRegionScheduler")
                && hasMethod(Bukkit.class, "getGlobalRegionScheduler")
                && hasMethod(Bukkit.class, "isOwnedByCurrentRegion", org.bukkit.entity.Entity.class)
                && hasMethod(org.bukkit.entity.Entity.class, "getScheduler")
                && classExists("net.kyori.adventure.text.Component")
                && javaFeatureVersion() >= 17;
        return new ServerVersion(Bukkit.getName() + " " + bukkitVersion, v[0], v[1], v[2], dataVersion, folia, regions);
    }

    /**
     * Reads a Bukkit version such as "1.8.8-R0.1-SNAPSHOT" or "26.1-R0.1-SNAPSHOT".
     *
     * @param bukkitVersion the version
     * @return major, minor and patch; 0s for parts that are missing
     */
    static int[] parse(String bukkitVersion) {
        Matcher m = VERSION.matcher(bukkitVersion == null ? "" : bukkitVersion.trim());
        if (!m.find()) {
            return new int[] {0, 0, 0};
        }
        return new int[] {
                Integer.parseInt(m.group(1)),
                Integer.parseInt(m.group(2)),
                m.group(3) == null ? 0 : Integer.parseInt(m.group(3))};
    }

    /**
     * The data version of a Minecraft release, for servers that do not report
     * theirs (before 1.13.2). From 1.13.2 on servers report it, and
     * this is only a fallback.
     *
     * @param major major version (1)
     * @param minor minor version
     * @param patch patch version
     * @return the data version
     */
    static int knownDataVersion(int major, int minor, int patch) {
        if (major != 1 || minor <= 8) {
            return major > 1 ? Integer.MAX_VALUE / 2 : DATA_VERSION_1_8;
        }
        switch (minor) {
            case 9:
                return patch >= 4 ? 184 : patch == 3 ? 183 : patch == 2 ? 176 : patch == 1 ? 175 : 169;
            case 10:
                return patch >= 2 ? 512 : patch == 1 ? 511 : 510;
            case 11:
                return patch >= 2 ? 922 : patch == 1 ? 921 : 819;
            case 12:
                return patch >= 2 ? 1343 : patch == 1 ? 1241 : 1139;
            case 13:
                return patch >= 2 ? 1631 : patch == 1 ? 1628 : 1519;
            default:
                // Newer servers report their data version; this is a last
                // resort that sorts after every release above.
                return 1631 + minor;
        }
    }

    @SuppressWarnings("deprecation") // getUnsafe has no replacement for the data version
    private static Integer reportedDataVersion() {
        try {
            Object unsafe = Bukkit.getUnsafe();
            return (Integer) unsafe.getClass().getMethod("getDataVersion").invoke(unsafe);
        } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
            return null;
        }
    }

    static boolean classExists(String name) {
        try {
            Class.forName(name, false, ServerVersion.class.getClassLoader());
            return true;
        } catch (ClassNotFoundException | LinkageError e) {
            return false;
        }
    }

    private static boolean hasMethod(Class<?> type, String name, Class<?>... params) {
        try {
            type.getMethod(name, params);
            return true;
        } catch (NoSuchMethodException | LinkageError e) {
            return false;
        }
    }

    /**
     * The running Java's feature version: 8 for Java 8, 17 for Java 17.
     *
     * @return the version
     */
    static int javaFeatureVersion() {
        String spec = System.getProperty("java.specification.version", "1.8");
        try {
            return spec.startsWith("1.") ? Integer.parseInt(spec.substring(2)) : Integer.parseInt(spec);
        } catch (NumberFormatException e) {
            return 8;
        }
    }

    /** @return for example "Paper 1.21.11-R0.1-SNAPSHOT" */
    public String name() {
        return name;
    }

    /** @return the major version: 1 for 1.8.8, 26 for 26.1 */
    public int major() {
        return major;
    }

    /** @return the minor version: 8 for 1.8.8 */
    public int minor() {
        return minor;
    }

    /** @return the patch version: 8 for 1.8.8 */
    public int patch() {
        return patch;
    }

    /** @return the data version of this server's items and worlds */
    public int dataVersion() {
        return dataVersion;
    }

    /** @return whether this is Folia */
    public boolean isFolia() {
        return folia;
    }

    /** @return whether Paper's region schedulers can be used (Paper 1.20+ and Folia, on Java 17+) */
    public boolean hasRegionSchedulers() {
        return regionSchedulers;
    }

    /** @return whether this is Minecraft 1.12 or older: items have numeric data values, not flattened ids */
    public boolean beforeFlattening() {
        return major == 1 && minor <= 12;
    }

    @Override
    public String toString() {
        return name + " (data version " + dataVersion + (folia ? ", Folia" : "") + ", Java " + javaFeatureVersion() + ")";
    }
}
