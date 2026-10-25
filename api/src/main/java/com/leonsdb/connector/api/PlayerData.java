// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class as a player collection: one record per player UUID, loaded
 * and cached as a single unit while the player is online.
 *
 * <pre>{@code
 * @PlayerData(namespace = "practicepvp", collection = "stats")
 * public class Stats {
 *     @Indexed public int kills;
 *     public int deaths;
 *     @Transactional public double coins;
 * }
 * }</pre>
 *
 * <p>Both names must follow the rules in {@link Names}.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface PlayerData {

    /** The namespace, normally the plugin's name, for example {@code "practicepvp"}. */
    String namespace();

    /** The collection inside the namespace, for example {@code "stats"}. */
    String collection();
}
