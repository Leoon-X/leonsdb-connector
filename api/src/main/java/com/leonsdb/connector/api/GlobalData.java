// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class as a global collection: records are keyed by any string,
 * for example bans, clans or server settings.
 *
 * <p>Both names must follow the rules in {@link Names}.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface GlobalData {

    /** The namespace, normally the plugin's name. */
    String namespace();

    /** The collection inside the namespace. */
    String collection();
}
