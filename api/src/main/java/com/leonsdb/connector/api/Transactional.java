// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a field whose changes must never be lost, typically money.
 *
 * <p>Normal fields are batched and merged on the Minecraft server and
 * written roughly once per second, so a crash of the Minecraft server can
 * lose the last second of changes. Changes to a {@code @Transactional} field
 * skip that buffer: they are sent to LeonsDB right away (still without
 * blocking the main thread) and only count as done once LeonsDB has stored
 * them durably.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Transactional {
}
