// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Keeps a numeric field in an always-sorted index on the server, which makes
 * top-N lists and "rank of player X" instant. Use it for leaderboard fields
 * such as kills, coins or playtime.
 *
 * <p>Every indexed field costs a little extra work on each write, so only
 * index fields that are actually ranked.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Indexed {
}
