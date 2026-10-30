// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Says an {@code int} or {@code long} field is a duration, and in which
 * unit. Nothing changes in how it is stored; tools that show your data,
 * such as the LeonsDB dashboard, show it as a duration ("7h 0m") instead
 * of a bare number, which could mean milliseconds as well as ticks.
 *
 * <pre>{@code
 * @Duration(Duration.Unit.TICKS) public long playtime;
 * @Duration(Duration.Unit.SECONDS) public int muteLeft;
 * }</pre>
 *
 * <p>In a class that also uses {@code java.time.Duration}, write this one
 * as {@code @com.leonsdb.connector.api.Duration}.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Duration {

    /** @return what one unit of the field is */
    Unit value();

    /** What one unit of a duration field is. */
    enum Unit {
        /** Milliseconds. */
        MILLIS,
        /** Seconds. */
        SECONDS,
        /** Minecraft server ticks, 50 ms each. */
        TICKS
    }
}
