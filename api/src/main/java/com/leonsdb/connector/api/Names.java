// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.util.regex.Pattern;

/**
 * Naming rules for namespaces, collections and fields. The server enforces
 * the same rules; checking them here gives a clear error at startup instead
 * of a rejected request later.
 *
 * <p>A valid name is 1 to 64 characters long, starts with a lowercase letter
 * and contains only lowercase letters, digits and underscores, for example
 * {@code practicepvp}, {@code player_stats} or {@code kills_2v2}.
 */
public final class Names {

    /** The longest name allowed. */
    public static final int MAX_LENGTH = 64;

    private static final Pattern VALID = Pattern.compile("[a-z][a-z0-9_]{0,63}");

    private Names() {
    }

    /**
     * Returns whether {@code name} follows the naming rules.
     *
     * @param name the name to check, may be null
     * @return true if the name is valid
     */
    public static boolean isValid(String name) {
        return name != null && VALID.matcher(name).matches();
    }

    /**
     * Checks a name and explains what is wrong with it.
     *
     * @param kind what the name is for, used in the error message (for example "namespace")
     * @param name the name to check
     * @return the name, unchanged
     * @throws IllegalArgumentException if the name is invalid
     */
    public static String require(String kind, String name) {
        if (isValid(name)) {
            return name;
        }
        String reason;
        if (name == null || name.isEmpty()) {
            reason = "it is empty";
        } else if (name.length() > MAX_LENGTH) {
            reason = "it is longer than " + MAX_LENGTH + " characters";
        } else if (!Character.isLetter(name.charAt(0)) || !Character.isLowerCase(name.charAt(0))) {
            reason = "it must start with a lowercase letter";
        } else {
            reason = "only lowercase letters, digits and underscores are allowed";
        }
        throw new IllegalArgumentException("invalid " + kind + " name \"" + name + "\": " + reason);
    }
}
