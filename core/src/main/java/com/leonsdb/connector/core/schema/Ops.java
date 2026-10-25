// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.schema;

import com.leonsdb.proto.v1.Comparison;
import com.leonsdb.proto.v1.Condition;
import com.leonsdb.proto.v1.Operation;
import com.leonsdb.proto.v1.Value;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Operations and conditions on encoded values, with the same rules as the
 * LeonsDB server. A Minecraft server uses them when another server's
 * transaction changes a player who plays here: it applies the change to
 * the player's live data, not to what is stored.
 */
public final class Ops {

    private Ops() {
    }

    /**
     * Applies operations in order.
     *
     * @param fields the current values; not modified
     * @param ops    the operations
     * @return the new values
     * @throws IllegalArgumentException if an operation cannot be applied
     *                                  (wrong type, overflow)
     */
    public static Map<String, Value> apply(Map<String, Value> fields, List<Operation> ops) {
        Map<String, Value> out = new HashMap<>(fields);
        for (Operation op : ops) {
            switch (op.getOpCase()) {
                case SET -> out.put(op.getField(), op.getSet());
                case UNSET -> out.remove(op.getField());
                case INCREMENT -> out.put(op.getField(), add(op.getField(), out.get(op.getField()), op.getIncrement()));
                default -> throw new IllegalArgumentException("operation on " + op.getField() + " is empty");
            }
        }
        return out;
    }

    private static Value add(String field, Value current, Value delta) {
        if (current != null && current.getKindCase() != delta.getKindCase()) {
            throw new IllegalArgumentException(field + " is not a number of the same type as the increment");
        }
        try {
            return switch (delta.getKindCase()) {
                case INT_VALUE -> Value.newBuilder().setIntValue(Math.addExact(current == null ? 0 : current.getIntValue(), delta.getIntValue())).build();
                case LONG_VALUE -> Value.newBuilder().setLongValue(Math.addExact(current == null ? 0 : current.getLongValue(), delta.getLongValue())).build();
                case DOUBLE_VALUE -> {
                    double v = (current == null ? 0 : current.getDoubleValue()) + delta.getDoubleValue();
                    if (!Double.isFinite(v)) {
                        throw new ArithmeticException("overflow");
                    }
                    yield Value.newBuilder().setDoubleValue(v).build();
                }
                default -> throw new IllegalArgumentException(field + ": only numbers can be incremented");
            };
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(field + " would overflow", e);
        }
    }

    /**
     * Whether a condition holds for a field's value. A field without a value
     * fails every comparison except MISSING; values that cannot be compared
     * are never equal and never ordered.
     *
     * @param c    the condition
     * @param have the field's value, or null
     * @return true if it holds
     */
    public static boolean holds(Condition c, Value have) {
        switch (c.getComparison()) {
            case COMPARISON_EXISTS:
                return have != null;
            case COMPARISON_MISSING:
                return have == null;
            default:
                break;
        }
        if (have == null) {
            return false;
        }
        Integer cmp = compare(have, c.getValue());
        return switch (c.getComparison()) {
            case COMPARISON_EQUAL -> cmp != null && cmp == 0;
            case COMPARISON_NOT_EQUAL -> cmp == null || cmp != 0;
            case COMPARISON_LESS -> cmp != null && cmp < 0;
            case COMPARISON_LESS_OR_EQUAL -> cmp != null && cmp <= 0;
            case COMPARISON_GREATER -> cmp != null && cmp > 0;
            case COMPARISON_GREATER_OR_EQUAL -> cmp != null && cmp >= 0;
            default -> false;
        };
    }

    /** Orders a and b, or returns null when they cannot be compared. */
    private static Integer compare(Value a, Value b) {
        BigDecimal x = number(a);
        BigDecimal y = number(b);
        if (x != null && y != null) {
            return x.compareTo(y);
        }
        if (a.getKindCase() != b.getKindCase()) {
            return null;
        }
        return switch (a.getKindCase()) {
            case TIMESTAMP_VALUE -> Long.compare(a.getTimestampValue(), b.getTimestampValue());
            case STRING_VALUE -> compareBytes(a.getStringValueBytes().toByteArray(), b.getStringValueBytes().toByteArray());
            case UUID_VALUE -> compareBytes(a.getUuidValue().toByteArray(), b.getUuidValue().toByteArray());
            default -> Objects.equals(a, b) ? 0 : null;
        };
    }

    private static BigDecimal number(Value v) {
        return switch (v.getKindCase()) {
            case INT_VALUE -> BigDecimal.valueOf(v.getIntValue());
            case LONG_VALUE -> BigDecimal.valueOf(v.getLongValue());
            case DOUBLE_VALUE -> new BigDecimal(v.getDoubleValue());
            default -> null;
        };
    }

    private static int compareBytes(byte[] a, byte[] b) {
        int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            int d = (a[i] & 0xff) - (b[i] & 0xff);
            if (d != 0) {
                return d;
            }
        }
        return Integer.compare(a.length, b.length);
    }

    /**
     * Renders a condition for messages, like the server does.
     *
     * @param c the condition
     * @return for example {@code coins >= 100}
     */
    public static String describe(Condition c) {
        String op = switch (c.getComparison()) {
            case COMPARISON_EQUAL -> "==";
            case COMPARISON_NOT_EQUAL -> "!=";
            case COMPARISON_LESS -> "<";
            case COMPARISON_LESS_OR_EQUAL -> "<=";
            case COMPARISON_GREATER -> ">";
            case COMPARISON_GREATER_OR_EQUAL -> ">=";
            case COMPARISON_EXISTS -> "exists";
            case COMPARISON_MISSING -> "missing";
            default -> "?";
        };
        if (c.getComparison() == Comparison.COMPARISON_EXISTS || c.getComparison() == Comparison.COMPARISON_MISSING) {
            return c.getField() + " " + op;
        }
        return c.getField() + " " + op + " " + text(c.getValue());
    }

    private static String text(Value v) {
        return switch (v.getKindCase()) {
            case INT_VALUE -> String.valueOf(v.getIntValue());
            case LONG_VALUE -> String.valueOf(v.getLongValue());
            case DOUBLE_VALUE -> String.valueOf(v.getDoubleValue());
            case BOOL_VALUE -> String.valueOf(v.getBoolValue());
            case STRING_VALUE -> "\"" + v.getStringValue() + "\"";
            case TIMESTAMP_VALUE -> java.time.Instant.ofEpochMilli(v.getTimestampValue()).toString();
            default -> "a " + v.getKindCase().name().toLowerCase(java.util.Locale.ROOT).replace("_value", "") + " value";
        };
    }
}
