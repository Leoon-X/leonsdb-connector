// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.importer;

import com.google.protobuf.ByteString;
import com.leonsdb.proto.v1.FieldType;
import com.leonsdb.proto.v1.Value;
import com.leonsdb.proto.v1.ValueList;
import com.leonsdb.proto.v1.ValueMap;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.Date;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

/**
 * Turns source values into LeonsDB values of the type a field is declared
 * with. A value that does not fit is refused with the reason, never
 * guessed at: 12.5 is not an int, and a number is only a timestamp when the
 * plan says whether it counts seconds or milliseconds.
 */
final class Convert {

    private Convert() {
    }

    /** A value that cannot be used; the message says why. */
    static final class Refused extends Exception {
        private static final long serialVersionUID = 1L;

        Refused(String message) {
            super(message, null, false, false);
        }
    }

    /**
     * @param v    the source value, not null
     * @param type the field's declared type
     * @param as   the plan's hint, or null
     * @return the LeonsDB value
     * @throws Refused if it does not fit
     */
    static Value value(Object v, FieldType type, String as) throws Refused {
        v = plain(v);
        return switch (type) {
            case FIELD_TYPE_INT -> {
                long l = integral(v, "an int");
                if (l != (int) l) {
                    throw new Refused(v + " is too large for an int field");
                }
                yield Value.newBuilder().setIntValue((int) l).build();
            }
            case FIELD_TYPE_LONG -> Value.newBuilder().setLongValue(integral(v, "a long")).build();
            case FIELD_TYPE_DOUBLE -> {
                double d;
                if (v instanceof Number n) {
                    d = n.doubleValue();
                } else if (v instanceof String s) {
                    try {
                        d = Double.parseDouble(s.trim());
                    } catch (NumberFormatException e) {
                        throw new Refused("\"" + s + "\" is not a number");
                    }
                } else {
                    throw new Refused(describe(v) + " is not a number");
                }
                if (!Double.isFinite(d)) {
                    throw new Refused(v + " is not a finite number");
                }
                yield Value.newBuilder().setDoubleValue(d).build();
            }
            case FIELD_TYPE_BOOL -> Value.newBuilder().setBoolValue(bool(v)).build();
            case FIELD_TYPE_STRING -> {
                if (v instanceof String s) {
                    yield Value.newBuilder().setStringValue(s).build();
                }
                if (v instanceof BigDecimal b) {
                    yield Value.newBuilder().setStringValue(b.toPlainString()).build();
                }
                if (v instanceof Number || v instanceof Boolean || v instanceof UUID) {
                    yield Value.newBuilder().setStringValue(v.toString()).build();
                }
                throw new Refused(describe(v) + " is not text");
            }
            case FIELD_TYPE_UUID -> Value.newBuilder().setUuidValue(ByteString.copyFrom(uuidBytes(v, as))).build();
            case FIELD_TYPE_TIMESTAMP -> Value.newBuilder().setTimestampValue(millis(v, as)).build();
            case FIELD_TYPE_BYTES -> Value.newBuilder().setBytesValue(ByteString.copyFrom(bytes(v, as))).build();
            case FIELD_TYPE_LIST -> {
                if (!(v instanceof List<?> l)) {
                    throw new Refused(describe(v) + " is not a list");
                }
                yield Value.newBuilder().setListValue(list(l)).build();
            }
            case FIELD_TYPE_MAP -> {
                if (!(v instanceof Map<?, ?> m)) {
                    throw new Refused(describe(v) + " is not a map");
                }
                yield Value.newBuilder().setMapValue(map(m)).build();
            }
            default -> throw new Refused("fields of type " + type + " cannot be imported");
        };
    }

    /** Whether fields of a type can be imported at all. */
    static boolean importable(FieldType type) {
        return switch (type) {
            case FIELD_TYPE_ITEM, FIELD_TYPE_LOCATION, FIELD_TYPE_INVENTORY, FIELD_TYPE_UNSPECIFIED, UNRECOGNIZED -> false;
            default -> true;
        };
    }

    /**
     * A player's UUID key.
     *
     * @param v the source value
     * @param as the plan's hint, or null
     * @return the 16 bytes
     * @throws Refused if it is not a UUID
     */
    static byte[] uuidBytes(Object v, String as) throws Refused {
        v = plain(v);
        if (v instanceof UUID u) {
            return bytes(u);
        }
        if (v instanceof byte[] b) {
            if (b.length != 16) {
                throw new Refused(b.length + " bytes are not a UUID (16 bytes)");
            }
            if ("java-legacy-uuid".equals(as)) {
                // The old MongoDB Java driver stored each half reversed.
                byte[] out = new byte[16];
                for (int i = 0; i < 8; i++) {
                    out[i] = b[7 - i];
                    out[8 + i] = b[15 - i];
                }
                return out;
            }
            return b.clone();
        }
        if (v instanceof String s) {
            String t = s.trim();
            if (t.matches("(?i)[0-9a-f]{32}")) {
                t = t.substring(0, 8) + "-" + t.substring(8, 12) + "-" + t.substring(12, 16) + "-" + t.substring(16, 20)
                        + "-" + t.substring(20);
            }
            if (t.matches("(?i)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")) {
                return bytes(UUID.fromString(t));
            }
            throw new Refused("\"" + s + "\" is not a UUID");
        }
        throw new Refused(describe(v) + " is not a UUID");
    }

    /**
     * The UUID an offline-mode server gives a player name.
     *
     * @param v the name
     * @return the 16 bytes
     * @throws Refused if it is not a player name
     */
    static byte[] offlineUuid(Object v) throws Refused {
        if (!(v instanceof String name) || !name.matches("[A-Za-z0-9_]{1,16}")) {
            throw new Refused(describe(v) + " is not a player name");
        }
        try {
            byte[] h = MessageDigest.getInstance("MD5").digest(("OfflinePlayer:" + name).getBytes(StandardCharsets.UTF_8));
            h[6] = (byte) ((h[6] & 0x0f) | 0x30);
            h[8] = (byte) ((h[8] & 0x3f) | 0x80);
            return h;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * A string id for a global collection.
     *
     * @param v the source value
     * @return the id
     * @throws Refused if it is not usable as an id
     */
    static String id(Object v) throws Refused {
        v = plain(v);
        String s;
        if (v instanceof String str) {
            s = str;
        } else if (v instanceof Number || v instanceof UUID) {
            s = v instanceof BigDecimal b ? b.toPlainString() : v.toString();
        } else {
            throw new Refused(describe(v) + " cannot be an id");
        }
        int len = s.getBytes(StandardCharsets.UTF_8).length;
        if (len == 0 || len > 512) {
            throw new Refused("an id must be 1 to 512 bytes, not " + len);
        }
        return s;
    }

    // --- details ---------------------------------------------------------------

    /** Driver-specific types as plain Java ones. */
    private static Object plain(Object v) {
        if (v instanceof org.bson.types.Decimal128 d) {
            return d.bigDecimalValue();
        }
        if (v instanceof org.bson.types.ObjectId id) {
            return id.toHexString();
        }
        if (v instanceof org.bson.types.Binary b) {
            return b.getData();
        }
        if (v instanceof java.sql.Timestamp t) {
            return t.toInstant();
        }
        if (v instanceof java.sql.Date d) {
            return d.toLocalDate();
        }
        if (v instanceof java.sql.Clob c) {
            try {
                return c.getSubString(1, (int) Math.min(Integer.MAX_VALUE, c.length()));
            } catch (java.sql.SQLException e) {
                return v;
            }
        }
        if (v instanceof java.sql.Blob b) {
            try {
                return b.getBytes(1, (int) Math.min(Integer.MAX_VALUE, b.length()));
            } catch (java.sql.SQLException e) {
                return v;
            }
        }
        if (v instanceof Date d) {
            return d.toInstant();
        }
        return v;
    }

    private static long integral(Object v, String what) throws Refused {
        if (v instanceof Integer || v instanceof Long || v instanceof Short || v instanceof Byte) {
            return ((Number) v).longValue();
        }
        BigDecimal b;
        if (v instanceof BigInteger i) {
            b = new BigDecimal(i);
        } else if (v instanceof BigDecimal d) {
            b = d;
        } else if (v instanceof Double || v instanceof Float) {
            double d = ((Number) v).doubleValue();
            if (!Double.isFinite(d)) {
                throw new Refused(v + " is not " + what);
            }
            b = BigDecimal.valueOf(d);
        } else if (v instanceof Boolean bool) {
            throw new Refused(bool + " is not " + what);
        } else if (v instanceof String s) {
            try {
                b = new BigDecimal(s.trim());
            } catch (NumberFormatException e) {
                throw new Refused("\"" + s + "\" is not " + what);
            }
        } else {
            throw new Refused(describe(v) + " is not " + what);
        }
        try {
            return b.stripTrailingZeros().longValueExact();
        } catch (ArithmeticException e) {
            throw new Refused(b.toPlainString() + " is not " + what + (b.scale() > 0 ? " (it has decimals)" : " (too large)"));
        }
    }

    private static boolean bool(Object v) throws Refused {
        if (v instanceof Boolean b) {
            return b;
        }
        if (v instanceof Number n && (n.doubleValue() == 0 || n.doubleValue() == 1)) {
            return n.doubleValue() == 1;
        }
        if (v instanceof String s) {
            switch (s.trim().toLowerCase(Locale.ROOT)) {
                case "true", "yes", "1", "on" -> { return true; }
                case "false", "no", "0", "off" -> { return false; }
                default -> { }
            }
        }
        throw new Refused(describe(v) + " is not true or false");
    }

    private static long millis(Object v, String as) throws Refused {
        if (v instanceof Instant i) {
            return i.toEpochMilli();
        }
        if (v instanceof OffsetDateTime o) {
            return o.toInstant().toEpochMilli();
        }
        if (v instanceof ZonedDateTime z) {
            return z.toInstant().toEpochMilli();
        }
        if (v instanceof LocalDateTime l) {
            return l.toInstant(ZoneOffset.UTC).toEpochMilli(); // stored without a zone: taken as UTC
        }
        if (v instanceof LocalDate d) {
            return d.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        }
        if (v instanceof Number || v instanceof String s && s.trim().matches("-?\\d+")) {
            long n = integral(v, "a number of seconds or milliseconds");
            if ("epoch-seconds".equals(as)) {
                try {
                    return Math.multiplyExact(n, 1000L);
                } catch (ArithmeticException e) {
                    throw new Refused(n + " seconds is out of range");
                }
            }
            if ("epoch-millis".equals(as)) {
                return n;
            }
            throw new Refused("the number " + n + " is only a time with as: epoch-seconds or as: epoch-millis in the plan");
        }
        if (v instanceof String s) {
            String t = s.trim();
            try {
                return OffsetDateTime.parse(t).toInstant().toEpochMilli();
            } catch (DateTimeParseException e) {
                // try without a zone
            }
            try {
                return LocalDateTime.parse(t.replace(' ', 'T')).toInstant(ZoneOffset.UTC).toEpochMilli();
            } catch (DateTimeParseException e) {
                // try a date
            }
            try {
                return LocalDate.parse(t).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
            } catch (DateTimeParseException e) {
                throw new Refused("\"" + s + "\" is not a date and time (use ISO 8601, like 2026-10-29T12:00:00Z)");
            }
        }
        throw new Refused(describe(v) + " is not a time");
    }

    private static byte[] bytes(Object v, String as) throws Refused {
        if (v instanceof byte[] b) {
            return b;
        }
        if (v instanceof String s) {
            if ("base64".equals(as)) {
                try {
                    return Base64.getDecoder().decode(s.trim());
                } catch (IllegalArgumentException e) {
                    throw new Refused("not Base64");
                }
            }
            if ("hex".equals(as)) {
                try {
                    return HexFormat.of().parseHex(s.trim());
                } catch (IllegalArgumentException e) {
                    throw new Refused("not hex");
                }
            }
            throw new Refused("text is only bytes with as: base64 or as: hex in the plan");
        }
        throw new Refused(describe(v) + " is not bytes");
    }

    private static ValueList list(List<?> l) throws Refused {
        ValueList.Builder b = ValueList.newBuilder();
        for (Object e : l) {
            b.addValues(generic(e));
        }
        return b.build();
    }

    private static ValueMap map(Map<?, ?> m) throws Refused {
        ValueMap.Builder b = ValueMap.newBuilder();
        for (var e : m.entrySet()) {
            if (!(e.getKey() instanceof String k)) {
                throw new Refused("map keys must be text, not " + describe(e.getKey()));
            }
            b.putEntries(k, generic(e.getValue()));
        }
        return b.build();
    }

    /** An element of a list or map, by its own type (lists and maps declare no element type). */
    private static Value generic(Object v) throws Refused {
        v = plain(v);
        if (v == null) {
            throw new Refused("lists and maps may not contain null");
        }
        if (v instanceof Integer || v instanceof Short || v instanceof Byte) {
            return Value.newBuilder().setIntValue(((Number) v).intValue()).build();
        }
        if (v instanceof Long l) {
            return Value.newBuilder().setLongValue(l).build();
        }
        if (v instanceof BigInteger || v instanceof BigDecimal) {
            BigDecimal b = v instanceof BigInteger i ? new BigDecimal(i) : (BigDecimal) v;
            try {
                return Value.newBuilder().setLongValue(b.stripTrailingZeros().longValueExact()).build();
            } catch (ArithmeticException e) {
                return value(b.doubleValue(), FieldType.FIELD_TYPE_DOUBLE, null);
            }
        }
        if (v instanceof Double || v instanceof Float) {
            return value(v, FieldType.FIELD_TYPE_DOUBLE, null);
        }
        if (v instanceof Boolean b) {
            return Value.newBuilder().setBoolValue(b).build();
        }
        if (v instanceof String s) {
            return Value.newBuilder().setStringValue(s).build();
        }
        if (v instanceof UUID u) {
            return Value.newBuilder().setUuidValue(ByteString.copyFrom(bytes(u))).build();
        }
        if (v instanceof Instant || v instanceof LocalDateTime || v instanceof OffsetDateTime || v instanceof LocalDate) {
            return Value.newBuilder().setTimestampValue(millis(v, null)).build();
        }
        if (v instanceof byte[] b) {
            return Value.newBuilder().setBytesValue(ByteString.copyFrom(b)).build();
        }
        if (v instanceof List<?> l) {
            return Value.newBuilder().setListValue(list(l)).build();
        }
        if (v instanceof Map<?, ?> m) {
            return Value.newBuilder().setMapValue(map(m)).build();
        }
        throw new Refused(describe(v) + " cannot be stored");
    }

    private static byte[] bytes(UUID u) {
        return ByteBuffer.allocate(16).putLong(u.getMostSignificantBits()).putLong(u.getLeastSignificantBits()).array();
    }

    static String describe(Object v) {
        if (v instanceof String s) {
            return "\"" + (s.length() > 40 ? s.substring(0, 40) + "..." : s) + "\"";
        }
        if (v instanceof byte[] b) {
            return b.length + " bytes";
        }
        if (v instanceof Map<?, ?>) {
            return "a map";
        }
        if (v instanceof List<?>) {
            return "a list";
        }
        return v + " (" + v.getClass().getSimpleName() + ")";
    }
}
