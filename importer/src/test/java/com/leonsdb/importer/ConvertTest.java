// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.importer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.leonsdb.proto.v1.FieldType;
import com.leonsdb.proto.v1.Value;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ConvertTest {

    private static Value v(Object o, FieldType t) throws Convert.Refused {
        return Convert.value(o, t, null);
    }

    private static String refused(Object o, FieldType t, String as) {
        return assertThrows(Convert.Refused.class, () -> Convert.value(o, t, as)).getMessage();
    }

    @Test
    void numbersMustFitExactly() throws Exception {
        assertEquals(7, v(7L, FieldType.FIELD_TYPE_INT).getIntValue());
        assertEquals(7, v(new BigDecimal("7.000"), FieldType.FIELD_TYPE_INT).getIntValue());
        assertEquals(7, v(" 7 ", FieldType.FIELD_TYPE_INT).getIntValue());
        assertEquals(7, v(7.0, FieldType.FIELD_TYPE_INT).getIntValue());
        assertTrue(refused(12.5, FieldType.FIELD_TYPE_INT, null).contains("decimals"));
        assertTrue(refused(3_000_000_000L, FieldType.FIELD_TYPE_INT, null).contains("too large"));
        assertTrue(refused("abc", FieldType.FIELD_TYPE_LONG, null).contains("not a long"));
        assertTrue(refused(true, FieldType.FIELD_TYPE_LONG, null).contains("not a long"));
        assertEquals(Long.MAX_VALUE, v(BigInteger.valueOf(Long.MAX_VALUE), FieldType.FIELD_TYPE_LONG).getLongValue());
        assertTrue(refused(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE), FieldType.FIELD_TYPE_LONG, null).contains("too large"));
        assertEquals(1.5, v("1.5", FieldType.FIELD_TYPE_DOUBLE).getDoubleValue());
        assertTrue(refused(Double.NaN, FieldType.FIELD_TYPE_DOUBLE, null).contains("finite"));
    }

    @Test
    void booleansAndText() throws Exception {
        assertTrue(v(1, FieldType.FIELD_TYPE_BOOL).getBoolValue());
        assertFalse(v("no", FieldType.FIELD_TYPE_BOOL).getBoolValue());
        assertTrue(v("TRUE", FieldType.FIELD_TYPE_BOOL).getBoolValue());
        assertTrue(refused(2, FieldType.FIELD_TYPE_BOOL, null).contains("true or false"));
        assertEquals("12", v(12, FieldType.FIELD_TYPE_STRING).getStringValue());
        assertEquals("0.10", v(new BigDecimal("0.10"), FieldType.FIELD_TYPE_STRING).getStringValue());
        assertTrue(refused(List.of(1), FieldType.FIELD_TYPE_STRING, null).contains("not text"));
    }

    @Test
    void uuidsInEveryCommonForm() throws Exception {
        UUID u = UUID.fromString("0f3a2c1e-6b7d-4e8f-9a0b-1c2d3e4f5a6b");
        byte[] want = Convert.uuidBytes(u, null);
        assertArrayEquals(want, Convert.uuidBytes("0f3a2c1e-6b7d-4e8f-9a0b-1c2d3e4f5a6b", null));
        assertArrayEquals(want, Convert.uuidBytes("0F3A2C1E6B7D4E8F9A0B1C2D3E4F5A6B", null));
        assertArrayEquals(want, Convert.uuidBytes(want.clone(), null));
        byte[] legacy = new byte[16];
        for (int i = 0; i < 8; i++) {
            legacy[i] = want[7 - i];
            legacy[8 + i] = want[15 - i];
        }
        assertArrayEquals(want, Convert.uuidBytes(legacy, "java-legacy-uuid"));
        assertThrows(Convert.Refused.class, () -> Convert.uuidBytes("Steve", null));
        assertThrows(Convert.Refused.class, () -> Convert.uuidBytes(new byte[15], null));
        assertEquals(16, v(u.toString(), FieldType.FIELD_TYPE_UUID).getUuidValue().size());
    }

    @Test
    void offlineUuidsMatchMinecraft() throws Exception {
        // The UUID an offline-mode server gives "Notch".
        assertEquals("b50ad385-829d-3141-a216-7e7d7539ba7f", Importer.uuidString(Convert.offlineUuid("Notch")));
        assertThrows(Convert.Refused.class, () -> Convert.offlineUuid("not a name!"));
    }

    @Test
    void timesNeedToSayTheirUnitWhenTheyAreNumbers() throws Exception {
        long t = Instant.parse("2026-10-29T12:00:00Z").toEpochMilli();
        assertEquals(t, v(Instant.ofEpochMilli(t), FieldType.FIELD_TYPE_TIMESTAMP).getTimestampValue());
        assertEquals(t, v(java.sql.Timestamp.from(Instant.ofEpochMilli(t)), FieldType.FIELD_TYPE_TIMESTAMP).getTimestampValue());
        assertEquals(t, v(LocalDateTime.parse("2026-10-29T12:00:00"), FieldType.FIELD_TYPE_TIMESTAMP).getTimestampValue());
        assertEquals(t, v("2026-10-29T12:00:00Z", FieldType.FIELD_TYPE_TIMESTAMP).getTimestampValue());
        assertEquals(t, v("2026-10-29 12:00:00", FieldType.FIELD_TYPE_TIMESTAMP).getTimestampValue());
        assertEquals(t, Convert.value(t / 1000, FieldType.FIELD_TYPE_TIMESTAMP, "epoch-seconds").getTimestampValue());
        assertEquals(t, Convert.value(t, FieldType.FIELD_TYPE_TIMESTAMP, "epoch-millis").getTimestampValue());
        assertTrue(refused(t, FieldType.FIELD_TYPE_TIMESTAMP, null).contains("as: epoch-seconds or as: epoch-millis"));
        assertTrue(refused("yesterday", FieldType.FIELD_TYPE_TIMESTAMP, null).contains("ISO 8601"));
    }

    @Test
    void bytesListsAndMaps() throws Exception {
        assertEquals(3, Convert.value("AQID", FieldType.FIELD_TYPE_BYTES, "base64").getBytesValue().size());
        assertEquals(2, Convert.value("cafe", FieldType.FIELD_TYPE_BYTES, "hex").getBytesValue().size());
        assertTrue(refused("cafe", FieldType.FIELD_TYPE_BYTES, null).contains("base64"));
        Value l = v(List.of("a", 1, 2L, 1.5, true), FieldType.FIELD_TYPE_LIST);
        assertEquals(5, l.getListValue().getValuesCount());
        assertEquals(Value.KindCase.INT_VALUE, l.getListValue().getValues(1).getKindCase());
        Value m = v(Map.of("home", Map.of("x", 1.5)), FieldType.FIELD_TYPE_MAP);
        assertEquals(1.5, m.getMapValue().getEntriesOrThrow("home").getMapValue().getEntriesOrThrow("x").getDoubleValue());
        java.util.List<Object> withNull = new java.util.ArrayList<>();
        withNull.add(null);
        assertTrue(refused(withNull, FieldType.FIELD_TYPE_LIST, null).contains("null"));
        assertTrue(refused(Map.of(1, 2), FieldType.FIELD_TYPE_MAP, null).contains("keys must be text"));
    }

    @Test
    void minecraftObjectsCannotBeImported() {
        assertFalse(Convert.importable(FieldType.FIELD_TYPE_ITEM));
        assertFalse(Convert.importable(FieldType.FIELD_TYPE_INVENTORY));
        assertFalse(Convert.importable(FieldType.FIELD_TYPE_LOCATION));
        assertTrue(Convert.importable(FieldType.FIELD_TYPE_LONG));
    }

    @Test
    void ids() throws Exception {
        assertEquals("42", Convert.id(42));
        assertEquals("red", Convert.id("red"));
        assertThrows(Convert.Refused.class, () -> Convert.id(""));
        assertThrows(Convert.Refused.class, () -> Convert.id("x".repeat(513)));
    }
}
