// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.leonsdb.proto.v1.Comparison;
import com.leonsdb.proto.v1.Condition;
import com.leonsdb.proto.v1.Operation;
import com.leonsdb.proto.v1.Unset;
import com.leonsdb.proto.v1.Value;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OpsTest {

    private static Value i(int v) {
        return Value.newBuilder().setIntValue(v).build();
    }

    private static Value l(long v) {
        return Value.newBuilder().setLongValue(v).build();
    }

    private static Value d(double v) {
        return Value.newBuilder().setDoubleValue(v).build();
    }

    private static Value s(String v) {
        return Value.newBuilder().setStringValue(v).build();
    }

    private static Operation inc(String f, Value by) {
        return Operation.newBuilder().setField(f).setIncrement(by).build();
    }

    private static Condition cond(Comparison c, Value v) {
        var b = Condition.newBuilder().setField("f").setComparison(c);
        if (v != null) {
            b.setValue(v);
        }
        return b.build();
    }

    @Test
    void appliesOperationsInOrder() {
        var out = Ops.apply(Map.of("coins", l(10), "title", s("x")), List.of(
                inc("coins", l(5)),
                inc("kills", i(2)), // a missing field starts at zero
                Operation.newBuilder().setField("title").setUnset(Unset.getDefaultInstance()).build(),
                Operation.newBuilder().setField("rank").setSet(s("vip")).build(),
                inc("coins", l(-1))));
        assertEquals(Map.of("coins", l(14), "kills", i(2), "rank", s("vip")), out);
    }

    @Test
    void refusesWhatTheServerRefuses() {
        assertThrows(IllegalArgumentException.class, () -> Ops.apply(Map.of("n", i(Integer.MAX_VALUE)), List.of(inc("n", i(1)))), "overflow");
        assertThrows(IllegalArgumentException.class, () -> Ops.apply(Map.of("n", i(1)), List.of(inc("n", l(1)))), "int + long");
        assertThrows(IllegalArgumentException.class, () -> Ops.apply(Map.of("n", s("a")), List.of(inc("n", s("b")))), "not a number");
        assertThrows(IllegalArgumentException.class, () -> Ops.apply(Map.of("n", d(Double.MAX_VALUE)), List.of(inc("n", d(Double.MAX_VALUE)))), "infinite");
    }

    @Test
    void conditionsCompareLikeTheServer() {
        assertTrue(Ops.holds(cond(Comparison.COMPARISON_GREATER_OR_EQUAL, i(100)), i(100)));
        assertFalse(Ops.holds(cond(Comparison.COMPARISON_GREATER_OR_EQUAL, i(100)), i(99)));
        assertTrue(Ops.holds(cond(Comparison.COMPARISON_EQUAL, l(5)), i(5)), "int and long compare as numbers");
        assertTrue(Ops.holds(cond(Comparison.COMPARISON_GREATER, d(9.2e18)), l(Long.MAX_VALUE)), "exact, no rounding");
        assertFalse(Ops.holds(cond(Comparison.COMPARISON_EQUAL, d(Long.MAX_VALUE)), l(Long.MAX_VALUE)));
        assertTrue(Ops.holds(cond(Comparison.COMPARISON_LESS, i(1)), d(0.5)));
        assertTrue(Ops.holds(cond(Comparison.COMPARISON_GREATER, s("a")), s("b")));
        assertFalse(Ops.holds(cond(Comparison.COMPARISON_LESS, i(1)), s("b")), "different types never order");
        assertTrue(Ops.holds(cond(Comparison.COMPARISON_NOT_EQUAL, i(1)), s("1")));
        assertFalse(Ops.holds(cond(Comparison.COMPARISON_GREATER_OR_EQUAL, i(0)), null), "missing fails comparisons");
        assertFalse(Ops.holds(cond(Comparison.COMPARISON_NOT_EQUAL, i(0)), null));
        assertTrue(Ops.holds(cond(Comparison.COMPARISON_MISSING, null), null));
        assertTrue(Ops.holds(cond(Comparison.COMPARISON_EXISTS, null), i(0)));
        assertEquals("f >= 100", Ops.describe(cond(Comparison.COMPARISON_GREATER_OR_EQUAL, i(100))));
    }
}
