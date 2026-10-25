// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.schema;

import com.leonsdb.proto.v1.Operation;
import com.leonsdb.proto.v1.Unset;
import com.leonsdb.proto.v1.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Turns the difference between two snapshots of an object into operations.
 *
 * <p>Whole-number fields that changed become one increment of the total
 * difference, so 40 kills between two saves become a single "kills +40".
 * Everything else is sent as its new value. A field without a stored value
 * yet is always set, never incremented, so that defaults other than zero
 * stay correct.
 */
public final class Diff {

    private Diff() {
    }

    /**
     * Computes the operations that turn {@code base} into {@code current}.
     *
     * @param schema  the class
     * @param base    what the server has (absent = no stored value)
     * @param current the object's current values (absent = null)
     * @param skip    fields that must not be written
     * @return the operations, empty if nothing changed
     */
    public static List<Operation> compute(ClassSchema<?> schema, Map<String, Value> base,
                                          Map<String, Value> current, Set<String> skip) {
        List<Operation> ops = new ArrayList<>();
        for (ClassSchema.FieldBinding f : schema.fields()) {
            String name = f.name();
            if (skip.contains(name)) {
                continue;
            }
            Value b = base.get(name);
            Value c = current.get(name);
            if (Objects.equals(b, c)) {
                continue;
            }
            Operation.Builder op = Operation.newBuilder().setField(name);
            if (c == null) {
                ops.add(op.setUnset(Unset.getDefaultInstance()).build());
            } else if (b != null && b.getKindCase() == Value.KindCase.INT_VALUE && c.getKindCase() == Value.KindCase.INT_VALUE) {
                long delta = (long) c.getIntValue() - b.getIntValue();
                if (delta >= Integer.MIN_VALUE && delta <= Integer.MAX_VALUE) {
                    ops.add(op.setIncrement(Value.newBuilder().setIntValue((int) delta)).build());
                } else {
                    ops.add(op.setSet(c).build());
                }
            } else if (b != null && b.getKindCase() == Value.KindCase.LONG_VALUE && c.getKindCase() == Value.KindCase.LONG_VALUE) {
                try {
                    long delta = Math.subtractExact(c.getLongValue(), b.getLongValue());
                    ops.add(op.setIncrement(Value.newBuilder().setLongValue(delta)).build());
                } catch (ArithmeticException e) {
                    ops.add(op.setSet(c).build());
                }
            } else {
                // Doubles are set, not incremented: adding a floating-point
                // difference could leave the stored value one rounding step
                // away from what the plugin sees.
                ops.add(op.setSet(c).build());
            }
        }
        return ops;
    }
}
