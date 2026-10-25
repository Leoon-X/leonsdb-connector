// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.schema;

import com.leonsdb.proto.v1.FieldType;
import com.leonsdb.proto.v1.Value;

/**
 * Converts between a Java type and a LeonsDB value.
 *
 * @param <T> the Java type
 */
public interface TypeCodec<T> {

    /**
     * The LeonsDB type values of this codec are stored as.
     *
     * @return the field type
     */
    FieldType fieldType();

    /**
     * Converts a Java value. Never called with null: null fields are stored
     * as absent.
     *
     * @param value the value
     * @return the encoded value
     * @throws IllegalArgumentException if the value cannot be stored
     */
    Value encode(T value);

    /**
     * Converts a stored value back.
     *
     * @param value the stored value
     * @return the Java value
     * @throws IllegalArgumentException if the stored value has the wrong type
     *                                  or cannot be represented
     */
    T decode(Value value);
}
