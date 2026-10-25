// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.schema;

import com.google.protobuf.ByteString;
import com.leonsdb.proto.v1.FieldType;
import com.leonsdb.proto.v1.Value;
import com.leonsdb.proto.v1.ValueList;
import com.leonsdb.proto.v1.ValueMap;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * The set of Java types LeonsDB can store, and how. Platforms add their own
 * types (for example Minecraft items) with {@link #register}.
 */
public final class Codecs {

    private final Map<Class<?>, TypeCodec<?>> byClass = new ConcurrentHashMap<>();

    private Codecs() {
    }

    /**
     * Creates a registry with all built-in types: int, long, double,
     * boolean (and their boxed forms), String, UUID, Instant, byte[], enums,
     * and Lists and Maps (with String keys) of supported types.
     *
     * @return a new registry
     */
    public static Codecs withDefaults() {
        Codecs c = new Codecs();
        TypeCodec<Integer> intCodec = simple(FieldType.FIELD_TYPE_INT, Value.KindCase.INT_VALUE,
                v -> Value.newBuilder().setIntValue(v).build(), Value::getIntValue);
        TypeCodec<Long> longCodec = simple(FieldType.FIELD_TYPE_LONG, Value.KindCase.LONG_VALUE,
                v -> Value.newBuilder().setLongValue(v).build(), Value::getLongValue);
        TypeCodec<Double> doubleCodec = simple(FieldType.FIELD_TYPE_DOUBLE, Value.KindCase.DOUBLE_VALUE,
                v -> {
                    if (v.isNaN() || v.isInfinite()) {
                        throw new IllegalArgumentException("NaN and infinite numbers cannot be stored");
                    }
                    return Value.newBuilder().setDoubleValue(v).build();
                }, Value::getDoubleValue);
        TypeCodec<Boolean> boolCodec = simple(FieldType.FIELD_TYPE_BOOL, Value.KindCase.BOOL_VALUE,
                v -> Value.newBuilder().setBoolValue(v).build(), Value::getBoolValue);

        c.register(int.class, intCodec);
        c.register(Integer.class, intCodec);
        c.register(long.class, longCodec);
        c.register(Long.class, longCodec);
        c.register(double.class, doubleCodec);
        c.register(Double.class, doubleCodec);
        c.register(boolean.class, boolCodec);
        c.register(Boolean.class, boolCodec);
        c.register(String.class, simple(FieldType.FIELD_TYPE_STRING, Value.KindCase.STRING_VALUE,
                v -> Value.newBuilder().setStringValue(v).build(), Value::getStringValue));
        c.register(UUID.class, simple(FieldType.FIELD_TYPE_UUID, Value.KindCase.UUID_VALUE,
                Codecs::encodeUuid, Codecs::decodeUuid));
        c.register(Instant.class, simple(FieldType.FIELD_TYPE_TIMESTAMP, Value.KindCase.TIMESTAMP_VALUE,
                v -> Value.newBuilder().setTimestampValue(v.toEpochMilli()).build(),
                v -> Instant.ofEpochMilli(v.getTimestampValue())));
        c.register(byte[].class, simple(FieldType.FIELD_TYPE_BYTES, Value.KindCase.BYTES_VALUE,
                v -> Value.newBuilder().setBytesValue(ByteString.copyFrom(v)).build(),
                v -> v.getBytesValue().toByteArray()));
        return c;
    }

    /**
     * Adds or replaces the codec for a class.
     *
     * @param type  the Java class
     * @param codec how to store it
     * @param <T>   the Java type
     */
    public <T> void register(Class<T> type, TypeCodec<T> codec) {
        byClass.put(Objects.requireNonNull(type, "type"), Objects.requireNonNull(codec, "codec"));
    }

    /**
     * Returns the codec for a (possibly generic) Java type.
     *
     * @param type the field's declared type
     * @return the codec
     * @throws IllegalArgumentException if the type cannot be stored
     */
    public TypeCodec<?> forType(Type type) {
        if (type instanceof Class<?> cls) {
            TypeCodec<?> codec = byClass.get(cls);
            if (codec != null) {
                return codec;
            }
            if (cls.isEnum()) {
                return enumCodec(cls);
            }
            if (List.class.isAssignableFrom(cls) || Map.class.isAssignableFrom(cls)) {
                throw new IllegalArgumentException(cls.getSimpleName()
                        + " needs its element type, for example List<String> or Map<String, Integer>");
            }
            throw new IllegalArgumentException(unsupported(cls));
        }
        if (type instanceof ParameterizedType p && p.getRawType() instanceof Class<?> raw) {
            Type[] args = p.getActualTypeArguments();
            if (raw == List.class) {
                return new ListCodec<>(forType(args[0]));
            }
            if (raw == Map.class) {
                if (args[0] != String.class) {
                    throw new IllegalArgumentException("Map keys must be String, got " + args[0].getTypeName());
                }
                return new MapCodec<>(forType(args[1]));
            }
            throw new IllegalArgumentException("only List<...> and Map<String, ...> are supported, not "
                    + type.getTypeName() + " (declare the field as List or Map, not a concrete class)");
        }
        throw new IllegalArgumentException("cannot store values of type " + type.getTypeName());
    }

    private static String unsupported(Class<?> cls) {
        if (cls == float.class || cls == Float.class) {
            return "float is not supported; use double";
        }
        if (cls == short.class || cls == Short.class || cls == byte.class || cls == Byte.class) {
            return cls.getSimpleName() + " is not supported; use int";
        }
        if (cls == char.class || cls == Character.class) {
            return "char is not supported; use String";
        }
        return "cannot store values of type " + cls.getName();
    }

    // --- Built-in codecs -----------------------------------------------------

    private static <T> TypeCodec<T> simple(FieldType fieldType, Value.KindCase kind,
                                           Function<T, Value> encode, Function<Value, T> decode) {
        return new TypeCodec<>() {
            @Override
            public FieldType fieldType() {
                return fieldType;
            }

            @Override
            public Value encode(T value) {
                return encode.apply(value);
            }

            @Override
            public T decode(Value value) {
                if (value.getKindCase() != kind) {
                    throw new IllegalArgumentException("expected " + kind + " but found " + value.getKindCase());
                }
                return decode.apply(value);
            }
        };
    }

    private static Value encodeUuid(UUID u) {
        ByteBuffer b = ByteBuffer.allocate(16);
        b.putLong(u.getMostSignificantBits()).putLong(u.getLeastSignificantBits());
        return Value.newBuilder().setUuidValue(ByteString.copyFrom(b.array())).build();
    }

    private static UUID decodeUuid(Value v) {
        ByteBuffer b = v.getUuidValue().asReadOnlyByteBuffer();
        if (b.remaining() != 16) {
            throw new IllegalArgumentException("UUID must be 16 bytes");
        }
        return new UUID(b.getLong(), b.getLong());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static TypeCodec<?> enumCodec(Class<?> cls) {
        Class<? extends Enum> enumType = (Class<? extends Enum>) cls;
        return new TypeCodec<Enum<?>>() {
            @Override
            public FieldType fieldType() {
                return FieldType.FIELD_TYPE_STRING;
            }

            @Override
            public Value encode(Enum<?> value) {
                return Value.newBuilder().setStringValue(value.name()).build();
            }

            @Override
            public Enum<?> decode(Value value) {
                if (value.getKindCase() != Value.KindCase.STRING_VALUE) {
                    throw new IllegalArgumentException("expected an enum name but found " + value.getKindCase());
                }
                // Throws IllegalArgumentException for names this version of
                // the plugin does not know; the field is then left untouched.
                return Enum.valueOf(enumType, value.getStringValue());
            }
        };
    }

    /** Lists are stored whole; elements must not be null. */
    private record ListCodec<E>(TypeCodec<E> element) implements TypeCodec<List<E>> {
        @Override
        public FieldType fieldType() {
            return FieldType.FIELD_TYPE_LIST;
        }

        @Override
        public Value encode(List<E> value) {
            ValueList.Builder b = ValueList.newBuilder();
            for (E e : value) {
                if (e == null) {
                    throw new IllegalArgumentException("lists cannot contain null");
                }
                b.addValues(element.encode(e));
            }
            return Value.newBuilder().setListValue(b).build();
        }

        @Override
        public List<E> decode(Value value) {
            if (value.getKindCase() != Value.KindCase.LIST_VALUE) {
                throw new IllegalArgumentException("expected LIST_VALUE but found " + value.getKindCase());
            }
            List<E> out = new ArrayList<>(value.getListValue().getValuesCount());
            for (Value v : value.getListValue().getValuesList()) {
                out.add(element.decode(v));
            }
            return out;
        }
    }

    /** Maps have String keys and are stored whole; values must not be null. */
    private record MapCodec<V>(TypeCodec<V> valueCodec) implements TypeCodec<Map<String, V>> {
        @Override
        public FieldType fieldType() {
            return FieldType.FIELD_TYPE_MAP;
        }

        @Override
        public Value encode(Map<String, V> value) {
            ValueMap.Builder b = ValueMap.newBuilder();
            for (Map.Entry<String, V> e : value.entrySet()) {
                if (e.getKey() == null || e.getValue() == null) {
                    throw new IllegalArgumentException("maps cannot contain null keys or values");
                }
                b.putEntries(e.getKey(), valueCodec.encode(e.getValue()));
            }
            return Value.newBuilder().setMapValue(b).build();
        }

        @Override
        public Map<String, V> decode(Value value) {
            if (value.getKindCase() != Value.KindCase.MAP_VALUE) {
                throw new IllegalArgumentException("expected MAP_VALUE but found " + value.getKindCase());
            }
            Map<String, V> out = new HashMap<>();
            value.getMapValue().getEntriesMap().forEach((k, v) -> out.put(k, valueCodec.decode(v)));
            return out;
        }
    }
}
