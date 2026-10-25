// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.schema;

import com.leonsdb.connector.api.GlobalData;
import com.leonsdb.connector.api.Indexed;
import com.leonsdb.connector.api.Names;
import com.leonsdb.connector.api.PlayerData;
import com.leonsdb.connector.api.Transactional;
import com.leonsdb.proto.v1.CollectionKind;
import com.leonsdb.proto.v1.FieldSchema;
import com.leonsdb.proto.v1.FieldType;
import com.leonsdb.proto.v1.Schema;
import com.leonsdb.proto.v1.Value;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * How one annotated data class is stored: its collection and the codec of
 * every field. Built once per class with {@link #of}, which also validates
 * the class so mistakes are reported when the plugin starts, not when a
 * player joins.
 *
 * <p>Every non-static, non-transient field is stored under its Java name.
 * Mark a field {@code transient} to keep it out of the database.
 *
 * @param <T> the data class
 */
public final class ClassSchema<T> {

    private static final Pattern FIELD_NAME = Pattern.compile("[A-Za-z][A-Za-z0-9_]{0,63}");

    /** One stored field. */
    public record FieldBinding(String name, Field field, TypeCodec<Object> codec,
                               boolean indexed, boolean transactional) {

        /**
         * The LeonsDB type of this field.
         *
         * @return the field type
         */
        public FieldType type() {
            return codec.fieldType();
        }
    }

    private final Class<T> type;
    private final String namespace;
    private final String collection;
    private final CollectionKind kind;
    private final Constructor<T> constructor;
    private final List<FieldBinding> fields;
    private final boolean hasTransactional;

    private ClassSchema(Class<T> type, String namespace, String collection, CollectionKind kind,
                        Constructor<T> constructor, List<FieldBinding> fields) {
        this.type = type;
        this.namespace = namespace;
        this.collection = collection;
        this.kind = kind;
        this.constructor = constructor;
        this.fields = Collections.unmodifiableList(fields);
        this.hasTransactional = fields.stream().anyMatch(FieldBinding::transactional);
    }

    /**
     * Inspects and validates a data class.
     *
     * @param type   a class annotated with {@link PlayerData} or {@link GlobalData}
     * @param codecs the types that can be stored
     * @param <T>    the data class
     * @return its schema
     * @throws IllegalArgumentException with a precise explanation if the class
     *                                  cannot be used
     */
    public static <T> ClassSchema<T> of(Class<T> type, Codecs codecs) {
        String where = type.getName();
        PlayerData player = type.getAnnotation(PlayerData.class);
        GlobalData global = type.getAnnotation(GlobalData.class);
        if ((player == null) == (global == null)) {
            throw new IllegalArgumentException(where + " must be annotated with exactly one of @PlayerData or @GlobalData");
        }
        String namespace = player != null ? player.namespace() : global.namespace();
        String collection = player != null ? player.collection() : global.collection();
        Names.require("namespace", namespace);
        Names.require("collection", collection);
        CollectionKind kind = player != null ? CollectionKind.COLLECTION_KIND_PLAYER : CollectionKind.COLLECTION_KIND_GLOBAL;

        if (type.isInterface() || Modifier.isAbstract(type.getModifiers())) {
            throw new IllegalArgumentException(where + " must be a concrete class");
        }
        if (type.getEnclosingClass() != null && !Modifier.isStatic(type.getModifiers())) {
            throw new IllegalArgumentException(where + " is an inner class; declare it 'static'");
        }
        Constructor<T> constructor;
        try {
            constructor = type.getDeclaredConstructor();
            constructor.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(where + " needs a constructor without parameters", e);
        }

        List<FieldBinding> bindings = new ArrayList<>();
        Set<String> names = new HashSet<>();
        for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                int mod = f.getModifiers();
                if (Modifier.isStatic(mod) || Modifier.isTransient(mod) || f.isSynthetic()) {
                    continue;
                }
                String fieldWhere = where + "." + f.getName();
                if (Modifier.isFinal(mod)) {
                    throw new IllegalArgumentException(fieldWhere + " is final; stored fields must be "
                            + "assignable (or mark it 'transient' to skip it)");
                }
                if (!FIELD_NAME.matcher(f.getName()).matches()) {
                    throw new IllegalArgumentException(fieldWhere + ": field names must start with a letter and "
                            + "contain only letters, digits and underscores (at most 64)");
                }
                if (!names.add(f.getName())) {
                    throw new IllegalArgumentException(fieldWhere + " hides a field of the same name in a superclass");
                }
                TypeCodec<?> codec;
                try {
                    codec = codecs.forType(f.getGenericType());
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(fieldWhere + ": " + e.getMessage(), e);
                }
                boolean indexed = f.isAnnotationPresent(Indexed.class);
                if (indexed && !sortable(codec.fieldType())) {
                    throw new IllegalArgumentException(fieldWhere + ": @Indexed works on int, long, double and Instant fields only");
                }
                f.setAccessible(true);
                @SuppressWarnings("unchecked")
                TypeCodec<Object> objCodec = (TypeCodec<Object>) codec;
                bindings.add(new FieldBinding(f.getName(), f, objCodec, indexed, f.isAnnotationPresent(Transactional.class)));
            }
        }
        if (bindings.isEmpty()) {
            throw new IllegalArgumentException(where + " has no fields to store");
        }

        ClassSchema<T> schema = new ClassSchema<>(type, namespace, collection, kind, constructor, bindings);
        // Fail now, not on the first join, if the defaults cannot be stored.
        schema.read(schema.newInstance());
        return schema;
    }

    private static boolean sortable(FieldType t) {
        return t == FieldType.FIELD_TYPE_INT || t == FieldType.FIELD_TYPE_LONG
                || t == FieldType.FIELD_TYPE_DOUBLE || t == FieldType.FIELD_TYPE_TIMESTAMP;
    }

    /** @return the data class */
    public Class<T> type() {
        return type;
    }

    /** @return the namespace */
    public String namespace() {
        return namespace;
    }

    /** @return the collection */
    public String collection() {
        return collection;
    }

    /** @return the collection kind */
    public CollectionKind kind() {
        return kind;
    }

    /** @return true for player collections */
    public boolean isPlayerData() {
        return kind == CollectionKind.COLLECTION_KIND_PLAYER;
    }

    /** @return the stored fields in declaration order */
    public List<FieldBinding> fields() {
        return fields;
    }

    /** @return true if any field is {@code @Transactional} */
    public boolean hasTransactional() {
        return hasTransactional;
    }

    /**
     * The schema to declare to the server.
     *
     * @return the protobuf schema
     */
    public Schema toProto() {
        Schema.Builder b = Schema.newBuilder();
        for (FieldBinding f : fields) {
            b.addFields(FieldSchema.newBuilder()
                    .setName(f.name()).setType(f.type())
                    .setIndexed(f.indexed()).setTransactional(f.transactional()));
        }
        return b.build();
    }

    /**
     * Creates an object with the class's default values.
     *
     * @return a new instance
     */
    public T newInstance() {
        try {
            return constructor.newInstance();
        } catch (InvocationTargetException e) {
            throw new IllegalStateException("constructor of " + type.getName() + " failed", e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("cannot create " + type.getName(), e);
        }
    }

    /**
     * Encodes the current field values. Null fields are absent.
     *
     * @param obj the object
     * @return field name to value, in declaration order
     * @throws IllegalArgumentException if a value cannot be stored
     */
    public Map<String, Value> read(T obj) {
        Map<String, Value> out = new LinkedHashMap<>();
        for (FieldBinding f : fields) {
            Object v;
            try {
                v = f.field().get(obj);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
            if (v != null) {
                try {
                    out.put(f.name(), f.codec().encode(v));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(type.getSimpleName() + "." + f.name() + ": " + e.getMessage(), e);
                }
            }
        }
        return out;
    }

    /**
     * Copies stored values into an object. Fields the record does not have
     * keep their default values, which is how newly added fields get their
     * initial value. Stored fields this class no longer declares are ignored.
     *
     * @param obj    the object to fill
     * @param values the stored fields
     * @return names of fields whose stored value could not be read; they keep
     *         their defaults and must never be written back
     */
    public Set<String> write(T obj, Map<String, Value> values) {
        Set<String> unreadable = new HashSet<>();
        for (FieldBinding f : fields) {
            Value v = values.get(f.name());
            if (v == null) {
                continue;
            }
            try {
                f.field().set(obj, f.codec().decode(v));
            } catch (IllegalArgumentException e) {
                unreadable.add(f.name());
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }
        return unreadable;
    }

    /**
     * Creates an object holding exactly the given values: fields without a
     * value are null (or keep their default if they are primitives, which
     * always have a value).
     *
     * @param values a snapshot from {@link #read}
     * @return a new, independent object
     */
    public T copyOf(Map<String, Value> values) {
        T obj = newInstance();
        for (FieldBinding f : fields) {
            if (!f.field().getType().isPrimitive()) {
                writeField(obj, f.name(), null);
            }
        }
        write(obj, values);
        return obj;
    }

    /**
     * Sets one field.
     *
     * @param obj   the object
     * @param name  the field
     * @param value the encoded value, or null to clear the field
     * @throws IllegalArgumentException if the value cannot be decoded
     */
    public void writeField(T obj, String name, Value value) {
        for (FieldBinding f : fields) {
            if (f.name().equals(name)) {
                try {
                    f.field().set(obj, value == null ? null : f.codec().decode(value));
                } catch (IllegalAccessException e) {
                    throw new IllegalStateException(e);
                }
                return;
            }
        }
        throw new IllegalArgumentException(type.getSimpleName() + " has no field " + name);
    }

    /**
     * Restricts stored values to the fields this class declares.
     *
     * @param values stored fields
     * @return only the declared ones
     */
    public Map<String, Value> declaredOnly(Map<String, Value> values) {
        Map<String, Value> out = new HashMap<>();
        for (FieldBinding f : fields) {
            Value v = values.get(f.name());
            if (v != null) {
                out.put(f.name(), v);
            }
        }
        return out;
    }
}
