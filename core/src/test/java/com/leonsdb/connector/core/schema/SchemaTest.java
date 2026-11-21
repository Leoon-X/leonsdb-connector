// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.leonsdb.connector.api.Duration;
import com.leonsdb.connector.api.GlobalData;
import com.leonsdb.connector.api.Indexed;
import com.leonsdb.connector.api.PlayerData;
import com.leonsdb.connector.api.Transactional;
import com.leonsdb.proto.v1.CollectionKind;
import com.leonsdb.proto.v1.DurationUnit;
import com.leonsdb.proto.v1.FieldType;
import com.leonsdb.proto.v1.Operation;
import com.leonsdb.proto.v1.Value;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class SchemaTest {

    enum Rank { MEMBER, VIP }

    @PlayerData(namespace = "pvp", collection = "stats")
    static class Stats {
        @Indexed public int kills;
        @Duration(Duration.Unit.TICKS) public long playtime;
        @Transactional public double coins;
        public boolean muted;
        public String nickname;
        public UUID lastKiller;
        public Instant firstJoin;
        public byte[] blob;
        public Rank rank = Rank.MEMBER;
        public int level = 1;
        public List<String> friends = new ArrayList<>();
        public Map<String, List<Integer>> scores = new HashMap<>();
        transient int notStored;
        static int alsoNotStored;
    }

    private final Codecs codecs = Codecs.withDefaults();

    @Test
    void readsAnnotationsAndFields() {
        ClassSchema<Stats> s = ClassSchema.of(Stats.class, codecs);
        assertEquals("pvp", s.namespace());
        assertEquals("stats", s.collection());
        assertEquals(CollectionKind.COLLECTION_KIND_PLAYER, s.kind());
        assertEquals(12, s.fields().size(), "transient and static fields must be skipped");
        var proto = s.toProto();
        var kills = proto.getFieldsList().stream().filter(f -> f.getName().equals("kills")).findFirst().orElseThrow();
        assertTrue(kills.getIndexed());
        assertEquals(FieldType.FIELD_TYPE_INT, kills.getType());
        assertTrue(s.hasTransactional());
        var scores = proto.getFieldsList().stream().filter(f -> f.getName().equals("scores")).findFirst().orElseThrow();
        assertEquals(FieldType.FIELD_TYPE_MAP, scores.getType());
        var playtime = proto.getFieldsList().stream().filter(f -> f.getName().equals("playtime")).findFirst().orElseThrow();
        assertEquals(DurationUnit.DURATION_UNIT_TICKS, playtime.getDuration(), "@Duration is declared to LeonsDB");
        assertEquals(DurationUnit.DURATION_UNIT_UNSPECIFIED, kills.getDuration());
    }

    @Test
    void roundTripsEveryType() {
        ClassSchema<Stats> s = ClassSchema.of(Stats.class, codecs);
        Stats in = new Stats();
        in.kills = 40;
        in.playtime = 9_000_000_000L;
        in.coins = 12.5;
        in.muted = true;
        in.nickname = "Steve";
        in.lastKiller = UUID.randomUUID();
        in.firstJoin = Instant.ofEpochMilli(1_700_000_000_123L);
        in.blob = new byte[] {1, 2, 3};
        in.rank = Rank.VIP;
        in.friends.add("Alex");
        in.scores.put("duels", List.of(3, 1));

        Map<String, Value> stored = s.read(in);
        Stats out = s.newInstance();
        Set<String> unreadable = s.write(out, stored);

        assertTrue(unreadable.isEmpty());
        assertEquals(stored, s.read(out), "decode(encode(x)) must equal x");
        assertEquals(Rank.VIP, out.rank);
        assertEquals(in.lastKiller, out.lastKiller);
        assertEquals(List.of(3, 1), out.scores.get("duels"));
    }

    @Test
    void nullFieldsAreAbsentAndMissingFieldsKeepDefaults() {
        ClassSchema<Stats> s = ClassSchema.of(Stats.class, codecs);
        Stats fresh = s.newInstance();
        assertFalse(s.read(fresh).containsKey("nickname"));

        Stats loaded = s.newInstance();
        s.write(loaded, Map.of("kills", Value.newBuilder().setIntValue(7).build()));
        assertEquals(7, loaded.kills);
        assertEquals(1, loaded.level, "fields missing from the record keep their default");
    }

    @Test
    void unreadableValuesAreReportedNotGuessed() {
        ClassSchema<Stats> s = ClassSchema.of(Stats.class, codecs);
        Stats obj = s.newInstance();
        Set<String> bad = s.write(obj, Map.of(
                "rank", Value.newBuilder().setStringValue("LEGEND").build(), // unknown enum constant
                "kills", Value.newBuilder().setStringValue("oops").build())); // wrong type
        assertEquals(Set.of("rank", "kills"), bad);
        assertEquals(Rank.MEMBER, obj.rank);
    }

    @PlayerData(namespace = "x", collection = "y")
    static class HasFloat {
        public float speed;
    }

    @PlayerData(namespace = "x", collection = "y")
    static class HasFinal {
        public final int a = 1;
    }

    @PlayerData(namespace = "x", collection = "y")
    static class RawList {
        @SuppressWarnings("rawtypes")
        public List list;
    }

    @PlayerData(namespace = "x", collection = "y")
    static class DurationString {
        @Duration(Duration.Unit.SECONDS) public String muteLeft;
    }

    @PlayerData(namespace = "pvp", collection = "x")
    static class IndexedString {
        @Indexed public String name;
    }

    @PlayerData(namespace = "Bad", collection = "y")
    static class BadNamespace {
        public int a;
    }

    static class NoAnnotation {
        public int a;
    }

    @PlayerData(namespace = "x", collection = "y")
    @GlobalData(namespace = "x", collection = "y")
    static class BothAnnotations {
        public int a;
    }

    @PlayerData(namespace = "x", collection = "y")
    static class NoDefaultConstructor {
        public int a;

        NoDefaultConstructor(int a) {
            this.a = a;
        }
    }

    @PlayerData(namespace = "x", collection = "y")
    class Inner {
        public int a;
    }

    @Test
    void invalidClassesAreRejectedWithAClearReason() {
        Map<Class<?>, String> cases = Map.of(
                HasFloat.class, "use double",
                HasFinal.class, "final",
                RawList.class, "element type",
                IndexedString.class, "@Indexed",
                DurationString.class, "@Duration",
                BadNamespace.class, "namespace",
                NoAnnotation.class, "exactly one",
                BothAnnotations.class, "exactly one",
                NoDefaultConstructor.class, "constructor",
                Inner.class, "static");
        cases.forEach((cls, hint) -> {
            var e = assertThrows(IllegalArgumentException.class, () -> ClassSchema.of(cls, codecs), cls.getSimpleName());
            assertTrue(e.getMessage().contains(hint), cls.getSimpleName() + ": " + e.getMessage());
        });
    }

    // --- Diff -----------------------------------------------------------------

    private static Value i(int v) {
        return Value.newBuilder().setIntValue(v).build();
    }

    private static Value l(long v) {
        return Value.newBuilder().setLongValue(v).build();
    }

    private static Value d(double v) {
        return Value.newBuilder().setDoubleValue(v).build();
    }

    private static Operation op(List<Operation> ops, String field) {
        return ops.stream().filter(o -> o.getField().equals(field)).findFirst().orElse(null);
    }

    @Test
    void diffMergesWholeNumbersIntoOneIncrement() {
        ClassSchema<Stats> s = ClassSchema.of(Stats.class, codecs);
        var ops = Diff.compute(s,
                Map.of("kills", i(10), "playtime", l(100), "coins", d(1.5)),
                Map.of("kills", i(50), "playtime", l(40), "coins", d(2.25)),
                Set.of());
        assertEquals(40, op(ops, "kills").getIncrement().getIntValue());
        assertEquals(-60, op(ops, "playtime").getIncrement().getLongValue());
        assertEquals(2.25, op(ops, "coins").getSet().getDoubleValue(), "doubles are set, never incremented");
    }

    @Test
    void diffSetsFieldsWithoutStoredValueAndUnsetsNulls() {
        ClassSchema<Stats> s = ClassSchema.of(Stats.class, codecs);
        var ops = Diff.compute(s,
                Map.of("nickname", Value.newBuilder().setStringValue("a").build()),
                Map.of("level", i(2)),
                Set.of());
        assertEquals(2, op(ops, "level").getSet().getIntValue(), "no stored value: set, not increment");
        assertTrue(op(ops, "nickname").hasUnset());
    }

    @Test
    void diffFallsBackToSetWhenTheDeltaWouldOverflow() {
        ClassSchema<Stats> s = ClassSchema.of(Stats.class, codecs);
        var ops = Diff.compute(s,
                Map.of("kills", i(Integer.MIN_VALUE), "playtime", l(Long.MIN_VALUE)),
                Map.of("kills", i(Integer.MAX_VALUE), "playtime", l(Long.MAX_VALUE)),
                Set.of());
        assertEquals(Integer.MAX_VALUE, op(ops, "kills").getSet().getIntValue());
        assertEquals(Long.MAX_VALUE, op(ops, "playtime").getSet().getLongValue());
    }

    @Test
    void diffSkipsUnchangedAndProtectedFields() {
        ClassSchema<Stats> s = ClassSchema.of(Stats.class, codecs);
        var same = Map.of("kills", i(5));
        assertTrue(Diff.compute(s, same, same, Set.of()).isEmpty());
        assertTrue(Diff.compute(s, Map.of("kills", i(1)), Map.of("kills", i(2)), Set.of("kills")).isEmpty());
    }

    @Test
    void storingNaNIsRefused() {
        ClassSchema<Stats> s = ClassSchema.of(Stats.class, codecs);
        Stats obj = s.newInstance();
        obj.coins = Double.NaN;
        assertThrows(IllegalArgumentException.class, () -> s.read(obj));
    }

    /** Stands in for a Minecraft item: this "server" writes data version 2 and loads up to 2. */
    static final class Thing {
        String name;
    }

    @GlobalData(namespace = "pvp", collection = "bags")
    static class Bag {
        public int count;
        public Thing thing;
    }

    private static Value item(int dataVersion, String data) {
        return Value.newBuilder().setItemValue(com.leonsdb.proto.v1.MinecraftItem.newBuilder()
                .setDataVersion(dataVersion).setData(com.google.protobuf.ByteString.copyFromUtf8(data))).build();
    }

    private ClassSchema<Bag> bags() {
        Codecs c = Codecs.withDefaults();
        c.register(Thing.class, new TypeCodec<Thing>() {
            @Override
            public FieldType fieldType() {
                return FieldType.FIELD_TYPE_ITEM;
            }

            @Override
            public Value encode(Thing t) {
                return item(2, "v2:" + t.name);
            }

            @Override
            public Thing decode(Value v) {
                if (v.getItemValue().getDataVersion() > 2) {
                    throw new IllegalArgumentException("newer");
                }
                String d = v.getItemValue().getData().toStringUtf8();
                Thing t = new Thing();
                t.name = d.substring(d.indexOf(':') + 1);
                return t;
            }
        });
        return ClassSchema.of(Bag.class, c);
    }

    @Test
    void anItemFromAnOlderVersionIsNotWrittenBackUnlessItChanged() {
        ClassSchema<Bag> s = bags();
        Map<String, Value> stored = new HashMap<>();
        stored.put("count", Value.newBuilder().setIntValue(1).build());
        stored.put("thing", item(1, "v1:sword")); // saved by an older version
        Bag bag = s.newInstance();
        Set<String> unreadable = s.write(bag, stored);
        assertTrue(unreadable.isEmpty());
        Map<String, Value> base = s.declaredOnly(stored);

        bag.count++;
        List<Operation> ops = Diff.compute(s, base, s.read(bag), unreadable);
        assertEquals(1, ops.size(), "only the counter changed: " + ops);
        assertEquals("count", ops.get(0).getField());

        bag.thing.name = "axe";
        ops = Diff.compute(s, base, s.read(bag), unreadable);
        assertEquals(2, ops.size(), "a changed item is written: " + ops);
    }

    @Test
    void anItemThisVersionCannotLoadIsKeptAsStored() {
        ClassSchema<Bag> s = bags();
        Map<String, Value> stored = new HashMap<>();
        stored.put("count", Value.newBuilder().setIntValue(1).build());
        stored.put("thing", item(3, "v3:sword")); // saved by a newer version
        Bag bag = s.newInstance();
        Set<String> unreadable = s.write(bag, stored);
        assertEquals(Set.of("thing"), unreadable);
        Map<String, Value> base = s.declaredOnly(stored);
        assertEquals(item(3, "v3:sword"), base.get("thing"));
        bag.count++;
        List<Operation> ops = Diff.compute(s, base, s.read(bag), unreadable);
        assertEquals(1, ops.size(), "the item is never written: " + ops);
    }
}
