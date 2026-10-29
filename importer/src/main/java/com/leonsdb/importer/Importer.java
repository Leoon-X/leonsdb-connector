// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.importer;

import com.google.protobuf.ByteString;
import com.leonsdb.proto.v1.ApplyOutcome;
import com.leonsdb.proto.v1.Collection;
import com.leonsdb.proto.v1.CollectionKind;
import com.leonsdb.proto.v1.FieldSchema;
import com.leonsdb.proto.v1.FieldType;
import com.leonsdb.proto.v1.ImportRecord;
import com.leonsdb.proto.v1.Value;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

/**
 * Runs a plan. Every import is first checked in full: the source is read and
 * every row converted, and nothing is written (this alone is the dry run).
 * Applying then reads the source again and loads each collection with one
 * bulk import, which stores everything or nothing.
 */
final class Importer {

    static final int BATCH_RECORDS = 5000;
    static final int BATCH_BYTES = 4 << 20;
    private static final int EXAMPLES = 3;
    private static final int SAMPLES = 3;

    /** Opens a source; tests can make one fail part way. */
    interface Sources {
        RowSource open(Plan.Source source, Plan.From from) throws ImportException;
    }

    private final Plan plan;
    private final LeonsDBTarget target;
    private final Sources sources;
    private final PrintStream progress;

    Importer(Plan plan, LeonsDBTarget target, Sources sources, PrintStream progress) {
        this.plan = plan;
        this.target = target;
        this.sources = sources;
        this.progress = progress;
    }

    /** Opens real sources. */
    static Sources sources(Drivers drivers) {
        return (source, from) -> switch (source.type()) {
            case MONGODB -> new MongoSource(source, from);
            case YAML -> new YamlSource(source, from);
            default -> new JdbcSource(source, from, drivers.driverFor(source));
        };
    }

    /**
     * What a run found and did.
     *
     * @param ok     true if the dry run is ready to apply, or the import was stored
     * @param report the report, as text
     */
    record Outcome(boolean ok, String report) {
    }

    /**
     * @param apply      store the records; otherwise only check
     * @param allowSkips apply even if rows had to be skipped
     * @return the outcome
     */
    Outcome run(boolean apply, boolean allowSkips) {
        StringBuilder r = new StringBuilder();
        r.append(apply ? "LeonsDB import" : "LeonsDB import dry run (nothing is written)").append('\n');
        r.append("Plan:   ").append(plan.file()).append('\n');
        r.append("Source: ").append(plan.source().describe()).append('\n');
        r.append("Into:   LeonsDB at ").append(plan.leonsdb().host()).append(':').append(plan.leonsdb().port()).append("\n\n");

        List<Pass> checked = new ArrayList<>();
        boolean blocked = false;
        for (int i = 0; i < plan.imports().size(); i++) {
            Plan.Import imp = plan.imports().get(i);
            progress.println("Checking " + imp.from().describe() + " -> " + imp.into() + " ...");
            Pass p = new Pass(imp);
            try {
                p.prepare(apply);
                if (p.problem == null) {
                    p.read(null);
                }
            } catch (ImportException e) {
                p.problem = e.getMessage();
            }
            checked.add(p);
            p.describe(r, i + 1);
            blocked |= p.problem != null || p.skipped > 0 && !allowSkips;
        }

        if (!apply) {
            boolean problems = checked.stream().anyMatch(p -> p.problem != null);
            boolean skips = checked.stream().anyMatch(p -> p.skipped > 0);
            r.append(problems ? "Result: not ready. Fix the problems above, then run the dry run again.\n"
                    : skips ? "Result: ready, but some rows would be skipped (see above). Fix them in the source, or run with "
                            + "--apply --allow-skips to import the rest.\n"
                            : "Result: ready. Nothing was written. Run again with --apply to import.\n");
            return new Outcome(!problems, r.toString());
        }
        if (blocked) {
            r.append("Result: nothing was imported, because of the ")
                    .append(checked.stream().anyMatch(p -> p.problem != null) ? "problems" : "skipped rows")
                    .append(" above").append(checked.stream().anyMatch(p -> p.problem != null) ? ""
                            : " (run with --allow-skips to import the rest)").append(".\n");
            return new Outcome(false, r.toString());
        }

        List<String> stored = new ArrayList<>();
        for (Pass p : checked) {
            progress.println("Importing " + p.imp.from().describe() + " -> " + p.imp.into() + " ...");
            try {
                long n = new Pass(p.imp).load(p.col, p.records, allowSkips);
                stored.add(p.imp.into() + ": " + n + " records");
            } catch (ImportException e) {
                r.append("Result: the import into ").append(p.imp.into()).append(" failed and stored nothing: ")
                        .append(e.getMessage()).append('\n');
                r.append(stored.isEmpty() ? "Nothing was imported.\n"
                        : "Imported before it: " + String.join(", ", stored) + ".\n");
                return new Outcome(false, r.toString());
            }
        }
        r.append("Result: imported ").append(String.join(", ", stored)).append(".\n");
        return new Outcome(true, r.toString());
    }

    /** One pass over one import's source. */
    private final class Pass {
        final Plan.Import imp;
        Collection col;
        boolean player;
        final Map<String, FieldType> types = new LinkedHashMap<>();
        String problem;
        long rows;
        long records;
        long skipped;
        final Map<String, long[]> skipCounts = new TreeMap<>();
        final Map<String, List<String>> skipWhere = new TreeMap<>();
        final Map<String, FieldStats> stats = new LinkedHashMap<>();
        final List<String> samples = new ArrayList<>();
        final Set<String> idKeys = new HashSet<>();
        final UuidSet uuidKeys = new UuidSet();

        Pass(Plan.Import imp) {
            this.imp = imp;
        }

        /** Checks the target collection. Sets problem when it cannot be imported into. */
        void prepare(boolean apply) throws ImportException {
            col = target.collection(imp.namespace(), imp.collection());
            if (col == null) {
                problem = "LeonsDB has no collection " + imp.into() + " yet. Start the plugin that stores it once with "
                        + "LeonsDB (it creates the collection and its fields), then run the import again.";
                return;
            }
            use(col);
            List<String> bad = new ArrayList<>();
            for (var f : imp.fields().entrySet()) {
                FieldType t = types.get(f.getKey());
                if (t == null) {
                    bad.add("\"" + f.getKey() + "\" is not a field of " + imp.into() + " (its fields: "
                            + String.join(", ", types.keySet()) + ")");
                } else if (!Convert.importable(t)) {
                    bad.add("\"" + f.getKey() + "\" holds " + typeName(t) + "s, which cannot be imported; leave it out");
                }
            }
            Plan.KeyFormat kf = imp.key().format();
            if (player && kf == Plan.KeyFormat.ID) {
                bad.add(imp.into() + " is a player collection: its key must be a UUID (key as: uuid or offline-uuid)");
            } else if (!player && kf != null && kf != Plan.KeyFormat.ID) {
                bad.add(imp.into() + " is a global collection: its key is a text id (key as: id)");
            }
            if (!bad.isEmpty()) {
                problem = String.join("; ", bad);
                return;
            }
            if (!target.isEmpty(imp.namespace(), imp.collection())) {
                problem = imp.into() + " already has records. A bulk import only loads an empty collection, so nothing "
                        + "already there can be overwritten.";
            }
        }

        void use(Collection c) {
            col = c;
            player = c.getKind() == CollectionKind.COLLECTION_KIND_PLAYER;
            for (FieldSchema f : c.getSchema().getFieldsList()) {
                types.put(f.getName(), f.getType());
            }
        }

        /**
         * Reads the whole source. With a sink, records go to it; without,
         * they are only checked and counted.
         */
        void read(java.util.function.Consumer<ImportRecord> sink) throws ImportException {
            try (RowSource src = sources.open(plan.source(), imp.from())) {
                src.forEach(row -> {
                    rows++;
                    ImportRecord rec;
                    try {
                        rec = convert(row);
                    } catch (Convert.Refused e) {
                        skipped++;
                        skipCounts.computeIfAbsent(e.getMessage(), k -> new long[1])[0]++;
                        List<String> w = skipWhere.computeIfAbsent(e.getMessage(), k -> new ArrayList<>());
                        if (w.size() < EXAMPLES) {
                            w.add(row.where());
                        }
                        return;
                    }
                    records++;
                    if (samples.size() < SAMPLES) {
                        samples.add(render(rec));
                    }
                    if (sink != null) {
                        sink.accept(rec);
                    }
                    if (rows % 100_000 == 0) {
                        progress.println("  " + rows + " rows read");
                    }
                });
            }
        }

        private ImportRecord convert(RowSource.Row row) throws Convert.Refused {
            ImportRecord.Builder b = ImportRecord.newBuilder();
            Object k = row.get(imp.key().from());
            if (k == null) {
                throw new Convert.Refused("no key (" + imp.key().from() + " is empty)");
            }
            if (player) {
                byte[] uuid = imp.key().format() == Plan.KeyFormat.OFFLINE_UUID ? Convert.offlineUuid(k)
                        : Convert.uuidBytes(k, null);
                if (!uuidKeys.add(uuid)) {
                    throw new Convert.Refused("the same player twice (" + uuidString(uuid) + "); the first is kept");
                }
                b.setPlayer(ByteString.copyFrom(uuid));
            } else {
                String id = Convert.id(k);
                if (!idKeys.add(id)) {
                    throw new Convert.Refused("the same id twice (\"" + id + "\"); the first is kept");
                }
                b.setId(id);
            }
            for (var f : imp.fields().entrySet()) {
                Object v = row.get(f.getValue().from());
                if (v == null) {
                    continue; // no value: the field keeps its default
                }
                Value value;
                try {
                    value = Convert.value(v, types.get(f.getKey()), f.getValue().as());
                } catch (Convert.Refused e) {
                    throw new Convert.Refused(f.getKey() + ": " + e.getMessage());
                }
                b.putFields(f.getKey(), value);
                stats.computeIfAbsent(f.getKey(), x -> new FieldStats()).add(value);
            }
            return b.build();
        }

        /** Applies: reads the source again and bulk-imports what it reads. */
        long load(Collection checkedCollection, long expected, boolean allowSkips) throws ImportException {
            use(checkedCollection);
            String id = target.begin(imp.namespace(), imp.collection());
            boolean committed = false;
            try {
                List<ImportRecord> batch = new ArrayList<>();
                int[] bytes = {0};
                long[] sent = {0};
                read(rec -> {
                    batch.add(rec);
                    bytes[0] += rec.getSerializedSize();
                    if (batch.size() >= BATCH_RECORDS || bytes[0] >= BATCH_BYTES) {
                        send(id, batch, sent);
                        bytes[0] = 0;
                    }
                });
                send(id, batch, sent);
                if (skipped > 0 && !allowSkips) {
                    throw new ImportException("the source changed since it was checked: " + skipped + " rows now have to be "
                            + "skipped. Run the dry run again.");
                }
                if (records != expected) {
                    progress.println("  note: the source changed since it was checked: " + records + " records instead of " + expected);
                }
                progress.println("  storing " + sent[0] + " records ...");
                long n = target.commit(id);
                committed = true;
                if (n != sent[0]) {
                    throw new ImportException("LeonsDB stored " + n + " records, but " + sent[0] + " were sent");
                }
                return n;
            } catch (UncheckedImport e) {
                throw e.cause;
            } finally {
                if (!committed) {
                    target.abort(id);
                }
            }
        }

        private void send(String id, List<ImportRecord> batch, long[] sent) {
            if (batch.isEmpty()) {
                return;
            }
            try {
                List<ApplyOutcome> results = target.add(id, batch);
                for (int i = 0; i < results.size(); i++) {
                    if (results.get(i).hasError()) {
                        var e = results.get(i).getError();
                        throw new ImportException("LeonsDB refused the record " + render(batch.get(i)) + ": " + e.getMessage()
                                + (e.getReason().isEmpty() ? "" : " (" + e.getReason() + ")"));
                    }
                }
                sent[0] += batch.size();
                batch.clear();
            } catch (ImportException e) {
                throw new UncheckedImport(e);
            }
        }

        void describe(StringBuilder r, int n) {
            r.append('[').append(n).append("] ").append(imp.from().describe()).append(" -> ").append(imp.into());
            if (col != null) {
                r.append(" (").append(player ? "player" : "global").append(" collection)");
            }
            r.append('\n');
            if (problem != null) {
                r.append("    PROBLEM: ").append(problem).append("\n\n");
                return;
            }
            r.append(String.format(java.util.Locale.ROOT, "    rows read:     %,d%n    would import:  %,d records%n    skipped:       %,d%n",
                    rows, records, skipped));
            for (var e : skipCounts.entrySet()) {
                r.append(String.format(java.util.Locale.ROOT, "      %,d x %s (%s%s)%n", e.getValue()[0], e.getKey(),
                        String.join(", ", skipWhere.get(e.getKey())), e.getValue()[0] > EXAMPLES ? ", ..." : ""));
            }
            r.append("    fields:\n");
            for (var f : imp.fields().entrySet()) {
                FieldStats s = stats.get(f.getKey());
                long present = s == null ? 0 : s.count;
                r.append(String.format(java.util.Locale.ROOT, "      %-16s %-9s from %-16s %,d rows (%d%%)%s%n", f.getKey(),
                        typeName(types.get(f.getKey())), f.getValue().from(), present,
                        records == 0 ? 0 : Math.round(100.0 * present / records), s == null ? "" : s.range()));
            }
            if (!samples.isEmpty()) {
                r.append("    first records:\n");
                samples.forEach(s -> r.append("      ").append(s).append('\n'));
            }
            r.append('\n');
        }
    }

    /** An ImportException through a lambda. */
    private static final class UncheckedImport extends RuntimeException {
        private static final long serialVersionUID = 1L;
        final transient ImportException cause;

        UncheckedImport(ImportException cause) {
            super(cause.getMessage(), cause, false, false);
            this.cause = cause;
        }
    }

    /** How often a field has a value, and its smallest and largest. */
    private static final class FieldStats {
        long count;
        Double min;
        Double max;
        boolean time;

        void add(Value v) {
            count++;
            double d;
            switch (v.getKindCase()) {
                case INT_VALUE -> d = v.getIntValue();
                case LONG_VALUE -> d = v.getLongValue();
                case DOUBLE_VALUE -> d = v.getDoubleValue();
                case TIMESTAMP_VALUE -> {
                    d = v.getTimestampValue();
                    time = true;
                }
                default -> {
                    return;
                }
            }
            min = min == null ? d : Math.min(min, d);
            max = max == null ? d : Math.max(max, d);
        }

        String range() {
            if (min == null) {
                return "";
            }
            if (time) {
                return "   " + Instant.ofEpochMilli(min.longValue()) + " to " + Instant.ofEpochMilli(max.longValue());
            }
            return "   min " + num(min) + " max " + num(max);
        }

        private static String num(double d) {
            return d == Math.rint(d) && Math.abs(d) < 1e15 ? String.format(java.util.Locale.ROOT, "%,d", (long) d)
                    : String.valueOf(d);
        }
    }

    /** Player UUIDs seen, in two longs each, for finding duplicates in large sources. */
    static final class UuidSet {
        private long[] table = new long[2 * 1024];
        private boolean[] used = new boolean[1024];
        private int size;

        boolean add(byte[] uuid) {
            ByteBuffer b = ByteBuffer.wrap(uuid);
            long hi = b.getLong();
            long lo = b.getLong();
            if (size * 2 >= used.length) {
                grow();
            }
            return insert(hi, lo);
        }

        private boolean insert(long hi, long lo) {
            int mask = used.length - 1;
            int i = (int) (mix(hi ^ (lo * 0x9E3779B97F4A7C15L)) & mask);
            while (used[i]) {
                if (table[2 * i] == hi && table[2 * i + 1] == lo) {
                    return false;
                }
                i = (i + 1) & mask;
            }
            used[i] = true;
            table[2 * i] = hi;
            table[2 * i + 1] = lo;
            size++;
            return true;
        }

        private void grow() {
            long[] oldTable = table;
            boolean[] oldUsed = used;
            table = new long[oldTable.length * 2];
            used = new boolean[oldUsed.length * 2];
            size = 0;
            for (int i = 0; i < oldUsed.length; i++) {
                if (oldUsed[i]) {
                    insert(oldTable[2 * i], oldTable[2 * i + 1]);
                }
            }
        }

        private static long mix(long x) {
            x ^= x >>> 33;
            x *= 0xff51afd7ed558ccdL;
            x ^= x >>> 33;
            return x;
        }
    }

    // --- rendering -----------------------------------------------------------------

    static String render(ImportRecord rec) {
        StringBuilder s = new StringBuilder(rec.hasPlayer() ? uuidString(rec.getPlayer().toByteArray()) : "\"" + rec.getId() + "\"");
        s.append(":");
        new TreeMap<>(rec.getFieldsMap()).forEach((k, v) -> s.append(' ').append(k).append('=').append(render(v)).append(','));
        if (s.charAt(s.length() - 1) == ',') {
            s.setLength(s.length() - 1);
        }
        return s.toString();
    }

    private static String render(Value v) {
        return switch (v.getKindCase()) {
            case INT_VALUE -> String.valueOf(v.getIntValue());
            case LONG_VALUE -> String.valueOf(v.getLongValue());
            case DOUBLE_VALUE -> String.valueOf(v.getDoubleValue());
            case BOOL_VALUE -> String.valueOf(v.getBoolValue());
            case STRING_VALUE -> "\"" + (v.getStringValue().length() > 40 ? v.getStringValue().substring(0, 40) + "..."
                    : v.getStringValue()) + "\"";
            case UUID_VALUE -> uuidString(v.getUuidValue().toByteArray());
            case TIMESTAMP_VALUE -> Instant.ofEpochMilli(v.getTimestampValue()).toString();
            case LIST_VALUE -> "[" + v.getListValue().getValuesCount() + " items]";
            case MAP_VALUE -> "{" + v.getMapValue().getEntriesCount() + " entries}";
            case BYTES_VALUE -> v.getBytesValue().size() + " bytes";
            default -> v.getKindCase().toString();
        };
    }

    static String uuidString(byte[] b) {
        ByteBuffer buf = ByteBuffer.wrap(b);
        return new UUID(buf.getLong(), buf.getLong()).toString();
    }

    private static String typeName(FieldType t) {
        return t == null ? "?" : t.name().substring("FIELD_TYPE_".length()).toLowerCase(java.util.Locale.ROOT);
    }
}
