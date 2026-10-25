// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.cache;

import com.leonsdb.connector.api.History;
import com.leonsdb.connector.api.LeonsDBException;
import com.leonsdb.connector.api.Rollback;
import com.leonsdb.connector.api.RollbackResult;
import com.leonsdb.connector.core.LeonsDBClient;
import com.leonsdb.connector.core.schema.ClassSchema;
import com.leonsdb.proto.v1.HistoryEntry;
import com.leonsdb.proto.v1.RecordRef;
import com.leonsdb.proto.v1.RecordRollback;
import com.leonsdb.proto.v1.RollbackProgress;
import com.leonsdb.proto.v1.RollbackRequest;
import com.leonsdb.proto.v1.Value;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Reads the history kept by LeonsDB and runs rollbacks. Rollbacks of players
 * online on this server reach their cached data the same way as another
 * server's transaction: LeonsDB asks this server to apply them.
 */
final class HistoryImpl implements History {

    private final LeonsDBService service;
    private final LeonsDBClient client;

    HistoryImpl(LeonsDBService service, LeonsDBClient client) {
        this.service = service;
        this.client = client;
    }

    @Override
    public <T> CompletableFuture<Optional<T>> player(UUID player, Class<T> type, Instant time) {
        ClassSchema<T> schema = playerSchema(type);
        return at(schema, LeonsDBService.playerRef(schema, Objects.requireNonNull(player, "player")), time);
    }

    @Override
    public <T> CompletableFuture<Optional<T>> global(Class<T> type, String id, Instant time) {
        ClassSchema<T> schema = globalSchema(type);
        return at(schema, globalRef(schema, id), time);
    }

    private <T> CompletableFuture<Optional<T>> at(ClassSchema<T> schema, RecordRef ref, Instant time) {
        Objects.requireNonNull(time, "time");
        return onMain(client.getAt(ref, time.toEpochMilli()).thenApply(r -> {
            if (!r.getFound()) {
                return Optional.<T>empty();
            }
            T obj = schema.newInstance();
            schema.write(obj, r.getRecord().getFieldsMap());
            return Optional.of(obj);
        }));
    }

    @Override
    public CompletableFuture<List<Change>> changes(UUID player, Class<?> type, int limit) {
        ClassSchema<?> schema = playerSchema(type);
        return changes(schema, LeonsDBService.playerRef(schema, Objects.requireNonNull(player, "player")), limit);
    }

    @Override
    public CompletableFuture<List<Change>> changes(Class<?> type, String id, int limit) {
        ClassSchema<?> schema = globalSchema(type);
        return changes(schema, globalRef(schema, id), limit);
    }

    private CompletableFuture<List<Change>> changes(ClassSchema<?> schema, RecordRef ref, int limit) {
        if (limit < 1 || limit > 1000) {
            throw new IllegalArgumentException("limit must be between 1 and 1000");
        }
        return onMain(client.listHistory(ref, limit).thenApply(r -> {
            List<Change> out = new ArrayList<>(r.getEntriesCount());
            for (HistoryEntry e : r.getEntriesList()) {
                Map<String, com.leonsdb.proto.v1.FieldChange> fields = new LinkedHashMap<>();
                Map<String, History.FieldChange> decoded = new LinkedHashMap<>();
                e.getChangesList().forEach(c -> fields.put(c.getField(), c));
                for (var f : schema.fields()) {
                    com.leonsdb.proto.v1.FieldChange c = fields.get(f.name());
                    if (c != null) {
                        decoded.put(f.name(), new History.FieldChange(
                                decode(f, c.hasBefore() ? c.getBefore() : null), decode(f, c.hasAfter() ? c.getAfter() : null)));
                    }
                }
                out.add(new Change(Instant.ofEpochMilli(e.getFirstTime()), Instant.ofEpochMilli(e.getLastTime()), e.getWrites(),
                        e.getExact(), e.getActor(), e.getCreated(), e.getDeleted(), Map.copyOf(decoded)));
            }
            return List.copyOf(out);
        }));
    }

    private static Object decode(ClassSchema.FieldBinding f, Value v) {
        if (v == null) {
            return null;
        }
        try {
            return f.codec().decode(v);
        } catch (IllegalArgumentException e) {
            return null; // a value this plugin version cannot read
        }
    }

    @Override
    public Rollback rollbackPlayer(UUID player, Instant to) {
        return new RollbackImpl(RollbackRequest.newBuilder().setPlayer(uuidBytes(Objects.requireNonNull(player, "player"))), to);
    }

    @Override
    public Rollback rollback(UUID player, Class<?> type, Instant to) {
        ClassSchema<?> schema = playerSchema(type);
        return new RollbackImpl(RollbackRequest.newBuilder().setRecord(LeonsDBService.playerRef(schema, Objects.requireNonNull(player, "player"))), to);
    }

    @Override
    public Rollback rollback(Class<?> type, String id, Instant to) {
        ClassSchema<?> schema = globalSchema(type);
        return new RollbackImpl(RollbackRequest.newBuilder().setRecord(globalRef(schema, id)), to);
    }

    private final class RollbackImpl implements Rollback {
        private final RollbackRequest.Builder request;

        RollbackImpl(RollbackRequest.Builder request, Instant to) {
            this.request = request.setToTime(Objects.requireNonNull(to, "to").toEpochMilli());
        }

        @Override
        public Rollback onlyFields(String... fields) {
            request.clearFields().addAllFields(Arrays.asList(fields));
            return this;
        }

        @Override
        public CompletableFuture<RollbackResult> preview() {
            return run(false);
        }

        @Override
        public CompletableFuture<RollbackResult> apply() {
            return run(true);
        }

        private CompletableFuture<RollbackResult> run(boolean apply) {
            RollbackRequest req = request.clone().setApply(apply).build();
            return onMain(client.rollback(req).thenApply(p -> toResult(p, apply)));
        }
    }

    private static RollbackResult toResult(RollbackProgress p, boolean apply) {
        List<RollbackResult.RecordRollback> records = new ArrayList<>();
        for (RecordRollback r : p.getRecordsList()) {
            RecordRef ref = r.getRef();
            String key = ref.hasPlayer() ? uuid(ref.getPlayer()).toString() : ref.getId();
            List<String> fields = r.getFieldsList().stream().map(com.leonsdb.proto.v1.FieldChange::getField).toList();
            records.add(new RollbackResult.RecordRollback(ref.getNamespace(), ref.getCollection(), key, fields,
                    r.getRemoved(), r.getRestored(), r.getError().isEmpty() ? null : r.getError()));
        }
        int changed = (int) (apply ? p.getRecordsChanged() : p.getRecordsToChange());
        return new RollbackResult(apply, changed, (int) p.getRecordsFailed(), List.copyOf(p.getUndoneActorsList()), List.copyOf(records));
    }

    private <T> ClassSchema<T> playerSchema(Class<T> type) {
        ClassSchema<T> schema = service.schema(Objects.requireNonNull(type, "type"));
        if (!schema.isPlayerData()) {
            throw new LeonsDBException(type.getName() + " is @GlobalData; give the record's id instead of a player");
        }
        return schema;
    }

    private <T> ClassSchema<T> globalSchema(Class<T> type) {
        ClassSchema<T> schema = service.schema(Objects.requireNonNull(type, "type"));
        if (schema.isPlayerData()) {
            throw new LeonsDBException(type.getName() + " is @PlayerData; give the player instead of an id");
        }
        return schema;
    }

    private static RecordRef globalRef(ClassSchema<?> schema, String id) {
        return RecordRef.newBuilder().setNamespace(schema.namespace()).setCollection(schema.collection())
                .setId(Objects.requireNonNull(id, "id")).build();
    }

    private static ByteString uuidBytes(UUID uuid) {
        ByteBuffer b = ByteBuffer.allocate(16);
        b.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
        return ByteString.copyFrom(b.array());
    }

    private static UUID uuid(ByteString b) {
        ByteBuffer buf = b.asReadOnlyByteBuffer();
        return new UUID(buf.getLong(), buf.getLong());
    }

    private <T> CompletableFuture<T> onMain(CompletableFuture<T> f) {
        var out = new CompletableFuture<T>();
        f.whenComplete((v, e) -> service.onMainThread(() -> {
            if (e != null) {
                out.completeExceptionally(e instanceof java.util.concurrent.CompletionException ce && ce.getCause() != null ? ce.getCause() : e);
            } else {
                out.complete(v);
            }
        }));
        return out;
    }
}
