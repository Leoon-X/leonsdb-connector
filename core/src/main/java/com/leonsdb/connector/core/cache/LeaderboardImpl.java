// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.cache;

import com.leonsdb.connector.api.Leaderboard;
import com.leonsdb.connector.api.Ranking;
import com.leonsdb.connector.core.LeonsDBClient;
import com.leonsdb.connector.core.schema.ClassSchema;
import com.leonsdb.proto.v1.RankedEntry;
import com.leonsdb.proto.v1.RecordRef;
import com.leonsdb.proto.v1.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/** A leaderboard backed by the server's sorted index. */
final class LeaderboardImpl<T> implements Leaderboard<T> {

    private final ClassSchema<T> schema;
    private final String field;
    private final boolean ascending;
    private final LeonsDBClient client;

    LeaderboardImpl(ClassSchema<T> schema, String field, Order order, LeonsDBClient client) {
        this.schema = schema;
        this.field = field;
        this.ascending = order == Order.LOWEST_FIRST;
        this.client = client;
    }

    @Override
    public CompletableFuture<List<Ranking<T>>> top(int limit) {
        return page(0, limit);
    }

    @Override
    public CompletableFuture<List<Ranking<T>>> page(int offset, int limit) {
        if (offset < 0 || limit < 1 || limit > 1000) {
            throw new IllegalArgumentException("offset must be >= 0 and limit between 1 and 1000");
        }
        return client.top(schema.namespace(), schema.collection(), field, offset, limit, ascending, true)
                .thenApply(resp -> {
                    List<Ranking<T>> out = new ArrayList<>(resp.getEntriesCount());
                    for (RankedEntry e : resp.getEntriesList()) {
                        T data = schema.newInstance();
                        if (e.hasRecord()) {
                            schema.write(data, e.getRecord().getFieldsMap());
                        }
                        RecordRef ref = e.getRef();
                        out.add(new Ranking<>(e.getRank(),
                                ref.hasPlayer() ? LeaseKeeper.uuid(ref.getPlayer()) : null,
                                ref.hasId() ? ref.getId() : null,
                                toNumber(e.getValue()), data));
                    }
                    return out;
                });
    }

    @Override
    public CompletableFuture<OptionalLong> rankOf(UUID player) {
        Objects.requireNonNull(player, "player");
        if (!schema.isPlayerData()) {
            throw new IllegalArgumentException(schema.type().getSimpleName() + " is global data; use rankOf(String id)");
        }
        return rank(LeonsDBService.playerRef(schema, player));
    }

    @Override
    public CompletableFuture<OptionalLong> rankOf(String id) {
        Objects.requireNonNull(id, "id");
        if (schema.isPlayerData()) {
            throw new IllegalArgumentException(schema.type().getSimpleName() + " is player data; use rankOf(UUID)");
        }
        return rank(RecordRef.newBuilder()
                .setNamespace(schema.namespace()).setCollection(schema.collection()).setId(id).build());
    }

    @Override
    public CompletableFuture<Long> size() {
        return client.top(schema.namespace(), schema.collection(), field, 0, 1, ascending, false)
                .thenApply(r -> r.getTotal());
    }

    private CompletableFuture<OptionalLong> rank(RecordRef ref) {
        return client.rank(ref, field, ascending)
                .thenApply(r -> r.getRanked() ? OptionalLong.of(r.getRank()) : OptionalLong.empty());
    }

    private static Number toNumber(Value v) {
        switch (v.getKindCase()) {
            case INT_VALUE:
                return v.getIntValue();
            case LONG_VALUE:
                return v.getLongValue();
            case DOUBLE_VALUE:
                return v.getDoubleValue();
            case TIMESTAMP_VALUE:
                return v.getTimestampValue();
            default:
                return null;
        }
    }
}
