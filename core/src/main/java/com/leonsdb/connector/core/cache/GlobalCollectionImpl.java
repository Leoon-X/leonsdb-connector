// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.cache;

import com.leonsdb.connector.api.GlobalCollection;
import com.leonsdb.connector.api.LeonsDBException;
import com.leonsdb.connector.core.LeonsDBClient;
import com.leonsdb.connector.core.RequestException;
import com.leonsdb.connector.core.schema.ClassSchema;
import com.leonsdb.connector.core.schema.Diff;
import com.leonsdb.proto.v1.RecordRef;
import com.leonsdb.proto.v1.Value;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A global collection. Not cached: several servers write these records, so
 * every update reads the current version and writes back only if nobody
 * changed it in between (optimistic concurrency), retrying otherwise.
 *
 * @param <T> the record type
 */
final class GlobalCollectionImpl<T> implements GlobalCollection<T> {

    /** How often an update is retried when other servers keep winning. */
    static final int MAX_ATTEMPTS = 30;

    /** Retry delays grow from this, doubling up to {@link #MAX_BACKOFF_MILLIS}. */
    private static final long BASE_BACKOFF_MILLIS = 2;
    private static final long MAX_BACKOFF_MILLIS = 250;

    private final ClassSchema<T> schema;
    private final LeonsDBClient client;

    GlobalCollectionImpl(ClassSchema<T> schema, LeonsDBClient client) {
        this.schema = schema;
        this.client = client;
    }

    @Override
    public CompletableFuture<Optional<T>> get(String id) {
        return client.get(ref(id)).thenApply(resp -> {
            if (!resp.getFound()) {
                return Optional.empty();
            }
            T obj = schema.newInstance();
            schema.write(obj, resp.getRecord().getFieldsMap());
            return Optional.of(obj);
        });
    }

    @Override
    public CompletableFuture<T> update(String id, Consumer<? super T> change) {
        Objects.requireNonNull(change, "change");
        return attempt(ref(id), change, 1);
    }

    private CompletableFuture<T> attempt(RecordRef ref, Consumer<? super T> change, int n) {
        return client.get(ref).thenCompose(resp -> {
            T obj = schema.newInstance();
            Set<String> unreadable = Set.of();
            Map<String, Value> base = Map.of();
            long version = 0;
            if (resp.getFound()) {
                unreadable = schema.write(obj, resp.getRecord().getFieldsMap());
                base = schema.declaredOnly(resp.getRecord().getFieldsMap());
                version = resp.getRecord().getVersion();
            }
            change.accept(obj);
            var ops = Diff.compute(schema, base, schema.read(obj), unreadable);
            if (ops.isEmpty()) {
                return CompletableFuture.completedFuture(obj);
            }
            return client.apply(ref, ops, 0, OptionalLong.of(version))
                    .thenApply(r -> obj)
                    .exceptionallyCompose(err -> {
                        Throwable cause = err instanceof CompletionException && err.getCause() != null ? err.getCause() : err;
                        if (cause instanceof RequestException re && re.isVersionMismatch()) {
                            if (n < MAX_ATTEMPTS) {
                                // Wait a random, growing time so servers that collided
                                // spread out instead of colliding again.
                                long cap = Math.min(MAX_BACKOFF_MILLIS, BASE_BACKOFF_MILLIS << Math.min(n, 16));
                                long delay = ThreadLocalRandom.current().nextLong(cap + 1);
                                return CompletableFuture.supplyAsync(() -> null,
                                                CompletableFuture.delayedExecutor(delay, TimeUnit.MILLISECONDS))
                                        .thenCompose(x -> attempt(ref, change, n + 1));
                            }
                            return CompletableFuture.failedFuture(new LeonsDBException(
                                    "Gave up updating " + ref.getId() + ": it kept changing on other servers", "VERSION_MISMATCH", re));
                        }
                        return CompletableFuture.failedFuture(cause);
                    });
        });
    }

    @Override
    public CompletableFuture<Boolean> delete(String id) {
        return client.delete(ref(id));
    }

    private RecordRef ref(String id) {
        Objects.requireNonNull(id, "id");
        return RecordRef.newBuilder()
                .setNamespace(schema.namespace()).setCollection(schema.collection()).setId(id)
                .build();
    }
}
