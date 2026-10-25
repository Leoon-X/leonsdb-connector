// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.cache;

import com.google.protobuf.ByteString;
import com.leonsdb.connector.api.DataNotLoadedException;
import com.leonsdb.connector.api.LeonsDBException;
import com.leonsdb.connector.api.Transaction;
import com.leonsdb.connector.core.LeonsDBClient;
import com.leonsdb.connector.core.RequestException;
import com.leonsdb.connector.core.schema.ClassSchema;
import com.leonsdb.connector.core.schema.Diff;
import com.leonsdb.proto.v1.GetResponse;
import com.leonsdb.proto.v1.Operation;
import com.leonsdb.proto.v1.RecordRef;
import com.leonsdb.proto.v1.TransactRequest;
import com.leonsdb.proto.v1.TransactResponse;
import com.leonsdb.proto.v1.TransactionWrite;
import com.leonsdb.proto.v1.Value;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A transaction across players and global records.
 *
 * <p>How it stays correct:
 *
 * <ul>
 *   <li>Changes run on copies. Nothing a plugin sees changes until the
 *       server has durably stored the whole transaction.</li>
 *   <li>The cached objects of online players are reserved first, one at a
 *       time in a fixed order (so two transactions cannot wait for each
 *       other). While reserved, no ordinary save of them starts, so the
 *       transaction can write them with the version the server has and
 *       fold any changes not saved yet into the same atomic write.</li>
 *   <li>Every other record is read and written back with its version: if
 *       another server changed it meanwhile the server refuses the whole
 *       transaction and it is retried on fresh data.</li>
 *   <li>Each request carries a random transaction id. When the answer is
 *       lost the same request is sent again, and the server recognises it,
 *       so a transaction is never applied twice.</li>
 * </ul>
 */
final class TransactionImpl implements Transaction {

    /** Most records in one transaction; the server's limit. */
    static final int MAX_RECORDS = 100;

    /** How long the plugin waits before hearing the outcome is not known yet. */
    static final Duration CONFIRM_TIMEOUT = Duration.ofSeconds(10);

    /** How often a transaction is rebuilt when other servers keep winning. */
    static final int MAX_ATTEMPTS = 30;

    private static final long UNCONFIRMED_RETRY_MILLIS = 1000;
    private static final long BASE_BACKOFF_MILLIS = 2;
    private static final long MAX_BACKOFF_MILLIS = 250;
    private static final long BUSY_BASE_BACKOFF_MILLIS = 10;
    private static final long BUSY_MAX_BACKOFF_MILLIS = 400;
    private static final String BUSY = "BUSY";
    private static final String NOT_LOADED = "PLAYER_NOT_LOADED";
    private static final SecureRandom RANDOM = new SecureRandom();

    private final LeonsDBService service;
    private final LeonsDBClient client;
    private final Platform platform;
    private final Logger log;
    private final List<Part<?>> parts = new ArrayList<>();
    private final CompletableFuture<Void> result = new CompletableFuture<>();
    private boolean started;
    private long startedNanos;

    /** One record's part. */
    private static final class Part<T> {
        final ClassSchema<T> schema;
        final RecordRef ref;
        final Consumer<? super T> change;
        final UUID player; // null for global records

        // Set on the main thread when the player is online here.
        PlayerSession session;
        CachedObject<T> cached;
        Map<String, Value> snapshot;

        // The current attempt.
        Map<String, Value> before;
        Map<String, Value> after;
        List<Operation> ops;
        // For players not cached here: the change as operations, which the
        // server the player is online on (if any) applies to its live data.
        List<Operation> ownerOps = List.of();
        long version;

        Part(ClassSchema<T> schema, RecordRef ref, Consumer<? super T> change, UUID player) {
            this.schema = schema;
            this.ref = ref;
            this.change = change;
            this.player = player;
        }

        String describe() {
            return schema.type().getSimpleName() + " of " + (player != null ? "player " + player : "\"" + ref.getId() + "\"");
        }
    }

    TransactionImpl(LeonsDBService service, LeonsDBClient client, Platform platform, Logger log) {
        this.service = service;
        this.client = client;
        this.platform = platform;
        this.log = log;
    }

    @Override
    public <T> Transaction update(UUID player, Class<T> type, Consumer<? super T> change) {
        Objects.requireNonNull(player, "player");
        Objects.requireNonNull(change, "change");
        ClassSchema<T> schema = service.schema(Objects.requireNonNull(type, "type"));
        if (!schema.isPlayerData()) {
            throw new LeonsDBException(type.getName() + " is @GlobalData; use update(" + type.getSimpleName() + ".class, id, change)");
        }
        return add(new Part<>(schema, LeonsDBService.playerRef(schema, player), change, player));
    }

    @Override
    public <T> Transaction update(Class<T> type, String id, Consumer<? super T> change) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(change, "change");
        ClassSchema<T> schema = service.schema(Objects.requireNonNull(type, "type"));
        if (schema.isPlayerData()) {
            throw new LeonsDBException(type.getName() + " is @PlayerData; use update(uuid, " + type.getSimpleName() + ".class, change)");
        }
        RecordRef ref = RecordRef.newBuilder()
                .setNamespace(schema.namespace()).setCollection(schema.collection()).setId(id).build();
        return add(new Part<>(schema, ref, change, null));
    }

    private synchronized Transaction add(Part<?> part) {
        if (started) {
            throw new IllegalStateException("this transaction was already committed");
        }
        for (Part<?> p : parts) {
            if (p.ref.equals(part.ref)) {
                throw new IllegalArgumentException(part.describe() + " is already part of this transaction; "
                        + "make all its changes in one update");
            }
        }
        if (parts.size() >= MAX_RECORDS) {
            throw new LeonsDBException("a transaction can change at most " + MAX_RECORDS + " records");
        }
        parts.add(part);
        return this;
    }

    @Override
    public CompletableFuture<Void> commit() {
        synchronized (this) {
            if (started) {
                throw new IllegalStateException("this transaction was already committed");
            }
            started = true;
        }
        startedNanos = System.nanoTime();
        if (parts.isEmpty()) {
            result.complete(null);
        } else if (platform.isMainThread()) {
            start(1);
        } else {
            platform.runOnMainThread(() -> start(1), Duration.ZERO);
        }
        return result;
    }

    /**
     * Main thread: finds the cached data of players online here and reserves
     * it. The snapshot the changes run on is only taken once every
     * reservation is held, so it includes the result of any transaction that
     * held them before: a second payment sees the coins the first removed.
     */
    private void start(int n) {
        List<Part<?>> cached = new ArrayList<>();
        for (Part<?> p : parts) {
            p.cached = null;
            p.session = null;
        }
        try {
            for (Part<?> p : parts) {
                if (p.player != null && bindCached(p)) {
                    cached.add(p);
                }
            }
        } catch (RuntimeException e) {
            cached.forEach(p -> p.session.endTransaction());
            result.completeExceptionally(e);
            return;
        }
        cached.sort(Comparator.comparing((Part<?> p) -> p.ref.getNamespace())
                .thenComparing(p -> p.ref.getCollection())
                .thenComparing(p -> p.ref.getPlayer(), ByteString.unsignedLexicographicalComparator()));
        CompletableFuture<Void> reserved = CompletableFuture.completedFuture(null);
        for (Part<?> p : cached) {
            reserved = reserved.thenCompose(x -> p.cached.reserve());
        }
        reserved.thenRun(() -> onMainThread(() -> {
            try {
                for (Part<?> p : cached) {
                    p.snapshot = p.cached.snapshot();
                }
            } catch (RuntimeException e) {
                fail(e);
                return;
            }
            attempt(n);
        }));
    }

    @SuppressWarnings("unchecked")
    private <T> boolean bindCached(Part<T> p) {
        PlayerSession session = service.writableSession(p.player);
        if (session == null) {
            return false; // offline, or online elsewhere: the server decides
        }
        CachedObject<T> obj = (CachedObject<T>) session.objects.get(p.schema.type());
        if (obj == null) {
            throw new DataNotLoadedException(p.schema.type().getSimpleName() + " of player " + p.player + " is not loaded yet");
        }
        p.cached = obj;
        p.session = session;
        session.beginTransaction();
        return true;
    }

    /** Reads the records that are not cached, runs the changes and sends the transaction. */
    private void attempt(int n) {
        List<CompletableFuture<GetResponse>> reads = new ArrayList<>();
        for (Part<?> p : parts) {
            reads.add(p.cached == null ? client.get(p.ref) : CompletableFuture.completedFuture(null));
        }
        CompletableFuture.allOf(reads.toArray(new CompletableFuture<?>[0])).whenComplete((ok, err) -> {
            if (err != null) {
                Throwable cause = unwrap(err);
                boolean temporary = !(cause instanceof RequestException re) || re.isTemporary();
                if (temporary && System.nanoTime() - startedNanos < CONFIRM_TIMEOUT.toNanos() && !service.isShutDown()) {
                    // Nothing was sent yet, so trying again is always safe.
                    later(UNCONFIRMED_RETRY_MILLIS, () -> attempt(n));
                } else {
                    fail(cause);
                }
                return;
            }
            List<Part<?>> sent = new ArrayList<>();
            TransactRequest.Builder req = TransactRequest.newBuilder().setTransactionId(randomId());
            try {
                for (int i = 0; i < parts.size(); i++) {
                    Part<?> p = parts.get(i);
                    prepare(p, reads.get(i).join());
                    if (!p.ops.isEmpty()) {
                        sent.add(p);
                        req.addWrites(TransactionWrite.newBuilder().setRef(p.ref).addAllOperations(p.ops)
                                .setEpoch(p.cached != null ? p.cached.epoch() : 0).setExpectedVersion(p.version)
                                .addAllOwnerOperations(p.ownerOps));
                    }
                }
            } catch (RuntimeException e) {
                fail(e); // a change threw, for example TransactionAbortedException
                return;
            }
            if (sent.isEmpty()) {
                onCommitted(sent, TransactResponse.getDefaultInstance());
            } else {
                send(req.build(), sent, n, System.nanoTime());
            }
        });
    }

    /** Runs a part's change on a copy and works out what to write. */
    private static <T> void prepare(Part<T> p, GetResponse stored) {
        T obj;
        Map<String, Value> base;
        Set<String> unreadable;
        if (p.cached != null) {
            obj = p.schema.copyOf(p.snapshot);
            base = p.cached.base();
            p.version = p.cached.version();
            unreadable = p.cached.unreadable();
        } else {
            obj = p.schema.newInstance();
            if (stored.getFound()) {
                unreadable = p.schema.write(obj, stored.getRecord().getFieldsMap());
                base = p.schema.declaredOnly(stored.getRecord().getFieldsMap());
                p.version = stored.getRecord().getVersion();
            } else {
                unreadable = Set.of();
                base = Map.of();
                p.version = 0;
            }
        }
        p.before = p.schema.read(obj);
        p.change.accept(obj);
        p.after = p.schema.read(obj);
        p.ops = Diff.compute(p.schema, base, p.after, unreadable);
        if (p.player != null && p.cached == null) {
            // Relative to what the change started from: "coins +10", so the
            // owner adds 10 to whatever its live data holds.
            p.ownerOps = Diff.compute(p.schema, p.before, p.after, unreadable);
            if (p.ownerOps.isEmpty()) {
                p.ops = List.of(); // the change changed nothing
            }
        }
    }

    private void send(TransactRequest req, List<Part<?>> sent, int n, long firstSentNanos) {
        client.transact(req).whenComplete((resp, err) -> {
            if (err == null) {
                onCommitted(sent, resp);
                return;
            }
            Throwable cause = unwrap(err);
            RequestException re = cause instanceof RequestException r ? r : null;
            if (re != null && re.isVersionMismatch()) {
                int i = re.writeIndex();
                Part<?> p = i >= 0 && i < sent.size() ? sent.get(i) : null;
                if (p != null && p.cached == null && n < MAX_ATTEMPTS) {
                    // Another server changed this record since we read it.
                    // Nothing was stored; run the changes again on fresh data.
                    long cap = Math.min(MAX_BACKOFF_MILLIS, BASE_BACKOFF_MILLIS << Math.min(n, 16));
                    later(ThreadLocalRandom.current().nextLong(cap + 1), () -> attempt(n + 1));
                    return;
                }
                fail(new LeonsDBException(p != null && p.cached != null
                        ? "The cached data of " + p.describe() + " was out of date; nothing was changed. Try again."
                        : "Gave up: records kept changing on other servers; nothing was changed", "VERSION_MISMATCH", re));
                return;
            }
            if (re != null && (BUSY.equals(re.reason()) || NOT_LOADED.equals(re.reason())) && n < MAX_ATTEMPTS && !service.isShutDown()) {
                // The player is in another transaction on their server, maybe one
                // waiting for a player we hold, or is just switching servers.
                // Nothing was stored. Let go of ours so the other can finish,
                // and try again after a random pause.
                releaseAll();
                long cap = Math.min(BUSY_MAX_BACKOFF_MILLIS, BUSY_BASE_BACKOFF_MILLIS << Math.min(n, 16));
                later(ThreadLocalRandom.current().nextLong(cap / 2, cap + 1), () -> onMainThread(() -> start(n + 1)));
                return;
            }
            if (re == null || re.isTemporary()) {
                // The server may or may not have stored it. Sending the very
                // same request again is safe: a committed transaction id is
                // recognised and not applied twice.
                if (System.nanoTime() - firstSentNanos > CONFIRM_TIMEOUT.toNanos() && !result.isDone()) {
                    log.warning("A transaction could not be confirmed yet; retrying in the background: " + cause.getMessage());
                    result.completeExceptionally(new LeonsDBException(
                            "LeonsDB did not confirm the transaction in time. It will be completed or refused in the "
                                    + "background, never applied twice.", "NOT_CONFIRMED", cause));
                }
                if (service.isShutDown()) {
                    log.severe("The server is stopping while a transaction is unconfirmed; whether it was stored is "
                            + "not known. Changes: " + req.getWritesList());
                    releaseAll();
                    return;
                }
                later(UNCONFIRMED_RETRY_MILLIS, () -> send(req, sent, n, firstSentNanos));
                return;
            }
            fail(re); // refused: nothing was stored
        });
    }

    private void onCommitted(List<Part<?>> sent, TransactResponse resp) {
        for (int i = 0; i < sent.size(); i++) {
            Part<?> p = sent.get(i);
            if (p.cached != null) {
                p.cached.committed(p.after, resp.getResults(i).getVersion());
            }
        }
        Runnable finish = () -> {
            for (Part<?> p : parts) {
                if (p.cached == null) {
                    continue;
                }
                boolean changed = sent.contains(p);
                try {
                    if (changed) {
                        applyToCache(p);
                    }
                } catch (RuntimeException e) {
                    log.log(Level.SEVERE, "Could not update the cached " + p.describe() + " after a transaction", e);
                } finally {
                    p.cached.release(changed);
                    p.session.endTransaction();
                }
            }
            if (!result.complete(null)) {
                log.info("A transaction that could not be confirmed in time has now been stored.");
            }
        };
        onMainThread(finish);
    }

    /**
     * Main thread: brings the transaction's changes into the cached object.
     * The object may have changed since the snapshot (the game kept
     * running): numbers get the transaction's difference added, so both
     * changes count; other fields take the transaction's value.
     */
    private static <T> void applyToCache(Part<T> p) {
        p.cached.applyChange(p.before, p.after);
    }

    static Value merge(Value before, Value after, Value current) {
        if (before == null || after == null || current == null || Objects.equals(before, current)) {
            return after;
        }
        if (before.getKindCase() == Value.KindCase.INT_VALUE && after.getKindCase() == Value.KindCase.INT_VALUE
                && current.getKindCase() == Value.KindCase.INT_VALUE) {
            long v = (long) current.getIntValue() + ((long) after.getIntValue() - before.getIntValue());
            return v == (int) v ? Value.newBuilder().setIntValue((int) v).build() : after;
        }
        if (before.getKindCase() == Value.KindCase.LONG_VALUE && after.getKindCase() == Value.KindCase.LONG_VALUE
                && current.getKindCase() == Value.KindCase.LONG_VALUE) {
            try {
                long delta = Math.subtractExact(after.getLongValue(), before.getLongValue());
                return Value.newBuilder().setLongValue(Math.addExact(current.getLongValue(), delta)).build();
            } catch (ArithmeticException e) {
                return after;
            }
        }
        return after;
    }

    private void fail(Throwable err) {
        releaseAll();
        if (!result.completeExceptionally(err)) {
            log.warning("A transaction that could not be confirmed in time was refused; nothing was changed: "
                    + err.getMessage());
        }
    }

    private void releaseAll() {
        for (Part<?> p : parts) {
            if (p.cached != null) {
                p.cached.release(false);
                p.session.endTransaction();
            }
        }
    }

    private void onMainThread(Runnable task) {
        service.onMainThread(task);
    }

    private static void later(long millis, Runnable task) {
        CompletableFuture.delayedExecutor(millis, TimeUnit.MILLISECONDS).execute(task);
    }

    private static ByteString randomId() {
        byte[] id = new byte[16];
        RANDOM.nextBytes(id);
        return ByteString.copyFrom(id);
    }

    private static Throwable unwrap(Throwable t) {
        return t instanceof CompletionException && t.getCause() != null ? t.getCause() : t;
    }
}
