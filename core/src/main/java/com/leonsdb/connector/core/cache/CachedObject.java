// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.cache;

import com.leonsdb.connector.core.LeonsDBClient;
import com.leonsdb.connector.core.RequestException;
import com.leonsdb.connector.core.schema.ClassSchema;
import com.leonsdb.connector.core.schema.Diff;
import com.leonsdb.proto.v1.Operation;
import com.leonsdb.proto.v1.RecordRef;
import com.leonsdb.proto.v1.Value;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.LongSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * One cached data object of one player, and the machinery that saves it.
 *
 * <p>Threading: the object's fields are only read (snapshotted) and changed
 * on the platform's main thread. Snapshots are immutable maps, so everything
 * after that point (diffing, sending, retrying) can run on any thread. The
 * save state below is guarded by {@code this}.
 *
 * <p>Exactly-once: each write carries the version the server is expected to
 * be at. If a write's response is lost after the server applied it, the
 * retry fails with a version mismatch instead of applying twice; the object
 * then re-reads the server's state and sends only what is still missing.
 * This relies on one writer per player record, which player ownership
 * leases enforce: every write carries the owner's epoch, and a write from a
 * server that lost ownership is refused.
 *
 * @param <T> the data class
 */
public final class CachedObject<T> {

    private static final int MAX_RESYNCS = 5;

    /** During an outage, warn at most this often per record. */
    private static final long WARN_INTERVAL_MILLIS = 30_000;

    private final ClassSchema<T> schema;
    private final RecordRef ref;
    private final T instance;
    private final Set<String> unreadable;
    private final LeonsDBClient client;
    private final Logger log;
    private final LongSupplier epoch;
    private final Runnable onOwnershipLost;

    // Guarded by this.
    private Map<String, Value> base;
    private long version;
    private boolean inFlight;
    private Map<String, Value> queued;
    private final List<CompletableFuture<Void>> queuedWaiters = new ArrayList<>();
    private List<Operation> lastRejected;
    private RequestException lastRejection;
    private long lastWarnMillis;
    private boolean failing;
    // A transaction holds this object: no save may start until it ends.
    private boolean reserved;
    private final ArrayDeque<CompletableFuture<Void>> reservations = new ArrayDeque<>();

    CachedObject(ClassSchema<T> schema, RecordRef ref, T instance, Set<String> unreadable,
                 Map<String, Value> base, long version, LeonsDBClient client, Logger log,
                 LongSupplier epoch, Runnable onOwnershipLost) {
        this.schema = schema;
        this.ref = ref;
        this.instance = instance;
        this.unreadable = Set.copyOf(unreadable);
        this.base = new HashMap<>(base);
        this.version = version;
        this.client = client;
        this.log = log;
        this.epoch = epoch;
        this.onOwnershipLost = onOwnershipLost;
    }

    /** @return the cached object plugins change */
    public T instance() {
        return instance;
    }

    /** @return the class schema */
    public ClassSchema<T> schema() {
        return schema;
    }

    /** @return the record reference */
    public RecordRef ref() {
        return ref;
    }

    /**
     * Reads the object's current values. Main thread only.
     *
     * @return an immutable snapshot
     */
    public Map<String, Value> snapshot() {
        return Map.copyOf(schema.read(instance));
    }

    /**
     * Queues a snapshot to be saved. If nothing is in flight it is sent right
     * away; otherwise it replaces any older queued snapshot and is sent when
     * the current write finishes.
     *
     * @param snapshot the values to save
     * @param waiter   completed when this snapshot is durably stored, or
     *                 failed if that cannot be confirmed now; may be null
     */
    public void submit(Map<String, Value> snapshot, CompletableFuture<Void> waiter) {
        synchronized (this) {
            queued = snapshot;
            if (waiter != null) {
                queuedWaiters.add(waiter);
            }
            if (inFlight) {
                return;
            }
        }
        sendQueued(0);
    }

    /**
     * Whether there is nothing in flight and nothing queued.
     *
     * @return true when idle
     */
    public synchronized boolean idle() {
        return !inFlight && queued == null && !reserved;
    }

    /**
     * The ops that would be sent for a snapshot, for diagnostics and
     * recovery files.
     *
     * @param snapshot the values
     * @return the pending operations
     */
    public synchronized List<Operation> pendingOps(Map<String, Value> snapshot) {
        return Diff.compute(schema, base, snapshot, unreadable);
    }

    // --- Sending ---------------------------------------------------------

    private void sendQueued(int resyncs) {
        Map<String, Value> snapshot;
        List<CompletableFuture<Void>> waiters;
        List<Operation> ops;
        long expected;
        synchronized (this) {
            if (inFlight || reserved || queued == null) {
                return;
            }
            snapshot = queued;
            waiters = new ArrayList<>(queuedWaiters);
            queued = null;
            queuedWaiters.clear();
            ops = Diff.compute(schema, base, snapshot, unreadable);
            if (!ops.isEmpty() && ops.equals(lastRejected)) {
                // The server already rejected exactly this change. Resending
                // it every second would only flood the log.
                RequestException rejection = lastRejection;
                waiters.forEach(w -> w.completeExceptionally(rejection));
                return;
            }
            if (ops.isEmpty()) {
                waiters.forEach(w -> w.complete(null));
                return;
            }
            inFlight = true;
            expected = version;
        }
        client.save(ref, ops, epoch.getAsLong(), OptionalLong.of(expected))
                .whenComplete((resp, err) -> {
                    if (err == null) {
                        onSaved(snapshot, resp.getVersion(), waiters);
                    } else {
                        onFailed(snapshot, ops, waiters, unwrap(err), resyncs);
                    }
                });
    }

    private void onSaved(Map<String, Value> snapshot, long newVersion, List<CompletableFuture<Void>> waiters) {
        synchronized (this) {
            Map<String, Value> next = new HashMap<>(base);
            for (ClassSchema.FieldBinding f : schema.fields()) {
                if (unreadable.contains(f.name())) {
                    continue;
                }
                Value v = snapshot.get(f.name());
                if (v == null) {
                    next.remove(f.name());
                } else {
                    next.put(f.name(), v);
                }
            }
            base = next;
            version = newVersion;
            inFlight = false;
            lastRejected = null;
            lastRejection = null;
            if (failing) {
                failing = false;
                log.info("Saving " + describe() + " works again; all changes are stored.");
            }
        }
        waiters.forEach(w -> w.complete(null));
        afterIdle(0);
    }

    private void onFailed(Map<String, Value> snapshot, List<Operation> ops, List<CompletableFuture<Void>> waiters,
                          Throwable err, int resyncs) {
        RequestException re = err instanceof RequestException r ? r : null;

        if (re != null && "STALE_EPOCH".equals(re.reason())) {
            // Another server owns this player now. Our changes must not be
            // saved over its data; the service drops this player.
            synchronized (this) {
                inFlight = false;
                queued = null;
                queuedWaiters.clear();
            }
            waiters.forEach(w -> w.completeExceptionally(err));
            onOwnershipLost.run();
            grantReservation();
            return;
        }

        if (re != null && re.isVersionMismatch() && resyncs < MAX_RESYNCS) {
            // An earlier write probably landed although its answer was lost.
            // Adopt the server's state and send whatever is still missing.
            client.get(ref).whenComplete((got, getErr) -> {
                synchronized (this) {
                    if (getErr == null) {
                        base = got.getFound() ? schema.declaredOnly(got.getRecord().getFieldsMap()) : Map.of();
                        version = got.getFound() ? got.getRecord().getVersion() : 0;
                    }
                    inFlight = false;
                    if (queued == null) {
                        queued = snapshot; // retry this snapshot
                    }
                    queuedWaiters.addAll(0, waiters);
                }
                if (getErr == null) {
                    afterIdle(resyncs + 1);
                } else {
                    failQueued(unwrap(getErr));
                    grantReservation();
                }
            });
            return;
        }

        boolean temporary = re == null || re.isTemporary() || re.isVersionMismatch();
        synchronized (this) {
            inFlight = false;
            if (!temporary) {
                lastRejected = ops;
                lastRejection = re;
            }
        }
        if (temporary) {
            warnRetrying(err);
        } else {
            log.log(Level.SEVERE, "LeonsDB rejected changes to " + describe() + ": " + err.getMessage()
                    + ". The changes are kept in memory but cannot be saved until the data is changed again. "
                    + "Rejected operations: " + ops);
        }
        waiters.forEach(w -> w.completeExceptionally(err));
        // A newer snapshot may have been queued meanwhile; it is sent on the
        // next tick rather than immediately, so a dead server is not hammered.
        grantReservation();
    }

    // --- Transactions ------------------------------------------------------

    /**
     * Main thread: brings a stored change into the instance. The instance may
     * have changed since {@code before} was taken (the game kept running):
     * numbers get the change's difference added, so both changes count;
     * other fields take the change's value.
     *
     * @param before the values the change started from
     * @param after  the values it stored
     */
    void applyChange(Map<String, Value> before, Map<String, Value> after) {
        Map<String, Value> current = schema.read(instance);
        for (ClassSchema.FieldBinding f : schema.fields()) {
            String name = f.name();
            Value b = before.get(name);
            Value a = after.get(name);
            if (java.util.Objects.equals(b, a) || unreadable.contains(name)) {
                continue;
            }
            schema.writeField(instance, name, TransactionImpl.merge(b, a, current.get(name)));
        }
    }

    /**
     * Reserves this object for a transaction. Completes once no save is in
     * flight; from then on no save starts until {@link #release}. While
     * reserved, {@link #base()} and {@link #version()} are what the server
     * has.
     *
     * @return completes when the reservation is held
     */
    CompletableFuture<Void> reserve() {
        synchronized (this) {
            if (!inFlight && !reserved) {
                reserved = true;
                return CompletableFuture.completedFuture(null);
            }
            CompletableFuture<Void> waiting = new CompletableFuture<>();
            reservations.add(waiting);
            return waiting;
        }
    }

    /**
     * Like {@link #reserve()}, but for another server's transaction: if a
     * transaction holds or waits for this object already, it does not wait
     * (two servers waiting for each other's players would deadlock).
     *
     * @return the reservation, or null if a transaction is in the way
     */
    CompletableFuture<Void> reserveUnlessBusy() {
        synchronized (this) {
            if (reserved || !reservations.isEmpty()) {
                return null;
            }
        }
        return reserve();
    }

    /** @return what the server has; stable while reserved */
    synchronized Map<String, Value> base() {
        return Map.copyOf(base);
    }

    /** @return the server's version; stable while reserved */
    synchronized long version() {
        return version;
    }

    /** @return fields that must never be written */
    Set<String> unreadable() {
        return unreadable;
    }

    /** @return the ownership epoch writes carry */
    long epoch() {
        return epoch.getAsLong();
    }

    /**
     * Records that a transaction stored {@code stored} at {@code newVersion}.
     * The object stays reserved until {@link #release}.
     *
     * @param stored     the values the transaction wrote
     * @param newVersion the record's new version
     */
    synchronized void committed(Map<String, Value> stored, long newVersion) {
        Map<String, Value> next = new HashMap<>(base);
        for (ClassSchema.FieldBinding f : schema.fields()) {
            if (unreadable.contains(f.name())) {
                continue;
            }
            Value v = stored.get(f.name());
            if (v == null) {
                next.remove(f.name());
            } else {
                next.put(f.name(), v);
            }
        }
        base = next;
        version = newVersion;
    }

    /**
     * Ends a reservation. After a committed transaction call it on the main
     * thread once the instance holds the transaction's changes: a snapshot
     * queued meanwhile predates them and would undo them, so it is replaced
     * by a fresh one.
     *
     * @param committed whether the transaction changed this record
     */
    void release(boolean committed) {
        List<CompletableFuture<Void>> failed = List.of();
        RuntimeException snapshotError = null;
        synchronized (this) {
            reserved = false;
            if (committed && queued != null) {
                try {
                    queued = snapshot();
                } catch (IllegalArgumentException e) {
                    snapshotError = e;
                    queued = null;
                    failed = new ArrayList<>(queuedWaiters);
                    queuedWaiters.clear();
                }
            }
        }
        for (CompletableFuture<Void> w : failed) {
            w.completeExceptionally(snapshotError);
        }
        afterIdle(0);
    }

    /** Hands the object to a waiting transaction, or else sends queued changes. */
    private void afterIdle(int resyncs) {
        if (!grantReservation()) {
            sendQueued(resyncs);
        }
    }

    private boolean grantReservation() {
        CompletableFuture<Void> next;
        synchronized (this) {
            if (inFlight || reserved || reservations.isEmpty()) {
                return false;
            }
            reserved = true;
            next = reservations.poll();
        }
        next.complete(null);
        return true;
    }

    private void failQueued(Throwable err) {
        List<CompletableFuture<Void>> waiters;
        synchronized (this) {
            waiters = new ArrayList<>(queuedWaiters);
            queuedWaiters.clear();
            queued = null;
        }
        warnRetrying(err);
        waiters.forEach(w -> w.completeExceptionally(err));
    }

    private void warnRetrying(Throwable err) {
        long now = System.currentTimeMillis();
        synchronized (this) {
            failing = true;
            if (now - lastWarnMillis < WARN_INTERVAL_MILLIS) {
                return;
            }
            lastWarnMillis = now;
        }
        log.log(Level.WARNING, "Could not save " + describe() + " yet; changes are kept in memory and retried: "
                + err.getMessage());
    }

    private String describe() {
        String key = ref.hasPlayer() ? uuidOf(ref) : ref.getId();
        return schema.namespace() + "." + schema.collection() + " of " + key;
    }

    private static String uuidOf(RecordRef ref) {
        var b = ref.getPlayer().asReadOnlyByteBuffer();
        return new java.util.UUID(b.getLong(), b.getLong()).toString();
    }

    private static Throwable unwrap(Throwable t) {
        return t instanceof CompletionException && t.getCause() != null ? t.getCause() : t;
    }
}
