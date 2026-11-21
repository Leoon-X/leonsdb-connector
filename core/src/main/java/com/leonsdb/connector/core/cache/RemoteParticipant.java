// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.cache;

import com.google.protobuf.ByteString;
import com.leonsdb.connector.core.Jdk;
import com.leonsdb.connector.core.LeonsDBClient;
import com.leonsdb.connector.core.schema.ClassSchema;
import com.leonsdb.connector.core.schema.Diff;
import com.leonsdb.connector.core.schema.Ops;
import com.leonsdb.proto.v1.Condition;
import com.leonsdb.proto.v1.GetTransactionOutcomeResponse;
import com.leonsdb.proto.v1.LeaseEvent;
import com.leonsdb.proto.v1.Operation;
import com.leonsdb.proto.v1.PreparedRequest;
import com.leonsdb.proto.v1.Value;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Logger;

/**
 * Takes part in transactions that other Minecraft servers (or websites)
 * run on players who play here.
 *
 * <p>LeonsDB sends a prepare: this server reserves the player's cached
 * object (so no ordinary save of it starts), checks the conditions and
 * applies the operations to a copy of the live data, and answers with
 * what to write. LeonsDB then commits the whole transaction or not, and
 * says which; only then does the cached object change. If the outcome
 * does not arrive, this server asks for it until it knows. The reservation
 * is held until then, so the record is never saved over a transaction
 * whose outcome is unknown.
 */
final class RemoteParticipant {

    /** Ask for the outcome if it has not arrived after this long, and then again. */
    static final Duration OUTCOME_POLL = Duration.ofSeconds(2);

    private final LeonsDBService service;
    private final LeonsDBClient client;
    private final Supplier<String> holderId;
    private final java.util.function.ToLongFunction<UUID> leaseEpoch;
    private final ScheduledExecutorService scheduler;
    private final Logger log;
    private final Map<String, Prepared> prepared = new ConcurrentHashMap<>();

    /** One prepared write, waiting for its outcome. */
    private static final class Prepared {
        final LeaseEvent request;
        final PlayerSession session;
        final CachedObject<?> obj;
        final Map<String, Value> before;
        final Map<String, Value> after;

        Prepared(LeaseEvent request, PlayerSession session, CachedObject<?> obj,
                 Map<String, Value> before, Map<String, Value> after) {
            this.request = request;
            this.session = session;
            this.obj = obj;
            this.before = before;
            this.after = after;
        }
    }

    RemoteParticipant(LeonsDBService service, LeonsDBClient client, Supplier<String> holderId,
                      java.util.function.ToLongFunction<UUID> leaseEpoch, ScheduledExecutorService scheduler, Logger log) {
        this.leaseEpoch = leaseEpoch;
        this.service = service;
        this.client = client;
        this.holderId = holderId;
        this.scheduler = scheduler;
        this.log = log;
    }

    /** Handles KIND_PREPARE and KIND_OUTCOME events. */
    void onEvent(LeaseEvent ev) {
        switch (ev.getKind()) {
            case KIND_PREPARE:
                prepare(ev);
                break;
            case KIND_OUTCOME:
                finish(key(ev), ev.getCommitted(), ev.getVersion());
                break;
            default:
                break;
        }
    }

    /** @return writes prepared and not decided yet, for tests */
    int pending() {
        return prepared.size();
    }

    private void prepare(LeaseEvent ev) {
        UUID uuid = LeaseKeeper.uuid(ev.getPlayer());
        if (!service.cachesPlayerCollection(ev.getRef().getNamespace(), ev.getRef().getCollection())) {
            // No plugin here keeps this record in memory, so nothing here can
            // save over it: the operations apply to the stored record as
            // they are, under our ownership, and LeonsDB checks the
            // conditions against the stored record.
            long epoch = leaseEpoch.applyAsLong(uuid);
            if (epoch == 0) {
                refuse(ev, "PLAYER_NOT_LOADED", "the player is not on this server any more; try again");
                return;
            }
            client.prepared(PreparedRequest.newBuilder()
                    .setHolderId(holderId.get()).setTransactionId(ev.getTransactionId()).setPrepareId(ev.getPrepareId())
                    .setWriteIndex(ev.getWriteIndex()).setOk(true).setEpoch(epoch).addAllOperations(ev.getOperationsList())
                    .build());
            return;
        }
        PlayerSession session = service.writableSession(uuid);
        CachedObject<?> obj = session == null ? null : objectFor(session, ev);
        if (obj == null) {
            refuse(ev, "PLAYER_NOT_LOADED", "the player's data is not loaded on this server (joining or leaving); try again");
            return;
        }
        if (!session.beginTransaction()) {
            refuse(ev, "PLAYER_NOT_LOADED", "the player's data is not loaded on this server (joining or leaving); try again");
            return;
        }
        java.util.concurrent.CompletableFuture<Void> reservation = obj.reserveUnlessBusy();
        if (reservation == null) {
            session.endTransaction();
            refuse(ev, "BUSY", "the player is in another transaction right now; try again");
            return;
        }
        reservation.thenRun(() -> service.onPlayerThread(session.uuid, () -> prepareReserved(ev, session, obj)));
    }

    /** The player's thread, with the object reserved. */
    private void prepareReserved(LeaseEvent ev, PlayerSession session, CachedObject<?> obj) {
        Map<String, Value> before;
        Map<String, Value> after;
        try {
            before = obj.snapshot();
            for (Condition c : ev.getConditionsList()) {
                if (!Ops.holds(c, before.get(c.getField()))) {
                    release(session, obj);
                    refuse(ev, "CONDITION_FAILED", "condition " + Ops.describe(c) + " does not hold for "
                            + obj.schema().type().getSimpleName() + " of player " + session.uuid);
                    return;
                }
            }
            after = Ops.apply(before, ev.getOperationsList());
            obj.schema().copyOf(after); // the result must fit the plugin's class
        } catch (RuntimeException e) {
            release(session, obj);
            refuse(ev, "INVALID_OPERATION", e.getMessage());
            return;
        }

        // Write the live data with the change, as this server would save it.
        // Operations on fields the plugin does not declare (or cannot read)
        // are not in the cache; they apply to the stored record as they are.
        List<Operation> write = new ArrayList<>(Diff.compute(obj.schema(), obj.base(), after, obj.unreadable()));
        Set<String> declared = new java.util.HashSet<>();
        obj.schema().fields().forEach(f -> declared.add(f.name()));
        for (Operation op : ev.getOperationsList()) {
            if (!declared.contains(op.getField()) || obj.unreadable().contains(op.getField())) {
                write.add(op);
            }
        }

        String key = key(ev);
        prepared.put(key, new Prepared(ev, session, obj, before, after));
        client.prepared(PreparedRequest.newBuilder()
                .setHolderId(holderId.get()).setTransactionId(ev.getTransactionId()).setPrepareId(ev.getPrepareId())
                .setWriteIndex(ev.getWriteIndex()).setOk(true)
                .setEpoch(obj.epoch()).setExpectedVersion(obj.version()).addAllOperations(write)
                .build());
        // If the answer is lost, LeonsDB gives up waiting and the outcome says so.
        pollLater(key);
    }

    private void pollLater(String key) {
        scheduler.schedule(() -> poll(key), OUTCOME_POLL.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void poll(String key) {
        Prepared p = prepared.get(key);
        if (p == null) {
            return;
        }
        client.transactionOutcome(p.request.getTransactionId(), p.request.getPrepareId()).whenComplete((resp, err) -> {
            if (err != null || resp.getOutcome() == GetTransactionOutcomeResponse.Outcome.OUTCOME_PENDING) {
                pollLater(key); // keep the reservation until the outcome is known
                return;
            }
            boolean committed = resp.getOutcome() == GetTransactionOutcomeResponse.Outcome.OUTCOME_COMMITTED;
            int i = p.request.getWriteIndex();
            finish(key, committed, committed && i < resp.getVersionsCount() ? resp.getVersions(i) : 0);
        });
    }

    private void finish(String key, boolean committed, long version) {
        Prepared p = prepared.remove(key);
        if (p == null) {
            return; // not prepared here, or already finished
        }
        if (!committed) {
            release(p.session, p.obj);
            return;
        }
        p.obj.committed(p.after, version);
        service.onPlayerThread(p.session.uuid, () -> {
            try {
                p.obj.applyChange(p.before, p.after);
            } catch (RuntimeException e) {
                log.log(java.util.logging.Level.SEVERE, "Could not update cached data after another server's transaction", e);
            } finally {
                p.obj.release(true);
                p.session.endTransaction();
            }
        });
    }

    private static void release(PlayerSession session, CachedObject<?> obj) {
        obj.release(false);
        session.endTransaction();
    }

    private void refuse(LeaseEvent ev, String reason, String message) {
        client.prepared(PreparedRequest.newBuilder()
                .setHolderId(holderId.get()).setTransactionId(ev.getTransactionId()).setPrepareId(ev.getPrepareId())
                .setWriteIndex(ev.getWriteIndex()).setOk(false).setRefusalReason(reason).setRefusalMessage(message)
                .build());
    }

    private static CachedObject<?> objectFor(PlayerSession session, LeaseEvent ev) {
        for (CachedObject<?> obj : session.all()) {
            ClassSchema<?> s = obj.schema();
            if (s.namespace().equals(ev.getRef().getNamespace()) && s.collection().equals(ev.getRef().getCollection())) {
                return obj;
            }
        }
        return null;
    }

    private static String key(LeaseEvent ev) {
        return hex(ev.getPrepareId()) + ":" + ev.getWriteIndex();
    }

    private static String hex(ByteString b) {
        return Jdk.hex(b.toByteArray());
    }
}
