// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core;

import com.google.protobuf.ByteString;
import com.leonsdb.proto.v1.AcquireLeaseRequest;
import com.leonsdb.proto.v1.AcquireLeaseResponse;
import com.leonsdb.proto.v1.ApplyRequest;
import com.leonsdb.proto.v1.ApplyResponse;
import com.leonsdb.proto.v1.Collection;
import com.leonsdb.proto.v1.DataServiceGrpc;
import com.leonsdb.proto.v1.DeleteRequest;
import com.leonsdb.proto.v1.DeleteResponse;
import com.leonsdb.proto.v1.EnsureCollectionRequest;
import com.leonsdb.proto.v1.EnsureCollectionResponse;
import com.leonsdb.proto.v1.GetRequest;
import com.leonsdb.proto.v1.GetResponse;
import com.leonsdb.proto.v1.LeaseEvent;
import com.leonsdb.proto.v1.LeaseServiceGrpc;
import com.leonsdb.proto.v1.Operation;
import com.leonsdb.proto.v1.Record;
import com.leonsdb.proto.v1.RecordRef;
import com.leonsdb.proto.v1.ReleaseLeaseRequest;
import com.leonsdb.proto.v1.ReleaseLeaseResponse;
import com.leonsdb.proto.v1.RenewLeasesRequest;
import com.leonsdb.proto.v1.RenewLeasesResponse;
import com.leonsdb.proto.v1.TransactRequest;
import com.leonsdb.proto.v1.TransactResponse;
import com.leonsdb.proto.v1.TransactionResult;
import com.leonsdb.proto.v1.TransactionWrite;
import com.leonsdb.proto.v1.Value;
import com.leonsdb.proto.v1.WatchLeasesRequest;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An in-memory stand-in for the LeonsDB server with the same versioning
 * rules, plus fault injection. Used to test how the connector behaves when
 * the network misbehaves.
 */
public final class FakeServer implements AutoCloseable {

    /** One stored record. */
    public static final class Stored {
        public long version;
        public final Map<String, Value> fields = new HashMap<>();
    }

    private final Map<RecordRef, Stored> records = new HashMap<>();
    private final Server server;
    private final String name = InProcessServerBuilder.generateName();

    /** Every Apply request received, in order. */
    public final List<ApplyRequest> applies = new CopyOnWriteArrayList<>();
    /** Fail the next N calls without doing anything (server unreachable). */
    public final AtomicInteger failNext = new AtomicInteger();
    /** Apply the next N writes but fail the response (answer lost). */
    public final AtomicInteger dropResponseAfterApply = new AtomicInteger();
    /** Hold Apply calls until released (simulates a slow server). */
    public volatile java.util.concurrent.CountDownLatch holdApplies;
    /** Every Transact request received, in order, including retries. */
    public final List<TransactRequest> transacts = new CopyOnWriteArrayList<>();
    /** Hold Transact calls until released. */
    public volatile java.util.concurrent.CountDownLatch holdTransacts;
    /** Hold ReleaseLease calls until released. */
    public volatile java.util.concurrent.CountDownLatch holdReleases;
    /** Behave like a server from before 0.9: ignore the load of Acquire. */
    public volatile boolean oldServer;
    /** Reads of single records (Get calls). */
    public final java.util.concurrent.atomic.AtomicInteger gets = new java.util.concurrent.atomic.AtomicInteger();
    private final Map<ByteString, TransactResponse> committedTransactions = new HashMap<>();

    /** Lease state per player: who owns it and with which epoch. */
    public static final class Lease {
        public String holder;
        public long epoch;
    }

    private final Map<ByteString, Lease> leases = new HashMap<>();
    private final List<StreamObserver<LeaseEvent>> watchers = new CopyOnWriteArrayList<>();

    /**
     * Current owner of a player, or null.
     *
     * @param player 16-byte UUID
     * @return holder id or null
     */
    public synchronized String ownerOf(ByteString player) {
        Lease l = leases.get(player);
        return l == null ? null : l.holder;
    }

    /**
     * The player's current epoch.
     *
     * @param player the player's UUID bytes
     * @return the epoch, 0 if never owned
     */
    public synchronized long epochOf(ByteString player) {
        Lease l = leases.get(player);
        return l == null ? 0 : l.epoch;
    }

    /**
     * Another server takes the player, as after an expired lease. Optionally
     * tells the old owner, like the real server does.
     *
     * @param player 16-byte UUID
     * @param notify whether to send a LOST event
     */
    public void takeOver(ByteString player, boolean notify) {
        synchronized (this) {
            Lease l = leases.computeIfAbsent(player, p -> new Lease());
            l.holder = "other-server";
            l.epoch++;
        }
        if (notify) {
            send(LeaseEvent.Kind.KIND_LOST, player);
        }
    }

    /**
     * Another server asks for the player.
     *
     * @param player 16-byte UUID
     */
    public void requestHandover(ByteString player) {
        send(LeaseEvent.Kind.KIND_HANDOVER_REQUESTED, player);
    }

    /**
     * A proxy says the player is about to switch servers.
     *
     * @param player 16-byte UUID
     */
    public void expectSwitch(ByteString player) {
        send(LeaseEvent.Kind.KIND_SWITCH_EXPECTED, player);
    }

    /** The instance id reported to clients; change it to simulate a restore. */
    public volatile String instanceId = "fake-instance-1";

    /** Ends every lease stream, as when the server restarts. */
    public void dropWatchers() {
        var all = List.copyOf(watchers);
        watchers.clear();
        all.forEach(w -> w.onError(Status.UNAVAILABLE.asRuntimeException()));
    }

    private final class Info extends com.leonsdb.proto.v1.ServerServiceGrpc.ServerServiceImplBase {
        @Override
        public void getInfo(com.leonsdb.proto.v1.GetInfoRequest req, StreamObserver<com.leonsdb.proto.v1.GetInfoResponse> obs) {
            obs.onNext(com.leonsdb.proto.v1.GetInfoResponse.newBuilder().setServerVersion("fake").setInstanceId(instanceId)
                    .setMinProtocolVersion(1).setMaxProtocolVersion(1).build());
            obs.onCompleted();
        }
    }

    private void send(LeaseEvent.Kind kind, ByteString player) {
        // Like the real server, events only reach a stream that is open: wait
        // for the client's stream, so a test never sends into the void.
        long deadline = System.nanoTime() + 5_000_000_000L;
        while (watchers.isEmpty() && System.nanoTime() < deadline) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        var ev = LeaseEvent.newBuilder().setKind(kind).setPlayer(player).setRequestedBy("other-server").build();
        watchers.forEach(w -> w.onNext(ev));
    }

    public FakeServer() throws IOException {
        server = InProcessServerBuilder.forName(name).addService(new Service()).addService(new Leases())
                .addService(new Info()).build().start();
    }

    /**
     * A client connected to this server.
     *
     * @return a client
     */
    public LeonsDBClient client() {
        var channel = InProcessChannelBuilder.forName(name).build();
        return new LeonsDBClient(channel,
                new ConnectionSettings("fake", 1, false, "ldb_test", Duration.ofSeconds(5)), "test");
    }

    /**
     * Reads a stored record.
     *
     * @param ref the record
     * @return the record, or null
     */
    public synchronized Stored get(RecordRef ref) {
        return records.get(ref);
    }

    /**
     * Stores a record directly, as if another server had written it.
     *
     * @param ref    the record
     * @param fields its fields
     */
    public synchronized void put(RecordRef ref, Map<String, Value> fields) {
        Stored s = records.computeIfAbsent(ref, r -> new Stored());
        s.version++;
        s.fields.putAll(fields);
    }

    @Override
    public void close() {
        server.shutdownNow();
    }

    private boolean injectFailure(StreamObserver<?> obs) {
        if (failNext.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            obs.onError(Status.UNAVAILABLE.withDescription("injected outage").asRuntimeException());
            return true;
        }
        return false;
    }

    private final class Service extends DataServiceGrpc.DataServiceImplBase {
        @Override
        public void ensureCollection(EnsureCollectionRequest req, StreamObserver<EnsureCollectionResponse> obs) {
            if (injectFailure(obs)) {
                return;
            }
            obs.onNext(EnsureCollectionResponse.newBuilder().setCreated(true).setCollection(Collection.newBuilder()
                    .setNamespace(req.getNamespace()).setName(req.getCollection()).setKind(req.getKind())
                    .setSchema(req.getSchema())).build());
            obs.onCompleted();
        }

        @Override
        public void get(GetRequest req, StreamObserver<GetResponse> obs) {
            gets.incrementAndGet();
            if (injectFailure(obs)) {
                return;
            }
            GetResponse resp;
            synchronized (FakeServer.this) {
                Stored s = records.get(req.getRef());
                resp = s == null ? GetResponse.newBuilder().setFound(false).build()
                        : GetResponse.newBuilder().setFound(true).setRecord(Record.newBuilder()
                        .setRef(req.getRef()).putAllFields(s.fields).setVersion(s.version)).build();
            }
            obs.onNext(resp);
            obs.onCompleted();
        }

        @Override
        public void apply(ApplyRequest req, StreamObserver<ApplyResponse> obs) {
            var hold = holdApplies;
            if (hold != null) {
                try {
                    hold.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (injectFailure(obs)) {
                return;
            }
            applies.add(req);
            long newVersion;
            synchronized (FakeServer.this) {
                Lease lease = req.getRef().hasPlayer() ? leases.get(req.getRef().getPlayer()) : null;
                boolean owned = lease != null && lease.holder != null;
                if ((owned && req.getEpoch() != lease.epoch) || (!owned && req.getRef().hasPlayer() && lease != null && req.getEpoch() != 0)) {
                    Metadata trailers = new Metadata();
                    trailers.put(Metadata.Key.of("leonsdb-error-reason", Metadata.ASCII_STRING_MARSHALLER), "STALE_EPOCH");
                    obs.onError(Status.FAILED_PRECONDITION.withDescription("not the owner").asRuntimeException(trailers));
                    return;
                }
                Stored s = records.get(req.getRef());
                long have = s == null ? 0 : s.version;
                if (req.hasExpectedVersion() && req.getExpectedVersion() != have) {
                    Metadata trailers = new Metadata();
                    trailers.put(Metadata.Key.of("leonsdb-error-reason", Metadata.ASCII_STRING_MARSHALLER), "VERSION_MISMATCH");
                    obs.onError(Status.ABORTED.withDescription("version mismatch").asRuntimeException(trailers));
                    return;
                }
                Map<String, Value> next = new HashMap<>(s == null ? Map.of() : s.fields);
                for (Operation op : req.getOperationsList()) {
                    switch (op.getOpCase()) {
                        case SET -> next.put(op.getField(), op.getSet());
                        case UNSET -> next.remove(op.getField());
                        case INCREMENT -> next.put(op.getField(), add(next.get(op.getField()), op.getIncrement()));
                        default -> throw new IllegalStateException();
                    }
                }
                Stored target = s == null ? new Stored() : s;
                target.fields.clear();
                target.fields.putAll(next);
                target.version = have + 1;
                records.put(req.getRef(), target);
                newVersion = target.version;
            }
            if (dropResponseAfterApply.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                obs.onError(Status.DEADLINE_EXCEEDED.withDescription("injected lost response").asRuntimeException());
                return;
            }
            obs.onNext(ApplyResponse.newBuilder().setVersion(newVersion).build());
            obs.onCompleted();
        }

        @Override
        public void transact(TransactRequest req, StreamObserver<TransactResponse> obs) {
            var hold = holdTransacts;
            if (hold != null) {
                try {
                    hold.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (injectFailure(obs)) {
                return;
            }
            transacts.add(req);
            TransactResponse resp;
            synchronized (FakeServer.this) {
                TransactResponse done = committedTransactions.get(req.getTransactionId());
                if (done != null) {
                    resp = done.toBuilder().setAlreadyCommitted(true).build();
                } else {
                    // Check everything first, then apply everything.
                    for (int i = 0; i < req.getWritesCount(); i++) {
                        TransactionWrite w = req.getWrites(i);
                        String refused = refusal(w.getRef(), w.getEpoch());
                        if (refused == null && w.hasExpectedVersion()) {
                            Stored s = records.get(w.getRef());
                            if ((s == null ? 0 : s.version) != w.getExpectedVersion()) {
                                refused = "VERSION_MISMATCH";
                            }
                        }
                        if (refused != null) {
                            Metadata trailers = new Metadata();
                            trailers.put(Metadata.Key.of("leonsdb-error-reason", Metadata.ASCII_STRING_MARSHALLER), refused);
                            trailers.put(Metadata.Key.of("leonsdb-write-index", Metadata.ASCII_STRING_MARSHALLER), String.valueOf(i));
                            Status st = refused.equals("VERSION_MISMATCH") ? Status.ABORTED : Status.FAILED_PRECONDITION;
                            obs.onError(st.withDescription("write " + i + ": " + refused).asRuntimeException(trailers));
                            return;
                        }
                    }
                    TransactResponse.Builder out = TransactResponse.newBuilder();
                    for (TransactionWrite w : req.getWritesList()) {
                        out.addResults(TransactionResult.newBuilder().setVersion(store(w.getRef(), w.getOperationsList())));
                    }
                    resp = out.build();
                    committedTransactions.put(req.getTransactionId(), resp);
                }
            }
            if (dropResponseAfterApply.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                obs.onError(Status.DEADLINE_EXCEEDED.withDescription("injected lost response").asRuntimeException());
                return;
            }
            obs.onNext(resp);
            obs.onCompleted();
        }

        @Override
        public void delete(DeleteRequest req, StreamObserver<DeleteResponse> obs) {
            if (injectFailure(obs)) {
                return;
            }
            boolean existed;
            synchronized (FakeServer.this) {
                existed = records.remove(req.getRef()) != null;
            }
            obs.onNext(DeleteResponse.newBuilder().setExisted(existed).build());
            obs.onCompleted();
        }
    }

    /** A simple lease service: grants free players, refuses owned ones. */
    private final class Leases extends LeaseServiceGrpc.LeaseServiceImplBase {
        @Override
        public void acquire(AcquireLeaseRequest req,
                            StreamObserver<AcquireLeaseResponse> obs) {
            if (injectFailure(obs)) {
                return;
            }
            long epoch;
            synchronized (FakeServer.this) {
                Lease l = leases.computeIfAbsent(req.getPlayer(), p -> new Lease());
                if (l.holder != null && !l.holder.equals(req.getHolderId())) {
                    Metadata trailers = new Metadata();
                    trailers.put(Metadata.Key.of("leonsdb-error-reason", Metadata.ASCII_STRING_MARSHALLER), "OWNED_ELSEWHERE");
                    obs.onError(Status.FAILED_PRECONDITION.withDescription("owned by " + l.holder).asRuntimeException(trailers));
                    return;
                }
                if (l.holder == null) {
                    l.holder = req.getHolderId();
                    l.epoch++;
                }
                epoch = l.epoch;
            }
            var resp = AcquireLeaseResponse.newBuilder().setEpoch(epoch).setTtlMs(10_000);
            if (!oldServer && req.getLoadCount() > 0) {
                resp.setLoaded(true);
                synchronized (FakeServer.this) {
                    for (String c : req.getLoadList()) {
                        String[] nc = c.split("[.]", 2);
                        RecordRef ref = RecordRef.newBuilder().setNamespace(nc[0]).setCollection(nc[1]).setPlayer(req.getPlayer()).build();
                        Stored s = records.get(ref);
                        if (s != null) {
                            resp.addRecords(Record.newBuilder().setRef(ref).putAllFields(s.fields).setVersion(s.version));
                        }
                    }
                }
            }
            obs.onNext(resp.build());
            obs.onCompleted();
        }

        @Override
        public void renew(RenewLeasesRequest req,
                          StreamObserver<RenewLeasesResponse> obs) {
            obs.onNext(RenewLeasesResponse.newBuilder().setTtlMs(10_000).build());
            obs.onCompleted();
        }

        @Override
        public void release(ReleaseLeaseRequest req,
                            StreamObserver<ReleaseLeaseResponse> obs) {
            var hold = holdReleases;
            if (hold != null) {
                try {
                    hold.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            boolean released = false;
            synchronized (FakeServer.this) {
                Lease l = leases.get(req.getPlayer());
                if (l != null && req.getHolderId().equals(l.holder) && l.epoch == req.getEpoch()) {
                    l.holder = null;
                    released = true;
                }
            }
            obs.onNext(ReleaseLeaseResponse.newBuilder().setReleased(released).build());
            obs.onCompleted();
        }

        @Override
        public void watch(WatchLeasesRequest req,
                          StreamObserver<LeaseEvent> obs) {
            watchers.add(obs);
        }
    }

    /** Why the owner rule refuses a write, like the real server, or null. Caller holds the lock. */
    private String refusal(RecordRef ref, long epoch) {
        Lease lease = ref.hasPlayer() ? leases.get(ref.getPlayer()) : null;
        boolean owned = lease != null && lease.holder != null;
        if (owned && epoch == 0) {
            return "PLAYER_ONLINE";
        }
        if ((owned && epoch != lease.epoch) || (!owned && lease != null && epoch != 0)) {
            return "STALE_EPOCH";
        }
        return null;
    }

    /** Applies operations to a record and returns its new version. Caller holds the lock. */
    private long store(RecordRef ref, List<Operation> ops) {
        Stored s = records.computeIfAbsent(ref, r -> new Stored());
        for (Operation op : ops) {
            switch (op.getOpCase()) {
                case SET -> s.fields.put(op.getField(), op.getSet());
                case UNSET -> s.fields.remove(op.getField());
                case INCREMENT -> s.fields.put(op.getField(), add(s.fields.get(op.getField()), op.getIncrement()));
                default -> throw new IllegalStateException();
            }
        }
        return ++s.version;
    }

    private static Value add(Value current, Value delta) {
        return switch (delta.getKindCase()) {
            case INT_VALUE -> Value.newBuilder().setIntValue((current == null ? 0 : current.getIntValue()) + delta.getIntValue()).build();
            case LONG_VALUE -> Value.newBuilder().setLongValue((current == null ? 0 : current.getLongValue()) + delta.getLongValue()).build();
            case DOUBLE_VALUE -> Value.newBuilder().setDoubleValue((current == null ? 0 : current.getDoubleValue()) + delta.getDoubleValue()).build();
            default -> throw new IllegalArgumentException("not numeric");
        };
    }

    /**
     * All Apply requests' operations, flattened.
     *
     * @return the operations
     */
    public List<Operation> allOps() {
        List<Operation> out = new ArrayList<>();
        applies.forEach(a -> out.addAll(a.getOperationsList()));
        return out;
    }
}
