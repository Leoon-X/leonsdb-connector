// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.leonsdb.connector.api.LeonsDBException;
import com.leonsdb.connector.api.ServerInfo;
import com.google.protobuf.ByteString;
import com.leonsdb.proto.v1.AcquireLeaseRequest;
import com.leonsdb.proto.v1.AcquireLeaseResponse;
import com.leonsdb.proto.v1.ApplyRequest;
import com.leonsdb.proto.v1.ApplyResponse;
import com.leonsdb.proto.v1.ChannelMessage;
import com.leonsdb.proto.v1.GetAtRequest;
import com.leonsdb.proto.v1.GetAtResponse;
import com.leonsdb.proto.v1.HistoryServiceGrpc;
import com.leonsdb.proto.v1.ListHistoryRequest;
import com.leonsdb.proto.v1.ListHistoryResponse;
import com.leonsdb.proto.v1.RecordRollback;
import com.leonsdb.proto.v1.RollbackProgress;
import com.leonsdb.proto.v1.RollbackRequest;
import com.leonsdb.proto.v1.CollectionKind;
import com.leonsdb.proto.v1.DataServiceGrpc;
import com.leonsdb.proto.v1.DeleteRequest;
import com.leonsdb.proto.v1.EnsureCollectionRequest;
import com.leonsdb.proto.v1.EnsureCollectionResponse;
import com.leonsdb.proto.v1.ExpectRequest;
import com.leonsdb.proto.v1.ExpectResponse;
import com.leonsdb.proto.v1.GetInfoRequest;
import com.leonsdb.proto.v1.GetInfoResponse;
import com.leonsdb.proto.v1.GetTransactionOutcomeRequest;
import com.leonsdb.proto.v1.GetTransactionOutcomeResponse;
import com.leonsdb.proto.v1.GetRequest;
import com.leonsdb.proto.v1.GetResponse;
import com.leonsdb.proto.v1.LeaseEvent;
import com.leonsdb.proto.v1.LeaseServiceGrpc;
import com.leonsdb.proto.v1.MessageServiceGrpc;
import com.leonsdb.proto.v1.Operation;
import com.leonsdb.proto.v1.PreparedRequest;
import com.leonsdb.proto.v1.PreparedResponse;
import com.leonsdb.proto.v1.PublishRequest;
import com.leonsdb.proto.v1.PublishResponse;
import com.leonsdb.proto.v1.RankRequest;
import com.leonsdb.proto.v1.RankResponse;
import com.leonsdb.proto.v1.RecordRef;
import com.leonsdb.proto.v1.ReleaseLeaseRequest;
import com.leonsdb.proto.v1.ReleaseLeaseResponse;
import com.leonsdb.proto.v1.RenewLeasesRequest;
import com.leonsdb.proto.v1.RenewLeasesResponse;
import com.leonsdb.proto.v1.Schema;
import com.leonsdb.proto.v1.SubscribeRequest;
import com.leonsdb.proto.v1.ServerServiceGrpc;
import com.leonsdb.proto.v1.TopRequest;
import com.leonsdb.proto.v1.TopResponse;
import com.leonsdb.proto.v1.TransactRequest;
import com.leonsdb.proto.v1.TransactResponse;
import com.leonsdb.proto.v1.WatchLeasesRequest;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A connection to one LeonsDB server.
 *
 * <p>All methods are non-blocking and return a {@link CompletableFuture}.
 * Never call {@code join()} or {@code get()} on these futures from the
 * Minecraft main thread. Failed futures complete with
 * {@link RequestException} (or {@link IncompatibleServerException}).
 *
 * <p>The client is thread-safe. Create one per server and close it on
 * shutdown.
 */
public final class LeonsDBClient implements AutoCloseable {

    /** The API protocol version this connector speaks (see GetInfo). */
    public static final int PROTOCOL_VERSION = 1;

    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> REASON =
            Metadata.Key.of("leonsdb-error-reason", Metadata.ASCII_STRING_MARSHALLER);
    private static final Metadata.Key<String> WRITE_INDEX =
            Metadata.Key.of("leonsdb-write-index", Metadata.ASCII_STRING_MARSHALLER);

    private final ManagedChannel channel;
    // The channel's threads, ours rather than gRPC's shared ones, so close()
    // can wait until they have ended: a plugin's classes are gone once it is
    // disabled, and a thread still running then fails to load any. Null for
    // channels made by tests.
    private volatile io.grpc.netty.shaded.io.netty.channel.EventLoopGroup eventLoop;
    private volatile java.util.concurrent.ExecutorService callbacks;
    private final ConnectionSettings settings;
    private final String clientName;
    private final DataServiceGrpc.DataServiceFutureStub data;
    private final LeaseServiceGrpc.LeaseServiceFutureStub leases;
    private final ClientInterceptor[] authInterceptors;
    // Saves, loads, leases and published messages go over the wire protocol
    // when the server offers it (null: turned off with -Dleonsdb.wire=false);
    // subscriptions and lease watches use gRPC.
    private final WireChannel wire;
    // Saves made at the same time go together (ApplyMany).
    private final ApplyBatcher saves;

    /**
     * Creates a client. The network connection is opened lazily on the first
     * request and re-established automatically if it drops.
     *
     * @param settings   where to connect
     * @param clientName identifies this client in server logs, for example
     *                   "leonsdb-paper/0.1.0"
     * @return the client
     */
    public static LeonsDBClient connect(ConnectionSettings settings, String clientName) {
        var eventLoop = new io.grpc.netty.shaded.io.netty.channel.MultiThreadIoEventLoopGroup(2,
                new io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory("leonsdb-grpc", true),
                io.grpc.netty.shaded.io.netty.channel.nio.NioIoHandler.newFactory());
        var callbacks = java.util.concurrent.Executors.newCachedThreadPool(
                new io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory("leonsdb-grpc-callback", true));
        var builder = NettyChannelBuilder.forAddress(settings.host(), settings.port())
                .eventLoopGroup(eventLoop)
                .channelType(io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel.class)
                // After close(), a late call fails at once on the caller's thread.
                .executor(task -> {
                    try {
                        callbacks.execute(task);
                    } catch (java.util.concurrent.RejectedExecutionException closed) {
                        task.run();
                    }
                })
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true);
        if (settings.tls()) {
            builder.useTransportSecurity();
        } else {
            builder.usePlaintext();
        }
        var client = new LeonsDBClient(builder.build(), settings, clientName);
        client.eventLoop = eventLoop;
        client.callbacks = callbacks;
        return client;
    }

    // Visible for tests, which pass an in-process channel.
    LeonsDBClient(ManagedChannel channel, ConnectionSettings settings, String clientName) {
        this.channel = Objects.requireNonNull(channel, "channel");
        this.settings = Objects.requireNonNull(settings, "settings");
        this.clientName = Objects.requireNonNull(clientName, "clientName");
        if (settings.token() != null) {
            Metadata headers = new Metadata();
            headers.put(AUTHORIZATION, "Bearer " + settings.token());
            this.authInterceptors = new ClientInterceptor[] {MetadataUtils.newAttachHeadersInterceptor(headers)};
        } else {
            this.authInterceptors = new ClientInterceptor[0];
        }
        this.wire = Boolean.parseBoolean(System.getProperty("leonsdb.wire", "true"))
                ? new WireChannel(channel, settings.tls(), settings.token(), clientName) : null;
        io.grpc.Channel hot = wire != null ? wire : channel;
        this.data = DataServiceGrpc.newFutureStub(hot).withInterceptors(authInterceptors);
        this.leases = LeaseServiceGrpc.newFutureStub(hot).withInterceptors(authInterceptors);
        this.saves = new ApplyBatcher(
                req -> call("Apply", stub().apply(req)),
                req -> call("ApplyMany", stub().applyMany(req)),
                outcome -> {
                    var e = outcome.getError();
                    Status.Code code = Status.fromCodeValue(e.getCode()).getCode();
                    return new RequestException("LeonsDB Apply at " + settings.host() + ":" + settings.port() + " failed (" + code + ")"
                            + (e.getMessage().isEmpty() ? "" : ": " + e.getMessage()),
                            code, e.getReason().isEmpty() ? null : e.getReason(), -1, null);
                });
    }

    /**
     * Asks the server for its version and checks that it speaks our protocol.
     *
     * @return the server info, or a failure with {@link IncompatibleServerException}
     */
    public CompletableFuture<ServerInfo> fetchServerInfo() {
        var request = GetInfoRequest.newBuilder()
                .setClientProtocolVersion(PROTOCOL_VERSION)
                .setClientName(clientName)
                .build();
        var stub = ServerServiceGrpc.newFutureStub(channel).withDeadlineAfter(timeoutMillis(), TimeUnit.MILLISECONDS);
        return call("GetInfo", stub.getInfo(request)).thenApply(r -> {
            if (wire != null) {
                wire.target(settings.host(), r.getWirePort());
            }
            var info = toServerInfo(r);
            if (!info.supports(PROTOCOL_VERSION)) {
                throw new IncompatibleServerException(info, PROTOCOL_VERSION);
            }
            return info;
        });
    }

    /**
     * Creates a collection if needed and declares its fields.
     *
     * @param namespace  namespace
     * @param collection collection
     * @param kind       player or global
     * @param schema     declared fields
     * @return the server's answer
     */
    public CompletableFuture<EnsureCollectionResponse> ensureCollection(String namespace, String collection,
                                                                      CollectionKind kind, Schema schema) {
        var req = EnsureCollectionRequest.newBuilder()
                .setNamespace(namespace).setCollection(collection).setKind(kind).setSchema(schema).build();
        return call("EnsureCollection", stub().ensureCollection(req));
    }

    /**
     * Reads a record.
     *
     * @param ref the record
     * @return the response; {@code found} is false for a missing record
     */
    public CompletableFuture<GetResponse> get(RecordRef ref) {
        return call("Get", stub().get(GetRequest.newBuilder().setRef(ref).build()));
    }

    /**
     * Atomically applies operations to a record.
     *
     * @param ref             the record
     * @param ops             the operations
     * @param epoch           the writer's fencing epoch
     * @param expectedVersion if present, the version the record must have
     * @return the new version and touched fields
     */
    public CompletableFuture<ApplyResponse> apply(RecordRef ref, List<Operation> ops, long epoch, OptionalLong expectedVersion) {
        var req = ApplyRequest.newBuilder().setRef(ref).addAllOperations(ops).setEpoch(epoch);
        expectedVersion.ifPresent(req::setExpectedVersion);
        return saves.apply(req.build());
    }

    /**
     * Saves operations to a record, as {@link #apply} does, but the response
     * carries only the new version: for a caller that holds the values
     * already, which saves the server encoding and sending them.
     *
     * @param ref             the record
     * @param ops             the operations
     * @param epoch           the writer's fencing epoch
     * @param expectedVersion if present, the version the record must have
     * @return the new version, without fields
     */
    public CompletableFuture<ApplyResponse> save(RecordRef ref, List<Operation> ops, long epoch, OptionalLong expectedVersion) {
        var req = ApplyRequest.newBuilder().setRef(ref).addAllOperations(ops).setEpoch(epoch).setOmitFields(true);
        expectedVersion.ifPresent(req::setExpectedVersion);
        return saves.apply(req.build());
    }

    /**
     * Atomically applies a transaction.
     *
     * @param request the writes and the transaction id
     * @return the result of each write
     */
    public CompletableFuture<TransactResponse> transact(TransactRequest request) {
        return call("Transact", stub().transact(request));
    }

    /**
     * Deletes a record.
     *
     * @param ref the record
     * @return true if it existed
     */
    public CompletableFuture<Boolean> delete(RecordRef ref) {
        return call("Delete", stub().delete(DeleteRequest.newBuilder().setRef(ref).build()))
                .thenApply(r -> r.getExisted());
    }

    /**
     * Reads a page of a leaderboard.
     *
     * @param namespace      namespace
     * @param collection     collection
     * @param field          indexed field
     * @param offset         entries to skip
     * @param limit          entries to return
     * @param ascending      lowest first
     * @param includeRecords also return the records
     * @return the page and the total number of ranked records
     */
    public CompletableFuture<TopResponse> top(String namespace, String collection, String field,
                                              int offset, int limit, boolean ascending, boolean includeRecords) {
        return call("Top", stub().top(TopRequest.newBuilder()
                .setNamespace(namespace).setCollection(collection).setField(field)
                .setOffset(offset).setLimit(limit).setAscending(ascending).setIncludeRecords(includeRecords)
                .build()));
    }

    /**
     * Reads one record's position on a leaderboard.
     *
     * @param ref       the record
     * @param field     indexed field
     * @param ascending lowest first
     * @return the rank, if ranked
     */
    public CompletableFuture<RankResponse> rank(RecordRef ref, String field, boolean ascending) {
        return call("Rank", stub().rank(RankRequest.newBuilder().setRef(ref).setField(field).setAscending(ascending).build()));
    }

    /**
     * Takes ownership of a player, waiting for the current owner to hand over.
     *
     * @param player     16-byte UUID
     * @param holderId   this connector instance's identity
     * @param holderName readable name of this Minecraft server
     * @param waitMillis how long the server may wait for a handover
     * @return the epoch to write with
     */
    public CompletableFuture<AcquireLeaseResponse> acquireLease(ByteString player, String holderId, String holderName, long waitMillis) {
        return acquireLease(player, holderId, holderName, waitMillis, java.util.List.of());
    }

    /**
     * Takes ownership of a player and reads their records in the given
     * player collections in the same call. Check {@code loaded} in the
     * answer: servers before 0.9 do not read them.
     *
     * @param player     the player's UUID bytes
     * @param holderId   this connector instance's identity
     * @param holderName this server's name
     * @param waitMillis how long to wait for a handover
     * @param load       collections as "namespace.collection"
     * @return the epoch and the records that exist
     */
    public CompletableFuture<AcquireLeaseResponse> acquireLease(ByteString player, String holderId, String holderName, long waitMillis,
                                                              java.util.List<String> load) {
        return acquireLease(player, holderId, holderName, waitMillis, load, false);
    }

    /**
     * Like {@link #acquireLease(ByteString, String, String, long, java.util.List)},
     * for a holder that calls {@link #joined} once the player has joined.
     *
     * @param player       the player's UUID bytes
     * @param holderId     this connector instance's identity
     * @param holderName   this server's name
     * @param waitMillis   how long to wait for a handover
     * @param load         collections as "namespace.collection"
     * @param confirmsJoin whether this holder confirms joins
     * @return the epoch and the records that exist
     */
    public CompletableFuture<AcquireLeaseResponse> acquireLease(ByteString player, String holderId, String holderName, long waitMillis,
                                                              java.util.List<String> load, boolean confirmsJoin) {
        var req = AcquireLeaseRequest.newBuilder()
                .setPlayer(player).setHolderId(holderId).setHolderName(holderName)
                .setWaitMs((int) Math.min(waitMillis, 30_000)).addAllLoad(load).setConfirmsJoin(confirmsJoin).build();
        // The call itself may take as long as the wait, plus the usual timeout.
        var stub = leases.withDeadlineAfter(timeoutMillis() + waitMillis, TimeUnit.MILLISECONDS);
        return call("AcquireLease", stub.acquire(req));
    }

    /**
     * Takes back a player this server handed over for a server switch that
     * did not complete: like {@link #acquireLease}, but once nobody owns the
     * player, never asking the owner to hand over. Fails with
     * OWNED_ELSEWHERE while the player is owned or wanted elsewhere, and with
     * UNIMPLEMENTED on servers before 0.10.
     *
     * @param player     the player's UUID bytes
     * @param holderId   this connector instance's identity
     * @param holderName this server's name
     * @param waitMillis how long to wait for the player to be free
     * @param load       collections as "namespace.collection"
     * @return the epoch and the records that exist
     */
    public CompletableFuture<AcquireLeaseResponse> takeBackLease(ByteString player, String holderId, String holderName,
                                                               long waitMillis, java.util.List<String> load) {
        return takeBackLease(player, holderId, holderName, waitMillis, load, false);
    }

    /**
     * Like {@link #takeBackLease(ByteString, String, String, long, java.util.List)},
     * for a holder that calls {@link #joined} for its players.
     *
     * @param player       the player's UUID bytes
     * @param holderId     this connector instance's identity
     * @param holderName   this server's name
     * @param waitMillis   how long to wait for the player to be free
     * @param load         collections as "namespace.collection"
     * @param confirmsJoin whether this holder confirms joins
     * @return the epoch and the records that exist
     */
    public CompletableFuture<AcquireLeaseResponse> takeBackLease(ByteString player, String holderId, String holderName,
                                                               long waitMillis, java.util.List<String> load, boolean confirmsJoin) {
        var req = AcquireLeaseRequest.newBuilder()
                .setPlayer(player).setHolderId(holderId).setHolderName(holderName)
                .setWaitMs((int) Math.min(waitMillis, 30_000)).addAllLoad(load).setConfirmsJoin(confirmsJoin).build();
        var stub = leases.withDeadlineAfter(timeoutMillis() + waitMillis, TimeUnit.MILLISECONDS);
        return call("TakeBackLease", stub.takeBack(req));
    }

    /**
     * Says a player this holder owns has joined it. Fails with
     * UNIMPLEMENTED on servers from before join confirmations.
     *
     * @param player   the player's UUID bytes
     * @param holderId this connector instance's identity
     * @param epoch    the epoch the player joined under
     * @return false if this holder does not own the player with this epoch
     */
    public CompletableFuture<Boolean> joined(ByteString player, String holderId, long epoch) {
        var stub = leases.withDeadlineAfter(timeoutMillis(), TimeUnit.MILLISECONDS);
        return call("Joined", stub.joined(com.leonsdb.proto.v1.JoinedRequest.newBuilder()
                .setPlayer(player).setHolderId(holderId).setEpoch(epoch).build()))
                .thenApply(com.leonsdb.proto.v1.JoinedResponse::getConfirmed);
    }

    /**
     * Who owns a player now, and whether the player has joined that server.
     * Fails with UNIMPLEMENTED on servers from before join confirmations.
     *
     * @param player the player's UUID bytes
     * @return the owner, if any
     */
    public CompletableFuture<com.leonsdb.proto.v1.GetOwnerResponse> getOwner(ByteString player) {
        var stub = leases.withDeadlineAfter(timeoutMillis(), TimeUnit.MILLISECONDS);
        return call("GetOwner", stub.getOwner(com.leonsdb.proto.v1.GetOwnerRequest.newBuilder().setPlayer(player).build()));
    }

    /**
     * Renews all leases of this connector instance.
     *
     * @param holderId this connector instance's identity
     * @return lost players and handover requests
     */
    public CompletableFuture<RenewLeasesResponse> renewLeases(String holderId) {
        var stub = leases.withDeadlineAfter(timeoutMillis(), TimeUnit.MILLISECONDS);
        return call("RenewLeases", stub.renew(RenewLeasesRequest.newBuilder().setHolderId(holderId).build()));
    }

    /**
     * Gives up ownership of a player.
     *
     * @param player   16-byte UUID
     * @param holderId this connector instance's identity
     * @param epoch    the epoch of the lease being released
     * @return true if it was released
     */
    public CompletableFuture<Boolean> releaseLease(ByteString player, String holderId, long epoch) {
        var stub = leases.withDeadlineAfter(timeoutMillis(), TimeUnit.MILLISECONDS);
        return call("ReleaseLease", stub.release(ReleaseLeaseRequest.newBuilder()
                .setPlayer(player).setHolderId(holderId).setEpoch(epoch).build()))
                .thenApply(ReleaseLeaseResponse::getReleased);
    }

    /**
     * Answers a request to take part in another server's transaction.
     *
     * @param request the answer
     * @return completes when the server received it
     */
    public CompletableFuture<PreparedResponse> prepared(PreparedRequest request) {
        var stub = leases.withDeadlineAfter(timeoutMillis(), TimeUnit.MILLISECONDS);
        return call("Prepared", stub.prepared(request));
    }

    /**
     * Asks what became of a transaction this server prepared a write for.
     *
     * @param transactionId the transaction
     * @param prepareId     the attempt that was prepared
     * @return the outcome
     */
    public CompletableFuture<GetTransactionOutcomeResponse> transactionOutcome(ByteString transactionId, ByteString prepareId) {
        var stub = leases.withDeadlineAfter(timeoutMillis(), TimeUnit.MILLISECONDS);
        return call("GetTransactionOutcome", stub.getTransactionOutcome(GetTransactionOutcomeRequest.newBuilder()
                .setTransactionId(transactionId).setPrepareId(prepareId).build()));
    }

    /**
     * Says a player is about to join another server (a proxy's hint): the
     * server that has the player saves them now, and LeonsDB reads their
     * data into memory. Ownership does not change.
     *
     * @param player 16-byte UUID
     * @param target name of the server the player is about to join; empty
     *               for a first join
     * @return true if a server had the player and was told
     */
    public CompletableFuture<Boolean> expect(ByteString player, String target) {
        var stub = leases.withDeadlineAfter(timeoutMillis(), TimeUnit.MILLISECONDS);
        return call("Expect", stub.expect(ExpectRequest.newBuilder().setPlayer(player).setTarget(target).build()))
                .thenApply(ExpectResponse::getOwnerTold);
    }

    /**
     * Streams lease events for this connector instance until the stream
     * fails or is cancelled.
     *
     * @param holderId   this connector instance's identity
     * @param holderName readable name of this Minecraft server
     * @param observer   receives the events; pass a ClientResponseObserver to
     *                   be able to cancel the stream
     */
    public void watchLeases(String holderId, String holderName, StreamObserver<LeaseEvent> observer) {
        LeaseServiceGrpc.newStub(channel).withInterceptors(authInterceptors)
                .watch(WatchLeasesRequest.newBuilder().setHolderId(holderId).setHolderName(holderName).build(), observer);
    }

    /**
     * Publishes a message on a channel.
     *
     * @param channelName {@code <namespace>.<name>}
     * @param text        the message
     * @param sender      who sends it, for example this server's name
     * @return how many subscribers received it
     */
    public CompletableFuture<PublishResponse> publish(String channelName, String text, String sender) {
        var stub = MessageServiceGrpc.newFutureStub(wire != null ? wire : channel).withInterceptors(authInterceptors)
                .withDeadlineAfter(timeoutMillis(), TimeUnit.MILLISECONDS);
        return call("Publish", stub.publish(PublishRequest.newBuilder()
                .setChannel(channelName).setText(text).setSender(sender).build()));
    }

    /**
     * Streams the messages of channels until the stream fails or is
     * cancelled.
     *
     * @param channels the channels
     * @param observer receives the messages; pass a ClientResponseObserver to
     *                 be able to cancel the stream
     */
    public void subscribe(java.util.List<String> channels, StreamObserver<ChannelMessage> observer) {
        // Over gRPC: on a busy server, subscriptions over the wire protocol
        // slowed down the publishing of messages (benchmark round d3c).
        MessageServiceGrpc.newStub(channel).withInterceptors(authInterceptors)
                .subscribe(SubscribeRequest.newBuilder().addAllChannels(channels).build(), observer);
    }

    /**
     * Reads a record as it was at a time in the past.
     *
     * @param ref    the record
     * @param timeMs milliseconds since the Unix epoch
     * @return the record then, if it existed
     */
    public CompletableFuture<GetAtResponse> getAt(RecordRef ref, long timeMs) {
        var stub = HistoryServiceGrpc.newFutureStub(channel).withInterceptors(authInterceptors)
                .withDeadlineAfter(timeoutMillis(), TimeUnit.MILLISECONDS);
        return call("GetAt", stub.getAt(GetAtRequest.newBuilder().setRef(ref).setTime(timeMs).build()));
    }

    /**
     * Lists a record's history, newest first.
     *
     * @param ref   the record
     * @param limit at most this many entries
     * @return the entries
     */
    public CompletableFuture<ListHistoryResponse> listHistory(RecordRef ref, int limit) {
        var stub = HistoryServiceGrpc.newFutureStub(channel).withInterceptors(authInterceptors)
                .withDeadlineAfter(timeoutMillis(), TimeUnit.MILLISECONDS);
        return call("ListHistory", stub.listHistory(ListHistoryRequest.newBuilder().setRef(ref).setLimit(limit).build()));
    }

    /**
     * Runs a rollback and collects its progress messages.
     *
     * @param request the rollback
     * @return the final progress message, with the records of all messages
     */
    public CompletableFuture<RollbackProgress> rollback(RollbackRequest request) {
        var result = new CompletableFuture<RollbackProgress>();
        var records = new java.util.ArrayList<RecordRollback>();
        HistoryServiceGrpc.newStub(channel).withInterceptors(authInterceptors)
                .withDeadlineAfter(10, TimeUnit.MINUTES)
                .rollback(request, new StreamObserver<>() {
                    private RollbackProgress last;

                    @Override
                    public void onNext(RollbackProgress p) {
                        records.addAll(p.getRecordsList());
                        last = p;
                    }

                    @Override
                    public void onError(Throwable t) {
                        result.completeExceptionally(translate("Rollback", t));
                    }

                    @Override
                    public void onCompleted() {
                        if (last == null || !last.getDone()) {
                            result.completeExceptionally(new LeonsDBException("LeonsDB ended the rollback without a result"));
                            return;
                        }
                        result.complete(last.toBuilder().clearRecords().addAllRecords(records).build());
                    }
                });
        return result;
    }

    // Visible for tests: subscribe over the wire protocol, which serves
    // streams of answers as well.
    void subscribeOverWire(java.util.List<String> channels, StreamObserver<ChannelMessage> observer) {
        MessageServiceGrpc.newStub(wire).withInterceptors(authInterceptors)
                .subscribe(SubscribeRequest.newBuilder().addAllChannels(channels).build(), observer);
    }

    // Visible for tests: whether saves, loads and leases use the wire protocol.
    boolean usesWireProtocol() {
        return wire != null && wire.connected();
    }

    /**
     * Closes the connection, giving in-flight requests up to five seconds to
     * finish.
     */
    @Override
    public void close() {
        if (wire != null) {
            wire.shutdown();
        }
        channel.shutdown();
        try {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
            stopThreads(5);
        } catch (InterruptedException e) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /** Ends the channel's threads, waiting up to the given seconds for each kind. */
    private void stopThreads(long seconds) throws InterruptedException {
        var loop = eventLoop;
        if (loop != null) {
            loop.shutdownGracefully(0, seconds, TimeUnit.SECONDS).await(seconds, TimeUnit.SECONDS);
        }
        var cb = callbacks;
        if (cb != null) {
            cb.shutdown();
            if (!cb.awaitTermination(seconds, TimeUnit.SECONDS)) {
                cb.shutdownNow();
            }
        }
    }

    /**
     * Closes the connection immediately, failing requests still in flight.
     * For emergencies; prefer {@link #close()}.
     */
    public void abort() {
        if (wire != null) {
            wire.shutdown();
        }
        channel.shutdownNow();
        try {
            stopThreads(0);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private DataServiceGrpc.DataServiceFutureStub stub() {
        return data.withDeadlineAfter(timeoutMillis(), TimeUnit.MILLISECONDS);
    }

    private long timeoutMillis() {
        return settings.requestTimeout().toMillis();
    }

    private <T> CompletableFuture<T> call(String method, ListenableFuture<T> future) {
        var result = new CompletableFuture<T>();
        Futures.addCallback(future, new FutureCallback<T>() {
            @Override
            public void onSuccess(T value) {
                result.complete(value);
            }

            @Override
            public void onFailure(Throwable t) {
                result.completeExceptionally(translate(method, t));
            }
        }, MoreExecutors.directExecutor());
        return result;
    }

    private LeonsDBException translate(String method, Throwable t) {
        Status status = Status.fromThrowable(t);
        String reason = null;
        int writeIndex = -1;
        if (t instanceof StatusRuntimeException sre && sre.getTrailers() != null) {
            reason = sre.getTrailers().get(REASON);
            String index = sre.getTrailers().get(WRITE_INDEX);
            if (index != null) {
                try {
                    writeIndex = Integer.parseInt(index);
                } catch (NumberFormatException ignored) {
                    // a malformed trailer is treated as absent
                }
            }
        }
        String desc = status.getDescription() == null ? "" : ": " + status.getDescription();
        return new RequestException("LeonsDB " + method + " at " + settings.host() + ":" + settings.port()
                + " failed (" + status.getCode() + ")" + desc, status.getCode(), reason, writeIndex, t);
    }

    private static ServerInfo toServerInfo(GetInfoResponse r) {
        return new ServerInfo(r.getServerVersion(), r.getBuildCommit(),
                r.getMinProtocolVersion(), r.getMaxProtocolVersion(), r.getInstanceId());
    }
}
