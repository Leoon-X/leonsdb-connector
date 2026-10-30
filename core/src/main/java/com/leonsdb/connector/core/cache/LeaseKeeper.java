// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.cache;

import com.google.protobuf.ByteString;
import com.leonsdb.connector.api.LeonsDBException;
import com.leonsdb.connector.core.LeonsDBClient;
import com.leonsdb.proto.v1.LeaseEvent;
import com.leonsdb.proto.v1.WatchLeasesRequest;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Holds this Minecraft server's ownership leases: acquires them when
 * players join, renews them in the background, releases them when players
 * leave, and listens for other servers asking for a player.
 *
 * <p>Each connector instance has its own random holder id, so a restarted
 * Minecraft server never mistakes itself for its previous run.
 */
final class LeaseKeeper {

    private static final long DEFAULT_TTL_MILLIS = 10_000;
    private static final long WATCH_RETRY_MILLIS = 1_000;

    private final LeonsDBClient client;
    private final Logger log;
    private final ScheduledExecutorService scheduler;
    private final String holderId = UUID.randomUUID().toString();
    private final String holderName;

    /** Players this server owns, with their epochs. */
    private final Map<UUID, Long> held = new ConcurrentHashMap<>();

    private volatile Consumer<UUID> onHandoverRequested = uuid -> { };
    private volatile Consumer<UUID> onLost = uuid -> { };
    private volatile Consumer<LeaseEvent> onTransaction = ev -> { };
    private volatile Runnable onReplaced = () -> { };
    private volatile Consumer<UUID> onSwitchExpected = uuid -> { };
    /** The instance id of the database, from the last time it was asked. */
    private volatile String instanceId;
    private volatile long ttlMillis = DEFAULT_TTL_MILLIS;
    private volatile boolean stopped;
    private volatile java.util.concurrent.ScheduledFuture<?> renewTask;
    private volatile ClientCallStreamObserver<WatchLeasesRequest> watchCall;

    LeaseKeeper(LeonsDBClient client, String holderName, Logger log, ScheduledExecutorService scheduler) {
        this.client = client;
        this.holderName = holderName;
        this.log = log;
        this.scheduler = scheduler;
    }

    /**
     * Starts renewing and watching.
     *
     * @param handover called when another server waits for one of our players
     * @param lost     called when we lost a player (our lease expired and another server took it)
     * @param transaction called with KIND_PREPARE and KIND_OUTCOME events
     * @param replaced    called when LeonsDB answers with another instance id
     *                    (its data was restored from a backup)
     * @param switchExpected called when a proxy says one of our players is
     *                       about to switch servers (a hint; nothing changes)
     */
    void start(Consumer<UUID> handover, Consumer<UUID> lost, Consumer<LeaseEvent> transaction, Runnable replaced,
               Consumer<UUID> switchExpected) {
        this.onSwitchExpected = switchExpected;
        this.onHandoverRequested = handover;
        this.onLost = lost;
        this.onTransaction = transaction;
        this.onReplaced = replaced;
        watch();
        scheduleRenew();
    }

    /**
     * Takes ownership of a player. Blocks; never call on the main thread.
     *
     * @param uuid the player
     * @param wait how long to wait for another server to hand the player over
     * @return the epoch to write with
     * @throws LeonsDBException if ownership could not be obtained in time
     */
    long acquire(UUID uuid, Duration wait) {
        return acquire(uuid, wait, java.util.List.of()).getEpoch();
    }

    /**
     * Takes ownership of a player and reads their records in the given
     * collections in the same call (see {@link LeonsDBClient#acquireLease}).
     * Blocks; never call on the main thread.
     *
     * @param uuid the player
     * @param wait how long to wait for another server to hand the player over
     * @param load collections to read, as "namespace.collection"
     * @return the answer, with the epoch to write with
     * @throws LeonsDBException if ownership could not be obtained in time
     */
    com.leonsdb.proto.v1.AcquireLeaseResponse acquire(UUID uuid, Duration wait, java.util.List<String> load) {
        return acquire(uuid, wait, load, null);
    }

    /**
     * Like {@link #acquire(UUID, Duration, java.util.List)}, also telling LeonsDB the player's name.
     *
     * @param uuid the player
     * @param wait how long to wait for another server to hand the player over
     * @param load collections to read, as "namespace.collection"
     * @param name the player's name, or null
     * @return the answer, with the epoch to write with
     */
    com.leonsdb.proto.v1.AcquireLeaseResponse acquire(UUID uuid, Duration wait, java.util.List<String> load, String name) {
        try {
            var resp = client.acquireLease(bytes(uuid), holderId, holderName, wait.toMillis(), load, name, true)
                    .get(wait.toMillis() + 10_000, TimeUnit.MILLISECONDS);
            if (resp.getTtlMs() > 0 && resp.getTtlMs() != ttlMillis) {
                ttlMillis = resp.getTtlMs();
                scheduleRenew();
            }
            held.put(uuid, resp.getEpoch());
            return resp;
        } catch (ExecutionException e) {
            throw new LeonsDBException(e.getCause().getMessage(), e.getCause());
        } catch (TimeoutException e) {
            throw new LeonsDBException("LeonsDB did not grant ownership in time", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LeonsDBException("Interrupted while acquiring ownership", e);
        }
    }

    /**
     * Takes back a player handed over for a switch that did not complete,
     * once nobody owns them (see {@link LeonsDBClient#takeBackLease}).
     *
     * @param uuid the player
     * @param wait how long to wait for the player to be free
     * @param load collections to read, as "namespace.collection"
     * @param name the player's name, or null
     * @return completes with the answer, or fails if the player is not free
     */
    java.util.concurrent.CompletableFuture<com.leonsdb.proto.v1.AcquireLeaseResponse> takeBack(
            UUID uuid, Duration wait, java.util.List<String> load, String name) {
        return client.takeBackLease(bytes(uuid), holderId, holderName, wait.toMillis(), load, name, true).thenApply(resp -> {
            held.put(uuid, resp.getEpoch());
            return resp;
        });
    }

    /**
     * Tells LeonsDB that a player this server owns has joined it. Failures
     * are only logged: the server they came from then keeps them a little
     * longer before deciding they logged in somewhere else.
     *
     * @param uuid the player
     */
    void joined(UUID uuid) {
        Long epoch = held.get(uuid);
        if (epoch == null) {
            return;
        }
        client.joined(bytes(uuid), holderId, epoch).whenComplete((ok, err) -> {
            if (err != null) {
                log.log(Level.FINE, "Could not confirm that " + uuid + " joined", err);
            }
        });
    }

    /**
     * Who owns a player now, and whether they joined that server.
     *
     * @param uuid the player
     * @return the owner; fails if LeonsDB did not answer
     */
    java.util.concurrent.CompletableFuture<com.leonsdb.proto.v1.GetOwnerResponse> owner(UUID uuid) {
        return client.getOwner(bytes(uuid));
    }

    /**
     * Gives up ownership once the player's data is saved. Failures are only
     * logged: an unreleased lease simply expires.
     *
     * <p>The returned future completes once LeonsDB has handled the release
     * (or it failed). Until then the player must not be loaded again here:
     * LeonsDB would grant this server the player at the same epoch, and the
     * late release would then end the new ownership.
     *
     * @param uuid the player
     * @return completes when the release is done; never fails
     */
    java.util.concurrent.CompletableFuture<Void> release(UUID uuid) {
        Long epoch = held.remove(uuid);
        if (epoch == null) {
            return java.util.concurrent.CompletableFuture.completedFuture(null);
        }
        return client.releaseLease(bytes(uuid), holderId, epoch).handle((ok, err) -> {
            if (err != null) {
                log.log(Level.FINE, "Could not release ownership of " + uuid + "; it will expire on its own", err);
            }
            return null;
        });
    }

    /**
     * Releases all leases and stops renewing. Waits briefly, so players can
     * join another server right away after this one stops.
     *
     * @param timeout how long to wait for the releases
     */
    void stop(Duration timeout) {
        stopWatching();
        var releases = held.entrySet().stream()
                .map(e -> client.releaseLease(bytes(e.getKey()), holderId, e.getValue()))
                .toArray(java.util.concurrent.CompletableFuture<?>[]::new);
        held.clear();
        try {
            java.util.concurrent.CompletableFuture.allOf(releases).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            log.fine("Not all leases could be released; they expire on their own");
        }
    }

    /**
     * Stops renewing and listening, without releasing anything. Leases that
     * are not released expire on their own.
     */
    void stopWatching() {
        stopped = true;
        if (renewTask != null) {
            renewTask.cancel(false);
        }
        var call = watchCall;
        if (call != null) {
            call.cancel("connector stopping", null);
        }
    }

    /**
     * Whether this server currently owns the player.
     *
     * @param uuid the player
     * @return true if owned
     */
    boolean owns(UUID uuid) {
        return held.containsKey(uuid);
    }

    /**
     * The epoch this server owns the player with.
     *
     * @param uuid the player
     * @return the epoch, or 0 if not owned
     */
    long epoch(UUID uuid) {
        return held.getOrDefault(uuid, 0L);
    }

    /** @return this connector instance's identity */
    String holderId() {
        return holderId;
    }

    /**
     * Forgets a lease without releasing it (it was lost).
     *
     * @param uuid the player
     */
    void forget(UUID uuid) {
        held.remove(uuid);
    }

    // --- Background work -------------------------------------------------

    private synchronized void scheduleRenew() {
        if (stopped) {
            return;
        }
        if (renewTask != null) {
            renewTask.cancel(false);
        }
        long period = Math.max(500, ttlMillis / 3);
        renewTask = scheduler.scheduleAtFixedRate(this::renew, period, period, TimeUnit.MILLISECONDS);
    }

    private void renew() {
        if (held.isEmpty()) {
            return;
        }
        client.renewLeases(holderId).whenComplete((resp, err) -> {
            if (err != null) {
                log.warning("Could not renew player ownership: " + err.getMessage()
                        + ". If this continues, players can be taken over by other servers.");
                return;
            }
            resp.getLostList().forEach(b -> lost(uuid(b)));
            resp.getHandoverRequestedList().forEach(b -> onHandoverRequested.accept(uuid(b)));
        });
    }

    // Every (re)connection of the stream also asks which database this is:
    // LeonsDB restarts to restore a backup, so the stream always drops then.
    // The stream opens right away, so no event is missed meanwhile; until the
    // check is done, stale writes are refused by their epochs anyway.
    private void watch() {
        if (stopped) {
            return;
        }
        openWatch();
        checkInstance();
    }

    private void checkInstance() {
        client.fetchServerInfo().whenComplete((info, err) -> {
            if (err != null) {
                if (!stopped) {
                    scheduler.schedule(this::checkInstance, WATCH_RETRY_MILLIS, TimeUnit.MILLISECONDS);
                }
                return;
            }
            String known = instanceId;
            instanceId = info.instanceId();
            if (known != null && !known.equals(info.instanceId())) {
                held.clear();
                onReplaced.run();
            }
        });
    }

    private void openWatch() {
        if (stopped) {
            return;
        }
        client.watchLeases(holderId, holderName, new ClientResponseObserver<WatchLeasesRequest, LeaseEvent>() {
            @Override
            public void beforeStart(ClientCallStreamObserver<WatchLeasesRequest> call) {
                watchCall = call;
            }

            @Override
            public void onNext(LeaseEvent ev) {
                UUID uuid = uuid(ev.getPlayer());
                switch (ev.getKind()) {
                    case KIND_HANDOVER_REQUESTED -> onHandoverRequested.accept(uuid);
                    case KIND_LOST -> lost(uuid);
                    case KIND_PREPARE, KIND_OUTCOME -> onTransaction.accept(ev);
                    case KIND_SWITCH_EXPECTED -> onSwitchExpected.accept(uuid);
                    default -> { }
                }
            }

            @Override
            public void onError(Throwable t) {
                rewatch();
            }

            @Override
            public void onCompleted() {
                rewatch();
            }
        });
    }

    private void rewatch() {
        if (!stopped) {
            scheduler.schedule(this::watch, WATCH_RETRY_MILLIS, TimeUnit.MILLISECONDS);
        }
    }

    private void lost(UUID uuid) {
        if (held.remove(uuid) != null) {
            onLost.accept(uuid);
        }
    }

    static ByteString bytes(UUID uuid) {
        ByteBuffer b = ByteBuffer.allocate(16);
        b.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
        return ByteString.copyFrom(b.array());
    }

    static UUID uuid(ByteString bytes) {
        ByteBuffer b = bytes.asReadOnlyByteBuffer();
        return new UUID(b.getLong(), b.getLong());
    }
}
