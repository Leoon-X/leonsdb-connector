// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.cache;

import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import com.leonsdb.connector.api.Channel;
import com.leonsdb.connector.api.DataNotLoadedException;
import com.leonsdb.connector.api.GlobalCollection;
import com.leonsdb.connector.api.History;
import com.leonsdb.connector.api.Leaderboard;
import com.leonsdb.connector.api.LeonsDBException;
import com.leonsdb.connector.api.LeonsDBProvider;
import com.leonsdb.connector.api.PlayerHandle;
import com.leonsdb.connector.api.Transaction;
import com.leonsdb.connector.core.LeonsDBClient;
import com.leonsdb.connector.core.schema.ClassSchema;
import com.leonsdb.connector.core.schema.Codecs;
import com.leonsdb.proto.v1.ApplyRequest;
import com.leonsdb.proto.v1.GetResponse;
import com.leonsdb.proto.v1.RecordRef;
import com.leonsdb.proto.v1.Value;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The platform-independent connector: registered classes, player ownership,
 * the player cache and its save loop. A platform plugin (Paper, which also
 * covers Folia) creates one, calls {@link #start}, installs it into
 * {@link com.leonsdb.connector.api.LeonsDB} and calls the lifecycle methods
 * from its events:
 *
 * <ul>
 *   <li>{@link #loadPlayer} before a player joins (blocking, off the main thread),</li>
 *   <li>{@link #flushTick} periodically on the main thread,</li>
 *   <li>{@link #unloadPlayer} when a player leaves (on the player's thread),</li>
 *   <li>{@link #shutdown} when the server stops.</li>
 * </ul>
 *
 * <p>Ownership: before a player's data is loaded, this server takes the
 * player's lease, so only one server at a time can write a player's data.
 * When another server asks for the player (a server switch), this server
 * saves the player's data and then releases the lease; the other server only
 * loads the data after that, so it always sees the latest state.
 */
public final class LeonsDBService implements LeonsDBProvider {

    /**
     * Connector settings.
     *
     * @param serverName    readable name of this Minecraft server, shown to
     *                      other servers waiting for a player
     * @param loadTimeout   how long loading a joining player's data may take
     * @param leaseWait     how long a join may wait for another server to
     *                      hand the player over
     * @param handoverGrace how long a player may stay on this server after
     *                      their data was handed to another server before
     *                      being disconnected
     * @param recoveryDir   where unsaved changes are written if the server is
     *                      unreachable at shutdown
     */
    public record Settings(String serverName, Duration loadTimeout, Duration leaseWait,
                           Duration handoverGrace, Path recoveryDir) {

        /** Validates the settings. */
        public Settings {
            Objects.requireNonNull(serverName, "serverName");
            Objects.requireNonNull(loadTimeout, "loadTimeout");
            Objects.requireNonNull(leaseWait, "leaseWait");
            Objects.requireNonNull(handoverGrace, "handoverGrace");
            Objects.requireNonNull(recoveryDir, "recoveryDir");
        }
    }

    private static final Duration REGISTER_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration UNLOAD_RETRY = Duration.ofSeconds(2);

    private final LeonsDBClient client;
    private final Codecs codecs;
    private final Logger log;
    private final Platform platform;
    private final Settings settings;

    private final Map<Class<?>, ClassSchema<?>> schemas = new ConcurrentHashMap<>();
    private final Map<UUID, PlayerSession> sessions = new ConcurrentHashMap<>();
    private final Set<Class<?>> warnedTransactional = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService background;
    private final LeaseKeeper leases;
    private final RemoteParticipant participant;
    private final Channels channels;
    private volatile boolean shutDown;

    /**
     * Creates the service. Call {@link #start} before players can join.
     *
     * @param client   connection to the server
     * @param codecs   storable types, including platform types
     * @param log      where to log
     * @param platform the Minecraft server software
     * @param settings connector settings
     */
    public LeonsDBService(LeonsDBClient client, Codecs codecs, Logger log, Platform platform, Settings settings) {
        this.client = client;
        this.codecs = codecs;
        this.log = log;
        this.platform = platform;
        this.settings = settings;
        this.background = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "LeonsDB background");
            t.setDaemon(true);
            return t;
        });
        this.leases = new LeaseKeeper(client, settings.serverName(), log, background);
        this.participant = new RemoteParticipant(this, client, leases::holderId, leases::epoch, background, log);
        this.channels = new Channels(client, platform, settings.serverName(), background, log);
    }

    /** Starts renewing ownership and listening for handover requests. */
    public void start() {
        leases.start(this::onHandoverRequested, this::onLost, participant::onEvent, this::onDatabaseReplaced,
                this::onSwitchExpected);
    }

    // --- LeonsDBProvider -------------------------------------------------------

    @Override
    public void register(Class<?>... classes) {
        for (Class<?> type : classes) {
            ClassSchema<?> schema;
            try {
                schema = ClassSchema.of(type, codecs);
            } catch (IllegalArgumentException e) {
                throw new LeonsDBException("Cannot use " + type.getName() + ": " + e.getMessage(), e);
            }
            try {
                client.ensureCollection(schema.namespace(), schema.collection(), schema.kind(), schema.toProto())
                        .get(REGISTER_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                throw new LeonsDBException("Cannot register " + type.getName() + ": " + e.getCause().getMessage(), e.getCause());
            } catch (TimeoutException e) {
                throw new LeonsDBException("Cannot register " + type.getName() + ": LeonsDB did not answer in time", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new LeonsDBException("Interrupted while registering " + type.getName(), e);
            }
            ClassSchema<?> previous = schemas.put(type, schema);
            if (previous == null && schema.isPlayerData() && !sessions.isEmpty()) {
                loadForOnlinePlayers(schema);
            }
        }
    }

    @Override
    public PlayerHandle player(UUID player) {
        return new PlayerHandleImpl(this, player);
    }

    @Override
    public <T> GlobalCollection<T> global(Class<T> type) {
        ClassSchema<T> schema = schema(type);
        if (schema.isPlayerData()) {
            throw new LeonsDBException(type.getName() + " is @PlayerData; use LeonsDB.player(uuid).get(...)");
        }
        return new GlobalCollectionImpl<>(schema, client);
    }

    @Override
    public Channel channel(String name) {
        return channels.channel(name);
    }

    @Override
    public Transaction transaction() {
        return new TransactionImpl(this, client, platform, log);
    }

    @Override
    public History history() {
        return new HistoryImpl(this, client);
    }

    @Override
    public <T> Leaderboard<T> leaderboard(Class<T> type, String field, Leaderboard.Order order) {
        ClassSchema<T> schema = schema(type);
        var binding = schema.fields().stream().filter(f -> f.name().equals(field)).findFirst()
                .orElseThrow(() -> new LeonsDBException(type.getSimpleName() + " has no field \"" + field + "\""));
        if (!binding.indexed()) {
            throw new LeonsDBException(type.getSimpleName() + "." + field + " is not @Indexed; mark it @Indexed to rank by it");
        }
        return new LeaderboardImpl<>(schema, field, order, client);
    }

    // --- Platform lifecycle ---------------------------------------------------

    /**
     * Takes ownership of a joining player and loads all their registered
     * data. Blocks, so call it from an asynchronous login event, never from
     * the main thread. If the player is still on another server (a server
     * switch), that server is asked to save and hand the player over first.
     *
     * @param uuid the player
     * @throws LeonsDBException if ownership or the data cannot be obtained;
     *                          the player must not be allowed to join, or
     *                          their (empty or outdated) data could
     *                          overwrite the real data later
     */
    public void loadPlayer(UUID uuid) {
        long deadline = System.nanoTime() + settings.loadTimeout().toNanos() + settings.leaseWait().toNanos();

        // If the player's previous session here is still being saved (a quick
        // rejoin), wait for it: loading now could read stale data.
        PlayerSession previous = sessions.get(uuid);
        if (previous != null && previous.isUnloading()) {
            try {
                previous.unloading.get(remaining(deadline), TimeUnit.NANOSECONDS);
            } catch (Exception e) {
                throw new LeonsDBException("Your data from your last visit is still being saved. Please try again in a moment.", e);
            }
        } else if (previous != null) {
            return; // already loaded
        }

        // Ownership and the player's records in one round trip.
        List<String> collections = new ArrayList<>();
        for (ClassSchema<?> schema : schemas.values()) {
            if (schema.isPlayerData()) {
                collections.add(schema.namespace() + "." + schema.collection());
            }
        }
        var acquired = leases.acquire(uuid, settings.leaseWait(), collections);

        PlayerSession session = new PlayerSession(uuid, acquired.getEpoch());
        List<CompletableFuture<Void>> loads = new ArrayList<>();
        Map<String, com.leonsdb.proto.v1.Record> loaded = new HashMap<>();
        acquired.getRecordsList().forEach(r -> loaded.put(r.getRef().getNamespace() + "." + r.getRef().getCollection(), r));
        for (ClassSchema<?> schema : schemas.values()) {
            if (!schema.isPlayerData()) {
                continue;
            }
            if (acquired.getLoaded()) {
                RecordRef ref = playerRef(schema, uuid);
                var rec = loaded.get(schema.namespace() + "." + schema.collection());
                GetResponse resp = rec == null ? GetResponse.getDefaultInstance() : GetResponse.newBuilder().setFound(true).setRecord(rec).build();
                putObject(session, fromResponse(session, schema, ref, resp));
            } else {
                loads.add(loadObject(session, schema)); // a server from before 0.9
            }
        }
        try {
            CompletableFuture.allOf(loads.toArray(new CompletableFuture<?>[0])).get(remaining(deadline), TimeUnit.NANOSECONDS);
        } catch (ExecutionException e) {
            releaseNow(uuid);
            throw new LeonsDBException("Could not load player data: " + e.getCause().getMessage(), e.getCause());
        } catch (TimeoutException e) {
            releaseNow(uuid);
            throw new LeonsDBException("Loading player data took too long", e);
        } catch (InterruptedException e) {
            leases.release(uuid);
            Thread.currentThread().interrupt();
            throw new LeonsDBException("Interrupted while loading player data", e);
        }
        sessions.put(uuid, session);
    }

    /** Releases a player after a failed load, before the player can retry. */
    private void releaseNow(UUID uuid) {
        try {
            leases.release(uuid).get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            // it expires on its own
        }
    }

    /**
     * Saves a leaving player's data, then releases their ownership and
     * removes the data from memory. Call on the player's thread (Paper: the
     * main thread; Folia: their region's). If LeonsDB is unreachable, saving
     * is retried in the background until it succeeds; the player keeps
     * being owned by this server meanwhile, so no other server can load
     * outdated data.
     *
     * @param uuid the player
     * @return completes when everything is saved and released
     */
    public CompletableFuture<Void> unloadPlayer(UUID uuid) {
        PlayerSession session = sessions.get(uuid);
        if (session == null) {
            return CompletableFuture.completedFuture(null);
        }
        CompletableFuture<Void> done = new CompletableFuture<>();
        Runnable save = () -> {
            Map<CachedObject<?>, Map<String, Value>> finals = new HashMap<>();
            for (CachedObject<?> obj : session.all()) {
                try {
                    finals.put(obj, obj.snapshot());
                } catch (IllegalArgumentException e) {
                    log.log(Level.SEVERE, "Cannot save " + obj.schema().type().getSimpleName() + " of " + uuid + ": " + e.getMessage());
                }
            }
            saveUntilDone(session, finals, done);
        };
        // A transaction in progress changes the cached data when it commits;
        // the final snapshot must include that, or saving it would undo it.
        // The last transaction to end may do so on another thread, so the
        // snapshot is then taken on the player's.
        CompletableFuture<Void> already = session.startUnloading(done, () -> onPlayerThreadNow(uuid, save));
        return already != null ? already : done;
    }

    private void saveUntilDone(PlayerSession session, Map<CachedObject<?>, Map<String, Value>> finals,
                               CompletableFuture<Void> done) {
        if (session.lost) {
            // Another server owns the player now; our saves would be refused.
            done.complete(null);
            return;
        }
        List<CompletableFuture<Void>> saves = new ArrayList<>();
        finals.forEach((obj, snap) -> {
            CompletableFuture<Void> w = new CompletableFuture<>();
            obj.submit(snap, w);
            saves.add(w);
        });
        CompletableFuture.allOf(saves.toArray(new CompletableFuture<?>[0])).whenComplete((ok, err) -> {
            if (session.lost) {
                sessions.remove(session.uuid, session);
                done.complete(null);
            } else if (err == null) {
                // The player has left only once released: until then a rejoin
                // waits, so it cannot overtake the release and be granted the
                // old epoch, which the late release would end.
                leases.release(session.uuid).thenRun(() -> {
                    sessions.remove(session.uuid, session);
                    done.complete(null);
                });
            } else {
                background.schedule(() -> saveUntilDone(session, finals, done), UNLOAD_RETRY.toMillis(), TimeUnit.MILLISECONDS);
            }
        });
    }

    /**
     * Saves what changed since the last tick for every online player. Call
     * periodically (about once per second) on the main thread. Players whose
     * data belongs to another thread (Folia regions) are saved on theirs.
     */
    public void flushTick() {
        for (PlayerSession session : sessions.values()) {
            if (session.isUnloading() || session.lost) {
                continue;
            }
            if (platform.isPlayerThread(session.uuid)) {
                flush(session);
            } else {
                platform.runForPlayer(session.uuid, () -> flush(session), Duration.ZERO);
            }
        }
    }

    /** Saves what changed of one player. On the player's thread. */
    private void flush(PlayerSession session) {
        if (session.isUnloading() || session.lost || sessions.get(session.uuid) != session) {
            return;
        }
        for (CachedObject<?> obj : session.all()) {
            try {
                Map<String, Value> snap = obj.snapshot();
                if (obj.schema().hasTransactional()) {
                    warnIfTransactionalChanged(obj, snap);
                }
                obj.submit(snap, null);
            } catch (IllegalArgumentException e) {
                log.log(Level.SEVERE, "Cannot save " + obj.schema().type().getSimpleName() + " of " + session.uuid
                        + ": " + e.getMessage());
            }
        }
    }

    /**
     * Players whose data is in memory, for platforms that clean up after
     * logins that never completed.
     *
     * @return loaded players and when they were loaded
     */
    public Map<UUID, Long> loadedPlayers() {
        Map<UUID, Long> out = new HashMap<>();
        sessions.forEach((uuid, s) -> {
            if (!s.isUnloading()) {
                out.put(uuid, s.loadedAtMillis);
            }
        });
        return out;
    }

    /**
     * Saves everything, releases all ownership and stops. Call on the main
     * thread when the server stops. Changes that cannot be confirmed within
     * the timeout are written to recovery files and logged.
     *
     * @param timeout how long to wait for the server
     */
    public void shutdown(Duration timeout) {
        // Transactions still running finish first (on this thread, which
        // is the main thread), so the final save includes their changes.
        shutDown = true;
        long deadline = System.nanoTime() + timeout.toNanos() / 2;
        while (sessions.values().stream().anyMatch(PlayerSession::hasTransactions) && System.nanoTime() < deadline) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        List<CompletableFuture<Void>> pending = new ArrayList<>();
        Map<CachedObject<?>, Map<String, Value>> finals = new HashMap<>();
        for (PlayerSession session : sessions.values()) {
            if (session.lost) {
                continue;
            }
            if (session.isUnloading()) {
                pending.add(session.unloading);
            }
            for (CachedObject<?> obj : session.all()) {
                Map<String, Value> snap;
                try {
                    snap = obj.snapshot();
                } catch (IllegalArgumentException e) {
                    log.log(Level.SEVERE, "Cannot save " + obj.schema().type().getSimpleName() + ": " + e.getMessage());
                    continue;
                }
                finals.put(obj, snap);
                CompletableFuture<Void> w = new CompletableFuture<>();
                obj.submit(snap, w);
                pending.add(w);
            }
        }
        boolean saved;
        try {
            CompletableFuture.allOf(pending.toArray(new CompletableFuture<?>[0])).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            log.info("All player data saved.");
            saved = true;
        } catch (Exception e) {
            writeRecoveryFiles(finals);
            saved = false;
        }
        // Release ownership so players can join another server right away.
        // If saving failed, keep the leases: they expire on their own, and
        // until then nobody loads data that is missing our last changes.
        if (saved) {
            leases.stop(Duration.ofSeconds(2));
        } else {
            leases.stopWatching();
        }
        channels.stop();
        background.shutdownNow();
    }

    // --- Ownership events --------------------------------------------------

    /** Another server is waiting for one of our players: save and hand over. */
    private void onHandoverRequested(UUID uuid) {
        PlayerSession session = sessions.get(uuid);
        if (session == null) {
            // Nothing in memory (for example a login that was refused after
            // the lease was taken): just let go.
            leases.release(uuid);
            return;
        }
        if (session.isUnloading()) {
            return; // the player is leaving anyway; release follows the save
        }
        platform.runForPlayer(uuid, () -> {
            if (sessions.get(uuid) != session || session.isUnloading()) {
                return;
            }
            log.info("Handing " + uuid + " over to another server.");
            unloadPlayer(uuid).thenRun(() -> platform.runForPlayer(uuid, () -> {
                // A normal server switch has moved the player on by now. If
                // they are still here, they logged in somewhere else.
                if (platform.isOnline(uuid) && !isLoaded(uuid)) {
                    platform.disconnect(uuid, "You joined from another server.");
                }
            }, settings.handoverGrace()));
        }, Duration.ZERO);
    }

    /**
     * A proxy says the player is about to switch to another server: save
     * their changes now, so the handover that follows has little left to
     * write. Only a hint: the player stays ours, and their changes keep
     * being saved, until the other server actually asks for them.
     */
    private void onSwitchExpected(UUID uuid) {
        PlayerSession session = sessions.get(uuid);
        if (session == null || session.isUnloading() || session.lost) {
            return;
        }
        platform.runForPlayer(uuid, () -> flush(session), Duration.ZERO);
    }

    /**
     * Our lease expired and another server took the player (this server was
     * frozen or cut off). Our data is outdated and may no longer be written.
     */
    private void onLost(UUID uuid) {
        if (drop(uuid, "Your data is now in use on another server. Please reconnect.")) {
            log.severe("Lost ownership of " + uuid + ": another server took over because this server did not renew "
                    + "in time. Changes made here since the last save are discarded so they cannot overwrite newer data.");
        }
    }

    /**
     * LeonsDB answers with another instance id: its data was restored from a
     * backup (or replaced). Everything cached here is from before, and the
     * players' ownership ended with the restore, so every player is let go
     * without saving; they load the restored data when they join again.
     */
    private void onDatabaseReplaced() {
        int n = 0;
        for (UUID uuid : List.copyOf(sessions.keySet())) {
            if (drop(uuid, "The network's data was restored from a backup. Please reconnect.")) {
                n++;
            }
        }
        log.severe("LeonsDB is a different database now (its instance id changed, for example after restoring a backup). "
                + "Players disconnected without saving: " + n + "; they load the restored data when they join again.");
    }

    private boolean drop(UUID uuid, String message) {
        leases.forget(uuid);
        PlayerSession session = sessions.remove(uuid);
        if (session == null) {
            return false;
        }
        session.lost = true;
        if (session.unloading != null) {
            session.unloading.complete(null);
        }
        platform.runForPlayer(uuid, () -> {
            if (platform.isOnline(uuid)) {
                platform.disconnect(uuid, message);
            }
        }, Duration.ZERO);
        return true;
    }

    // --- Internals -----------------------------------------------------------

    /** Returns the cached object, or throws if it is not loaded. */
    <T> CachedObject<T> cached(UUID uuid, Class<T> type, boolean forWriting) {
        ClassSchema<T> schema = schema(type);
        if (!schema.isPlayerData()) {
            throw new LeonsDBException(type.getName() + " is @GlobalData; use LeonsDB.global(...)");
        }
        PlayerSession session = sessions.get(uuid);
        // While a player is leaving or being handed over, plugins may still
        // read the cached object, but changes are no longer saved.
        if (session == null || session.lost || (forWriting && session.isUnloading())) {
            throw new DataNotLoadedException("Data of player " + uuid + " is not loaded (offline, joining or moved to another server)");
        }
        @SuppressWarnings("unchecked")
        CachedObject<T> obj = (CachedObject<T>) session.objects.get(type);
        if (obj == null) {
            throw new DataNotLoadedException(type.getSimpleName() + " of player " + uuid + " is not loaded yet");
        }
        return obj;
    }

    /**
     * The session of a player whose cached data a transaction may change, or
     * null if the player is not online here (or is leaving).
     */
    PlayerSession writableSession(UUID uuid) {
        PlayerSession s = sessions.get(uuid);
        return s == null || s.isUnloading() || s.lost ? null : s;
    }

    /**
     * Whether a plugin here registered a player collection, so its records
     * are cached while players are online.
     */
    boolean cachesPlayerCollection(String namespace, String collection) {
        for (ClassSchema<?> s : schemas.values()) {
            if (s.isPlayerData() && s.namespace().equals(namespace) && s.collection().equals(collection)) {
                return true;
            }
        }
        return false;
    }

    /** Runs a task on the main thread; during shutdown, which blocks it, right away. */
    void onMainThread(Runnable task) {
        if (shutDown) {
            task.run();
        } else {
            platform.runOnMainThread(task, Duration.ZERO);
        }
    }

    /**
     * Writes this server prepared for other servers' transactions whose
     * outcome it does not know yet. For diagnostics and tests.
     *
     * @return the number of undecided writes
     */
    public int pendingRemoteWrites() {
        return participant.pending();
    }

    /**
     * Runs a task on the thread that may touch the player's data; during
     * shutdown, which blocks it, right away.
     */
    void onPlayerThread(UUID uuid, Runnable task) {
        if (shutDown) {
            task.run();
        } else {
            platform.runForPlayer(uuid, task, Duration.ZERO);
        }
    }

    /** Like {@link #onPlayerThread}, but at once when already on that thread. */
    void onPlayerThreadNow(UUID uuid, Runnable task) {
        if (shutDown || platform.isPlayerThread(uuid)) {
            task.run();
        } else {
            platform.runForPlayer(uuid, task, Duration.ZERO);
        }
    }

    /** @return true once shutdown has begun */
    boolean isShutDown() {
        return shutDown;
    }

    boolean isLoaded(UUID uuid) {
        PlayerSession s = sessions.get(uuid);
        return s != null && !s.isUnloading() && !s.lost;
    }

    List<CachedObject<?>> objectsOf(UUID uuid) {
        PlayerSession s = sessions.get(uuid);
        if (s == null || s.isUnloading() || s.lost) {
            throw new DataNotLoadedException("Data of player " + uuid + " is not loaded");
        }
        return new ArrayList<>(s.all());
    }

    void requirePlayerThread(UUID uuid, String what) {
        if (!platform.isPlayerThread(uuid)) {
            throw new IllegalStateException(what + " must be called on the main thread (on Folia: the thread of the "
                    + "player's region)");
        }
    }

    @SuppressWarnings("unchecked")
    <T> ClassSchema<T> schema(Class<T> type) {
        ClassSchema<T> schema = (ClassSchema<T>) schemas.get(type);
        if (schema == null) {
            throw new LeonsDBException(type.getName() + " is not registered; call LeonsDB.register("
                    + type.getSimpleName() + ".class) in your plugin's onEnable");
        }
        return schema;
    }

    private static <T> void putObject(PlayerSession session, CachedObject<T> obj) {
        session.objects.put(obj.schema().type(), obj);
    }

    private <T> CompletableFuture<Void> loadObject(PlayerSession session, ClassSchema<T> schema) {
        RecordRef ref = playerRef(schema, session.uuid);
        return client.get(ref).thenAccept(resp -> {
            CachedObject<T> obj = fromResponse(session, schema, ref, resp);
            session.objects.put(schema.type(), obj);
        });
    }

    private <T> CachedObject<T> fromResponse(PlayerSession session, ClassSchema<T> schema, RecordRef ref, GetResponse resp) {
        T instance = schema.newInstance();
        Set<String> unreadable = Set.of();
        Map<String, Value> base = Map.of();
        long version = 0;
        if (resp.getFound()) {
            Map<String, Value> fields = resp.getRecord().getFieldsMap();
            unreadable = schema.write(instance, fields);
            base = schema.declaredOnly(fields);
            version = resp.getRecord().getVersion();
            if (!unreadable.isEmpty()) {
                log.warning("Fields " + unreadable + " of " + schema.type().getSimpleName()
                        + " have stored values this plugin version cannot read; they keep their defaults "
                        + "and are not saved, so the stored values stay intact.");
            }
        }
        return new CachedObject<>(schema, ref, instance, unreadable, base, version, client, log,
                session::epoch, () -> onLost(session.uuid));
    }

    private void loadForOnlinePlayers(ClassSchema<?> schema) {
        for (PlayerSession session : sessions.values()) {
            if (!session.isUnloading() && !session.lost) {
                loadObject(session, schema).exceptionally(e -> {
                    log.log(Level.WARNING, "Could not load " + schema.type().getSimpleName() + " for online player "
                            + session.uuid + ": " + e.getMessage());
                    return null;
                });
            }
        }
    }

    /**
     * {@code @Transactional} fields should be changed with update(), which
     * saves and confirms immediately. A change found by the periodic save
     * means the plugin changed one directly; it is still saved, but up to a
     * second later and without confirmation. Warn once per class so
     * developers notice.
     */
    private void warnIfTransactionalChanged(CachedObject<?> obj, Map<String, Value> snap) {
        if (warnedTransactional.contains(obj.schema().type())) {
            return;
        }
        List<String> changed = obj.directlyChangedTransactional(snap);
        if (!changed.isEmpty() && warnedTransactional.add(obj.schema().type())) {
            log.warning(obj.schema().type().getSimpleName() + "." + changed.get(0) + " is @Transactional but was "
                    + "changed directly. It will be saved, but for immediate, confirmed saves change it with "
                    + "LeonsDB.player(uuid).update(" + obj.schema().type().getSimpleName() + ".class, ...).");
        }
    }

    static RecordRef playerRef(ClassSchema<?> schema, UUID uuid) {
        ByteBuffer b = ByteBuffer.allocate(16);
        b.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
        return RecordRef.newBuilder()
                .setNamespace(schema.namespace()).setCollection(schema.collection())
                .setPlayer(ByteString.copyFrom(b.array()))
                .build();
    }

    private void writeRecoveryFiles(Map<CachedObject<?>, Map<String, Value>> finals) {
        int written = 0;
        for (var e : finals.entrySet()) {
            CachedObject<?> obj = e.getKey();
            var ops = obj.pendingOps(e.getValue());
            if (ops.isEmpty()) {
                continue;
            }
            String text = TextFormat.printer().printToString(
                    ApplyRequest.newBuilder().setRef(obj.ref()).addAllOperations(ops).build());
            try {
                Files.createDirectories(settings.recoveryDir());
                Path file = settings.recoveryDir().resolve(obj.schema().namespace() + "-" + obj.schema().collection() + "-"
                        + System.currentTimeMillis() + "-" + written + ".txt");
                Files.writeString(file, text, StandardCharsets.UTF_8);
                written++;
            } catch (IOException ioe) {
                log.log(Level.SEVERE, "Could not write recovery file; unsaved change: " + text, ioe);
            }
        }
        if (written > 0) {
            log.severe("LeonsDB could not be reached during shutdown. " + written + " unsaved change(s) were written to "
                    + settings.recoveryDir() + " so they can be restored.");
        }
    }

    private static long remaining(long deadlineNanos) {
        return Math.max(0, deadlineNanos - System.nanoTime());
    }
}
