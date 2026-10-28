// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.velocity;

import com.leonsdb.connector.core.RequestException;
import io.grpc.Status;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Tells LeonsDB which server a player is about to join. A hint never holds
 * up a connection: it is sent and not waited for, and when LeonsDB cannot
 * take it, players switch exactly as they do without this plugin.
 */
final class SwitchHints {

    /** While LeonsDB is unreachable, warn at most this often. */
    static final long WARN_INTERVAL_MILLIS = 60_000;

    private final BiFunction<UUID, String, CompletableFuture<Boolean>> send;
    private final Consumer<String> warn;
    private final Consumer<String> info;
    private final LongSupplier clock;
    private final AtomicBoolean disabled = new AtomicBoolean();
    private final AtomicBoolean confirmed = new AtomicBoolean();
    private final AtomicLong lastWarning = new AtomicLong(Long.MIN_VALUE / 2);

    /**
     * @param send  sends one hint: the player and the server they are about
     *              to join; completes with whether a server had the player
     * @param warn  logs a warning
     * @param info  logs a message
     * @param clock milliseconds, for spacing out warnings
     */
    SwitchHints(BiFunction<UUID, String, CompletableFuture<Boolean>> send, Consumer<String> warn, Consumer<String> info,
                LongSupplier clock) {
        this.send = send;
        this.warn = warn;
        this.info = info;
        this.clock = clock;
    }

    /**
     * The proxy is about to connect a player to a server.
     *
     * @param player  the player
     * @param current the server they are on, or null when they are joining
     *                the network
     * @param target  the server they are about to join
     */
    void beforeConnect(UUID player, String current, String target) {
        if (disabled.get() || target.equals(current)) {
            return;
        }
        CompletableFuture<Boolean> sent;
        try {
            sent = send.apply(player, target);
        } catch (RuntimeException e) {
            failed(e);
            return;
        }
        sent.whenComplete((told, err) -> {
            if (err != null) {
                failed(err);
            } else if (told && confirmed.compareAndSet(false, true)) {
                // Once, so an admin sees the setup works end to end.
                info.accept("Switch hints work: the server " + player + " was on was told to save them before the switch to "
                        + target + ".");
            }
        });
    }

    /** @return true once hints were turned off because LeonsDB cannot take them */
    boolean disabled() {
        return disabled.get();
    }

    private void failed(Throwable err) {
        Throwable cause = err instanceof CompletionException && err.getCause() != null ? err.getCause() : err;
        Status.Code code = cause instanceof RequestException re ? re.code() : Status.fromThrowable(cause).getCode();
        if (code == Status.Code.UNIMPLEMENTED) {
            if (disabled.compareAndSet(false, true)) {
                warn.accept("This LeonsDB server does not take switch hints (it is older than this plugin). "
                        + "Hints are off; players switch as before. Update LeonsDB to turn them on.");
            }
        } else if (code == Status.Code.PERMISSION_DENIED || code == Status.Code.UNAUTHENTICATED) {
            if (disabled.compareAndSet(false, true)) {
                warn.accept("LeonsDB refused this proxy's token (" + cause.getMessage() + "). Hints are off; players "
                        + "switch as before. Create a token with: leonsdb token create -name <proxy> -role proxy");
            }
        } else {
            long now = clock.getAsLong();
            long last = lastWarning.get();
            if (now - last >= WARN_INTERVAL_MILLIS && lastWarning.compareAndSet(last, now)) {
                warn.accept("Could not reach LeonsDB to send a switch hint: " + cause.getMessage()
                        + ". Players switch as before; hints resume when it is reachable.");
            }
        }
    }
}
