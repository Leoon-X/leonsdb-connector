// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.paper;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * Paper's queue of tasks the main thread runs as soon as it is idle: between
 * ticks, and while it waits for the next one. Paper handles players' packets
 * there, so a task in it runs at the same safe points as a player's chat or
 * clicks, without waiting for the next tick. A player handed over to another
 * server is saved from it, so the other server gets them sooner.
 *
 * <p>The queue is not part of Paper's plugin API. It is only used once a check
 * at startup shows that it runs tasks on the main thread; otherwise, and on
 * Folia, everything goes through the scheduler.
 */
final class IdleQueue {

    /** How long the startup check waits for the server to start ticking. */
    static final Duration CHECK_TIMEOUT = Duration.ofMinutes(2);

    private IdleQueue() {
    }

    /**
     * The queue, or why there is none.
     *
     * @param executor the queue, or null
     * @param reason   why there is none, or null
     */
    record Lookup(Executor executor, String reason) {
    }

    /**
     * Looks for the queue: Paper's {@code CraftServer#getServer()} is the
     * Minecraft server, which is an {@link Executor} for its main thread.
     *
     * @param craftServer {@code Bukkit.getServer()}
     * @param folia       whether this is Folia, which has no such queue
     * @return the queue, or why there is none
     */
    static Lookup find(Object craftServer, boolean folia) {
        if (folia) {
            return new Lookup(null, "Folia has no idle-time task queue");
        }
        Object server;
        try {
            server = craftServer.getClass().getMethod("getServer").invoke(craftServer);
        } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
            return new Lookup(null, "this server has no CraftServer#getServer");
        }
        if (server instanceof Executor executor) {
            return new Lookup(executor, null);
        }
        return new Lookup(null, "CraftServer#getServer is not a task queue here");
    }

    /**
     * Sends a task that does nothing through the queue from another thread.
     *
     * @param queue        the queue
     * @param onMainThread whether the calling thread is the main thread
     * @param timeout      how long to wait for the task to run
     * @return completes with true if the task ran on the main thread, false
     *         if it ran elsewhere, failed or did not run in time
     */
    static CompletableFuture<Boolean> check(Executor queue, BooleanSupplier onMainThread, Duration timeout) {
        CompletableFuture<Boolean> ran = new CompletableFuture<>();
        Thread sender = new Thread(() -> {
            try {
                queue.execute(() -> ran.complete(onMainThread.getAsBoolean()));
            } catch (RuntimeException e) {
                ran.complete(false);
            }
        }, "LeonsDB idle queue check");
        sender.setDaemon(true);
        sender.start();
        return ran.completeOnTimeout(false, timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Runs a task from the queue. If the queue runs it somewhere other than
     * the main thread (once the server has stopped it runs them on the
     * caller's thread), or refuses it, the task goes to {@code fallback} instead.
     *
     * @param queue        the queue
     * @param task         a task for the main thread
     * @param onMainThread whether the calling thread is the main thread
     * @param fallback     schedules a task on the main thread the usual way
     */
    static void execute(Executor queue, Runnable task, BooleanSupplier onMainThread, Consumer<Runnable> fallback) {
        try {
            queue.execute(() -> {
                if (onMainThread.getAsBoolean()) {
                    task.run();
                } else {
                    fallback.accept(task);
                }
            });
        } catch (RejectedExecutionException e) {
            fallback.accept(task);
        }
    }
}
