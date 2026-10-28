// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.paper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class IdleQueueTest {

    /** Stands in for the main thread and its queue. */
    private final ExecutorService main = Executors.newSingleThreadExecutor(r -> new Thread(r, "main"));
    private final BooleanSupplier onMain = () -> Thread.currentThread().getName().equals("main");

    @AfterEach
    void tearDown() {
        main.shutdownNow();
    }

    public static final class PaperLike {
        private final Object server;

        PaperLike(Object server) {
            this.server = server;
        }

        public Object getServer() {
            return server;
        }
    }

    public static final class Throwing {
        public Object getServer() {
            throw new IllegalStateException("not yet");
        }
    }

    @Test
    void theMinecraftServerIsTheQueue() {
        Executor server = Runnable::run;
        assertSame(server, IdleQueue.find(new PaperLike(server), false).executor());
    }

    @Test
    void withoutTheQueueTheSchedulerIsUsedAndTheReasonGiven() {
        for (var lookup : List.of(
                IdleQueue.find(new Object(), false),
                IdleQueue.find(new PaperLike("not an executor"), false),
                IdleQueue.find(new Throwing(), false),
                IdleQueue.find(new PaperLike((Executor) Runnable::run), true))) {
            assertNull(lookup.executor());
            assertNotNull(lookup.reason());
        }
        assertTrue(IdleQueue.find(new PaperLike((Executor) Runnable::run), true).reason().contains("Folia"));
    }

    @Test
    void theCheckPassesOnlyIfTheTaskRunsOnTheMainThread() throws Exception {
        assertTrue(IdleQueue.check(main, onMain, Duration.ofSeconds(5)).get(10, TimeUnit.SECONDS));
        // A stopped server runs queued tasks on the caller's thread.
        assertFalse(IdleQueue.check(Runnable::run, onMain, Duration.ofSeconds(5)).get(10, TimeUnit.SECONDS));
        Executor refusing = r -> {
            throw new RejectedExecutionException();
        };
        assertFalse(IdleQueue.check(refusing, onMain, Duration.ofSeconds(5)).get(10, TimeUnit.SECONDS));
        Executor neverRuns = r -> { };
        assertFalse(IdleQueue.check(neverRuns, onMain, Duration.ofMillis(100)).get(10, TimeUnit.SECONDS));
    }

    @Test
    void tasksRunOnceOnTheMainThread() throws Exception {
        List<String> threads = new CopyOnWriteArrayList<>();
        AtomicInteger fallbacks = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            IdleQueue.execute(main, () -> {
                threads.add(Thread.currentThread().getName());
                done.countDown();
            }, onMain, t -> fallbacks.incrementAndGet());
        }
        assertTrue(done.await(5, TimeUnit.SECONDS));
        assertEquals(List.of("main"), threads.stream().distinct().toList());
        assertEquals(10, threads.size());
        assertEquals(0, fallbacks.get());
    }

    @Test
    void aTaskTheQueueWouldRunOffTheMainThreadGoesToTheScheduler() {
        List<Runnable> scheduled = new ArrayList<>();
        AtomicInteger ran = new AtomicInteger();
        Runnable task = ran::incrementAndGet;

        IdleQueue.execute(Runnable::run, task, onMain, scheduled::add); // stopped server
        IdleQueue.execute(r -> {
            throw new RejectedExecutionException();
        }, task, onMain, scheduled::add);

        assertEquals(0, ran.get(), "never run on the wrong thread");
        assertEquals(List.of(task, task), scheduled, "scheduled the usual way instead, once each");
    }
}
