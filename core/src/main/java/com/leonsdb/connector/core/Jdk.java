// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Small parts of the Java library that came after Java 8. The connector runs
 * on Minecraft servers from 1.8 (Java 8) to the newest (Java 21 and later),
 * so it is built for Java 8 and uses these instead.
 *
 * <p>Internal to the connector: not part of its API.
 */
public final class Jdk {

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    // For delay(). Its one thread ends when it has had nothing to do for a
    // few seconds, so it never outlives the plugin for long.
    private static final ScheduledThreadPoolExecutor TIMER;

    static {
        TIMER = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "leonsdb-delay");
            t.setDaemon(true);
            return t;
        });
        TIMER.setKeepAliveTime(5, TimeUnit.SECONDS);
        TIMER.allowCoreThreadTimeOut(true);
        TIMER.setRemoveOnCancelPolicy(true);
    }

    private Jdk() {
    }

    /**
     * An unmodifiable copy, like Java 10's {@code List.copyOf}: nulls are refused.
     *
     * @param items the items
     * @param <E>   the item type
     * @return the copy
     */
    public static <E> List<E> listCopy(Collection<? extends E> items) {
        List<E> copy = new ArrayList<>(items.size());
        for (E e : items) {
            copy.add(Objects.requireNonNull(e, "element"));
        }
        return Collections.unmodifiableList(copy);
    }

    /**
     * An unmodifiable copy, like Java 10's {@code Set.copyOf}: nulls are refused.
     *
     * @param items the items
     * @param <E>   the item type
     * @return the copy
     */
    public static <E> Set<E> setCopy(Collection<? extends E> items) {
        Set<E> copy = new LinkedHashSet<>(Math.max(4, items.size() * 2));
        for (E e : items) {
            copy.add(Objects.requireNonNull(e, "element"));
        }
        return Collections.unmodifiableSet(copy);
    }

    /**
     * An unmodifiable copy, like Java 10's {@code Map.copyOf}: null keys and
     * values are refused.
     *
     * @param map the map
     * @param <K> the key type
     * @param <V> the value type
     * @return the copy
     */
    public static <K, V> Map<K, V> mapCopy(Map<? extends K, ? extends V> map) {
        Map<K, V> copy = new LinkedHashMap<>(Math.max(4, map.size() * 2));
        for (Map.Entry<? extends K, ? extends V> e : map.entrySet()) {
            copy.put(Objects.requireNonNull(e.getKey(), "key"), Objects.requireNonNull(e.getValue(), "value"));
        }
        return Collections.unmodifiableMap(copy);
    }

    /**
     * An unmodifiable list of the given items, like Java 9's {@code List.of}.
     *
     * @param items the items, none null
     * @param <E>   the item type
     * @return the list
     */
    @SafeVarargs
    public static <E> List<E> listOf(E... items) {
        List<E> list = new ArrayList<>(items.length);
        for (E e : items) {
            list.add(Objects.requireNonNull(e, "element"));
        }
        return Collections.unmodifiableList(list);
    }

    /**
     * A set of the given items, like Java 9's {@code Set.of}.
     *
     * @param items the items, none null
     * @param <E>   the item type
     * @return the set
     */
    @SafeVarargs
    public static <E> Set<E> setOf(E... items) {
        Set<E> set = new LinkedHashSet<>(Math.max(4, items.length * 2));
        for (E e : items) {
            set.add(Objects.requireNonNull(e, "element"));
        }
        return Collections.unmodifiableSet(set);
    }

    /**
     * A future that has already failed, like Java 9's {@code CompletableFuture.failedFuture}.
     *
     * @param error the failure
     * @param <T>   the result type
     * @return the failed future
     */
    public static <T> CompletableFuture<T> failed(Throwable error) {
        CompletableFuture<T> f = new CompletableFuture<>();
        f.completeExceptionally(error);
        return f;
    }

    /**
     * Java 12's {@code exceptionallyCompose}: if the future fails, the result
     * is the future the handler returns; otherwise it is the future's value.
     *
     * @param future  the future
     * @param handler turns the failure into another future
     * @param <T>     the result type
     * @return the combined future
     */
    public static <T> CompletableFuture<T> exceptionallyCompose(
            CompletableFuture<T> future, Function<Throwable, ? extends CompletableFuture<T>> handler) {
        CompletableFuture<T> out = new CompletableFuture<>();
        future.whenComplete((value, err) -> {
            if (err == null) {
                out.complete(value);
                return;
            }
            CompletableFuture<T> next;
            try {
                next = handler.apply(err);
            } catch (Throwable t) {
                out.completeExceptionally(t);
                return;
            }
            next.whenComplete((v, e) -> {
                if (e == null) {
                    out.complete(v);
                } else {
                    out.completeExceptionally(e instanceof CompletionException && e.getCause() != null ? e.getCause() : e);
                }
            });
        });
        return out;
    }

    /**
     * A future that completes after a delay, on the common pool, like
     * {@code CompletableFuture.supplyAsync(() -> null, CompletableFuture.delayedExecutor(millis, MILLISECONDS))}
     * in Java 9.
     *
     * @param millis the delay in milliseconds
     * @return completes with null after the delay
     */
    public static CompletableFuture<Void> delay(long millis) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        runLater(millis, () -> f.complete(null));
        return f;
    }

    /**
     * Runs a task on the common pool after a delay, like Java 9's
     * {@code CompletableFuture.delayedExecutor(millis, MILLISECONDS).execute(task)}.
     *
     * @param millis the delay in milliseconds
     * @param task   the task
     */
    public static void runLater(long millis, Runnable task) {
        TIMER.schedule(() -> ForkJoinPool.commonPool().execute(task), millis, TimeUnit.MILLISECONDS);
    }

    /**
     * Reads a stream to its end, like Java 9's {@code InputStream.readAllBytes}.
     *
     * @param in the stream
     * @return its bytes
     * @throws IOException if reading fails
     */
    public static byte[] readAllBytes(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(32, in.available()));
        byte[] buf = new byte[8192];
        int n;
        while ((n = in.read(buf)) != -1) {
            out.write(buf, 0, n);
        }
        return out.toByteArray();
    }

    /**
     * Lower-case hexadecimal, like Java 17's {@code HexFormat.of().formatHex}.
     *
     * @param bytes the bytes
     * @return two characters per byte
     */
    public static String hex(byte[] bytes) {
        char[] out = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            out[2 * i] = HEX[(bytes[i] >> 4) & 0xf];
            out[2 * i + 1] = HEX[bytes[i] & 0xf];
        }
        return new String(out);
    }

    /**
     * Starts a daemon thread, like Java 21's {@code Thread.ofPlatform().daemon().name(name).start(task)}.
     *
     * @param name the thread's name
     * @param task what it runs
     * @return the started thread
     */
    public static Thread startDaemon(String name, Runnable task) {
        Thread t = new Thread(task, name);
        t.setDaemon(true);
        t.start();
        return t;
    }
}
