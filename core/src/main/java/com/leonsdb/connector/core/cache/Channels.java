// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core.cache;

import com.leonsdb.connector.api.Channel;
import com.leonsdb.connector.api.ChannelMessage;
import com.leonsdb.connector.api.ChannelSubscription;
import com.leonsdb.connector.api.LeonsDBException;
import com.leonsdb.connector.core.Jdk;
import com.leonsdb.connector.core.LeonsDBClient;
import com.leonsdb.proto.v1.SubscribeRequest;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * Message channels. Each channel this server listens to has one stream from
 * LeonsDB, shared by all its listeners, opened with the first listener and
 * closed with the last; a dropped stream is reopened after a second.
 */
final class Channels {

    private static final Pattern NAME = Pattern.compile("[a-z][a-z0-9_]{0,63}\\.[a-z][a-z0-9_]{0,63}");
    private static final Duration RECONNECT = Duration.ofSeconds(1);

    private final LeonsDBClient client;
    private final Platform platform;
    private final String serverName;
    private final ScheduledExecutorService scheduler;
    private final Logger log;
    private final Map<String, Hub> hubs = new ConcurrentHashMap<>();
    private volatile boolean stopped;

    Channels(LeonsDBClient client, Platform platform, String serverName, ScheduledExecutorService scheduler, Logger log) {
        this.client = client;
        this.platform = platform;
        this.serverName = serverName;
        this.scheduler = scheduler;
        this.log = log;
    }

    Channel channel(String name) {
        if (!NAME.matcher(name).matches()) {
            throw new LeonsDBException("Channel names look like \"network.chat\": <namespace>.<name>, lowercase letters, "
                    + "digits and underscores; got \"" + name + "\"");
        }
        return new Channel() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public CompletableFuture<Integer> publish(String text) {
                Objects.requireNonNull(text, "text");
                return client.publish(name, text, serverName).thenApply(r -> r.getDelivered());
            }

            @Override
            public ChannelSubscription subscribe(Consumer<ChannelMessage> listener) {
                Objects.requireNonNull(listener, "listener");
                return Channels.this.subscribe(name, listener);
            }
        };
    }

    /** Stops every stream. */
    void stop() {
        stopped = true;
        hubs.values().forEach(Hub::cancel);
        hubs.clear();
    }

    private ChannelSubscription subscribe(String name, Consumer<ChannelMessage> listener) {
        Hub hub;
        synchronized (hubs) {
            hub = hubs.computeIfAbsent(name, Hub::new);
            hub.listeners.add(listener);
            hub.open();
        }
        return new ChannelSubscription() {
            private boolean closed;

            @Override
            public void close() {
                synchronized (hubs) {
                    if (closed) {
                        return;
                    }
                    closed = true;
                    hub.listeners.remove(listener);
                    if (hub.listeners.isEmpty()) {
                        hubs.remove(name, hub);
                        hub.cancel();
                    }
                }
            }
        };
    }

    /** One channel's stream and listeners. */
    private final class Hub {
        final String name;
        final List<Consumer<ChannelMessage>> listeners = new CopyOnWriteArrayList<>();
        private volatile ClientCallStreamObserver<SubscribeRequest> call;
        private volatile boolean open;
        private volatile boolean cancelled;
        private volatile boolean warned;

        Hub(String name) {
            this.name = name;
        }

        void open() {
            if (open || cancelled || stopped) {
                return;
            }
            open = true;
            client.subscribe(Jdk.listOf(name), new ClientResponseObserver<SubscribeRequest, com.leonsdb.proto.v1.ChannelMessage>() {
                @Override
                public void beforeStart(ClientCallStreamObserver<SubscribeRequest> c) {
                    call = c;
                }

                @Override
                public void onNext(com.leonsdb.proto.v1.ChannelMessage m) {
                    warned = false;
                    if (m.getDropped()) {
                        log.warning("Missed " + m.getText() + " message(s) on channel " + name
                                + ": this server could not keep up with them.");
                        return;
                    }
                    ChannelMessage msg = new ChannelMessage(m.getChannel(), m.getText(), m.getSender(), Instant.ofEpochMilli(m.getTime()));
                    platform.runOnMainThread(() -> {
                        for (Consumer<ChannelMessage> l : listeners) {
                            try {
                                l.accept(msg);
                            } catch (RuntimeException e) {
                                log.log(java.util.logging.Level.WARNING, "A listener of channel " + name + " failed", e);
                            }
                        }
                    }, Duration.ZERO);
                }

                @Override
                public void onError(Throwable t) {
                    open = false;
                    if (io.grpc.Status.fromThrowable(t).getCode() == io.grpc.Status.Code.PERMISSION_DENIED) {
                        log.severe("This server's token may not listen to channel " + name);
                        return;
                    }
                    if (!cancelled && !warned) {
                        warned = true;
                        log.warning("Lost the connection for channel " + name + "; reconnecting. Messages sent "
                                + "meanwhile are missed: " + t.getMessage());
                    }
                    reopenLater();
                }

                @Override
                public void onCompleted() {
                    open = false;
                    reopenLater();
                }
            });
        }

        private void reopenLater() {
            if (!cancelled && !stopped) {
                scheduler.schedule(this::open, RECONNECT.toMillis(), TimeUnit.MILLISECONDS);
            }
        }

        void cancel() {
            cancelled = true;
            ClientCallStreamObserver<SubscribeRequest> c = call;
            if (c != null) {
                c.cancel("unsubscribed", null);
            }
        }
    }
}
