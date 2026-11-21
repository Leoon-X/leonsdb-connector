// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core;

import com.google.protobuf.InvalidProtocolBufferException;
import com.leonsdb.proto.v1.WireError;
import com.leonsdb.proto.v1.WireHello;
import com.leonsdb.proto.v1.WireWelcome;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Deadline;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

/**
 * A gRPC channel that sends unary calls and streams of answers (such as
 * subscriptions) over the LeonsDB wire protocol (docs/wire-protocol.md), a
 * lean binary protocol over TCP, and everything else over gRPC.
 *
 * <p>The wire protocol runs the same server handlers as gRPC; it only costs
 * less per call: many calls share one connection, and calls that are ready
 * together go out in one write. Results and errors (status, description,
 * reason trailers) are the same, so the generated stubs work unchanged.
 *
 * <p>Until the wire connection is up, and whenever it is down, calls use
 * gRPC. A call that was sent but whose connection broke before the answer
 * fails with {@code UNAVAILABLE}, as it can over gRPC: it may or may not
 * have run. A stream whose connection broke fails the same way.
 */
final class WireChannel extends Channel {
    static final int PROTOCOL_VERSION = 1;

    private static final Logger LOG = Logger.getLogger(WireChannel.class.getName());
    private static final byte HELLO = 1;
    private static final byte WELCOME = 2;
    private static final byte REQUEST = 3;
    private static final byte RESPONSE = 4;
    private static final byte ERROR = 5;
    private static final byte MESSAGE = 6;
    private static final byte HEADER = 7;
    private static final byte CANCEL = 8;
    private static final int MAX_FRAME = (16 << 20) + 64;
    private static final long RETRY_NANOS = TimeUnit.SECONDS.toNanos(5);
    private static final long CONNECT_TIMEOUT_MILLIS = 5_000;

    private final Channel grpc;
    private final String clientName;
    private final String token;
    private final boolean tls;
    // Answers are handed to callers on these threads, never on the reader.
    private final Executor callbacks;
    private final ScheduledExecutorService timers;

    private volatile Connection connection;
    private volatile String host;
    private volatile int port; // 0: the server has no wire protocol
    private final AtomicBoolean connecting = new AtomicBoolean();
    private volatile long nextAttempt;
    private volatile boolean closed;

    WireChannel(Channel grpc, boolean tls, String token, String clientName) {
        this.grpc = grpc;
        this.tls = tls;
        this.token = token == null ? "" : token;
        this.clientName = clientName;
        this.callbacks = Executors.newCachedThreadPool(daemon("leonsdb-wire-callback"));
        this.timers = Executors.newSingleThreadScheduledExecutor(daemon("leonsdb-wire-timer"));
    }

    /**
     * Tells the channel where the server's wire protocol is (from GetInfo).
     * Port 0 means the server has it off; calls then use gRPC.
     */
    void target(String host, int port) {
        if (port == this.port && host.equals(this.host)) {
            return;
        }
        this.host = host;
        this.port = port;
        Connection old = connection;
        if (old != null) {
            old.fail(new IOException("the server's wire address changed"));
        }
        nextAttempt = 0;
        connectionOrNull();
    }

    /** Whether calls currently go over the wire protocol. */
    boolean connected() {
        Connection c = connection;
        return c != null && c.alive.get();
    }

    void shutdown() {
        closed = true;
        Connection c = connection;
        if (c != null) {
            c.fail(new IOException("the client was closed"));
        }
        timers.shutdownNow();
    }

    @Override
    public <Q, R> ClientCall<Q, R> newCall(MethodDescriptor<Q, R> method, CallOptions options) {
        MethodDescriptor.MethodType type = method.getType();
        if (type != MethodDescriptor.MethodType.UNARY && type != MethodDescriptor.MethodType.SERVER_STREAMING) {
            return grpc.newCall(method, options);
        }
        Connection c = connectionOrNull();
        Integer id = c == null ? null : c.methods.get("/" + method.getFullMethodName());
        if (id == null) {
            return grpc.newCall(method, options);
        }
        if (type == MethodDescriptor.MethodType.SERVER_STREAMING) {
            return new WireStream<>(c, id, method, options);
        }
        return new WireCall<>(c, id, method, options);
    }

    @Override
    public String authority() {
        return grpc.authority();
    }

    // Returns the live connection, or null and starts connecting in the
    // background (callers never wait for a connection).
    private Connection connectionOrNull() {
        Connection c = connection;
        if (c != null && c.alive.get()) {
            return c;
        }
        if (closed || port == 0 || System.nanoTime() < nextAttempt || !connecting.compareAndSet(false, true)) {
            return null;
        }
        String h = host;
        int p = port;
        Jdk.startDaemon("leonsdb-wire-connect", () -> {
            try {
                Connection fresh = Connection.open(this, h, p);
                if (closed) {
                    fresh.fail(new IOException("the client was closed"));
                } else {
                    connection = fresh;
                    LOG.fine("Using the LeonsDB wire protocol at " + h + ":" + p);
                }
            } catch (IOException | RuntimeException e) {
                nextAttempt = System.nanoTime() + RETRY_NANOS;
                LOG.log(Level.FINE, "LeonsDB wire protocol at " + h + ":" + p + " not available; using gRPC", e);
            } finally {
                connecting.set(false);
            }
        });
        return null;
    }

    private void lost(Connection c, Throwable why) {
        if (connection == c) {
            connection = null;
            nextAttempt = System.nanoTime() + RETRY_NANOS;
            if (!closed) {
                LOG.log(Level.INFO, "LeonsDB wire connection lost (" + why.getMessage() + "); using gRPC until it is back");
            }
        }
    }

    private static java.util.concurrent.ThreadFactory daemon(String name) {
        AtomicInteger n = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, name + "-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    // --- one connection ----------------------------------------------------

    private static final class Connection {
        final WireChannel owner;
        final Socket socket;
        final Map<String, Integer> methods;
        final Map<Integer, Answered> pending = new ConcurrentHashMap<>();
        final LinkedBlockingQueue<byte[]> out = new LinkedBlockingQueue<>();
        final AtomicInteger ids = new AtomicInteger();
        final AtomicBoolean alive = new AtomicBoolean(true);

        private Connection(WireChannel owner, Socket socket, Map<String, Integer> methods) {
            this.owner = owner;
            this.socket = socket;
            this.methods = methods;
        }

        static Connection open(WireChannel owner, String host, int port) throws IOException {
            Socket s = new Socket();
            try {
                s.connect(new InetSocketAddress(host, port), (int) CONNECT_TIMEOUT_MILLIS);
                s.setTcpNoDelay(true);
                if (owner.tls) {
                    SSLSocket ssl = (SSLSocket) ((SSLSocketFactory) SSLSocketFactory.getDefault())
                            .createSocket(s, host, port, true);
                    SSLParameters params = ssl.getSSLParameters();
                    params.setEndpointIdentificationAlgorithm("HTTPS");
                    ssl.setSSLParameters(params);
                    ssl.startHandshake();
                    s = ssl;
                }
                s.setSoTimeout((int) CONNECT_TIMEOUT_MILLIS);
                OutputStream os = s.getOutputStream();
                DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream(), 64 << 10));
                byte[] hello = WireHello.newBuilder().setProtocolVersion(PROTOCOL_VERSION)
                        .setToken(owner.token).setClientName(owner.clientName).build().toByteArray();
                os.write(frame(HELLO, null, hello));
                os.flush();
                Frame f = readFrame(in);
                if (f.type == ERROR) {
                    WireError e = WireError.parseFrom(ByteBuffer.wrap(f.body, 4, f.body.length - 4));
                    throw new IOException("refused: " + Status.fromCodeValue(e.getCode()).getCode() + " " + e.getMessage());
                }
                if (f.type != WELCOME) {
                    throw new IOException("unexpected frame " + f.type);
                }
                WireWelcome w = WireWelcome.parseFrom(f.body);
                Map<String, Integer> methods = new HashMap<>();
                for (int i = 0; i < w.getMethodsCount(); i++) {
                    methods.put(w.getMethods(i), i);
                }
                s.setSoTimeout(0);
                Connection c = new Connection(owner, s, Jdk.mapCopy(methods));
                Socket sock = s;
                Jdk.startDaemon("leonsdb-wire-writer", () -> c.write(sock));
                Jdk.startDaemon("leonsdb-wire-reader", () -> c.read(in));
                return c;
            } catch (IOException | RuntimeException e) {
                s.close();
                throw e;
            }
        }

        void send(int id, Answered call, byte[] frame) {
            pending.put(id, call);
            if (!alive.get()) {
                pending.remove(id);
                call.finish(Status.UNAVAILABLE.withDescription("the wire connection is closed"), new Metadata());
                return;
            }
            out.add(frame);
        }

        private void write(Socket s) {
            try (OutputStream os = new BufferedOutputStream(s.getOutputStream(), 64 << 10)) {
                while (alive.get()) {
                    byte[] f = out.poll(1, TimeUnit.SECONDS);
                    if (f == null) {
                        continue;
                    }
                    os.write(f);
                    // Everything that is ready goes out in one write.
                    while ((f = out.poll()) != null) {
                        os.write(f);
                    }
                    os.flush();
                }
            } catch (IOException e) {
                fail(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail(new IOException("interrupted"));
            }
        }

        private void read(DataInputStream in) {
            try {
                while (true) {
                    Frame f = readFrame(in);
                    if (f.type < RESPONSE || f.type > HEADER || f.body.length < 4) {
                        throw new IOException("unexpected frame " + f.type);
                    }
                    int id = ByteBuffer.wrap(f.body).getInt();
                    // A stream's messages and header leave it running; a
                    // response or error ends a call or a stream.
                    Answered call = f.type == MESSAGE || f.type == HEADER ? pending.get(id) : pending.remove(id);
                    if (call != null) {
                        call.answer(f.type, f.body);
                    }
                }
            } catch (IOException e) {
                fail(e);
            }
        }

        void fail(Throwable why) {
            if (!alive.compareAndSet(true, false)) {
                return;
            }
            try {
                socket.close();
            } catch (IOException ignored) {
                // closing anyway
            }
            owner.lost(this, why);
            for (Integer id : pending.keySet()) {
                Answered call = pending.remove(id);
                if (call != null) {
                    call.finish(Status.UNAVAILABLE.withDescription("LeonsDB wire connection lost: " + why.getMessage())
                            .withCause(why), new Metadata());
                }
            }
        }
    }

    // --- one call ----------------------------------------------------------

    /** A call or stream waiting for frames from the server. */
    private interface Answered {
        void answer(byte type, byte[] body);

        void finish(Status status, Metadata trailers);
    }

    private final class WireCall<Q, R> extends ClientCall<Q, R> implements Answered {
        private final Connection conn;
        private final int method;
        private final MethodDescriptor<Q, R> descriptor;
        private final CallOptions options;
        private final AtomicBoolean done = new AtomicBoolean();
        private Listener<R> listener;
        private byte[] request;
        private int id;
        private ScheduledFuture<?> timer;

        WireCall(Connection conn, int method, MethodDescriptor<Q, R> descriptor, CallOptions options) {
            this.conn = conn;
            this.method = method;
            this.descriptor = descriptor;
            this.options = options;
        }

        @Override
        public void start(Listener<R> listener, Metadata headers) {
            this.listener = listener;
        }

        @Override
        public void request(int numMessages) {
            // A unary call has one answer; nothing to do.
        }

        @Override
        public void sendMessage(Q message) {
            try (InputStream in = descriptor.streamRequest(message)) {
                request = Jdk.readAllBytes(in);
            } catch (IOException e) {
                finish(Status.INTERNAL.withDescription("could not encode the request").withCause(e), new Metadata());
            }
        }

        @Override
        public void halfClose() {
            if (done.get()) {
                return;
            }
            if (request == null) {
                finish(Status.INTERNAL.withDescription("no request"), new Metadata());
                return;
            }
            id = conn.ids.incrementAndGet();
            long timeoutMillis = 0;
            Deadline deadline = options.getDeadline();
            if (deadline != null) {
                timeoutMillis = deadline.timeRemaining(TimeUnit.MILLISECONDS);
                if (timeoutMillis <= 0) {
                    finish(Status.DEADLINE_EXCEEDED.withDescription("deadline exceeded before sending"), new Metadata());
                    return;
                }
                timer = timers.schedule(() -> {
                    conn.pending.remove(id);
                    finish(Status.DEADLINE_EXCEEDED.withDescription("no answer from LeonsDB in time"), new Metadata());
                }, timeoutMillis, TimeUnit.MILLISECONDS);
            }
            ByteBuffer hdr = ByteBuffer.allocate(10);
            hdr.putInt(id).putShort((short) method).putInt(deadline == null ? 0 : (int) Math.min(timeoutMillis + 1, Integer.MAX_VALUE));
            conn.send(id, this, frame(REQUEST, hdr.array(), request));
        }

        @Override
        public void cancel(String message, Throwable cause) {
            conn.pending.remove(id);
            finish(Status.CANCELLED.withDescription(message).withCause(cause), new Metadata());
        }

        @Override
        public void answer(byte type, byte[] body) {
            if (type == RESPONSE) {
                R response;
                try {
                    response = descriptor.parseResponse(new ByteArrayInputStream(body, 4, body.length - 4));
                } catch (RuntimeException e) {
                    finish(Status.INTERNAL.withDescription("malformed response").withCause(e), new Metadata());
                    return;
                }
                if (done.compareAndSet(false, true)) {
                    cancelTimer();
                    callbacks.execute(() -> {
                        listener.onHeaders(new Metadata());
                        listener.onMessage(response);
                        listener.onClose(Status.OK, new Metadata());
                    });
                }
                return;
            }
            WireError e;
            try {
                e = WireError.parseFrom(ByteBuffer.wrap(body, 4, body.length - 4));
            } catch (InvalidProtocolBufferException ex) {
                finish(Status.INTERNAL.withDescription("malformed error").withCause(ex), new Metadata());
                return;
            }
            Metadata trailers = new Metadata();
            e.getTrailersMap().forEach((k, v) -> trailers.put(Metadata.Key.of(k, Metadata.ASCII_STRING_MARSHALLER), v));
            finish(Status.fromCodeValue(e.getCode()).withDescription(e.getMessage()), trailers);
        }

        @Override
        public void finish(Status status, Metadata trailers) {
            if (!done.compareAndSet(false, true)) {
                return;
            }
            cancelTimer();
            callbacks.execute(() -> listener.onClose(status, trailers));
        }

        private void cancelTimer() {
            if (timer != null) {
                timer.cancel(false);
            }
        }
    }

    // --- one stream --------------------------------------------------------

    /**
     * A stream of answers to one request (a subscription). The server sends
     * a header frame when it has begun, a message frame per answer, and a
     * response (empty) or error frame at its end; cancelling it sends a
     * cancel frame. Its listener hears of it in order, one event at a time.
     */
    private final class WireStream<Q, R> extends ClientCall<Q, R> implements Answered {
        private final Connection conn;
        private final int method;
        private final MethodDescriptor<Q, R> descriptor;
        private final CallOptions options;
        private final AtomicBoolean done = new AtomicBoolean();
        private final InOrder events = new InOrder(callbacks);
        private Listener<R> listener;
        private boolean headersDelivered; // only touched by events
        private byte[] request;
        private volatile int id;
        private ScheduledFuture<?> timer;

        WireStream(Connection conn, int method, MethodDescriptor<Q, R> descriptor, CallOptions options) {
            this.conn = conn;
            this.method = method;
            this.descriptor = descriptor;
            this.options = options;
        }

        @Override
        public void start(Listener<R> listener, Metadata headers) {
            this.listener = listener;
        }

        @Override
        public void request(int numMessages) {
            // Messages are delivered as they come; the server drops what a
            // subscriber cannot keep up with, and says how many.
        }

        @Override
        public void sendMessage(Q message) {
            try (InputStream in = descriptor.streamRequest(message)) {
                request = Jdk.readAllBytes(in);
            } catch (IOException e) {
                finish(Status.INTERNAL.withDescription("could not encode the request").withCause(e), new Metadata());
            }
        }

        @Override
        public void halfClose() {
            if (done.get()) {
                return;
            }
            if (request == null) {
                finish(Status.INTERNAL.withDescription("no request"), new Metadata());
                return;
            }
            int sid = conn.ids.incrementAndGet();
            long timeoutMillis = 0;
            Deadline deadline = options.getDeadline();
            if (deadline != null) {
                timeoutMillis = deadline.timeRemaining(TimeUnit.MILLISECONDS);
                if (timeoutMillis <= 0) {
                    finish(Status.DEADLINE_EXCEEDED.withDescription("deadline exceeded before sending"), new Metadata());
                    return;
                }
                timer = timers.schedule(() -> cancelWith(Status.DEADLINE_EXCEEDED.withDescription("the stream's deadline passed")),
                        timeoutMillis, TimeUnit.MILLISECONDS);
            }
            id = sid;
            ByteBuffer hdr = ByteBuffer.allocate(10);
            hdr.putInt(sid).putShort((short) method).putInt(deadline == null ? 0 : (int) Math.min(timeoutMillis + 1, Integer.MAX_VALUE));
            conn.send(sid, this, frame(REQUEST, hdr.array(), request));
        }

        @Override
        public void cancel(String message, Throwable cause) {
            cancelWith(Status.CANCELLED.withDescription(message).withCause(cause));
        }

        private void cancelWith(Status status) {
            int sid = id;
            if (sid != 0 && conn.pending.remove(sid) != null && conn.alive.get()) {
                conn.out.add(frame(CANCEL, ByteBuffer.allocate(4).putInt(sid).array(), new byte[0]));
            }
            finish(status, new Metadata());
        }

        @Override
        public void answer(byte type, byte[] body) {
            switch (type) {
                case HEADER:
                    events.execute(this::deliverHeaders);
                    break;
                case MESSAGE: {
                    R message;
                    try {
                        message = descriptor.parseResponse(new ByteArrayInputStream(body, 4, body.length - 4));
                    } catch (RuntimeException e) {
                        cancelWith(Status.INTERNAL.withDescription("malformed message").withCause(e));
                        return;
                    }
                    events.execute(() -> {
                        if (!done.get()) {
                            deliverHeaders();
                            listener.onMessage(message);
                        }
                    });
                    break;
                }
                case RESPONSE:
                    finish(Status.OK, new Metadata());
                    break;
                default: {
                    WireError e;
                    try {
                        e = WireError.parseFrom(ByteBuffer.wrap(body, 4, body.length - 4));
                    } catch (InvalidProtocolBufferException ex) {
                        finish(Status.INTERNAL.withDescription("malformed error").withCause(ex), new Metadata());
                        return;
                    }
                    Metadata trailers = new Metadata();
                    e.getTrailersMap().forEach((k, v) -> trailers.put(Metadata.Key.of(k, Metadata.ASCII_STRING_MARSHALLER), v));
                    finish(Status.fromCodeValue(e.getCode()).withDescription(e.getMessage()), trailers);
                    break;
                }
            }
        }

        private void deliverHeaders() {
            if (!headersDelivered) {
                headersDelivered = true;
                listener.onHeaders(new Metadata());
            }
        }

        @Override
        public void finish(Status status, Metadata trailers) {
            if (!done.compareAndSet(false, true)) {
                return;
            }
            if (timer != null) {
                timer.cancel(false);
            }
            events.execute(() -> listener.onClose(status, trailers));
        }
    }

    /** Runs tasks on an executor one at a time, in the order given. */
    private static final class InOrder implements Executor {
        private final Executor pool;
        private final java.util.concurrent.ConcurrentLinkedQueue<Runnable> tasks = new java.util.concurrent.ConcurrentLinkedQueue<>();
        private final AtomicBoolean running = new AtomicBoolean();

        InOrder(Executor pool) {
            this.pool = pool;
        }

        @Override
        public void execute(Runnable task) {
            tasks.add(task);
            schedule();
        }

        private void schedule() {
            if (running.compareAndSet(false, true)) {
                pool.execute(this::drain);
            }
        }

        private void drain() {
            try {
                Runnable task;
                while ((task = tasks.poll()) != null) {
                    try {
                        task.run();
                    } catch (RuntimeException e) {
                        LOG.log(Level.WARNING, "A LeonsDB stream listener failed", e);
                    }
                }
            } finally {
                running.set(false);
                if (!tasks.isEmpty()) {
                    schedule();
                }
            }
        }
    }

    // --- framing -----------------------------------------------------------

    private static final class Frame {
        final byte type;
        final byte[] body;

        Frame(byte type, byte[] body) {
            this.type = type;
            this.body = body;
        }
    }

    private static Frame readFrame(DataInputStream in) throws IOException {
        int n;
        try {
            n = in.readInt();
        } catch (EOFException e) {
            throw new IOException("the server closed the connection", e);
        }
        if (n < 1 || n > MAX_FRAME) {
            throw new IOException("frame of " + n + " bytes");
        }
        byte type = in.readByte();
        byte[] body = new byte[n - 1];
        in.readFully(body);
        return new Frame(type, body);
    }

    private static byte[] frame(byte type, byte[] header, byte[] message) {
        int hlen = header == null ? 0 : header.length;
        ByteArrayOutputStream b = new ByteArrayOutputStream(5 + hlen + message.length);
        ByteBuffer start = ByteBuffer.allocate(5).putInt(1 + hlen + message.length).put(type);
        b.write(start.array(), 0, 5);
        if (header != null) {
            b.write(header, 0, header.length);
        }
        b.write(message, 0, message.length);
        return b.toByteArray();
    }
}
