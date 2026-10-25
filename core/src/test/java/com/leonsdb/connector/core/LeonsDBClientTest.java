// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.leonsdb.connector.api.LeonsDBException;
import com.leonsdb.proto.v1.GetInfoRequest;
import com.leonsdb.proto.v1.GetInfoResponse;
import com.leonsdb.proto.v1.ServerServiceGrpc;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class LeonsDBClientTest {

    private Server server;
    private LeonsDBClient client;

    /** A fake server whose GetInfo answer each test controls. */
    private static final class FakeServerService extends ServerServiceGrpc.ServerServiceImplBase {
        final AtomicReference<GetInfoRequest> lastRequest = new AtomicReference<>();
        volatile GetInfoResponse response;
        volatile Status failure;

        @Override
        public void getInfo(GetInfoRequest request, StreamObserver<GetInfoResponse> observer) {
            lastRequest.set(request);
            if (failure != null) {
                observer.onError(failure.asRuntimeException());
                return;
            }
            observer.onNext(response);
            observer.onCompleted();
        }
    }

    private FakeServerService start() throws Exception {
        var service = new FakeServerService();
        var name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name).directExecutor().addService(service).build().start();
        var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        var settings = new ConnectionSettings("test", 1, false, null, Duration.ofSeconds(2));
        client = new LeonsDBClient(channel, settings, "test-client");
        return service;
    }

    private static GetInfoResponse info(int min, int max) {
        return GetInfoResponse.newBuilder()
                .setServerVersion("0.1.0")
                .setBuildCommit("abc123")
                .setMinProtocolVersion(min)
                .setMaxProtocolVersion(max)
                .setInstanceId("0123456789abcdef0123456789abcdef")
                .build();
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (client != null) {
            client.close();
        }
        if (server != null) {
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void fetchesServerInfoAndSendsClientDetails() throws Exception {
        var service = start();
        service.response = info(1, 1);

        var result = client.fetchServerInfo().get(5, TimeUnit.SECONDS);

        assertEquals("0.1.0", result.serverVersion());
        assertEquals("0123456789abcdef0123456789abcdef", result.instanceId());
        assertEquals(LeonsDBClient.PROTOCOL_VERSION, service.lastRequest.get().getClientProtocolVersion());
        assertEquals("test-client", service.lastRequest.get().getClientName());
    }

    @Test
    void rejectsServerThatIsTooOld() throws Exception {
        start().response = info(0, LeonsDBClient.PROTOCOL_VERSION - 1);
        var e = assertThrows(ExecutionException.class, () -> client.fetchServerInfo().get(5, TimeUnit.SECONDS));
        var cause = assertInstanceOf(IncompatibleServerException.class, e.getCause());
        assertTrue(cause.getMessage().contains("update the LeonsDB server"), cause.getMessage());
    }

    @Test
    void rejectsServerThatIsTooNew() throws Exception {
        start().response = info(LeonsDBClient.PROTOCOL_VERSION + 1, LeonsDBClient.PROTOCOL_VERSION + 3);
        var e = assertThrows(ExecutionException.class, () -> client.fetchServerInfo().get(5, TimeUnit.SECONDS));
        var cause = assertInstanceOf(IncompatibleServerException.class, e.getCause());
        assertTrue(cause.getMessage().contains("update the connector"), cause.getMessage());
    }

    @Test
    void reportsServerErrorsAsLeonsDBException() throws Exception {
        start().failure = Status.UNAVAILABLE.withDescription("going away");
        var e = assertThrows(ExecutionException.class, () -> client.fetchServerInfo().get(5, TimeUnit.SECONDS));
        var cause = assertInstanceOf(LeonsDBException.class, e.getCause());
        assertTrue(cause.getMessage().contains("UNAVAILABLE"), cause.getMessage());
        assertTrue(cause.getMessage().contains("going away"), cause.getMessage());
    }
}
