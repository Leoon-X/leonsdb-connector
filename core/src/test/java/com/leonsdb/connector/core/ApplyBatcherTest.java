// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.leonsdb.proto.v1.ApplyManyRequest;
import com.leonsdb.proto.v1.ApplyManyResponse;
import com.leonsdb.proto.v1.ApplyOutcome;
import com.leonsdb.proto.v1.ApplyRequest;
import com.leonsdb.proto.v1.ApplyResponse;
import com.leonsdb.proto.v1.RecordRef;
import com.leonsdb.proto.v1.WriteError;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ApplyBatcherTest {
    private final ScheduledExecutorService disk = Executors.newScheduledThreadPool(4);

    @AfterEach
    void stop() {
        disk.shutdownNow();
    }

    private static ApplyRequest save(int player) {
        return ApplyRequest.newBuilder().setRef(RecordRef.newBuilder().setNamespace("n").setCollection("c")
                .setId("p" + player)).setEpoch(player).build();
    }

    // Answers after 5 ms, like a write waiting for the disk.
    private <T> CompletableFuture<T> later(java.util.function.Supplier<T> answer) {
        var f = new CompletableFuture<T>();
        disk.schedule(() -> {
            try {
                f.complete(answer.get());
            } catch (RuntimeException e) {
                f.completeExceptionally(e);
            }
        }, 5, TimeUnit.MILLISECONDS);
        return f;
    }

    @Test
    void eachSaveGetsItsOwnResultAndSavesAreGathered() throws Exception {
        List<Integer> batchSizes = java.util.Collections.synchronizedList(new ArrayList<>());
        var batcher = new ApplyBatcher(
                req -> later(() -> {
                    if (req.getEpoch() % 7 == 0) {
                        throw new RequestException("refused", Status.Code.FAILED_PRECONDITION, "STALE_EPOCH", -1, null);
                    }
                    return ApplyResponse.newBuilder().setVersion(req.getEpoch()).build();
                }),
                req -> later(() -> {
                    batchSizes.add(req.getWritesCount());
                    var resp = ApplyManyResponse.newBuilder();
                    for (ApplyRequest w : req.getWritesList()) {
                        if (w.getEpoch() % 7 == 0) {
                            resp.addResults(ApplyOutcome.newBuilder().setError(WriteError.newBuilder()
                                    .setCode(Status.Code.FAILED_PRECONDITION.value()).setReason("STALE_EPOCH")));
                        } else {
                            resp.addResults(ApplyOutcome.newBuilder().setResponse(ApplyResponse.newBuilder().setVersion(w.getEpoch())));
                        }
                    }
                    return resp.build();
                }),
                outcome -> new RequestException("refused", Status.Code.FAILED_PRECONDITION, outcome.getError().getReason(), -1, null));
        List<CompletableFuture<ApplyResponse>> results = new ArrayList<>();
        for (int i = 1; i <= 400; i++) {
            results.add(batcher.apply(save(i)));
        }
        for (int i = 1; i <= 400; i++) {
            var f = results.get(i - 1);
            if (i % 7 == 0) {
                var e = org.junit.jupiter.api.Assertions.assertThrows(ExecutionException.class, () -> f.get(5, TimeUnit.SECONDS));
                assertTrue(e.getCause() instanceof RequestException re && "STALE_EPOCH".equals(re.reason()));
            } else {
                assertEquals(i, f.get(5, TimeUnit.SECONDS).getVersion());
            }
        }
        assertTrue(batchSizes.stream().mapToInt(Integer::intValue).max().orElse(0) > 10,
                "saves were not gathered: " + batchSizes);
    }

    @Test
    void anOlderServerGetsSingleApplys() throws Exception {
        AtomicInteger manyCalls = new AtomicInteger();
        var batcher = new ApplyBatcher(
                req -> later(() -> ApplyResponse.newBuilder().setVersion(req.getEpoch()).build()),
                req -> {
                    manyCalls.incrementAndGet();
                    return CompletableFuture.failedFuture(new RequestException("no", Status.Code.UNIMPLEMENTED, null, -1, null));
                },
                outcome -> new IllegalStateException());
        List<CompletableFuture<ApplyResponse>> results = new ArrayList<>();
        for (int round = 0; round < 3; round++) {
            for (int i = 1; i <= 50; i++) {
                results.add(batcher.apply(save(i)));
            }
            for (var f : results) {
                f.get(5, TimeUnit.SECONDS);
            }
        }
        assertTrue(manyCalls.get() <= ApplyBatcher.MAX_IN_FLIGHT, "kept trying ApplyMany: " + manyCalls.get());
    }

    @Test
    void twoSavesOfOneRecordGoInDifferentCalls() throws Exception {
        List<ApplyManyRequest> calls = java.util.Collections.synchronizedList(new ArrayList<>());
        var batcher = new ApplyBatcher(
                req -> later(() -> ApplyResponse.getDefaultInstance()),
                req -> later(() -> {
                    calls.add(req);
                    var resp = ApplyManyResponse.newBuilder();
                    req.getWritesList().forEach(w -> resp.addResults(ApplyOutcome.newBuilder().setResponse(ApplyResponse.getDefaultInstance())));
                    return resp.build();
                }),
                outcome -> new IllegalStateException());
        List<CompletableFuture<ApplyResponse>> results = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            results.add(batcher.apply(save(i % 20)));
        }
        for (var f : results) {
            f.get(5, TimeUnit.SECONDS);
        }
        for (ApplyManyRequest call : calls) {
            var refs = new java.util.HashSet<ByteString>();
            for (ApplyRequest w : call.getWritesList()) {
                assertTrue(refs.add(w.getRef().toByteString()), "a record twice in one call");
            }
        }
    }
}
