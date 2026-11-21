// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core;

import com.google.protobuf.ByteString;
import com.leonsdb.proto.v1.ApplyManyRequest;
import com.leonsdb.proto.v1.ApplyManyResponse;
import com.leonsdb.proto.v1.ApplyOutcome;
import com.leonsdb.proto.v1.ApplyRequest;
import com.leonsdb.proto.v1.ApplyResponse;
import io.grpc.Status;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Gathers saves into batches. A save is sent at once while fewer than
 * {@link #MAX_IN_FLIGHT} calls are on their way; saves that come meanwhile
 * go together in the next call ({@code ApplyMany}), which the server stores
 * with one wait for the disk. Under light load nothing waits; under heavy
 * load (many players saving at once) batches grow. Each save still gets its
 * own result, exactly as a single Apply would give it.
 *
 * <p>A server without ApplyMany (before 0.9) gets single Applys only.
 */
final class ApplyBatcher {
    static final int MAX_IN_FLIGHT = 8;
    static final int MAX_BATCH = 500;

    private static final class Pending {
        final ApplyRequest request;
        final CompletableFuture<ApplyResponse> result;

        Pending(ApplyRequest request, CompletableFuture<ApplyResponse> result) {
            this.request = request;
            this.result = result;
        }
    }

    private final Function<ApplyRequest, CompletableFuture<ApplyResponse>> single;
    private final Function<ApplyManyRequest, CompletableFuture<ApplyManyResponse>> many;
    private final Function<ApplyOutcome, RuntimeException> refusal;

    private final List<Pending> queue = new ArrayList<>();
    private int inFlight;
    private volatile boolean manyUnsupported;

    /**
     * @param single  sends one Apply
     * @param many    sends one ApplyMany; fails as a whole only when the call failed
     * @param refusal turns a refused write into the exception a single Apply would fail with
     */
    ApplyBatcher(Function<ApplyRequest, CompletableFuture<ApplyResponse>> single,
                 Function<ApplyManyRequest, CompletableFuture<ApplyManyResponse>> many,
                 Function<ApplyOutcome, RuntimeException> refusal) {
        this.single = single;
        this.many = many;
        this.refusal = refusal;
    }

    CompletableFuture<ApplyResponse> apply(ApplyRequest request) {
        Pending p = new Pending(request, new CompletableFuture<>());
        List<Pending> batch = null;
        synchronized (this) {
            queue.add(p);
            if (inFlight < MAX_IN_FLIGHT) {
                batch = takeLocked();
                inFlight++;
            }
        }
        if (batch != null) {
            send(batch);
        }
        return p.result;
    }

    // At most one save per record in a batch; a second one waits for the next.
    private List<Pending> takeLocked() {
        List<Pending> batch = new ArrayList<>();
        Set<ByteString> records = new HashSet<>();
        List<Pending> rest = new ArrayList<>();
        for (Pending p : queue) {
            if (batch.size() < MAX_BATCH && records.add(p.request.getRef().toByteString())) {
                batch.add(p);
            } else {
                rest.add(p);
            }
        }
        queue.clear();
        queue.addAll(rest);
        return batch;
    }

    private void send(List<Pending> batch) {
        CompletableFuture<?> sent;
        if (batch.size() == 1 || manyUnsupported) {
            List<CompletableFuture<ApplyResponse>> calls = new ArrayList<>(batch.size());
            for (Pending p : batch) {
                calls.add(single.apply(p.request).whenComplete((r, err) -> complete(p, r, err)));
            }
            sent = CompletableFuture.allOf(calls.toArray(new CompletableFuture<?>[0]));
        } else {
            ApplyManyRequest.Builder req = ApplyManyRequest.newBuilder();
            batch.forEach(p -> req.addWrites(p.request));
            sent = many.apply(req.build()).handle((resp, err) -> {
                if (err != null) {
                    Throwable cause = err instanceof java.util.concurrent.CompletionException && err.getCause() != null
                            ? err.getCause() : err;
                    if (cause instanceof RequestException && ((RequestException) cause).code() == Status.Code.UNIMPLEMENTED) {
                        // An older server: from now on, one Apply per save.
                        manyUnsupported = true;
                        batch.forEach(p -> single.apply(p.request).whenComplete((r, e) -> complete(p, r, e)));
                        return null;
                    }
                    batch.forEach(p -> p.result.completeExceptionally(cause));
                    return null;
                }
                for (int i = 0; i < batch.size(); i++) {
                    Pending p = batch.get(i);
                    if (i >= resp.getResultsCount()) {
                        p.result.completeExceptionally(new IllegalStateException("LeonsDB returned no result for this save"));
                    } else if (resp.getResults(i).hasError()) {
                        p.result.completeExceptionally(refusal.apply(resp.getResults(i)));
                    } else {
                        p.result.complete(resp.getResults(i).getResponse());
                    }
                }
                return null;
            });
        }
        sent.whenComplete((r, e) -> next());
    }

    private static void complete(Pending p, ApplyResponse r, Throwable err) {
        if (err != null) {
            p.result.completeExceptionally(err instanceof java.util.concurrent.CompletionException && err.getCause() != null
                    ? err.getCause() : err);
        } else {
            p.result.complete(r);
        }
    }

    private void next() {
        List<Pending> batch;
        synchronized (this) {
            if (queue.isEmpty()) {
                inFlight--;
                return;
            }
            batch = takeLocked();
        }
        send(batch);
    }
}
