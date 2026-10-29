// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.importer;

import com.leonsdb.proto.v1.AbortBulkImportRequest;
import com.leonsdb.proto.v1.ApplyOutcome;
import com.leonsdb.proto.v1.BeginBulkImportRequest;
import com.leonsdb.proto.v1.BulkImportRequest;
import com.leonsdb.proto.v1.Collection;
import com.leonsdb.proto.v1.CommitBulkImportRequest;
import com.leonsdb.proto.v1.DataServiceGrpc;
import com.leonsdb.proto.v1.ImportRecord;
import com.leonsdb.proto.v1.ListCollectionsRequest;
import com.leonsdb.proto.v1.ScanRequest;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.MetadataUtils;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** The LeonsDB server an import goes to. */
final class LeonsDBTarget implements AutoCloseable {

    private static final Metadata.Key<String> REASON = Metadata.Key.of("leonsdb-error-reason", Metadata.ASCII_STRING_MARSHALLER);

    private final Plan.Target target;
    private final ManagedChannel channel;
    private final DataServiceGrpc.DataServiceBlockingStub data;

    LeonsDBTarget(Plan.Target target) {
        this.target = target;
        NettyChannelBuilder b = NettyChannelBuilder.forAddress(target.host(), target.port()).maxInboundMessageSize(16 << 20);
        if (target.tls()) {
            b.useTransportSecurity();
        } else {
            b.usePlaintext();
        }
        channel = b.build();
        Metadata md = new Metadata();
        md.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + target.token());
        data = DataServiceGrpc.newBlockingStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(md));
    }

    private DataServiceGrpc.DataServiceBlockingStub stub(int seconds) {
        return data.withDeadlineAfter(seconds, TimeUnit.SECONDS);
    }

    /**
     * @param namespace  the namespace
     * @param collection the collection
     * @return the collection with its declared fields, or null if it does not exist
     * @throws ImportException if LeonsDB cannot be asked
     */
    Collection collection(String namespace, String collection) throws ImportException {
        try {
            for (Collection c : stub(30).listCollections(ListCollectionsRequest.newBuilder().setNamespace(namespace).build())
                    .getCollectionsList()) {
                if (c.getNamespace().equals(namespace) && c.getName().equals(collection)) {
                    return c;
                }
            }
            return null;
        } catch (StatusRuntimeException e) {
            throw failure("list the collections", e);
        }
    }

    /** @return whether the collection has no records */
    boolean isEmpty(String namespace, String collection) throws ImportException {
        try {
            return stub(30).scan(ScanRequest.newBuilder().setNamespace(namespace).setCollection(collection).setPageSize(1).build())
                    .getRecordsCount() == 0;
        } catch (StatusRuntimeException e) {
            throw failure("read " + namespace + "." + collection, e);
        }
    }

    String begin(String namespace, String collection) throws ImportException {
        try {
            return stub(60).beginBulkImport(BeginBulkImportRequest.newBuilder().setNamespace(namespace)
                    .setCollection(collection).build()).getImportId();
        } catch (StatusRuntimeException e) {
            throw failure("start the import into " + namespace + "." + collection, e);
        }
    }

    /** @return each record's outcome, in order */
    List<ApplyOutcome> add(String importId, List<ImportRecord> records) throws ImportException {
        try {
            return stub(300).bulkImport(BulkImportRequest.newBuilder().setImportId(importId).addAllRecords(records).build())
                    .getResultsList();
        } catch (StatusRuntimeException e) {
            throw failure("send records", e);
        }
    }

    long commit(String importId) throws ImportException {
        try {
            return stub(3600).commitBulkImport(CommitBulkImportRequest.newBuilder().setImportId(importId).build()).getImported();
        } catch (StatusRuntimeException e) {
            throw failure("store the import", e);
        }
    }

    void abort(String importId) {
        try {
            stub(60).abortBulkImport(AbortBulkImportRequest.newBuilder().setImportId(importId).build());
        } catch (StatusRuntimeException e) {
            // an import left alone is given up after 10 minutes anyway
        }
    }

    private ImportException failure(String what, StatusRuntimeException e) {
        Status s = e.getStatus();
        String reason = e.getTrailers() == null ? null : e.getTrailers().get(REASON);
        String hint = switch (s.getCode()) {
            case UNAVAILABLE, DEADLINE_EXCEEDED -> " Is LeonsDB running at " + target.host() + ":" + target.port() + "?";
            case UNAUTHENTICATED -> " The token is not valid.";
            case PERMISSION_DENIED -> " The token may not do this; use one that may write every field of the collection, "
                    + "for example the admin token.";
            default -> "";
        };
        if ("PLAYER_ONLINE".equals(reason)) {
            hint = " Some of the players are online on a server right now. Stop the Minecraft servers (or wait until "
                    + "those players have left), then run the import again. Nothing was stored.";
        } else if ("COLLECTION_NOT_EMPTY".equals(reason)) {
            hint = " A bulk import only loads an empty collection, so nothing already there can be overwritten.";
        }
        return new ImportException("Could not " + what + ": " + s.getCode()
                + (s.getDescription() != null ? " (" + s.getDescription() + ")" : "") + "." + hint, e);
    }

    @Override
    public void close() {
        channel.shutdownNow();
        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
