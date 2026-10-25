// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.core;

import com.leonsdb.connector.api.LeonsDBException;
import io.grpc.Status;

/**
 * A request to the server failed. Carries the gRPC status code so callers
 * can tell temporary problems from permanent ones.
 */
public final class RequestException extends LeonsDBException {

    private final transient Status.Code code;
    private final int writeIndex;

    RequestException(String message, Status.Code code, String reason, int writeIndex, Throwable cause) {
        super(message, reason, cause);
        this.code = code;
        this.writeIndex = writeIndex;
    }

    /**
     * For a refused transaction: which write (0-based) was refused.
     *
     * @return the index, or -1 when the error is not about one write
     */
    public int writeIndex() {
        return writeIndex;
    }

    /**
     * The gRPC status code.
     *
     * @return the code
     */
    public Status.Code code() {
        return code;
    }

    /**
     * Whether retrying later may succeed: the server was unreachable,
     * overloaded or too slow. Such failures never mean the data was rejected.
     *
     * @return true for temporary failures
     */
    public boolean isTemporary() {
        return switch (code) {
            case UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, ABORTED, INTERNAL, UNKNOWN, CANCELLED -> true;
            default -> false;
        };
    }

    /**
     * Whether the server rejected the write because the record changed
     * since it was read.
     *
     * @return true for a version mismatch
     */
    public boolean isVersionMismatch() {
        return "VERSION_MISMATCH".equals(reason());
    }
}
