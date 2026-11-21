// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.util.Objects;
import java.util.UUID;

/**
 * One entry of a {@link Leaderboard}.
 *
 * @param <T> the record type
 */
public final class Ranking<T> {

    private final long rank;
    private final UUID player;
    private final String id;
    private final Number value;
    private final T data;

    /**
     * Creates an entry.
     *
     * @param rank   1 for the best; equal values share a rank
     * @param player the player, for player collections (otherwise null)
     * @param id     the record's key, for global collections (otherwise null)
     * @param value  the ranked value: an Integer, Long or Double, or a Long of
     *               epoch milliseconds for Instant fields
     * @param data   the full record
     */
    public Ranking(long rank, UUID player, String id, Number value, T data) {
        this.rank = rank;
        this.player = player;
        this.id = id;
        this.value = value;
        this.data = data;
    }

    /** @return 1 for the best; equal values share a rank */
    public long rank() {
        return rank;
    }

    /** @return the player, for player collections (otherwise null) */
    public UUID player() {
        return player;
    }

    /** @return the record's key, for global collections (otherwise null) */
    public String id() {
        return id;
    }

    /**
     * @return the ranked value: an Integer, Long or Double, or a Long of
     *         epoch milliseconds for Instant fields
     */
    public Number value() {
        return value;
    }

    /** @return the full record */
    public T data() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Ranking)) {
            return false;
        }
        Ranking<?> r = (Ranking<?>) o;
        return rank == r.rank && Objects.equals(player, r.player) && Objects.equals(id, r.id)
                && Objects.equals(value, r.value) && Objects.equals(data, r.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rank, player, id, value, data);
    }

    @Override
    public String toString() {
        return "Ranking[rank=" + rank + ", player=" + player + ", id=" + id + ", value=" + value + ", data=" + data + "]";
    }
}
