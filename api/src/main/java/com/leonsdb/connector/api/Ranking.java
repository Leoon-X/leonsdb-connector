// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.util.UUID;

/**
 * One entry of a {@link Leaderboard}.
 *
 * @param rank   1 for the best; equal values share a rank
 * @param player the player, for player collections (otherwise null)
 * @param id     the record's key, for global collections (otherwise null)
 * @param value  the ranked value: an Integer, Long or Double, or a Long of
 *               epoch milliseconds for Instant fields
 * @param data   the full record
 * @param <T>    the record type
 */
public record Ranking<T>(long rank, UUID player, String id, Number value, T data) {
}
