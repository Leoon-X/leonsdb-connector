// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.util.List;

/**
 * What a rollback changed, or would change.
 *
 * @param applied        false for a preview
 * @param recordsChanged records changed (applied), or that would change
 * @param recordsFailed  records that could not be changed; apply again
 * @param undoneBy       names of the tokens whose changes are undone
 * @param records        every record that differs
 */
public record RollbackResult(boolean applied, int recordsChanged, int recordsFailed, List<String> undoneBy,
                             List<RecordRollback> records) {

    /**
     * One record of a rollback.
     *
     * @param namespace  its namespace
     * @param collection its collection
     * @param key        the player UUID or the record id
     * @param fields     the fields set back
     * @param removed    it did not exist at that time and is emptied or deleted
     * @param restored   it was deleted since and comes back
     * @param error      why it failed, or null
     */
    public record RecordRollback(String namespace, String collection, String key, List<String> fields,
                                 boolean removed, boolean restored, String error) {
    }
}
