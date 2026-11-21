// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.util.List;
import java.util.Objects;

/**
 * What a rollback changed, or would change.
 */
public final class RollbackResult {

    private final boolean applied;
    private final int recordsChanged;
    private final int recordsFailed;
    private final List<String> undoneBy;
    private final List<RecordRollback> records;

    /**
     * Creates a result.
     *
     * @param applied        false for a preview
     * @param recordsChanged records changed (applied), or that would change
     * @param recordsFailed  records that could not be changed; apply again
     * @param undoneBy       names of the tokens whose changes are undone
     * @param records        every record that differs
     */
    public RollbackResult(boolean applied, int recordsChanged, int recordsFailed, List<String> undoneBy,
                          List<RecordRollback> records) {
        this.applied = applied;
        this.recordsChanged = recordsChanged;
        this.recordsFailed = recordsFailed;
        this.undoneBy = undoneBy;
        this.records = records;
    }

    /** @return false for a preview */
    public boolean applied() {
        return applied;
    }

    /** @return records changed (applied), or that would change */
    public int recordsChanged() {
        return recordsChanged;
    }

    /** @return records that could not be changed; apply again */
    public int recordsFailed() {
        return recordsFailed;
    }

    /** @return names of the tokens whose changes are undone */
    public List<String> undoneBy() {
        return undoneBy;
    }

    /** @return every record that differs */
    public List<RecordRollback> records() {
        return records;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RollbackResult)) {
            return false;
        }
        RollbackResult r = (RollbackResult) o;
        return applied == r.applied && recordsChanged == r.recordsChanged && recordsFailed == r.recordsFailed
                && Objects.equals(undoneBy, r.undoneBy) && Objects.equals(records, r.records);
    }

    @Override
    public int hashCode() {
        return Objects.hash(applied, recordsChanged, recordsFailed, undoneBy, records);
    }

    @Override
    public String toString() {
        return "RollbackResult[applied=" + applied + ", recordsChanged=" + recordsChanged + ", recordsFailed="
                + recordsFailed + ", undoneBy=" + undoneBy + ", records=" + records + "]";
    }

    /**
     * One record of a rollback.
     */
    public static final class RecordRollback {

        private final String namespace;
        private final String collection;
        private final String key;
        private final List<String> fields;
        private final boolean removed;
        private final boolean restored;
        private final String error;

        /**
         * Creates one record's part of a rollback.
         *
         * @param namespace  its namespace
         * @param collection its collection
         * @param key        the player UUID or the record id
         * @param fields     the fields set back
         * @param removed    it did not exist at that time and is emptied or deleted
         * @param restored   it was deleted since and comes back
         * @param error      why it failed, or null
         */
        public RecordRollback(String namespace, String collection, String key, List<String> fields,
                              boolean removed, boolean restored, String error) {
            this.namespace = namespace;
            this.collection = collection;
            this.key = key;
            this.fields = fields;
            this.removed = removed;
            this.restored = restored;
            this.error = error;
        }

        /** @return its namespace */
        public String namespace() {
            return namespace;
        }

        /** @return its collection */
        public String collection() {
            return collection;
        }

        /** @return the player UUID or the record id */
        public String key() {
            return key;
        }

        /** @return the fields set back */
        public List<String> fields() {
            return fields;
        }

        /** @return it did not exist at that time and is emptied or deleted */
        public boolean removed() {
            return removed;
        }

        /** @return it was deleted since and comes back */
        public boolean restored() {
            return restored;
        }

        /** @return why it failed, or null */
        public String error() {
            return error;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof RecordRollback)) {
                return false;
            }
            RecordRollback r = (RecordRollback) o;
            return removed == r.removed && restored == r.restored && Objects.equals(namespace, r.namespace)
                    && Objects.equals(collection, r.collection) && Objects.equals(key, r.key)
                    && Objects.equals(fields, r.fields) && Objects.equals(error, r.error);
        }

        @Override
        public int hashCode() {
            return Objects.hash(namespace, collection, key, fields, removed, restored, error);
        }

        @Override
        public String toString() {
            return "RecordRollback[namespace=" + namespace + ", collection=" + collection + ", key=" + key
                    + ", fields=" + fields + ", removed=" + removed + ", restored=" + restored + ", error=" + error + "]";
        }
    }
}
