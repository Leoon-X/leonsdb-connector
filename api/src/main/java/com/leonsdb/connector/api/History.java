// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.connector.api;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * The past of your data: what a record looked like at a given time, what
 * changed it, and turning it back.
 *
 * <pre>{@code
 * // Bob's kit as it was an hour ago
 * LeonsDB.history().player(bob, Kit.class, Instant.now().minus(Duration.ofHours(1)))
 *        .thenAccept(kit -> ...);
 *
 * // Undo everything that happened to Bob in the last 10 minutes
 * LeonsDB.history().rollbackPlayer(bob, Instant.now().minus(Duration.ofMinutes(10))).apply();
 * }</pre>
 *
 * <p>Works for online and offline players alike. Writes to a record within
 * one minute are kept as one step, so a time inside such a minute gives the
 * state from before that minute: never later than asked for. Changes to
 * {@link Transactional} fields are kept one by one. How far back history
 * goes is set on the LeonsDB server (7 days in full detail by default).
 *
 * <p>The futures complete on the main thread.
 */
public interface History {

    /**
     * A player's data as it was at a time.
     *
     * @param player the player
     * @param type   a registered {@link PlayerData} class
     * @param time   the moment
     * @param <T>    the data type
     * @return the data then, or empty if the player had none; fields that
     *         had no value have their defaults
     */
    <T> CompletableFuture<Optional<T>> player(UUID player, Class<T> type, Instant time);

    /**
     * A global record as it was at a time.
     *
     * @param type a registered {@link GlobalData} class
     * @param id   the record's id
     * @param time the moment
     * @param <T>  the data type
     * @return the record then, or empty if it did not exist
     */
    <T> CompletableFuture<Optional<T>> global(Class<T> type, String id, Instant time);

    /**
     * A player's recent changes, newest first.
     *
     * @param player the player
     * @param type   a registered {@link PlayerData} class
     * @param limit  at most this many (up to 1000)
     * @return the changes
     */
    CompletableFuture<List<Change>> changes(UUID player, Class<?> type, int limit);

    /**
     * A global record's recent changes, newest first.
     *
     * @param type  a registered {@link GlobalData} class
     * @param id    the record's id
     * @param limit at most this many (up to 1000)
     * @return the changes
     */
    CompletableFuture<List<Change>> changes(Class<?> type, String id, int limit);

    /**
     * Prepares turning all of a player's data (every collection of every
     * plugin) back to how it was at a time.
     *
     * @param player the player
     * @param to     the moment to go back to
     * @return the rollback, to preview or apply
     */
    Rollback rollbackPlayer(UUID player, Instant to);

    /**
     * Prepares turning one collection of a player's data back.
     *
     * @param player the player
     * @param type   a registered {@link PlayerData} class
     * @param to     the moment to go back to
     * @return the rollback, to preview or apply
     */
    Rollback rollback(UUID player, Class<?> type, Instant to);

    /**
     * Prepares turning a global record back.
     *
     * @param type a registered {@link GlobalData} class
     * @param id   the record's id
     * @param to   the moment to go back to
     * @return the rollback, to preview or apply
     */
    Rollback rollback(Class<?> type, String id, Instant to);

    /**
     * One change of a record. When several writes in a row were kept as one
     * change, {@code writes} says how many.
     */
    final class Change {

        private final Instant firstTime;
        private final Instant lastTime;
        private final int writes;
        private final boolean exact;
        private final String by;
        private final boolean created;
        private final boolean deleted;
        private final Map<String, FieldChange> fields;

        /**
         * Creates a change.
         *
         * @param firstTime when the first write happened
         * @param lastTime  when the last write happened
         * @param writes    how many writes this change holds
         * @param exact     a single write, never merged with others
         * @param by        the name of the token that made it (a Minecraft
         *                  server's, a website's, ...)
         * @param created   the record was created
         * @param deleted   the record was deleted
         * @param fields    each changed field of your class, before and after
         */
        public Change(Instant firstTime, Instant lastTime, int writes, boolean exact, String by,
                      boolean created, boolean deleted, Map<String, FieldChange> fields) {
            this.firstTime = firstTime;
            this.lastTime = lastTime;
            this.writes = writes;
            this.exact = exact;
            this.by = by;
            this.created = created;
            this.deleted = deleted;
            this.fields = fields;
        }

        /** @return when the first write happened */
        public Instant firstTime() {
            return firstTime;
        }

        /** @return when the last write happened */
        public Instant lastTime() {
            return lastTime;
        }

        /** @return how many writes this change holds */
        public int writes() {
            return writes;
        }

        /** @return a single write, never merged with others */
        public boolean exact() {
            return exact;
        }

        /** @return the name of the token that made it (a Minecraft server's, a website's, ...) */
        public String by() {
            return by;
        }

        /** @return the record was created */
        public boolean created() {
            return created;
        }

        /** @return the record was deleted */
        public boolean deleted() {
            return deleted;
        }

        /** @return each changed field of your class, before and after */
        public Map<String, FieldChange> fields() {
            return fields;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Change)) {
                return false;
            }
            Change c = (Change) o;
            return writes == c.writes && exact == c.exact && created == c.created && deleted == c.deleted
                    && Objects.equals(firstTime, c.firstTime) && Objects.equals(lastTime, c.lastTime)
                    && Objects.equals(by, c.by) && Objects.equals(fields, c.fields);
        }

        @Override
        public int hashCode() {
            return Objects.hash(firstTime, lastTime, writes, exact, by, created, deleted, fields);
        }

        @Override
        public String toString() {
            return "Change[firstTime=" + firstTime + ", lastTime=" + lastTime + ", writes=" + writes + ", exact="
                    + exact + ", by=" + by + ", created=" + created + ", deleted=" + deleted + ", fields=" + fields + "]";
        }
    }

    /**
     * A field's value before and after a change, as your class's field type.
     * Null means the field had no value.
     */
    final class FieldChange {

        private final Object before;
        private final Object after;

        /**
         * Creates a field change.
         *
         * @param before the value before
         * @param after  the value after
         */
        public FieldChange(Object before, Object after) {
            this.before = before;
            this.after = after;
        }

        /** @return the value before */
        public Object before() {
            return before;
        }

        /** @return the value after */
        public Object after() {
            return after;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof FieldChange)) {
                return false;
            }
            FieldChange f = (FieldChange) o;
            return Objects.equals(before, f.before) && Objects.equals(after, f.after);
        }

        @Override
        public int hashCode() {
            return Objects.hash(before, after);
        }

        @Override
        public String toString() {
            return "FieldChange[before=" + before + ", after=" + after + "]";
        }
    }
}
