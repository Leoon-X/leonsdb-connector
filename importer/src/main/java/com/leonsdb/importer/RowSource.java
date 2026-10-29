// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.importer;

import java.util.List;
import java.util.Map;

/**
 * Rows of a source, read one at a time so that large sources fit in memory.
 * A source only ever reads.
 */
interface RowSource extends AutoCloseable {

    /** One row. */
    interface Row {
        /**
         * A value: a column of an SQL row, or a dotted path into a document
         * (stats.kills). "$file" is a YAML file's name without its extension,
         * "$key" the key of an entry in a YAML section.
         *
         * @param path the column or path
         * @return the value, or null if it is absent or null
         */
        Object get(String path);

        /** @return where the row is, for reports: "row 17", "userdata/abc.yml" */
        String where();
    }

    /** Receives rows. */
    interface RowConsumer {
        /**
         * @param row the row
         * @throws ImportException to stop reading
         */
        void accept(Row row) throws ImportException;
    }

    /**
     * Reads every row.
     *
     * @param consumer receives each row
     * @throws ImportException if the source cannot be read
     */
    void forEach(RowConsumer consumer) throws ImportException;

    @Override
    void close();

    /**
     * Follows a dotted path through nested maps.
     *
     * @param root a document
     * @param path the path
     * @return the value, or null
     */
    static Object path(Object root, String path) {
        Object cur = root;
        if (cur instanceof Map<?, ?> m && m.containsKey(path)) {
            return m.get(path); // a key that itself contains dots
        }
        for (String part : path.split("\\.", -1)) {
            if (cur instanceof Map<?, ?> m) {
                cur = m.get(part);
            } else if (cur instanceof List<?> l && part.matches("\\d+")) {
                int i = Integer.parseInt(part);
                cur = i < l.size() ? l.get(i) : null;
            } else {
                return null;
            }
            if (cur == null) {
                return null;
            }
        }
        return cur;
    }
}
