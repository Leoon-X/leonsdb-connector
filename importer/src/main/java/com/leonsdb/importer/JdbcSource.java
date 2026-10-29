// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.importer;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * Rows of a table or SELECT query in MySQL, SQLite or H2. The source is never
 * changed: only a single SELECT is sent, the connection is read-only where
 * the database supports it, and it is never committed (only rolled back).
 */
final class JdbcSource implements RowSource {

    private final Plan.Source source;
    private final Plan.From from;
    private final Connection conn;

    JdbcSource(Plan.Source source, Plan.From from, Driver driver) throws ImportException {
        this.source = source;
        this.from = from;
        Properties props = new Properties();
        if (source.user() != null) {
            props.setProperty("user", source.user());
        }
        if (source.password() != null) {
            props.setProperty("password", source.password());
        }
        String url = source.url();
        switch (source.type()) {
            case SQLITE -> props.setProperty("open_mode", "1"); // SQLITE_OPEN_READONLY: never creates a file
            case H2 -> {
                // H2 creates an empty database for a path that does not exist.
                if (!url.toUpperCase(Locale.ROOT).contains("IFEXISTS=")) {
                    url += ";IFEXISTS=TRUE";
                }
            }
            default -> { }
        }
        try {
            Connection c = driver.connect(url, props);
            if (c == null) {
                throw new ImportException("The " + source.type() + " driver does not accept the URL " + source.describe());
            }
            conn = c;
            conn.setReadOnly(true);
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new ImportException("Cannot open " + source.describe() + ": " + e.getMessage() + hint(e), e);
        }
    }

    private String hint(SQLException e) {
        String m = String.valueOf(e.getMessage()).toLowerCase(Locale.ROOT);
        if (source.type() == Plan.SourceType.H2 && (m.contains("not found") || m.contains("does not exist"))) {
            return ". Check the path in source.url (without the .mv.db ending).";
        }
        if (source.type() == Plan.SourceType.H2 && m.contains("locked")) {
            return ". The database is in use: stop the server that has it open, then run the import again.";
        }
        if (source.type() == Plan.SourceType.SQLITE && m.contains("unable to open")) {
            return ". Check the path in source.url.";
        }
        return "";
    }

    @Override
    public void forEach(RowConsumer consumer) throws ImportException {
        String sql = from.query() != null ? from.query() : "SELECT * FROM " + from.table();
        if (!Sql.isSelect(sql)) {
            throw new ImportException("Only SELECT queries are allowed: the importer never changes the source");
        }
        try (PreparedStatement st = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
            // Rows are streamed, not loaded at once (MySQL streams with MIN_VALUE).
            st.setFetchSize(source.type() == Plan.SourceType.MYSQL ? Integer.MIN_VALUE : 1000);
            try (ResultSet rs = st.executeQuery()) {
                ResultSetMetaData md = rs.getMetaData();
                Map<String, Integer> columns = new HashMap<>();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    columns.putIfAbsent(md.getColumnLabel(i).toLowerCase(Locale.ROOT), i);
                }
                long n = 0;
                while (rs.next()) {
                    long row = ++n;
                    Object[] values = new Object[md.getColumnCount() + 1];
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        values[i] = rs.getObject(i);
                    }
                    consumer.accept(new Row() {
                        @Override
                        public Object get(String path) {
                            Integer i = columns.get(path.toLowerCase(Locale.ROOT));
                            if (i == null) {
                                throw new MissingColumn(path, columns.keySet());
                            }
                            return values[i];
                        }

                        @Override
                        public String where() {
                            return "row " + row;
                        }
                    });
                }
            }
        } catch (SQLException e) {
            throw new ImportException("Cannot read " + from.describe() + " from " + source.describe() + ": " + e.getMessage(), e);
        } catch (MissingColumn e) {
            throw new ImportException(from.describe() + " has no column \"" + e.column + "\"; its columns are: "
                    + String.join(", ", new java.util.TreeSet<>(e.columns)));
        }
    }

    /** A plan names a column the table does not have. */
    private static final class MissingColumn extends RuntimeException {
        private static final long serialVersionUID = 1L;
        final String column;
        final transient java.util.Set<String> columns;

        MissingColumn(String column, java.util.Set<String> columns) {
            super(column, null, false, false);
            this.column = column;
            this.columns = columns;
        }
    }

    /** For tests: the connection, to check it refuses writes. */
    Connection connection() {
        return conn;
    }

    @Override
    public void close() {
        try {
            conn.rollback(); // nothing is ever committed
        } catch (SQLException ignored) {
            // closing anyway
        }
        try {
            conn.close();
        } catch (SQLException ignored) {
            // nothing to do
        }
    }
}
