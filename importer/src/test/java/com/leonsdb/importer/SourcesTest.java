// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.importer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SourcesTest {

    @TempDir
    Path dir;

    /** A test driver jar (MySQL's, H2's) from the build, as the importer would download it. */
    static Path testDriver(String name) {
        for (String p : System.getProperty("leonsdb.test.drivers", "").split(java.io.File.pathSeparator)) {
            if (Path.of(p).getFileName().toString().startsWith(name)) {
                return Path.of(p);
            }
        }
        throw new IllegalStateException("no test driver " + name);
    }

    private static List<Map<String, Object>> rows(RowSource src, String... paths) throws ImportException {
        List<Map<String, Object>> out = new ArrayList<>();
        src.forEach(r -> {
            Map<String, Object> m = new java.util.LinkedHashMap<>();
            for (String p : paths) {
                m.put(p, r.get(p));
            }
            m.put("@", r.where());
            out.add(m);
        });
        return out;
    }

    @Test
    void sqliteIsReadOnlyAndNeverCreatesAFile() throws Exception {
        Path db = dir.resolve("stats.db");
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db)) {
            c.createStatement().execute("CREATE TABLE stats (uuid TEXT, kills INTEGER, name TEXT)");
            c.createStatement().execute("INSERT INTO stats VALUES ('0f3a2c1e-6b7d-4e8f-9a0b-1c2d3e4f5a6b', 12, 'Steve'), ('x', NULL, 'Alex')");
        }
        var source = new Plan.Source(Plan.SourceType.SQLITE, "jdbc:sqlite:" + db, null, null, null, null, null);
        try (JdbcSource src = new JdbcSource(source, new Plan.From("stats", null, null, null, null, null, null), new org.sqlite.JDBC())) {
            var rows = rows(src, "uuid", "KILLS", "name");
            assertEquals(2, rows.size());
            assertEquals(12, rows.get(0).get("KILLS"), "columns are matched without case");
            assertNull(rows.get(1).get("KILLS"));
            assertEquals("row 2", rows.get(1).get("@"));
            assertThrows(SQLException.class, () -> src.connection().createStatement().executeUpdate("DELETE FROM stats"));
            ImportException e = assertThrows(ImportException.class, () -> rows(src, "kils"));
            assertTrue(e.getMessage().contains("no column \"kils\""), e.getMessage());
        }
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + db)) {
            var rs = c.createStatement().executeQuery("SELECT COUNT(*) FROM stats");
            rs.next();
            assertEquals(2, rs.getInt(1), "the source is unchanged");
        }
        Path missing = dir.resolve("missing.db");
        var none = new Plan.Source(Plan.SourceType.SQLITE, "jdbc:sqlite:" + missing, null, null, null, null, null);
        assertThrows(ImportException.class, () -> new JdbcSource(none, new Plan.From("t", null, null, null, null, null, null), new org.sqlite.JDBC()));
        assertFalse(Files.exists(missing), "no empty database was created");
    }

    @Test
    void h2NeverCreatesAnEmptyDatabaseForAWrongPath() throws Exception {
        java.sql.Driver h2 = Drivers.fromJar(testDriver("h2-"), Drivers.H2.driverClass());
        Path base = dir.resolve("plugindata");
        try (Connection c = h2.connect("jdbc:h2:" + base, new java.util.Properties())) {
            c.createStatement().execute("CREATE TABLE balances (player UUID, amount DECIMAL(20,2), updated TIMESTAMP)");
            c.createStatement().execute("INSERT INTO balances VALUES ('0f3a2c1e-6b7d-4e8f-9a0b-1c2d3e4f5a6b', 99.50, TIMESTAMP '2026-10-29 12:00:00')");
        }
        var source = new Plan.Source(Plan.SourceType.H2, "jdbc:h2:" + base, null, null, null, null, null);
        try (JdbcSource src = new JdbcSource(source, new Plan.From(null, "SELECT player, amount, updated FROM balances", null, null, null, null, null), h2)) {
            var rows = rows(src, "player", "amount", "updated");
            assertEquals(1, rows.size());
            assertTrue(rows.get(0).get("player") instanceof java.util.UUID);
            assertEquals(0, new java.math.BigDecimal("99.50").compareTo((java.math.BigDecimal) rows.get(0).get("amount")));
        }
        Path wrong = dir.resolve("typo");
        var none = new Plan.Source(Plan.SourceType.H2, "jdbc:h2:" + wrong, null, null, null, null, null);
        ImportException e = assertThrows(ImportException.class,
                () -> new JdbcSource(none, new Plan.From("balances", null, null, null, null, null, null), h2));
        assertTrue(e.getMessage().contains("path"), e.getMessage());
        assertFalse(Files.exists(dir.resolve("typo.mv.db")), "no empty database was created");
    }

    @Test
    void onlySelectQueriesAreSent() {
        assertTrue(Sql.isSelect("SELECT * FROM t"));
        assertTrue(Sql.isSelect("  select a, b from t where c = ';' ;"));
        assertTrue(Sql.isSelect("-- the stats\nSELECT * FROM t"));
        assertTrue(Sql.isSelect("WITH x AS (SELECT 1) SELECT * FROM x"));
        assertFalse(Sql.isSelect("DELETE FROM t"));
        assertFalse(Sql.isSelect("SELECT 1; DROP TABLE t"));
        assertFalse(Sql.isSelect("/* SELECT */ UPDATE t SET a = 1"));
        assertFalse(Sql.isSelect("WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x"));
        assertFalse(Sql.isSelect("SELECT * FROM t INTO OUTFILE '/tmp/x'"));
        assertFalse(Sql.isSelect("SELECT * FROM t FOR UPDATE"));
    }

    @Test
    void yamlFilesOneRecordEach() throws Exception {
        Path ud = Files.createDirectories(dir.resolve("Essentials/userdata"));
        Files.writeString(ud.resolve("0f3a2c1e-6b7d-4e8f-9a0b-1c2d3e4f5a6b.yml"), """
                money: '1250.5'
                last-account-name: Steve
                homes:
                  home: { world: world, x: 10.5 }
                timestamps: { logout: 1761739200000 }
                """);
        Files.writeString(ud.resolve("notes.txt"), "not yaml");
        var source = new Plan.Source(Plan.SourceType.YAML, null, null, null, null, dir.resolve("Essentials"), null);
        try (YamlSource src = new YamlSource(source, new Plan.From(null, null, null, null, "userdata/*.yml", null, null))) {
            var rows = rows(src, "$file", "money", "homes.home.x", "timestamps.logout", "missing.path");
            assertEquals(1, rows.size());
            assertEquals("0f3a2c1e-6b7d-4e8f-9a0b-1c2d3e4f5a6b", rows.get(0).get("$file"));
            assertEquals("1250.5", rows.get(0).get("money"));
            assertEquals(10.5, rows.get(0).get("homes.home.x"));
            assertNull(rows.get(0).get("missing.path"));
            assertEquals("userdata/0f3a2c1e-6b7d-4e8f-9a0b-1c2d3e4f5a6b.yml", rows.get(0).get("@"));
        }
    }

    @Test
    void yamlOneFileWithASection() throws Exception {
        Files.writeString(dir.resolve("data.yml"), """
                version: 3
                players:
                  Steve: { kills: 4 }
                  Alex: { kills: 9 }
                """);
        var source = new Plan.Source(Plan.SourceType.YAML, null, null, null, null, dir, null);
        try (YamlSource src = new YamlSource(source, new Plan.From(null, null, null, null, null, "data.yml", "players"))) {
            var rows = rows(src, "$key", "kills");
            assertEquals(List.of("Steve", "Alex"), rows.stream().map(r -> r.get("$key")).toList());
            assertEquals(9, rows.get(1).get("kills"));
        }
        try (YamlSource src = new YamlSource(source, new Plan.From(null, null, null, null, null, "data.yml", "version"))) {
            assertThrows(ImportException.class, () -> rows(src, "$key"));
        }
    }
}
