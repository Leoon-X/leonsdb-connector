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
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PlanTest {

    @TempDir
    Path dir;

    private static final Map<String, String> ENV = Map.of("TOKEN", "ldb_secret", "MYSQL_PASSWORD", "hunter2");

    private Plan load(String yaml) throws Exception {
        Path f = dir.resolve("plan.yml");
        Files.writeString(f, yaml);
        return Plan.load(f, ENV);
    }

    private String error(String yaml) {
        return assertThrows(ImportException.class, () -> load(yaml)).getMessage();
    }

    private static final String LEONSDB = "leonsdb: { server: 127.0.0.1:7420, token-env: TOKEN }\n";

    @Test
    void aMySqlPlan() throws Exception {
        Plan p = load("""
                source:
                  type: mysql
                  url: jdbc:mysql://127.0.0.1:3306/survival
                  user: importer
                  password-env: MYSQL_PASSWORD
                """ + LEONSDB + """
                imports:
                  - from: { table: player_stats }
                    into: example.stats
                    key: uuid
                    fields:
                      kills: kills
                      coins: { from: balance }
                      lastSeen: { from: last_seen, as: epoch-seconds }
                """);
        assertEquals(Plan.SourceType.MYSQL, p.source().type());
        assertEquals("hunter2", p.source().password());
        assertEquals("ldb_secret", p.leonsdb().token());
        assertEquals(7420, p.leonsdb().port());
        Plan.Import i = p.imports().get(0);
        assertEquals("example", i.namespace());
        assertEquals("stats", i.collection());
        assertEquals("player_stats", i.from().table());
        assertEquals("uuid", i.key().from());
        assertNull(i.key().format());
        assertEquals(java.util.List.of("kills", "coins", "lastSeen"), java.util.List.copyOf(i.fields().keySet()));
        assertEquals("epoch-seconds", i.fields().get("lastSeen").as());
        assertFalse(p.source().describe().contains("hunter2"));
    }

    @Test
    void passwordsInUrlsAreNeverShown() throws Exception {
        Plan p = load("source: { type: mysql, url: \"jdbc:mysql://h/db?user=a&password=hunter2\" }\n" + LEONSDB
                + "imports: [ { from: { table: t }, into: a.b, key: k, fields: { x: x } } ]\n");
        assertFalse(p.source().describe().contains("hunter2"), p.source().describe());
        Plan m = load("source: { type: mongodb, url: \"mongodb://admin:hunter2@h:27017/db\" }\n" + LEONSDB
                + "imports: [ { from: { collection: players }, into: a.b, key: _id, fields: { x: x } } ]\n");
        assertFalse(m.source().describe().contains("hunter2"), m.source().describe());
    }

    @Test
    void mongoAndYamlPlans() throws Exception {
        Plan m = load("source: { type: mongodb, url: mongodb://127.0.0.1:27017, database: mc }\n" + LEONSDB + """
                imports:
                  - from: { collection: players, filter: { banned: false } }
                    into: example.stats
                    key: { from: uuid, as: uuid }
                    fields: { kills: stats.kills }
                """);
        assertEquals("mc", m.source().database());
        assertEquals(Map.of("banned", false), m.imports().get(0).from().filter());
        assertEquals(Plan.KeyFormat.UUID, m.imports().get(0).key().format());

        Files.createDirectories(dir.resolve("Essentials"));
        Plan y = load("source: { type: yaml, dir: Essentials }\n" + LEONSDB + """
                imports:
                  - from: { files: "userdata/*.yml" }
                    into: example.stats
                    key: $file
                    fields: { coins: money }
                """);
        assertEquals(dir.resolve("Essentials").normalize(), y.source().dir());
        assertEquals("userdata/*.yml", y.imports().get(0).from().files());
    }

    @Test
    void mistakesAreNamedPrecisely() {
        String src = "source: { type: sqlite, url: \"jdbc:sqlite:data.db\" }\n";
        String imp = "imports: [ { from: { table: t }, into: a.b, key: k, fields: { x: x } } ]\n";
        assertTrue(error("source: { type: postgres, url: x }\n" + LEONSDB + imp).contains("use mysql, mongodb, sqlite, h2 or yaml"));
        assertTrue(error("source: { type: sqlite, url: \"jdbc:mysql://h/db\" }\n" + LEONSDB + imp).contains("not a sqlite URL"));
        assertTrue(error(src + LEONSDB + imp.replace("into: a.b", "into: stats")).contains("must be namespace.collection"));
        assertTrue(error(src + LEONSDB + imp.replace("key: k", "kee: k")).contains("unknown setting \"kee\""));
        assertTrue(error(src + "leonsdb: { server: h:1, token-env: NOPE }\n" + imp).contains("NOPE is not set"));
        assertTrue(error(src + LEONSDB + "imports: []\n").contains("at least one"));
        assertTrue(error(src + LEONSDB + imp.replace("{ table: t }", "{ table: t, query: \"SELECT 1\" }"))
                .contains("either table or query"));
        assertTrue(error(src + LEONSDB + imp.replace("{ table: t }", "{ query: \"DELETE FROM t\" }"))
                .contains("only SELECT queries"));
        assertTrue(error(src + LEONSDB + imp.replace("{ table: t }", "{ table: \"t; DROP TABLE t\" }"))
                .contains("not a plain table name"));
        assertTrue(error(src + LEONSDB + imp.replace("fields: { x: x }", "fields: { x: { from: x, as: weeks } }"))
                .contains("use one of"));
        assertTrue(error("source: { type: sqlite, url: \"jdbc:sqlite:a\", password-env: NOPE }\n" + LEONSDB + imp)
                .contains("NOPE is not set"));
        assertTrue(error(src + LEONSDB + "imports: [ { from: { table: t }, into: a.b, key: k, fields: {} } ]\n")
                .contains("at least one field"));
        assertTrue(error("{{{").contains("not valid YAML"));
    }

    @Test
    void yamlTagsThatMakeJavaObjectsAreRefused() {
        assertTrue(error("source: !!java.io.File [\"/etc/passwd\"]\n").contains("not valid YAML"));
    }
}
