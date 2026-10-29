// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.importer;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;

/**
 * What to import: a plan file, read from YAML. See docs/importing.md.
 *
 * @param source  where the data comes from
 * @param leonsdb the LeonsDB server it goes to
 * @param imports one entry per target collection
 * @param file    the plan file
 */
public record Plan(Source source, Target leonsdb, List<Import> imports, Path file) {

    /** The kinds of sources. */
    public enum SourceType { MYSQL, MONGODB, SQLITE, H2, YAML }

    /**
     * Where the data comes from.
     *
     * @param type     the kind of source
     * @param url      JDBC URL or MongoDB connection string; null for YAML
     * @param user     user name, or null
     * @param password password (from the environment), or null
     * @param database MongoDB database, or null for the one in the URL
     * @param dir      YAML: the folder file names are relative to
     * @param driver   a JDBC driver jar to use instead of the one LeonsDB downloads, or null
     */
    public record Source(SourceType type, String url, String user, String password, String database,
                         Path dir, Path driver) {

        /** @return the URL with any password hidden, for messages and reports */
        public String describe() {
            if (type == SourceType.YAML) {
                return "YAML files in " + dir;
            }
            String u = url.replaceAll("(?i)(password|pwd)=[^&;]*", "$1=***")
                    .replaceAll("(mongodb(\\+srv)?://[^:/@]+):[^@]*@", "$1:***@");
            return type.name().toLowerCase(java.util.Locale.ROOT) + " " + u + (user != null ? " (user " + user + ")" : "");
        }
    }

    /**
     * The LeonsDB server.
     *
     * @param host  host name
     * @param port  gRPC port
     * @param tls   whether to use TLS
     * @param token access token (from the environment)
     */
    public record Target(String host, int port, boolean tls, String token) {
    }

    /**
     * One import: rows of one table (or collection, or set of files) into
     * one LeonsDB collection.
     *
     * @param from       what to read
     * @param namespace  target namespace
     * @param collection target collection
     * @param key        how each row's key is made
     * @param fields     target field name to where its value comes from, in plan order
     */
    public record Import(From from, String namespace, String collection, Key key, Map<String, Field> fields) {

        /** @return "namespace.collection" */
        public String into() {
            return namespace + "." + collection;
        }
    }

    /**
     * What to read. Exactly the members the source type uses are set.
     *
     * @param table      SQL: a table to read all rows of
     * @param query      SQL: a SELECT query
     * @param collection MongoDB: the collection
     * @param filter     MongoDB: a filter document, or null
     * @param files      YAML: a glob of files, one record each (for example userdata/*.yml)
     * @param file       YAML: one file with many records
     * @param section    YAML with file: the map in it whose entries are the records, or null for the top level
     */
    public record From(String table, String query, String collection, Map<String, Object> filter,
                       String files, String file, String section) {

        /** @return a short description, for reports */
        public String describe() {
            if (table != null) {
                return "table " + table;
            }
            if (query != null) {
                return "query";
            }
            if (collection != null) {
                return "collection " + collection;
            }
            if (files != null) {
                return "files " + files;
            }
            return "file " + file + (section != null ? " (" + section + ")" : "");
        }
    }

    /** How a key is read. */
    public enum KeyFormat {
        /** A player UUID: dashed or 32 hex digits, 16 bytes, or a UUID value. */
        UUID,
        /** A player name on an offline-mode server: the UUID Minecraft gives it. */
        OFFLINE_UUID,
        /** A string id, for global collections. */
        ID
    }

    /**
     * Where each record's key comes from.
     *
     * @param from   column, field path, or $file / $key
     * @param format how to read it; null to choose by the collection's kind
     */
    public record Key(String from, KeyFormat format) {
    }

    /**
     * Where a field's value comes from.
     *
     * @param from column or field path
     * @param as   how to read an ambiguous value (epoch-seconds, epoch-millis,
     *             base64, hex, java-legacy-uuid), or null
     */
    public record Field(String from, String as) {
    }

    static final Set<String> AS_VALUES = Set.of("epoch-seconds", "epoch-millis", "base64", "hex", "java-legacy-uuid");

    /**
     * Reads a plan file.
     *
     * @param file the plan
     * @param env  environment variables (passwords and tokens come from there)
     * @return the plan
     * @throws ImportException if the plan is invalid; the message says where
     */
    public static Plan load(Path file, Map<String, String> env) throws ImportException {
        Object root;
        try (Reader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            LoaderOptions options = new LoaderOptions();
            options.setAllowDuplicateKeys(false);
            root = new Yaml(new SafeConstructor(options)).load(r);
        } catch (IOException e) {
            throw new ImportException("Cannot read the plan " + file + ": " + e.getMessage(), e);
        } catch (YAMLException e) {
            throw new ImportException("The plan " + file + " is not valid YAML: " + e.getMessage(), e);
        }
        Map<String, Object> top = map(root, "the plan");
        only(top, "the plan", "source", "leonsdb", "imports");
        Path dir = file.toAbsolutePath().getParent();
        Source source = source(map(required(top, "source", "the plan"), "source"), env, dir);
        Target target = target(map(required(top, "leonsdb", "the plan"), "leonsdb"), env);
        Object list = required(top, "imports", "the plan");
        if (!(list instanceof List<?> items) || items.isEmpty()) {
            throw new ImportException("imports: give a list of imports, at least one");
        }
        List<Import> imports = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            imports.add(importOf(map(items.get(i), "imports[" + (i + 1) + "]"), "imports[" + (i + 1) + "]", source.type()));
        }
        return new Plan(source, target, List.copyOf(imports), file);
    }

    private static Source source(Map<String, Object> m, Map<String, String> env, Path planDir) throws ImportException {
        only(m, "source", "type", "url", "user", "password-env", "database", "dir", "driver");
        String typeName = string(required(m, "type", "source"), "source.type");
        SourceType type;
        try {
            type = SourceType.valueOf(typeName.toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new ImportException("source.type: \"" + typeName + "\" is not a source LeonsDB can import from; "
                    + "use mysql, mongodb, sqlite, h2 or yaml");
        }
        String url = null;
        Path dir = null;
        if (type == SourceType.YAML) {
            only(m, "source", "type", "dir");
            dir = planDir.resolve(string(required(m, "dir", "source"), "source.dir")).normalize();
        } else {
            url = string(required(m, "url", "source"), "source.url");
        }
        if (type == SourceType.SQLITE && !url.startsWith("jdbc:sqlite:")
                || type == SourceType.H2 && !url.startsWith("jdbc:h2:")
                || type == SourceType.MYSQL && !url.startsWith("jdbc:mysql:") && !url.startsWith("jdbc:mariadb:")
                || type == SourceType.MONGODB && !url.startsWith("mongodb://") && !url.startsWith("mongodb+srv://")) {
            throw new ImportException("source.url: \"" + url + "\" is not a " + typeName + " URL"
                    + switch (type) {
                        case SQLITE -> " (jdbc:sqlite:path/to/file.db)";
                        case H2 -> " (jdbc:h2:path/to/file)";
                        case MYSQL -> " (jdbc:mysql://host:3306/database)";
                        default -> " (mongodb://host:27017)";
                    });
        }
        String password = null;
        if (m.containsKey("password-env")) {
            String var = string(m.get("password-env"), "source.password-env");
            password = env.get(var);
            if (password == null) {
                throw new ImportException("source.password-env: the environment variable " + var + " is not set");
            }
        }
        Path driver = null;
        if (m.containsKey("driver")) {
            if (type != SourceType.MYSQL && type != SourceType.H2) {
                throw new ImportException("source.driver: only MySQL and H2 sources take a driver jar");
            }
            driver = planDir.resolve(string(m.get("driver"), "source.driver")).normalize();
        }
        String database = m.containsKey("database") ? string(m.get("database"), "source.database") : null;
        if (database != null && type != SourceType.MONGODB) {
            throw new ImportException("source.database: only MongoDB sources take a database; for SQL it is part of the URL");
        }
        String user = m.containsKey("user") ? string(m.get("user"), "source.user") : null;
        return new Source(type, url, user, password, database, dir, driver);
    }

    private static Target target(Map<String, Object> m, Map<String, String> env) throws ImportException {
        only(m, "leonsdb", "server", "tls", "token-env");
        String server = string(required(m, "server", "leonsdb"), "leonsdb.server");
        int colon = server.lastIndexOf(':');
        int port;
        try {
            port = colon < 0 ? 7420 : Integer.parseInt(server.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new ImportException("leonsdb.server: \"" + server + "\" is not host:port");
        }
        String host = colon < 0 ? server : server.substring(0, colon);
        boolean tls = m.containsKey("tls") && Boolean.TRUE.equals(m.get("tls"));
        String var = string(required(m, "token-env", "leonsdb"), "leonsdb.token-env");
        String token = env.get(var);
        if (token == null || token.isBlank()) {
            throw new ImportException("leonsdb.token-env: the environment variable " + var + " is not set. "
                    + "Give it a LeonsDB token that may write the collections, for example the admin token.");
        }
        return new Target(host, port, tls, token);
    }

    private static Import importOf(Map<String, Object> m, String where, SourceType type) throws ImportException {
        only(m, where, "from", "into", "key", "fields");
        From from = from(map(required(m, "from", where), where + ".from"), where + ".from", type);
        String into = string(required(m, "into", where), where + ".into");
        int dot = into.indexOf('.');
        if (dot <= 0 || dot == into.length() - 1 || into.indexOf('.', dot + 1) >= 0) {
            throw new ImportException(where + ".into: \"" + into + "\" must be namespace.collection, for example example.stats");
        }
        Object k = required(m, "key", where);
        Key key;
        if (k instanceof String s) {
            key = new Key(s, null);
        } else {
            Map<String, Object> km = map(k, where + ".key");
            only(km, where + ".key", "from", "as");
            KeyFormat format = null;
            if (km.containsKey("as")) {
                String as = string(km.get("as"), where + ".key.as");
                format = switch (as) {
                    case "uuid" -> KeyFormat.UUID;
                    case "offline-uuid" -> KeyFormat.OFFLINE_UUID;
                    case "id" -> KeyFormat.ID;
                    default -> throw new ImportException(where + ".key.as: \"" + as + "\": use uuid, offline-uuid or id");
                };
            }
            key = new Key(string(required(km, "from", where + ".key"), where + ".key.from"), format);
        }
        Map<String, Field> fields = new LinkedHashMap<>();
        Map<String, Object> fm = map(required(m, "fields", where), where + ".fields");
        if (fm.isEmpty()) {
            throw new ImportException(where + ".fields: map at least one field");
        }
        for (var e : fm.entrySet()) {
            String w = where + ".fields." + e.getKey();
            if (e.getValue() instanceof String s) {
                fields.put(e.getKey(), new Field(s, null));
            } else {
                Map<String, Object> f = map(e.getValue(), w);
                only(f, w, "from", "as");
                String as = f.containsKey("as") ? string(f.get("as"), w + ".as") : null;
                if (as != null && !AS_VALUES.contains(as)) {
                    throw new ImportException(w + ".as: \"" + as + "\": use one of " + String.join(", ", new java.util.TreeSet<>(AS_VALUES)));
                }
                fields.put(e.getKey(), new Field(string(required(f, "from", w), w + ".from"), as));
            }
        }
        return new Import(from, into.substring(0, dot), into.substring(dot + 1), key,
                java.util.Collections.unmodifiableMap(fields));
    }

    private static From from(Map<String, Object> m, String where, SourceType type) throws ImportException {
        switch (type) {
            case MYSQL, SQLITE, H2 -> {
                only(m, where, "table", "query");
                if (m.containsKey("table") == m.containsKey("query")) {
                    throw new ImportException(where + ": give either table or query");
                }
                String table = m.containsKey("table") ? string(m.get("table"), where + ".table") : null;
                if (table != null && !table.matches("[A-Za-z_][A-Za-z0-9_$]*(\\.[A-Za-z_][A-Za-z0-9_$]*)?")) {
                    throw new ImportException(where + ".table: \"" + table + "\" is not a plain table name; use query for anything else");
                }
                String query = m.containsKey("query") ? string(m.get("query"), where + ".query") : null;
                if (query != null && !Sql.isSelect(query)) {
                    throw new ImportException(where + ".query: only SELECT queries are allowed: the importer never changes the source");
                }
                return new From(table, query, null, null, null, null, null);
            }
            case MONGODB -> {
                only(m, where, "collection", "filter");
                Map<String, Object> filter = m.containsKey("filter") ? map(m.get("filter"), where + ".filter") : null;
                return new From(null, null, string(required(m, "collection", where), where + ".collection"), filter,
                        null, null, null);
            }
            default -> {
                only(m, where, "files", "file", "section");
                if (m.containsKey("files") == m.containsKey("file")) {
                    throw new ImportException(where + ": give either files (one record per file) or file (many records in one file)");
                }
                if (m.containsKey("section") && !m.containsKey("file")) {
                    throw new ImportException(where + ".section: only with file");
                }
                return new From(null, null, null, null,
                        m.containsKey("files") ? string(m.get("files"), where + ".files") : null,
                        m.containsKey("file") ? string(m.get("file"), where + ".file") : null,
                        m.containsKey("section") ? string(m.get("section"), where + ".section") : null);
            }
        }
    }

    // --- helpers -------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object o, String where) throws ImportException {
        if (!(o instanceof Map<?, ?> m)) {
            throw new ImportException(where + ": expected a map of settings");
        }
        for (Object k : m.keySet()) {
            if (!(k instanceof String)) {
                throw new ImportException(where + ": setting names must be text, not " + k);
            }
        }
        return (Map<String, Object>) m;
    }

    private static Object required(Map<String, Object> m, String key, String where) throws ImportException {
        Object v = m.get(key);
        if (v == null) {
            throw new ImportException(where + ": " + key + " is missing");
        }
        return v;
    }

    private static String string(Object o, String where) throws ImportException {
        if (o instanceof String s && !s.isBlank()) {
            return s;
        }
        throw new ImportException(where + ": expected text");
    }

    private static void only(Map<String, Object> m, String where, String... allowed) throws ImportException {
        Set<String> ok = Set.of(allowed);
        for (String k : m.keySet()) {
            if (!ok.contains(k)) {
                throw new ImportException(where + ": unknown setting \"" + k + "\"; allowed here: " + String.join(", ", allowed));
            }
        }
    }
}
