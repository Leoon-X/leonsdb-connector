// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.importer;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.YAMLException;

/**
 * Records in YAML files: one file per record (for example a plugin's
 * userdata/&lt;uuid&gt;.yml), or the entries of a map in one file. Only plain
 * YAML is read: tags that would create Java objects are refused.
 */
final class YamlSource implements RowSource {

    private final Path dir;
    private final Plan.From from;

    YamlSource(Plan.Source source, Plan.From from) throws ImportException {
        this.dir = source.dir();
        this.from = from;
        if (!Files.isDirectory(dir)) {
            throw new ImportException("source.dir: " + dir + " is not a folder");
        }
    }

    private static Object load(Path file) throws ImportException {
        LoaderOptions options = new LoaderOptions();
        options.setCodePointLimit(64 * 1024 * 1024);
        try (Reader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            return new Yaml(new SafeConstructor(options)).load(r);
        } catch (IOException e) {
            throw new ImportException("Cannot read " + file + ": " + e.getMessage(), e);
        } catch (YAMLException e) {
            throw new ImportException(file + " is not valid YAML: " + e.getMessage(), e);
        }
    }

    @Override
    public void forEach(RowConsumer consumer) throws ImportException {
        if (from.files() != null) {
            PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + from.files());
            List<Path> files;
            try (Stream<Path> walk = Files.walk(dir)) {
                files = walk.filter(Files::isRegularFile).filter(p -> matcher.matches(dir.relativize(p))).sorted().toList();
            } catch (IOException e) {
                throw new ImportException("Cannot list " + dir + ": " + e.getMessage(), e);
            }
            if (files.isEmpty()) {
                throw new ImportException("No files in " + dir + " match " + from.files());
            }
            for (Path f : files) {
                Object root = load(f);
                String rel = dir.relativize(f).toString().replace('\\', '/');
                String name = f.getFileName().toString();
                String base = name.contains(".") ? name.substring(0, name.lastIndexOf('.')) : name;
                consumer.accept(new Row() {
                    @Override
                    public Object get(String path) {
                        return "$file".equals(path) ? base : RowSource.path(root, path);
                    }

                    @Override
                    public String where() {
                        return rel;
                    }
                });
            }
            return;
        }
        Path file = dir.resolve(from.file()).normalize();
        Object root = load(file);
        Object section = from.section() == null ? root : RowSource.path(root, from.section());
        if (!(section instanceof Map<?, ?> entries)) {
            throw new ImportException(from.describe() + ": " + (from.section() == null ? "the file" : from.section())
                    + " is not a map of records");
        }
        for (var e : entries.entrySet()) {
            String key = String.valueOf(e.getKey());
            Object value = e.getValue();
            consumer.accept(new Row() {
                @Override
                public Object get(String path) {
                    return "$key".equals(path) ? key : RowSource.path(value, path);
                }

                @Override
                public String where() {
                    return from.file() + ": " + key;
                }
            });
        }
    }

    @Override
    public void close() {
        // files are closed after reading
    }
}
