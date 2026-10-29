// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.importer;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Driver;
import java.time.Duration;
import java.util.HexFormat;

/**
 * JDBC drivers. SQLite's is part of the importer. MySQL's and H2's are not
 * shipped with it (their licenses are copyleft): when a plan needs one, it
 * is downloaded once from Maven Central into a cache, and used only if its
 * SHA-256 is exactly the one pinned here. A plan can name a driver jar
 * instead (source.driver), for machines without internet access.
 */
final class Drivers {

    /**
     * A driver the importer downloads.
     *
     * @param name        for messages
     * @param license     for messages
     * @param path        its path on Maven Central
     * @param sha256      the SHA-256 it must have
     * @param driverClass the java.sql.Driver class in it
     */
    record Download(String name, String license, String path, String sha256, String driverClass) {
    }

    static final Download MYSQL = new Download("MySQL Connector/J 26.7.0",
            "GPL-2.0 with the Universal FOSS Exception",
            "com/mysql/mysql-connector-j/26.7.0/mysql-connector-j-26.7.0.jar",
            "69084713593a4aa8d07c383619b9639276f08bccf8faf1c562178147d389b1e1", "com.mysql.cj.jdbc.Driver");

    static final Download H2 = new Download("H2 2.5.250", "MPL-2.0 or EPL-1.0",
            "com/h2database/h2/2.5.250/h2-2.5.250.jar",
            "82a80a2ac06901b03cdb233c663d21c4f49c884bf6d0cf85022729e8db9ab86f", "org.h2.Driver");

    private final Path cache;
    private final String repository;
    private final PrintStream out;

    /**
     * @param cache      where downloaded drivers are kept
     * @param repository the Maven repository to download from
     * @param out        where to say what is downloaded
     */
    Drivers(Path cache, String repository, PrintStream out) {
        this.cache = cache;
        this.repository = repository.endsWith("/") ? repository : repository + "/";
        this.out = out;
    }

    /** @return drivers cached in the user's home, downloaded from Maven Central */
    static Drivers standard(PrintStream out) {
        return new Drivers(Path.of(System.getProperty("user.home"), ".leonsdb", "import-drivers"),
                System.getProperty("leonsdb.import.repository", "https://repo1.maven.org/maven2/"), out);
    }

    /**
     * The JDBC driver for a source.
     *
     * @param source the source
     * @return the driver
     * @throws ImportException if it cannot be had
     */
    Driver driverFor(Plan.Source source) throws ImportException {
        return switch (source.type()) {
            case SQLITE -> new org.sqlite.JDBC();
            case MYSQL -> source.driver() != null ? fromJar(source.driver(), MYSQL.driverClass()) : downloaded(MYSQL);
            case H2 -> source.driver() != null ? fromJar(source.driver(), H2.driverClass()) : downloaded(H2);
            default -> throw new IllegalArgumentException(source.type() + " has no JDBC driver");
        };
    }

    /**
     * A driver from Maven Central, downloaded the first time and checked
     * every time.
     *
     * @param d the driver
     * @return the driver
     * @throws ImportException if it cannot be downloaded or does not match
     */
    Driver downloaded(Download d) throws ImportException {
        Path jar = cache.resolve(d.path().substring(d.path().lastIndexOf('/') + 1));
        if (Files.exists(jar) && !d.sha256().equals(sha256(jar))) {
            try {
                Files.delete(jar); // damaged or replaced: never used
            } catch (IOException e) {
                throw new ImportException("The cached driver " + jar + " is damaged and cannot be removed: " + e.getMessage(), e);
            }
        }
        if (!Files.exists(jar)) {
            out.println("Downloading " + d.name() + " (" + d.license() + ") from " + repository + " ...");
            Path part;
            try {
                Files.createDirectories(cache);
                part = Files.createTempFile(cache, "download", ".part");
            } catch (IOException e) {
                throw new ImportException("Cannot write to " + cache + ": " + e.getMessage(), e);
            }
            try {
                HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20))
                        .followRedirects(HttpClient.Redirect.NORMAL).build();
                HttpResponse<InputStream> resp = http.send(HttpRequest.newBuilder(URI.create(repository + d.path()))
                        .timeout(Duration.ofMinutes(5)).header("User-Agent", "leonsdb-import").build(),
                        HttpResponse.BodyHandlers.ofInputStream());
                if (resp.statusCode() != 200) {
                    resp.body().close();
                    throw new IOException("HTTP " + resp.statusCode());
                }
                try (InputStream in = resp.body()) {
                    Files.copy(in, part, StandardCopyOption.REPLACE_EXISTING);
                }
                String got = sha256(part);
                if (!d.sha256().equals(got)) {
                    throw new ImportException("The downloaded " + d.name() + " is not the expected file (SHA-256 " + got
                            + " instead of " + d.sha256() + "), so it was not used. Download it yourself and set "
                            + "source.driver to the jar.");
                }
                Files.move(part, jar, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                throw new ImportException("Could not download " + d.name() + " from " + repository + ": " + e.getMessage()
                        + ". Without internet access, download " + d.path().substring(d.path().lastIndexOf('/') + 1)
                        + " yourself and set source.driver to the jar.", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ImportException("Interrupted while downloading " + d.name(), e);
            } finally {
                try {
                    Files.deleteIfExists(part);
                } catch (IOException ignored) {
                    // a leftover temporary file is harmless
                }
            }
        }
        return fromJar(jar, d.driverClass());
    }

    /**
     * Loads a driver from a jar.
     *
     * @param jar         the jar
     * @param driverClass the driver class in it
     * @return the driver
     * @throws ImportException if the jar has no such driver
     */
    static Driver fromJar(Path jar, String driverClass) throws ImportException {
        if (!Files.isRegularFile(jar)) {
            throw new ImportException("The driver jar " + jar + " does not exist");
        }
        try {
            // Kept open for the life of the importer: the driver loads classes lazily.
            URLClassLoader loader = new URLClassLoader(new URL[] {jar.toUri().toURL()}, Drivers.class.getClassLoader());
            return (Driver) Class.forName(driverClass, true, loader).getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException | IOException | ClassCastException | LinkageError e) {
            throw new ImportException("The jar " + jar + " has no JDBC driver " + driverClass + ": " + e, e);
        }
    }

    static String sha256(Path file) throws ImportException {
        try (InputStream in = Files.newInputStream(file)) {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] buf = new byte[64 * 1024];
            int n;
            while ((n = in.read(buf)) > 0) {
                md.update(buf, 0, n);
            }
            return HexFormat.of().formatHex(md.digest());
        } catch (IOException e) {
            throw new ImportException("Cannot read " + file + ": " + e.getMessage(), e);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
