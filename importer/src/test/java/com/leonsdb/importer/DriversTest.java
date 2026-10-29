// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

package com.leonsdb.importer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Drivers are downloaded once and used only if they are exactly the pinned file. */
class DriversTest {

    @TempDir
    Path cache;

    private HttpServer http;
    private final AtomicReference<byte[]> served = new AtomicReference<>();
    private final AtomicInteger downloads = new AtomicInteger();
    private Drivers drivers;
    private final ByteArrayOutputStream log = new ByteArrayOutputStream();
    private Drivers.Download h2;

    @BeforeEach
    void setUp() throws Exception {
        http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        http.createContext("/", ex -> {
            downloads.incrementAndGet();
            byte[] body = served.get();
            ex.sendResponseHeaders(body == null ? 404 : 200, body == null ? -1 : body.length);
            if (body != null) {
                ex.getResponseBody().write(body);
            }
            ex.close();
        });
        http.start();
        drivers = new Drivers(cache, "http://127.0.0.1:" + http.getAddress().getPort() + "/maven2", new PrintStream(log, true));
        Path real = SourcesTest.testDriver("h2-");
        h2 = new Drivers.Download("H2 (test)", "MPL-2.0 or EPL-1.0", "com/h2database/h2/x/" + real.getFileName(),
                Drivers.sha256(real), Drivers.H2.driverClass());
        served.set(Files.readAllBytes(real));
    }

    @AfterEach
    void tearDown() {
        http.stop(0);
    }

    @Test
    void theRealPinnedChecksumsMatchTheTestDrivers() throws Exception {
        // The build's test drivers are the very versions Drivers pins.
        assertEquals(Drivers.H2.sha256(), Drivers.sha256(SourcesTest.testDriver("h2-")));
        assertEquals(Drivers.MYSQL.sha256(), Drivers.sha256(SourcesTest.testDriver("mysql-connector-j-")));
    }

    @Test
    void downloadedOnceThenUsedFromTheCache() throws Exception {
        assertEquals("org.h2.Driver", drivers.downloaded(h2).getClass().getName());
        assertTrue(log.toString().contains("Downloading H2"));
        drivers.downloaded(h2);
        assertEquals(1, downloads.get(), "the second time it comes from the cache");
    }

    @Test
    void aFileThatIsNotThePinnedOneIsNeverUsed() throws Exception {
        served.set("not the driver".getBytes());
        ImportException e = assertThrows(ImportException.class, () -> drivers.downloaded(h2));
        assertTrue(e.getMessage().contains("not the expected file"), e.getMessage());
        try (var files = Files.list(cache)) {
            assertEquals(0, files.count(), "nothing is kept");
        }
    }

    @Test
    void aDamagedCacheIsReplaced() throws Exception {
        drivers.downloaded(h2);
        Path jar = cache.resolve(h2.path().substring(h2.path().lastIndexOf('/') + 1));
        Files.write(jar, "tampered".getBytes());
        assertEquals("org.h2.Driver", drivers.downloaded(h2).getClass().getName());
        assertEquals(2, downloads.get());
        assertEquals(h2.sha256(), Drivers.sha256(jar));
    }

    @Test
    void withoutInternetTheMessageSaysWhatToDo() {
        served.set(null);
        ImportException e = assertThrows(ImportException.class, () -> drivers.downloaded(h2));
        assertTrue(e.getMessage().contains("source.driver"), e.getMessage());
        assertFalse(Files.exists(cache.resolve(h2.path().substring(h2.path().lastIndexOf('/') + 1))));
    }
}
