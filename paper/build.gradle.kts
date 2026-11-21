// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

// The plugin for Bukkit, Spigot, Paper and Folia, from Minecraft 1.8 to the
// newest. The output jar bundles the connector and its dependencies,
// relocated so they can never clash with other plugins.
//
// One jar runs everywhere:
// - The main code is built for Java 8 against the Bukkit 1.8.8 API, so it
//   loads on every server. Anything newer (Paper's item format, the
//   connection close event, the idle-time task queue) is looked up when the
//   server starts.
// - The "modern" source set (package ...paper.modern) uses the region
//   schedulers of Paper and Folia (1.20+). It is built for Java 17 against
//   the oldest Folia API, and only loaded on servers that have them.
import java.io.DataInputStream
import java.util.zip.ZipFile

plugins {
    alias(libs.plugins.shadow)
}

// This module is a plugin, not a library: no javadoc or sources jars.
tasks.named("javadocJar") { enabled = false }
tasks.named("sourcesJar") { enabled = false }

val modern: SourceSet = sourceSets.create("modern")

dependencies {
    compileOnly(libs.spigot.api.legacy)
    implementation(project(":leonsdb-core"))

    "modernCompileOnly"(libs.folia.api)
    "modernCompileOnly"(project(":leonsdb-core"))
    "modernCompileOnly"(sourceSets.main.get().output)

    testImplementation(libs.spigot.api.legacy)
    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.processResources {
    val props = mapOf("version" to project.version)
    inputs.properties(props)
    filesMatching("plugin.yml") { expand(props) }
}

tasks.jar { from(modern.output) }

tasks.shadowJar {
    archiveBaseName = "LeonsDB-Paper"
    archiveClassifier = ""
    from(modern.output)
    // gRPC discovers load balancers and name resolvers through service files;
    // every entry from every jar must be merged or the plugin fails at runtime.
    // We only use the Bukkit and Paper APIs, never server internals by their
    // Mojang names, so Paper doesn't need to remap the jar on startup.
    manifest { attributes("paperweight-mappings-namespace" to "mojang") }
    mergeServiceFiles()
    filesMatching("META-INF/services/**") { duplicatesStrategy = DuplicatesStrategy.INCLUDE }
    val prefix = "com.leonsdb.libs"
    relocate("io.grpc", "$prefix.grpc")
    relocate("com.google", "$prefix.google")
    relocate("io.perfmark", "$prefix.perfmark")
    relocate("android.annotation", "$prefix.android.annotation")
    relocate("org.codehaus.mojo", "$prefix.mojo")
    // Native BoringSSL builds for every OS (about 15 MB). Netty falls back to
    // the JDK's own TLS.
    exclude("META-INF/native/*netty_tcnative*")
    // Protobuf source files, only needed by code generators.
    exclude("**/*.proto")
    // The API package is NOT relocated: plugins compile against it directly.
}

// Google's common API protos are pulled in by grpc-protobuf but never used by
// the connector, and they would add about 15 MB to the jar.
configurations.runtimeClasspath {
    exclude(group = "com.google.api.grpc", module = "proto-google-common-protos")
}

// Every class a Java 8 server can load must be Java 8 bytecode (class file
// version 52), including the bundled libraries. Only the modern package and
// the multi-release parts of libraries, which Java 8 never loads, may be newer.
val checkJava8 = tasks.register("checkJava8") {
    description = "Checks that the plugin jar runs on Java 8."
    group = "verification"
    val jarFile = tasks.shadowJar.flatMap { it.archiveFile }
    inputs.file(jarFile)
    doLast {
        val newer = mutableListOf<String>()
        ZipFile(jarFile.get().asFile).use { zip ->
            for (entry in zip.entries().asSequence()) {
                val name = entry.name
                if (!name.endsWith(".class") || name.endsWith("module-info.class")
                    || name.startsWith("META-INF/versions/")
                    || name.startsWith("com/leonsdb/connector/paper/modern/")) {
                    continue
                }
                val header = ByteArray(8)
                zip.getInputStream(entry).use { DataInputStream(it).readFully(header) }
                val major = ((header[6].toInt() and 0xff) shl 8) or (header[7].toInt() and 0xff)
                if (major > 52) {
                    newer += "$name (class file version $major)"
                }
            }
        }
        if (newer.isNotEmpty()) {
            throw GradleException("${newer.size} classes in the plugin jar need a Java newer than 8:\n  "
                    + newer.take(25).joinToString("\n  "))
        }
    }
}

tasks.check { dependsOn(checkJava8) }
tasks.assemble { dependsOn(tasks.shadowJar) }
