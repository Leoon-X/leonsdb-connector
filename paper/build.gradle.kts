// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

// The Paper plugin. The output jar bundles the connector and its
// dependencies, relocated so they can never clash with other plugins.
plugins {
    alias(libs.plugins.shadow)
}

// This module is a plugin, not a library: no javadoc or sources jars.
tasks.named("javadocJar") { enabled = false }
tasks.named("sourcesJar") { enabled = false }

dependencies {
    compileOnly(libs.paper.api)
    implementation(project(":leonsdb-core"))
    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.processResources {
    val props = mapOf("version" to project.version)
    inputs.properties(props)
    filesMatching("plugin.yml") { expand(props) }
}

tasks.shadowJar {
    archiveBaseName = "LeonsDB-Paper"
    archiveClassifier = ""
    // gRPC discovers load balancers and name resolvers through service files;
    // every entry from every jar must be merged or the plugin fails at runtime.
    // We only use the Paper API, never server internals, so Paper doesn't
    // need to remap the jar on startup (saves a few seconds per boot).
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
    // the JDK's own TLS, which is fully supported on Java 21.
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

tasks.assemble { dependsOn(tasks.shadowJar) }
