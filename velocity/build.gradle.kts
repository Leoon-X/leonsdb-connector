// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

// The Velocity plugin. It tells LeonsDB which server a player is about to
// join, so the switch waits less. It holds no player data. The output jar
// bundles the connector's client, relocated like the Paper plugin's.
plugins {
    alias(libs.plugins.shadow)
}

// This module is a plugin, not a library: no javadoc or sources jars.
tasks.named("javadocJar") { enabled = false }
tasks.named("sourcesJar") { enabled = false }

dependencies {
    compileOnly(libs.velocity.api)
    implementation(project(":leonsdb-core"))

    testImplementation(libs.velocity.api)
    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.processResources {
    val props = mapOf("version" to project.version)
    inputs.properties(props)
    filesMatching("velocity-plugin.json") { expand(props) }
}

tasks.shadowJar {
    archiveBaseName = "LeonsDB-Velocity"
    archiveClassifier = ""
    mergeServiceFiles()
    filesMatching("META-INF/services/**") { duplicatesStrategy = DuplicatesStrategy.INCLUDE }
    val prefix = "com.leonsdb.libs"
    relocate("io.grpc", "$prefix.grpc")
    // Guice's annotations stay as they are: Velocity injects the plugin with them.
    relocate("com.google", "$prefix.google") { exclude("com.google.inject.**") }
    relocate("io.perfmark", "$prefix.perfmark")
    relocate("android.annotation", "$prefix.android.annotation")
    relocate("org.codehaus.mojo", "$prefix.mojo")
    exclude("META-INF/native/*netty_tcnative*")
    exclude("**/*.proto")
}

configurations.runtimeClasspath {
    exclude(group = "com.google.api.grpc", module = "proto-google-common-protos")
}

tasks.assemble { dependsOn(tasks.shadowJar) }
