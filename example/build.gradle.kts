// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

// An example plugin that uses LeonsDB the way any plugin would: it depends
// only on the API, never on the connector's internals. Also used to test
// the connector on a real server.

// Not a library: no javadoc or sources jars.
tasks.named("javadocJar") { enabled = false }
tasks.named("sourcesJar") { enabled = false }

dependencies {
    compileOnly(libs.paper.api)
    compileOnly(project(":leonsdb-api"))
}

tasks.processResources {
    val props = mapOf("version" to project.version)
    inputs.properties(props)
    filesMatching("plugin.yml") { expand(props) }
}

tasks.jar {
    archiveBaseName = "LeonsDB-Example"
    manifest { attributes("paperweight-mappings-namespace" to "mojang") }
}
