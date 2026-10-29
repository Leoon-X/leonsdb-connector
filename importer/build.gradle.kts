// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

// The importer: moves a network's existing data (MySQL, MongoDB, SQLite, H2,
// YAML files) into LeonsDB with a bulk import. A command-line tool, built
// into one jar: java -jar leonsdb-import.jar plan.yml
plugins {
    alias(libs.plugins.shadow)
}

// A tool, not a library: no javadoc or sources jars.
tasks.named("javadocJar") { enabled = false }
tasks.named("sourcesJar") { enabled = false }

// JDBC drivers the importer downloads when a plan needs them; tests use the
// same versions from here instead of downloading.
val testDrivers by configurations.creating { isTransitive = false }

dependencies {
    implementation(project(":leonsdb-protocol"))
    implementation(libs.grpc.netty.shaded)
    implementation(libs.sqlite.jdbc)
    implementation(libs.mongodb.driver.sync)
    implementation(libs.snakeyaml)

    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testDrivers(libs.mysql.connector.j)
    testDrivers(libs.h2)
}

tasks.test {
    val drivers = files(testDrivers)
    inputs.files(drivers)
    jvmArgumentProviders.add(CommandLineArgumentProvider { listOf("-Dleonsdb.test.drivers=" + drivers.asPath) })
    // Against the real server, and against MySQL and MongoDB, when given:
    //   ./gradlew test -PleonsdbServer=... -PmysqlUrl=jdbc:mysql://... -PmongoUrl=mongodb://...
    systemProperty("leonsdb.server.binary", providers.gradleProperty("leonsdbServer").getOrElse(""))
    systemProperty("leonsdb.test.mysql", providers.gradleProperty("mysqlUrl").getOrElse(""))
    systemProperty("leonsdb.test.mongo", providers.gradleProperty("mongoUrl").getOrElse(""))
}

tasks.jar {
    manifest { attributes("Main-Class" to "com.leonsdb.importer.Main") }
}

tasks.shadowJar {
    archiveBaseName = "leonsdb-import"
    archiveClassifier = ""
    archiveVersion = ""
    // Netty (in gRPC) loads a native library; Java 22+ asks jars to say so.
    manifest { attributes("Main-Class" to "com.leonsdb.importer.Main", "Enable-Native-Access" to "ALL-UNNAMED") }
    // gRPC finds its transports through service files: merge them all.
    mergeServiceFiles()
    filesMatching("META-INF/services/**") { duplicatesStrategy = DuplicatesStrategy.INCLUDE }
    exclude("META-INF/native/*netty_tcnative*")
    exclude("**/*.proto")
}

configurations.runtimeClasspath {
    exclude(group = "com.google.api.grpc", module = "proto-google-common-protos")
}

tasks.assemble { dependsOn(tasks.shadowJar) }
