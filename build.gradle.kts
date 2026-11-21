// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

import com.github.jk1.license.filter.LicenseBundleNormalizer
import com.github.jk1.license.filter.DependencyFilter
import com.github.jk1.license.render.InventoryMarkdownReportRenderer
import com.github.jk1.license.render.ReportRenderer

plugins {
    alias(libs.plugins.license.report)
}

// License audit: `./gradlew checkLicense` fails if anything we ship depends
// on a library whose license is not in allowed-licenses.json. Copyleft
// licenses (GPL, LGPL, AGPL) are deliberately absent: the connector is
// Apache-2.0 and plugin developers must be able to use it in closed-source
// plugins. Compile-only dependencies such as the Paper API are not shipped
// and are not checked.
licenseReport {
    configurations = arrayOf("runtimeClasspath")
    allowedLicensesFile = file("allowed-licenses.json")
    filters = arrayOf<DependencyFilter>(LicenseBundleNormalizer())
    renderers = arrayOf<ReportRenderer>(InventoryMarkdownReportRenderer("third-party-licenses.md", "LeonsDB Connector"))
}

// The Java version a compile task builds for. Code that runs inside a
// Minecraft server targets Java 8, since servers 1.8 to 1.16 run on it; the
// Paper plugin's code for modern Paper and Folia (1.20+) targets Java 17;
// the tools, the Velocity and example plugins and all tests target Java 21.
fun javaRelease(project: String, task: String): Int {
    val onServer = setOf("leonsdb-api", "leonsdb-core", "leonsdb-protocol", "leonsdb-paper")
    return when {
        task == "compileModernJava" -> 17
        task == "compileJava" && project in onServer -> 8
        else -> 21
    }
}

// Settings shared by every connector module.
subprojects {
    val moduleName = name
    group = "com.leonsdb"
    // A release sets it: ./gradlew -PleonsdbVersion=1.2.3
    version = providers.gradleProperty("leonsdbVersion").getOrElse("0.1.0-SNAPSHOT")

    // The same sources give the same jars: releases are checked for it.
    tasks.withType<AbstractArchiveTask>().configureEach {
        isPreserveFileTimestamps = false
        isReproducibleFileOrder = true
    }

    apply(plugin = "java-library")

    extensions.configure<JavaPluginExtension> {
        withSourcesJar()
        withJavadocJar()
    }

    tasks.withType<JavaCompile>().configureEach {
        // One jar runs on every server from 1.8 (Java 8) to the newest, even
        // when Gradle itself runs on a newer JDK; see javaRelease.
        options.release = javaRelease(moduleName, name)
        options.encoding = "UTF-8"
        // -options: newer JDKs warn that Java 8 is an old target.
        options.compilerArgs.addAll(listOf("-Xlint:all", "-Xlint:-processing", "-Xlint:-serial", "-Xlint:-options"))
        // Warnings are errors in our own code. Generated protobuf code (the
        // protocol module) is exempt because we do not control it.
        if (project.name != "leonsdb-protocol") options.compilerArgs.add("-Werror")
    }

    tasks.withType<Javadoc>().configureEach {
        (options as StandardJavadocDocletOptions).apply {
            addStringOption("Xdoclint:all,-missing", "-quiet")
            encoding = "UTF-8"
        }
        // Generated protobuf classes don't need strict doc checks.
        isFailOnError = project.name != "leonsdb-protocol"
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        testLogging {
            events("failed")
            exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        }
    }

    tasks.withType<Jar>().configureEach {
        // Ship the license and the third-party notices with every jar.
        from(rootProject.file("LICENSE")) { into("META-INF") }
        from(rootProject.file("NOTICE")) { into("META-INF") }
    }
}
