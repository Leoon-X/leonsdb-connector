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

// Settings shared by every connector module.
subprojects {
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
        // Bytecode level 21 so the connector runs on every supported server,
        // even when Gradle itself runs on a newer JDK.
        options.release = 21
        options.encoding = "UTF-8"
        options.compilerArgs.addAll(listOf("-Xlint:all", "-Xlint:-processing", "-Xlint:-serial"))
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
