// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

rootProject.name = "leonsdb-connector"

include("protocol", "api", "core", "paper", "velocity", "example", "importer")

project(":protocol").name = "leonsdb-protocol"
project(":api").name = "leonsdb-api"
project(":core").name = "leonsdb-core"
project(":paper").name = "leonsdb-paper"
project(":velocity").name = "leonsdb-velocity"
project(":example").name = "leonsdb-example"
project(":importer").name = "leonsdb-importer"

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        maven("https://repo.papermc.io/repository/maven-public/") {
            content { includeGroupAndSubgroups("io.papermc") ; includeGroup("dev.folia") ; includeGroup("net.md-5") ; includeGroup("com.mojang") ; includeGroupAndSubgroups("net.kyori") ; includeGroup("com.velocitypowered") }
        }
        // Only for the Bukkit 1.8.8 API, which the Paper plugin compiles against.
        maven("https://hub.spigotmc.org/nexus/content/repositories/snapshots/") {
            content { includeGroup("org.spigotmc") }
        }
    }
}
