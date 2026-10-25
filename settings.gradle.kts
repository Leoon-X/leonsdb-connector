// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

rootProject.name = "leonsdb-connector"

include("protocol", "api", "core", "paper", "example")

project(":protocol").name = "leonsdb-protocol"
project(":api").name = "leonsdb-api"
project(":core").name = "leonsdb-core"
project(":paper").name = "leonsdb-paper"
project(":example").name = "leonsdb-example"

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        maven("https://repo.papermc.io/repository/maven-public/") {
            content { includeGroupAndSubgroups("io.papermc") ; includeGroup("net.md-5") ; includeGroup("com.mojang") ; includeGroupAndSubgroups("net.kyori") }
        }
    }
}
