// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

// The public API plugin developers compile against. It has no dependencies
// so it never conflicts with anything on a Minecraft server.
dependencies {
    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
