// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

// Platform-independent connector logic: networking, caching and write
// merging. Paper, Velocity and other platforms build on top of this.
dependencies {
    api(project(":leonsdb-api"))
    api(project(":leonsdb-protocol"))
    implementation(libs.grpc.netty.shaded)

    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation(libs.grpc.inprocess)
}

// Contract tests against the real server run when its binary is given:
//   ./gradlew test -PleonsdbServer=/path/to/leonsdb
tasks.test {
    systemProperty("leonsdb.server.binary", providers.gradleProperty("leonsdbServer").getOrElse(""))
}
