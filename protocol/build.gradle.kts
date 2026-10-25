// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Leoon-X

// Java classes generated from the protocol definitions in proto/.
// Kept in its own module so other JVM tools can reuse the wire protocol
// without pulling in the connector.
plugins {
    alias(libs.plugins.protobuf)
}

dependencies {
    api(libs.protobuf.java)
    api(libs.grpc.protobuf)
    api(libs.grpc.stub)
    compileOnly("org.apache.tomcat:annotations-api:6.0.53") // @Generated in grpc stubs
}

sourceSets {
    main {
        proto {
            srcDir(rootProject.file("proto"))
            exclude("gen/**")
        }
    }
}

protobuf {
    protoc { artifact = libs.protoc.get().toString() }
    plugins {
        create("grpc") { artifact = libs.grpc.protoc.gen.get().toString() }
    }
    generateProtoTasks {
        all().configureEach {
            plugins { create("grpc") }
        }
    }
}
