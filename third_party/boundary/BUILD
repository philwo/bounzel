package(default_visibility = ["//visibility:public"])

load("@//third_party/grpc:build_defs.bzl", "java_grpc_library")

licenses(["notice"])

exports_files(["LICENSE"])

filegroup(
    name = "srcs",
    srcs = glob(["**"]),
)

java_grpc_library(
    name = "boundary_java_grpc_proto",
    srcs = [":boundary_proto"],
    deps = [":boundary_java_proto"],
)

java_grpc_library(
    name = "cassy_java_grpc_proto",
    srcs = [":cassy_proto"],
    deps = [":cassy_java_proto"],
)

java_proto_library(
    name = "boundary_java_proto",
    deps = [":boundary_proto"],
)

java_proto_library(
    name = "cassy_java_proto",
    deps = [":cassy_proto"],
)

java_proto_library(
    name = "file_java_proto",
    deps = [":file_proto"],
)

java_proto_library(
    name = "digest_java_proto",
    deps = [":digest_proto"],
)

proto_library(
    name = "boundary_proto",
    srcs = ["de/geheimspeicher/boundary/boundary.proto"],
    deps = [
        ":cassy_proto",
        ":digest_proto",
        ":file_proto",
    ],
)

proto_library(
    name = "cassy_proto",
    srcs = ["de/geheimspeicher/boundary/cassy.proto"],
    deps = [
        ":digest_proto",
        ":file_proto",
    ],
)

proto_library(
    name = "file_proto",
    srcs = ["de/geheimspeicher/boundary/file.proto"],
    deps = [
        ":digest_proto",
    ],
)

proto_library(
    name = "digest_proto",
    srcs = ["de/geheimspeicher/boundary/digest.proto"],
)
