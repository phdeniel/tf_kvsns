# Description:
# KVSNS support for TensorFlow.

load(
    "//tensorflow:tensorflow.bzl",
    "tf_cc_test",
)

package(
    default_visibility = ["//visibility:public"],
    licenses = ["notice"],  # Apache 2.0
)

cc_library(
    name = "kvsns_file_system",
    srcs = ["kvsns_file_system.cc"],
    hdrs = ["kvsns_file_system.h"],
    copts = ["-g"],
    deps = [
        "//tensorflow/core:lib",
        "//tensorflow/core:lib_internal",
    ],
    alwayslink = 1,
)

tf_cc_test(
    name = "kvsns_file_system_test",
    size = "small",
    srcs = [
        "kvsns_file_system_test.cc",
    ],
    tags = [
        "manual",
	"notap",
    ],
    deps = [
        ":kvsns_file_system",
        "//tensorflow/core:lib",
        "//tensorflow/core:lib_internal",
        "//tensorflow/core:test",
        "//tensorflow/core:test_main",
    ],
)
