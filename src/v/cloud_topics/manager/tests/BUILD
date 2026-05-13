load("//bazel:test.bzl", "redpanda_cc_gtest")

redpanda_cc_gtest(
    name = "manager_notification_test",
    timeout = "short",
    srcs = ["manager_notification_test.cc"],
    deps = [
        "//src/v/cloud_topics/manager",
        "//src/v/cluster",
        "//src/v/cluster:notification",
        "//src/v/cluster:topic_configuration",
        "//src/v/cluster/utils:partition_change_notifier_api",
        "//src/v/model",
        "//src/v/test_utils:gtest",
        "@googletest//:gtest",
        "@seastar",
    ],
)
