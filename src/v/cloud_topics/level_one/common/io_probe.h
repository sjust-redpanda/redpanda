/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "metrics/metrics.h"

namespace cloud_topics::l1 {

/// Probe for the L1 io layer, which counts bytes read for objects whose reads
/// happen below the reader. Registered under the cloud_topics_level_one_reader
/// metric group so footer_read_bytes is unchanged from when the reader owned
/// it.
class io_probe {
public:
    io_probe();

    void register_footer_read(size_t bytes) { _footer_bytes_read += bytes; }

private:
    void setup_metrics();

    uint64_t _footer_bytes_read{0};

    metrics::internal_metric_groups _metrics;
};

} // namespace cloud_topics::l1
