/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "base/seastarx.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "container/chunked_vector.h"
#include "utils/named_type.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>

#include <expected>

namespace cloud_topics::l1 {

class io;
class simple_stm;

class garbage_collector {
public:
    garbage_collector(
      simple_stm* stm, io* io, preserve_imported_backing_fn preserve = {});

    using error = named_type<ss::sstring, struct gc_error_tag>;
    ss::future<std::expected<void, error>>
    remove_unreferenced_objects(ss::abort_source*);

    // Drops the metastore rows for `to_remove`, and deletes the backing object
    // storage (for an imported segment, the segment + .tx + .index) only for
    // `to_delete` -- a subset excluding imported segments whose topic opts to
    // preserve its tiered-storage data for recover-as-TS.
    ss::future<std::expected<void, error>> remove_objects(
      chunked_vector<object_location> to_remove,
      chunked_vector<object_location> to_delete,
      ss::abort_source*);

private:
    // Callers are expected to ensure this object outlives the stm and io.
    simple_stm* stm_;
    io* io_;
    preserve_imported_backing_fn preserve_imported_;
};

} // namespace cloud_topics::l1
