/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "redpanda/admin/services/cloud_topic_migration.h"

#include "base/vlog.h"
#include "cloud_storage/configuration.h"
#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/partition_manifest_downloader.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/remote_path_provider.h"
#include "cloud_storage/spillover_manifest.h"
#include "cloud_storage_clients/types.h"
#include "cloud_topics/level_one/metastore/replicated_metastore.h"
#include "cluster/topic_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "serde/protobuf/rpc.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>

#include <chrono>

using namespace std::chrono_literals;

namespace admin {

namespace {

// NOLINTNEXTLINE(*-non-const-global-variables,cert-err58-*)
ss::logger ctmlog{"admin_api_server/cloud_topic_migration_service"};

constexpr size_t max_sample_keys = 16;

// Accumulates the per-partition reclaim outcome before it is copied into the
// proto response.
struct partition_sweep {
    int64_t segments_scanned{0};
    int64_t segments_unreferenced{0};
    // Backing object keys (segment + .tx + .index per unreferenced segment).
    // std::vector because remote::delete_objects is only instantiated for
    // std::vector / std::deque.
    std::vector<cloud_storage_clients::object_key> keys;
    std::vector<ss::sstring> sample_keys;
};

// Appends the backing-object keys for one unreferenced segment and records a
// capped sample for operator inspection.
void reclaim_segment(
  partition_sweep& sweep, const cloud_storage::remote_segment_path& seg_path) {
    const auto& native = seg_path().native();
    if (sweep.sample_keys.size() < max_sample_keys) {
        sweep.sample_keys.push_back(native);
    }
    sweep.keys.emplace_back(std::filesystem::path{native});
    sweep.keys.emplace_back(
      std::filesystem::path{fmt::format("{}.tx", native)});
    sweep.keys.emplace_back(
      std::filesystem::path{fmt::format("{}.index", native)});
}

// Adds every segment in `manifest` whose entire Kafka range sits at or below
// the partition's live L1 start offset (i.e. the cloud topic has already
// retention-expired it) to the sweep. Segments that straddle or sit above the
// start offset are still referenced and left for ordinary L1 GC.
void sweep_manifest_segments(
  partition_sweep& sweep,
  const cloud_storage::partition_manifest& manifest,
  const cloud_storage::remote_path_provider& provider,
  kafka::offset start_offset) {
    for (auto it = manifest.begin(); it != manifest.end(); ++it) {
        const cloud_storage::segment_meta& meta = *it;
        ++sweep.segments_scanned;
        if (meta.next_kafka_offset() > start_offset) {
            continue;
        }
        ++sweep.segments_unreferenced;
        reclaim_segment(sweep, manifest.generate_segment_path(meta, provider));
    }
}

} // namespace

ss::future<proto::admin::reclaim_migrated_backing_response>
cloud_topic_migration_service_impl::reclaim_migrated_backing(
  serde::pb::rpc::context, proto::admin::reclaim_migrated_backing_request req) {
    model::topic_namespace tns{
      model::kafka_namespace, model::topic{req.get_topic()}};
    const auto& md = _topic_table->local().get_topic_metadata_ref(tns);
    if (!md) {
        throw serde::pb::rpc::not_found_exception("topic not found");
    }
    const auto& cfg = md->get().get_configuration();
    if (!cfg.is_cloud_topic()) {
        throw serde::pb::rpc::failed_precondition_exception(
          "topic is not a cloud topic");
    }
    auto topic_id = cfg.tp_id;
    if (!topic_id) {
        throw serde::pb::rpc::not_found_exception("topic missing id");
    }
    auto remote_rev = md->get().get_remote_revision();
    if (!remote_rev) {
        throw serde::pb::rpc::failed_precondition_exception(
          "topic has no remote revision");
    }
    const auto partition_count = cfg.partition_count;
    const bool preserve = cfg.properties.preserve_migrated_ts_objects.value_or(
      true);

    const bool want_delete = req.get_delete_unreferenced();
    if (want_delete && preserve) {
        throw serde::pb::rpc::failed_precondition_exception(
          "redpanda.cloud_topic.preserve_migrated_ts must be disabled before "
          "deleting; set it to false first so ongoing GC does not re-orphan "
          "segments after the sweep");
    }

    // Precondition: every partition must have cut over (migration complete)
    // before any backing is reclaimed. Gather the live L1 start offsets.
    std::vector<kafka::offset> start_offsets(partition_count);
    for (int32_t p = 0; p < partition_count; ++p) {
        model::topic_id_partition tidp{*topic_id, model::partition_id{p}};
        auto offsets = co_await _metastore->local().get_offsets(tidp);
        if (!offsets) {
            if (
              offsets.error()
              == cloud_topics::l1::metastore::errc::missing_ntp) {
                throw serde::pb::rpc::failed_precondition_exception(fmt::format(
                  "partition {} is absent from the metastore; migration is "
                  "incomplete",
                  p));
            }
            throw serde::pb::rpc::unavailable_exception(
              fmt::format("failed to read offsets for partition {}", p));
        }
        if (offsets->migrating) {
            throw serde::pb::rpc::failed_precondition_exception(fmt::format(
              "partition {} is still migrating; reclaim is only valid after "
              "every partition has cut over",
              p));
        }
        start_offsets[p] = offsets->start_offset;
    }

    auto bucket_cfg = cloud_storage::configuration::get_bucket_config().value();
    if (!bucket_cfg) {
        throw serde::pb::rpc::failed_precondition_exception(
          "cloud storage bucket is not configured");
    }
    cloud_storage_clients::bucket_name bucket{*bucket_cfg};

    cloud_storage::remote_path_provider provider(
      cfg.properties.remote_label,
      cfg.properties.remote_topic_namespace_override);

    proto::admin::reclaim_migrated_backing_response resp;
    resp.set_deleted(want_delete);

    auto& remote = _remote->local();
    for (int32_t p = 0; p < partition_count; ++p) {
        model::ntp ntp{tns.ns, tns.tp, model::partition_id{p}};
        const auto start_offset = start_offsets[p];

        ss::abort_source as;
        retry_chain_node rtc(as, 300s, 100ms);

        cloud_storage::partition_manifest manifest(ntp, *remote_rev);
        cloud_storage::partition_manifest_downloader dl(
          bucket, provider, ntp, *remote_rev, remote);
        auto dl_res = co_await dl.download_manifest(rtc, &manifest);
        if (dl_res.has_error()) {
            throw serde::pb::rpc::unavailable_exception(fmt::format(
              "failed to download partition manifest for partition {}: {}",
              p,
              dl_res.error()));
        }

        partition_sweep sweep;
        if (
          dl_res.value()
          != cloud_storage::find_partition_manifest_outcome::
            no_matching_manifest) {
            sweep_manifest_segments(sweep, manifest, provider, start_offset);

            // Walk the spillover manifests. The oldest history (most likely to
            // be orphaned) lives here. Skip a spillover whose entire range sits
            // at or above the live start offset; download the rest and check
            // per-segment.
            const auto& spillovers = manifest.get_spillover_map();
            for (auto it = spillovers.begin(); it != spillovers.end(); ++it) {
                if (it->base_kafka_offset() >= start_offset) {
                    continue;
                }
                cloud_storage::spillover_manifest_path_components comp{
                  .base = it->base_offset,
                  .last = it->committed_offset,
                  .base_kafka = it->base_kafka_offset(),
                  .next_kafka = it->next_kafka_offset(),
                  .base_ts = it->base_timestamp,
                  .last_ts = it->max_timestamp,
                };
                auto spill_path = provider.spillover_manifest_path(
                  manifest, comp);
                cloud_storage::spillover_manifest spill(ntp, *remote_rev);
                auto spill_res = co_await remote.download_manifest(
                  bucket,
                  {cloud_storage::manifest_format::serde,
                   cloud_storage::remote_manifest_path{spill_path}},
                  spill,
                  rtc);
                if (spill_res != cloud_storage::download_result::success) {
                    throw serde::pb::rpc::unavailable_exception(fmt::format(
                      "failed to download spillover manifest {} for partition "
                      "{}",
                      spill_path,
                      p));
                }
                sweep_manifest_segments(sweep, spill, provider, start_offset);
            }
        }

        int64_t objects_deleted = 0;
        if (want_delete && !sweep.keys.empty()) {
            auto del_res = co_await remote.delete_objects(
              bucket, std::move(sweep.keys), rtc);
            if (del_res != cloud_storage::upload_result::success) {
                throw serde::pb::rpc::unavailable_exception(fmt::format(
                  "failed to delete backing objects for partition {}", p));
            }
            objects_deleted = sweep.segments_unreferenced * 3;
        }

        vlog(
          ctmlog.info,
          "reclaim {} partition {}: scanned={} unreferenced={} deleted_objects="
          "{} (dry_run={})",
          tns.tp,
          p,
          sweep.segments_scanned,
          sweep.segments_unreferenced,
          objects_deleted,
          !want_delete);

        auto& pr = resp.get_partitions().emplace_back();
        pr.set_partition(p);
        pr.set_start_offset(start_offset());
        pr.set_segments_scanned(sweep.segments_scanned);
        pr.set_segments_referenced(
          sweep.segments_scanned - sweep.segments_unreferenced);
        pr.set_segments_unreferenced(sweep.segments_unreferenced);
        pr.set_objects_deleted(objects_deleted);
        for (auto& k : sweep.sample_keys) {
            pr.get_sample_object_keys().push_back(std::move(k));
        }
    }

    co_return resp;
}

} // namespace admin
