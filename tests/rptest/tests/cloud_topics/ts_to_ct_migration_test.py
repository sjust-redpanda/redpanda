# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.mark import matrix
from ducktape.tests.test import TestContext
from ducktape.utils.util import wait_until

from rptest.clients.admin.v2 import Admin as AdminV2, metastore_pb, ntp_pb
from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.redpanda import SISettings, CLOUD_TOPICS_CONFIG_STR
from rptest.tests.redpanda_test import RedpandaTest


class TsMigrationTest(RedpandaTest):
    """
    End-to-end test for the tiered-storage → cloud-topics migration path.

    A topic is first created in 'tiered' storage mode so that phase-1 records
    are archived via tiered storage (TS).  After at least one TS segment is
    uploaded the topic is promoted to 'cloud' or 'tiered_cloud' (parametrized),
    which records a migration boundary in the CTP STM.  Phase-2 records are
    then produced via the CT write path.  After the CT reconciler catches up,
    a consume from offset 0 must return all records in order — phase-1 records
    served via the TS passthrough reader and phase-2 records via CT.
    """

    NUM_PHASE1 = 500
    NUM_PHASE2 = 1000
    MSG_SIZE = 128
    TOPIC = "ts-migration-test"

    # Topics are created inside the test body after selecting storage mode.
    topics = ()

    def __init__(self, test_context: TestContext):
        si_settings = SISettings(test_context, fast_uploads=True)
        super().__init__(
            test_context=test_context,
            num_brokers=1,
            si_settings=si_settings,
            extra_rp_conf={
                CLOUD_TOPICS_CONFIG_STR: True,
                "cloud_topics_produce_batching_size_threshold": 65536,
                "enable_cluster_metadata_upload_loop": False,
            },
        )
        self.rpk = RpkTool(self.redpanda)
        self.admin = Admin(self.redpanda)
        self.admin_v2 = AdminV2(self.redpanda)

    @cluster(num_nodes=2)
    @matrix(storage_mode=[
        TopicSpec.STORAGE_MODE_CLOUD,
        TopicSpec.STORAGE_MODE_TIERED_CLOUD,
    ])
    def test_ts_to_ct_migration(self, storage_mode: str):
        if storage_mode == TopicSpec.STORAGE_MODE_TIERED_CLOUD:
            self.redpanda.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )

        self.rpk.create_topic(
            self.TOPIC,
            partitions=1,
            replicas=1,
            config={TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED},
        )

        # Phase 1: produce into the tiered topic
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            self.TOPIC,
            msg_size=self.MSG_SIZE,
            msg_count=self.NUM_PHASE1,
        )

        # Wait for TS archival to upload at least one segment
        def has_ts_segment() -> bool:
            manifest = self.admin.get_partition_manifest(self.TOPIC, 0)
            return len(manifest.get("segments", {})) >= 1

        wait_until(
            has_ts_segment,
            timeout_sec=120,
            backoff_sec=5,
            err_msg="No TS segment uploaded within 120s",
            retry_on_exc=True,
        )

        # Promote to cloud/tiered_cloud — records a migration boundary in the
        # CTP STM; subsequent writes go through the CT pipeline.
        self.rpk.alter_topic_config(
            self.TOPIC, TopicSpec.PROPERTY_STORAGE_MODE, storage_mode
        )

        # Phase 2: produce via CT write path
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.redpanda,
            self.TOPIC,
            msg_size=self.MSG_SIZE,
            msg_count=self.NUM_PHASE2,
        )

        total = self.NUM_PHASE1 + self.NUM_PHASE2

        # Wait for the CT reconciler to process all records
        def is_reconciled() -> bool:
            metastore = self.admin_v2.metastore()
            req = metastore_pb.GetOffsetsRequest(
                partition=ntp_pb.TopicPartition(topic=self.TOPIC, partition=0)
            )
            return metastore.get_offsets(req=req).offsets.next_offset >= total

        wait_until(
            is_reconciled,
            timeout_sec=180,
            backoff_sec=5,
            err_msg=f"CT reconciler did not process all {total} records within 180s",
            retry_on_exc=True,
        )

        # Consume from offset 0 and verify all records arrive in order.
        # Phase-1 records are served via the TS passthrough reader; phase-2
        # records via the CT L1/L0 reader.
        out = self.rpk.consume(
            self.TOPIC,
            n=total,
            partition=0,
            offset=0,
            format="%o\n",
            timeout=180,
            read_committed=True,
        )

        offsets = [int(line) for line in out.splitlines() if line.strip()]
        assert len(offsets) == total, (
            f"Expected {total} records, got {len(offsets)}"
        )
        assert offsets == list(range(total)), (
            f"Offset sequence broken: first={offsets[0]}, "
            f"last={offsets[-1]}, len={len(offsets)}"
        )
