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
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
)
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

    # Parameters for the transactional migration test.
    # msgs_per_transaction is intentionally large so that when migration fires
    # mid-stream, at least one in-flight transaction straddles the boundary.
    #
    # NUM_TX_MSGS_PHASE1 must be large enough that the total log volume for
    # phase 1 exceeds log_segment_size (1 MB, see extra_rp_conf below).
    # disk_space_manager::manage_data_disk() skips reclaim when
    # real_target_excess <= log_segment_size().  With TX_MSGS_PER_TXN=200 and
    # MSG_SIZE=128 each transactional batch is ~44 KB, so 50 transactions
    # (~10 000 messages) produce ~2.2 MB — well above the 1 MB threshold.
    TOPIC_TX = "ts-migration-tx-test"
    NUM_TX_MSGS_PHASE1 = 10000
    NUM_TX_MSGS_PHASE2 = 1000
    TX_MSGS_PER_TXN = 200
    TX_ABORT_RATE = 0.3

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
                # Remove cluster-level minimums that would clamp topic-level
                # segment.bytes and segment.ms to 1 MB / 10 min respectively,
                # preventing segment rolling in the transactions test.
                "log_segment_size_min": 1,
                "log_segment_ms_min": 1000,
                # Set log_segment_size to its minimum (1 MB).
                # disk_space_manager::manage_data_disk() skips reclaim when
                # real_target_excess <= log_segment_size().  The default is
                # 128 MB which would never be exceeded by a small test log.
                # Setting it to 1 MB (the minimum allowed by bounded_property)
                # reduces the threshold to 1 MB; NUM_TX_MSGS_PHASE1 is sized
                # so that the phase-1 log (~2.2 MB) comfortably exceeds it.
                "log_segment_size": 1048576,
                # Cloud storage housekeeping defaults to 5 minutes; reduce to
                # 1 second so that local-retention enforcement (which evicts
                # uploaded segments) runs promptly and the is_log_truncated
                # wait completes well within the 120-second timeout.
                "cloud_storage_housekeeping_interval_ms": 1000,
                # Run log GC every second (default: 10s) so that uploaded
                # segments are evicted promptly once mark_clean is applied.
                "log_compaction_interval_ms": 1000,
                # Enable and run the disk space manager every second.
                # disk_space_manager::run_loop() skips all work when
                # _target_size == 0 (i.e. no capacity target is set), so
                # retention_local_trim_interval alone does nothing.  Setting a
                # tiny cluster-level capacity target activates the manager,
                # which calls get_reclaimable_offsets() → set_cloud_gc_offset()
                # on each cloud/tiered partition — the path that respects
                # retention.local.target.bytes regardless of strict-mode flags.
                "retention_local_target_capacity_bytes": 1024,
                "retention_local_trim_interval": 1000,
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

        # Start phase-2 producer before promoting the topic so that produces are
        # in-flight when the config change lands.
        producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.TOPIC,
            msg_size=self.MSG_SIZE,
            msg_count=self.NUM_PHASE2,
        )
        try:
            producer.start()

            # Wait until the producer has acknowledged at least 50 records so
            # the config change is guaranteed to race with active writes.
            producer.wait_for_acks(50, timeout_sec=30, backoff_sec=0.5)

            # Promote to cloud/tiered_cloud — records a migration boundary in
            # the CTP STM.  Some phase-2 records will have been written as
            # tiered-storage records before this lands; they fall after the
            # boundary and are served via CT once reconciled.
            self.rpk.alter_topic_config(
                self.TOPIC, TopicSpec.PROPERTY_STORAGE_MODE, storage_mode
            )

            producer.wait(timeout_sec=120)
        finally:
            producer.stop()
            producer.free()

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

    @cluster(num_nodes=2)
    @matrix(storage_mode=[
        TopicSpec.STORAGE_MODE_CLOUD,
        TopicSpec.STORAGE_MODE_TIERED_CLOUD,
    ])
    def test_ts_to_ct_migration_transactions(self, storage_mode: str):
        """
        Verify that read_committed semantics are correct for Kafka transactions
        that touch the TS→CT migration boundary.  Two distinct scenarios are
        exercised in a single producer run:

        1. Aborted transactions fully within the TS range.  The producer runs
           and resolves some transactions (committed and aborted) before
           migration.  The test waits for prefix truncation so that _rm_stm
           discards its abort-index state for the S3 range.  After migration,
           reads of those pre-migration offsets must source aborted-transaction
           ranges from the per-segment tx_range_manifest in S3, not from
           _rm_stm, which no longer holds that state.

        2. Transactions straddling the migration boundary.  The producer is
           still running when the storage-mode change lands, so at least one
           transaction has data records in the TS (S3) range while its
           commit/abort control batch lands in the post-migration CT raft log.

        In both cases a read_committed consumer must see exactly the committed
        records and none of the aborted ones.  invalid_reads > 0 from the seq
        consumer is the failure signal.
        """
        if storage_mode == TopicSpec.STORAGE_MODE_TIERED_CLOUD:
            self.redpanda.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )

        # Small segments and tight local retention force the archiver to upload
        # quickly and prefix-truncate the local log, which is the precondition
        # for _rm_stm to drop its abort state for the S3 range.
        #
        # segment.ms is used instead of (or in addition to) segment.bytes
        # because transactional produce batches are large enough (~44 KB each
        # for msgs_per_transaction=200) to prevent reliable byte-based rolling.
        # A 1-second time limit guarantees multiple segments even when the
        # producer finishes before any byte threshold is crossed.
        self.rpk.create_topic(
            self.TOPIC_TX,
            partitions=1,
            replicas=1,
            config={
                TopicSpec.PROPERTY_STORAGE_MODE: TopicSpec.STORAGE_MODE_TIERED,
                "segment.bytes": str(32 * 1024),
                "segment.ms": "1000",
                "retention.local.target.bytes": str(64 * 1024),
            },
        )

        phase1_producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.TOPIC_TX,
            msg_size=self.MSG_SIZE,
            msg_count=self.NUM_TX_MSGS_PHASE1,
            use_transactions=True,
            transaction_abort_rate=self.TX_ABORT_RATE,
            msgs_per_transaction=self.TX_MSGS_PER_TXN,
        )

        try:
            phase1_producer.start()

            # Wait for at least one TS segment to reach S3.
            def has_ts_segment() -> bool:
                manifest = self.admin.get_partition_manifest(self.TOPIC_TX, 0)
                return len(manifest.get("segments", {})) >= 1

            wait_until(
                has_ts_segment,
                timeout_sec=120,
                backoff_sec=5,
                err_msg="No TS segment uploaded within 120s",
                retry_on_exc=True,
            )

            # Wait for prefix truncation.  Once local_log_start_offset > 0,
            # _rm_stm has taken a snapshot that drops abort-index entries fully
            # below start_offset.  From this point the per-segment
            # tx_range_manifest objects in S3 are the only correct source for
            # aborted-transaction ranges in the S3 portion of the log.
            def is_log_truncated() -> bool:
                status = self.admin.get_partition_cloud_storage_status(
                    self.TOPIC_TX, 0
                )
                return status.get("local_log_start_offset", 0) > 0

            wait_until(
                is_log_truncated,
                timeout_sec=120,
                backoff_sec=5,
                err_msg="Local log not prefix-truncated within 120s",
                retry_on_exc=True,
            )

            # Promote to CT while the producer is still running so that at
            # least one in-flight transaction straddles the boundary: its data
            # records land in the TS range while its commit/abort control batch
            # lands in the post-migration CT raft log.
            self.rpk.alter_topic_config(
                self.TOPIC_TX, TopicSpec.PROPERTY_STORAGE_MODE, storage_mode
            )

            phase1_producer.wait(timeout_sec=120)
        finally:
            phase1_producer.stop()
            phase1_producer.free()

        total_committed = (
            phase1_producer.produce_status.acked
            - phase1_producer.produce_status.aborted_transaction_messages
        )

        # Phase 2: produce post-migration transactional records via the CT
        # write path.  This is required for two reasons:
        #   1. The CT reconciler only registers the NTP in the metastore when
        #      it processes its first L0 batch.  Without phase-2 data there is
        #      nothing to reconcile and is_reconciled would time out with
        #      metastore::errc::not_found.
        #   2. Phase-2 records exercise read_committed semantics for
        #      transactions written entirely in CT mode alongside phase-1
        #      records served via the TS passthrough reader.
        phase2_producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.TOPIC_TX,
            msg_size=self.MSG_SIZE,
            msg_count=self.NUM_TX_MSGS_PHASE2,
            use_transactions=True,
            transaction_abort_rate=self.TX_ABORT_RATE,
            msgs_per_transaction=self.TX_MSGS_PER_TXN,
        )
        try:
            phase2_producer.start()
            phase2_producer.wait(timeout_sec=120)
        finally:
            phase2_producer.stop()
            phase2_producer.free()

        # Sentinel: one non-transactional record appended after the phase-2
        # transactional producer.  The CT reconciler uses read_committed_reader
        # internally and cannot advance LRO past a trailing all-aborted range
        # (read_committed_reader returns no batches for a range composed
        # entirely of aborted-transaction records).  By guaranteeing a
        # committed record at the very end of the log the sentinel ensures LRO
        # can always reach HWM, making the is_reconciled check reliable.
        sentinel_producer = KgoVerifierProducer(
            self.test_context,
            self.redpanda,
            self.TOPIC_TX,
            msg_size=self.MSG_SIZE,
            msg_count=1,
        )
        try:
            sentinel_producer.start()
            sentinel_producer.wait(timeout_sec=30)
        finally:
            sentinel_producer.stop()
            sentinel_producer.free()

        # Committed record count: acked includes both committed and aborted
        # data records; subtract the aborted ones.  Add 1 for the sentinel.
        total_committed += (
            phase2_producer.produce_status.acked
            - phase2_producer.produce_status.aborted_transaction_messages
            + 1
        )

        # Wait for the CT reconciler to process all post-migration records.
        # The HWM covers the full Kafka offset space (data + aborted records +
        # control batches), so next_offset >= HWM confirms the reconciler has
        # caught up to the end of the log.  The sentinel guarantees at least
        # one committed record at the tail so LRO can advance to HWM even when
        # all phase-2 transactions are aborted.
        def is_reconciled() -> bool:
            partitions = list(self.rpk.describe_topic(self.TOPIC_TX))
            if not partitions:
                return False
            hwm = partitions[0].high_watermark
            metastore = self.admin_v2.metastore()
            req = metastore_pb.GetOffsetsRequest(
                partition=ntp_pb.TopicPartition(
                    topic=self.TOPIC_TX, partition=0
                )
            )
            return metastore.get_offsets(req=req).offsets.next_offset >= hwm

        wait_until(
            is_reconciled,
            timeout_sec=180,
            backoff_sec=5,
            err_msg="CT reconciler did not catch up to HWM within 180s",
            retry_on_exc=True,
        )

        # Consume from offset 0 with read_committed isolation and validate the
        # full sequence.  The seq consumer checks that every committed record
        # appears in key order and that no records from aborted transactions are
        # present.
        #
        # loop=False causes kgo-verifier to read to the current LSO once and
        # exit rather than looping indefinitely.  This avoids the deadlock
        # where max_offsets_consumed can never reach max_offsets_produced when
        # the last produced transaction was aborted (aborted records are
        # filtered by read_committed, so the consumer never consumes their
        # offsets).
        #
        # The expected failure mode for the known bug: invalid_reads > 0
        # because frontend::aborted_transactions returns empty for pre-migration
        # S3 offsets (it queries _rm_stm, which has already discarded abort
        # state for those offsets during prefix truncation).  The read_committed
        # consumer then receives aborted records as if they were committed.
        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.redpanda,
            self.TOPIC_TX,
            msg_size=self.MSG_SIZE,
            use_transactions=True,
            loop=False,
        )

        try:
            consumer.start()
            consumer.wait(timeout_sec=180)
        finally:
            consumer.stop()
            consumer.free()

        status = consumer.consumer_status
        assert status.validator.invalid_reads == 0, (
            f"invalid_reads={status.validator.invalid_reads}: records from aborted "
            f"transactions appeared under read_committed isolation. "
            f"This indicates frontend::aborted_transactions returned empty for "
            f"pre-migration S3 offsets because _rm_stm discarded abort state "
            f"for those offsets during prefix truncation. "
            f"valid_reads={status.validator.valid_reads}, committed={total_committed}"
        )
        assert status.validator.valid_reads == total_committed, (
            f"valid_reads={status.validator.valid_reads} != committed={total_committed}: "
            f"some committed records were not returned under read_committed isolation"
        )
