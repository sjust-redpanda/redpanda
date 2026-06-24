"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'proto/redpanda/core/admin/v2/cloud_topic_migration.proto')
_sym_db = _symbol_database.Default()
from ......proto.redpanda.core.pbgen import options_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_options__pb2
from ......proto.redpanda.core.pbgen import rpc_pb2 as proto_dot_redpanda_dot_core_dot_pbgen_dot_rpc__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n8proto/redpanda/core/admin/v2/cloud_topic_migration.proto\x12\x16redpanda.core.admin.v2\x1a\'proto/redpanda/core/pbgen/options.proto\x1a#proto/redpanda/core/pbgen/rpc.proto"K\n\x1dReclaimMigratedBackingRequest\x12\r\n\x05topic\x18\x01 \x01(\t\x12\x1b\n\x13delete_unreferenced\x18\x02 \x01(\x08"\xc6\x01\n\x10PartitionReclaim\x12\x11\n\tpartition\x18\x01 \x01(\x05\x12\x14\n\x0cstart_offset\x18\x02 \x01(\x03\x12\x18\n\x10segments_scanned\x18\x03 \x01(\x03\x12\x1b\n\x13segments_referenced\x18\x04 \x01(\x03\x12\x1d\n\x15segments_unreferenced\x18\x05 \x01(\x03\x12\x17\n\x0fobjects_deleted\x18\x06 \x01(\x03\x12\x1a\n\x12sample_object_keys\x18\x07 \x03(\t"o\n\x1eReclaimMigratedBackingResponse\x12\x0f\n\x07deleted\x18\x01 \x01(\x08\x12<\n\npartitions\x18\x02 \x03(\x0b2(.redpanda.core.admin.v2.PartitionReclaim2\xae\x01\n\x1aCloudTopicMigrationService\x12\x8f\x01\n\x16ReclaimMigratedBacking\x125.redpanda.core.admin.v2.ReclaimMigratedBackingRequest\x1a6.redpanda.core.admin.v2.ReclaimMigratedBackingResponse"\x06\xea\x92\x19\x02\x10\x03B\x10\xea\x92\x19\x0cproto::adminb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'proto.redpanda.core.admin.v2.cloud_topic_migration_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'\xea\x92\x19\x0cproto::admin'
    _globals['_CLOUDTOPICMIGRATIONSERVICE'].methods_by_name['ReclaimMigratedBacking']._loaded_options = None
    _globals['_CLOUDTOPICMIGRATIONSERVICE'].methods_by_name['ReclaimMigratedBacking']._serialized_options = b'\xea\x92\x19\x02\x10\x03'
    _globals['_RECLAIMMIGRATEDBACKINGREQUEST']._serialized_start = 162
    _globals['_RECLAIMMIGRATEDBACKINGREQUEST']._serialized_end = 237
    _globals['_PARTITIONRECLAIM']._serialized_start = 240
    _globals['_PARTITIONRECLAIM']._serialized_end = 438
    _globals['_RECLAIMMIGRATEDBACKINGRESPONSE']._serialized_start = 440
    _globals['_RECLAIMMIGRATEDBACKINGRESPONSE']._serialized_end = 551
    _globals['_CLOUDTOPICMIGRATIONSERVICE']._serialized_start = 554
    _globals['_CLOUDTOPICMIGRATIONSERVICE']._serialized_end = 728