- Feature Name: Dynamic STM membership
- Status: proposed
- Start Date: 2026-07-29
- Authors: Samuel Just

# RFC: Dynamic STM membership

## 1. Motivation

Currently, the STM set for an existing partition on startup is determined in
`state_machine_registry::make_builder_for` by `is_applicable_for(ntp_config)`
for each STM.

For most STMs, `is_applicable_for()` can't change after the topic is created.
Where membership would have to track mutable state, we simply have the STM
present but inert:
- `write_at_offset_stm`'s predicate is `is_shadow_link_enabled(ntp)` — topic
  *identity* ("linkable"), so it exists on every user topic whether or not a
  link does
- `archival_metadata_stm` and `translation_stm`'s `is_applicable_for()` depend
  on cluster feature enablement flags (`cloud_storage_enabled` and
  `iceberg_enabled`), allowing them to be created after enabling the flag upon
  broker restart. Otherwise, as long as the flags are present, all topics which
  could be configured to use them have them.

`is_applicable_for()` driven membership has two problems:

- STMs aren't instantiated until broker restart, and the point at which the STM
  is created isn't coordinated across a partition's replicas.
- Round trips (on→off→on) can diverge. Dropping the STM due to
  `is_applicable_for()` at restart doesn't actually purge the local snapshot or
  kvstore entries. Recreation would end up depending on whether the local
  replica happens to retain the previous stale snapshot or kvstore state.

The above presents a problem for tsv1 → tsv2/cloud migrations. We want to be
able to remove `log_eviction_stm` and `archival_metadata_stm` and add `ctp_stm`
at the cutover point. Moreover, we need the first two to stick around until
we're actually ready to transition — after alter-config switches the topic
mode, the existing machinery would drop them upon restart.

One solution considered would be to do as the above STMs do and simply modify
`is_applicable_for()` for those STMs to ensure they are always present, but it
would be better to drop them and avoid the extra in-memory state and snapshot
overhead.

This RFC aims to overhaul the STM membership machinery to allow dynamic
addition and removal of STMs via the partition raft log:
- `is_applicable_for()` selects the STM set only at partition creation.
  Afterwards, existence is derived from `initial_recovery_snapshot` (§4).
- Addition and removal of STMs is mediated through two new replicated control
  batches.
- Cluster-flag-driven creation (`archival_metadata_stm`, `translation_stm`,
  `rm_stm`) is now handled by a per-partition reconciliation loop.

## 2. Goals / non-goals

**Goals**

- STM membership changes are driven through raft rather than through
  `is_applicable_for()` on broker restart.
- Allow dynamically installing an STM on a running partition via raft batch
  with a specified initial seed.
- Allow dynamically removing an STM on a running partition via raft batch.
- Support flag-driven STM creation via the leader reconciler (§8).
- No cost on partitions that never change membership.

**Non-goals**

- Multiple instances of an STM type per partition.
- Pause / resume distinct from remove / install.

## 3. Key decisions

1. `initial_recovery_snapshot` (§4) determines STM existence on restart: the
   member set is its key set. Entries carry `op_offset` — the offset of the
   install batch that created the member, 0 for members from partition
   creation. `is_applicable_for()` is never re-evaluated after partition
   creation.
2. Membership changes are two reserved batch types applied by
   `state_machine_manager` (§5). Partitions that never see one pay nothing.
3. `initial_recovery_snapshot` also carries `remove_floor`: the offset of the
   latest `stm_remove`. `take_snapshot(T)` requires `T ≥ remove_floor` (§9C).
   It also carries `reconciled_through`: the last raft snapshot whose key-set
   reconcile completed (§9B).
4. STMs are identified by `stm_name()` — already the key of `_machines` and
   `managed_snapshot`.
5. Installs carry a `creation_seed`: a small opaque blob the STM type
   interprets to construct the initial state.
6. Construction is uniform: every member, installed or not, is built through
   its normal factory path. The seed is delivered to the fresh instance via a
   single entry point (`apply_creation_seed(iobuf)`), and installed members
   afterwards recover exactly like static ones.
7. Registration is atomic with respect to the trim floor (I2–I3).
8. Removal teardown is synchronous: stop, purge, then erase the
   `initial_recovery_snapshot` entries and raise `remove_floor` in one update,
   all before any member observes the batch. An interrupted teardown is redone
   by replaying the batch (§7) or by the hydration reconcile (§9B).
9. The leader (§8) handles cluster-flag-driven creation via control batch.
10. Cluster feature activation gates switching to the new restart behavior and
    enables the new dynamic STM batches. A replica seeing a new control batch
    assumes feature activation.

## 4. `initial_recovery_snapshot`

`initial_recovery_snapshot` (`stm_manager.snapshot` in the partition work
directory) maps STM name → initial next offset. At the end of every
`state_machine_manager::start()`, `apply_initial_recovery_policy` applies
recorded next offsets, emplaces entries for new machines per their recovery
policy (`read_everything` / `skip_to_end`), erases entries the builder did not
produce, and rewrites the file. It is deleted only with the partition
(`remove_local_state`) and moves with the partition directory across shards.

Post-activation the erase step is gone and `initial_recovery_snapshot`
determines the STM set (§9A). Entries in `initial_recovery_snapshot` gain an
`op_offset`: offset of the install point in the log. For an STM installed via
the installation control batch, this will be the offset of that batch. For an
STM created on partition creation, it will be 0. Old-format entries decode
as 0.

`initial_recovery_snapshot` also gains a `remove_floor`: the offset of the
most recent `stm_remove` batch, 0 if none (old-format decodes as 0). It gates
snapshot collection (§9C). A `reconciled_through` field records the offset of
the last raft snapshot whose key-set reconcile (§9B) completed, 0 if none
(old-format decodes as 0).

As before, not new: replicated state must never seed from a mutable topic
property.

## 5. Control batches

Two new `model::record_batch_type` values, both added to
`offset_translator_batch_types()` so they do not consume Kafka offsets:

```cpp
stm_install = 43, // install named STM(s) from log-carried creation seeds
stm_remove = 44,  // remove named STM(s)
```

Payloads are serde envelopes owned by `raft`:

```cpp
struct stm_install_bundle {
    // name -> creation_seed, consumed at apply; not retained
    absl::flat_hash_map<ss::sstring, iobuf> creation_seeds;
};
struct stm_remove_bundle { std::vector<ss::sstring> stm_names; };
```

A bundle may name several STMs for failure atomicity.  Application is per-STM
and can lag, so nothing may rely on two STMs observing a batch simultaneously.
For the following migration work, the cutover is accordingly a sequence:
install `ctp_stm`; cut over (a `partition_properties_stm` command, out of scope
here); remove the defunct tsv1 STMs.

A `creation_seed` is subject to the same size limits as any replicated batch.
Installable STM types must `supports_snapshot_at_offset()`: local eviction
takes as-of-`T` snapshots unconditionally, and the manager-wide flag folds in
every member's.

Both types are recognised in `batch_applicator`. A control batch **stops the
read at its offset**: enactment completes before any later batch is consumed.
Enactment runs only in this foreground read; a background catch-up fiber
reaching a control batch finds it already enacted and delivers it to its
member as a no-op.

## 6. Install

Applying an `stm_install` batch at offset `X`:

1. Decode the bundle, block on applying further batches until step 3 completes.
2. For each name absent from `_machines`:
   - construct via the normal factory path and `apply_creation_seed(blob)`
   - `remove_local_state()` to discard any previous instance's state
   - without suspending:
     - `set_next(X + 1)` — constructed from the seed, it already reflects the
       batch and must not apply it
     - insert into `_machines`
     - `stm_hookset::add_stm` (I2, I3)
     - Mark it snapshot-hydrated (an unhydrated member wedges
       `ensure_local_snapshot_exists`, and with it truncation) and fold its
       `supports_snapshot_at_offset` into the manager's flag.
3. Update `initial_recovery_snapshot` (§4): new members, `op_offset = X`.
4. Resume the read: deliver `X` to the remaining members (a no-op for them) and
   continue at `X + 1`.

A name already present no-ops only if its entry's `op_offset` equals `X` (a
replayed batch). Any other `op_offset` — 0, or an older install — is a
different instance: torn down and rebuilt from the seed. This is load-bearing:
install authors see only the leader's `initial_recovery_snapshot`, which is
per-replica, so a builder-created member can legitimately receive an install
(e.g. a cluster-flag race at partition creation left the member on some
replicas' builders and not others). A no-op would fork the instances —
`f(config)` plus full history on one replica, `f(seed)` from `X + 1` on
another — violating I5.

We must ensure no member durably records progress past `X` before the new
STMs are in `initial_recovery_snapshot`. Blocking application of `X` until
step 3 completes ensures that any local snapshot they write stays below it
until the newly created STMs are ready. A crash prior to step 3 leaves every
member's durable resume point `≤ X`, and the install batch is replayed;
after step 3, `initial_recovery_snapshot` reconstructs the new members.

An installed STM is observable via `get<T>()` only once the batch has applied.
Control batches keep the standard replicate-and-wait contract: the author
learns offset `X` and waits until the manager has applied through `X`
(`wait_for_stm_name(name, deadline)` when the offset is not at hand).

## 7. Remove

Applying an `stm_remove` batch at offset `Y`:

1. Block on applying further batches until step 3 completes.
2. Teardown each present member:
   - acquire its background-apply mutex and `stop()`. This closes its gate, so
     no in-flight apply fiber or background snapshot can persist state after
     the purge.
   - `stm_hookset::remove_stm`
   - erase from `_machines`
   - `remove_local_state()`

   The removed STM may not have applied every batch `< Y` (background
   catch-up). This could lead to an observable difference in behavior
   due to side effects, but it's not worth waiting for application to
   catch up.
3. `initial_recovery_snapshot`: erase member entries and set
   `remove_floor = Y`.
4. Resume applying batches.

Purging synchronously simplifies recovery (we'll replay the batch if we failed
partway) and avoids needing to worry about an unfinished removal during
installation. The purge precedes the `initial_recovery_snapshot` erase, so an
interrupted teardown leaves the entry behind allowing us to complete the purge
on replay.

Removable STMs must purge *all* their persistent state in `remove_local_state`.
The base clears only the snapshot backend, the type-specific handler needs
to ensure that kvstore and files are taken care of as well.

## 8. Config reconciliation

Current STMs break down into a few categories:

1. identity-only predicates. These STMs' `is_applicable_for()` implementations
   depend only on the topic identity. Their existence is fixed at partition
   creation.
   - `tm_stm`
   - `id_allocator_stm`
   - `group_tx_tracker_stm`
   - `transform_offsets_stm`
   - datalake `coordinator_stm`
   - `partition_properties_stm`
   - `write_at_offset_stm` — the predicate ("shadow-linkable") is topic
     identity, so it exists on every user topic, linked or not.
2. flag-enabled. These STMs also depend on cluster enablement flags. With this
   RFC, the leader will be responsible for installing them upon seeing the
   flag.
   - `archival_metadata_stm`
   - `translation_stm`
   - `rm_stm`
3. topic-config. These STMs' existence depends on topic configuration.
   Currently, the related configurations cannot change, but with the follow-up
   tsv1 to tsv2/cloud migration work, these will be dynamically reconfigured at
   cutover using the mechanisms introduced in this RFC.
   - `archival_metadata_stm`
   - `log_eviction_stm`
   - `ctp_stm` — installed at cutover

Category 2 is currently handled by re-evaluating `is_applicable_for()` on
each replica on restart (the flags in question are `needs_restart`). With
this change, that won't work anymore, so the leader will be responsible on
election for adding each of those three as needed by re-evaluating the
`is_applicable_for()` predicate. We're going to choose to ignore removal —
for `archival_metadata_stm` at least, it's not clear that dropping it is
actually desirable.

On-election re-evaluation is sufficient for the flags themselves: they are
`needs_restart`, so a change requires a rolling restart, and any leadership
that survives the roll was acquired after its broker's own restart. The one
gap is the upgrade roll that activates this RFC's feature: leaders elected
during that roll, before cluster-wide activation, ran the diff while
authoring was still gated, and nothing re-runs it until they next lose and
regain leadership. A one-shot sweep of current leaders on feature activation
(the `partition_mode` sync shape) closes it. It only matters for a flag
enabled during that same roll — anything older was already installed by the
pre-upgrade restart path — but that roll is exactly when an operator is
touching cluster config.

## 9. Recovery

**A. Restart.** The STM member set is simply the `initial_recovery_snapshot`'s
keys.  The builder runs only for a genuinely new raft group — signalled by
the creating caller, not inferred from local state.  An
`initial_recovery_snapshot` missing when the log or on-disk STM state is
non-empty indicates corrupt local state: fail the partition loudly. (A replica
added to an existing group therefore starts with no members and builds its set
from hydration and log replay alone.)

- Every member STM constructs through the normal factory path (§4). State comes
  from the first of these that applies:
  1. A local snapshot at `≥ op_offset`: `start()`, the ordinary path. Any
    snapshot older than `op_offset` belongs to a dead instance — delete it and
    fall through.
  2. `op_offset ≥` the raft start offset: apply the install batch at `op_offset`
    with `next = op_offset + 1`. For `op_offset = 0` this case never arises —
    there is no creating batch, and state legitimately begins empty at the recorded
    floor.
  3. Otherwise: the member's raft-snapshot portion.

  Cases 1 and 3 work as currently.  For a dynamically installed STM, 2 is how
  we replay the installation batch.  Local eviction forces member snapshots
  prior to trim, so only hydration can move the log start past `op_offset`
  without one — leaving the covering raft snapshot (3).
- A key with no registered factory: drop the partition with a loud error,
  similar to §11.
- A member whose removal was interrupted mid-teardown still has its entry (the
  purge precedes the erase): it reconstructs and the removal is redone by
  replay or by the reconcile below.

The `initial_recovery_snapshot` write must complete before applying subsequent
batches (§6, §7).  A crash before that point leaves the batch re-readable, and
every control batch past the recovered state replays idempotently. If the
local raft snapshot's offset exceeds `reconciled_through`, the key-set
reconcile (§9B) re-runs before the apply fibers start.

**B. Raft snapshot install.** `managed_snapshot`s taken under the conditions
of this RFC with the membership flag guarantee that:

1. they contain exactly the STM set present at the snapshot's offset
2. all subsequent STM additions will be represented by a control batch in
   the log

When installing such a snapshot:
- Construct any key-set names absent locally through the normal factory
  path and apply the snapshot portion with `next = last_included + 1`,
  and set `op_offset =` the snapshot's offset.  Fail the partition if
  there is no registered factory.
- Any local members absent from the key set must have been removed —
  purge local state.

Enactment ends as §6/§7's does: update `initial_recovery_snapshot` — entries
for created members, erasure for torn-down ones, `reconciled_through =` the
snapshot's offset — before any member may durably advance past it. A crash
beforehand leaves `reconciled_through` behind the on-disk raft snapshot;
restart re-runs the reconcile (§9A).

When installing a snapshot generated prior to this RFC (no membership flag), we
can't assume that local STMs missing from the key set have been removed — they
may have been created after that snapshot via `is_applicable_for()`.  Apply the
portions that exist, creating STMs as needed.  Don't remove local STMs that
aren't in the snapshot.  Created members record `op_offset =` the snapshot's
offset, as in the flagged path.

The per-member background snapshot apply (one lagging member catching up) never
reconciles membership; the full reconcile runs only on the manager-wide path,
and §9A re-runs it at restart when `reconciled_through` trails the local raft
snapshot.

**C. Taking a snapshot.** Restrict `take_snapshot(T)` to `T ≥ remove_floor`.
Otherwise, we risk trying to construct a snapshot at T of an STM we no longer
have state for.  Only include STMs with `op_offset ≤ T`.  Both checks run
under the apply mutexes after `wait(T)`, serialised against enactment; the
no-argument overload (`T = last_applied`) satisfies both trivially.  One
caller cannot retry above `remove_floor`: recovery's on-demand learner
snapshot, whose `T` is fixed at reconfiguration time — it falls back to a
full snapshot instead.

## 10. Correctness

**A control batch replays until its effect is durably recorded, and the log
cannot drop it before then.** The first half comes from §6/§7's rule that batch
application blocks until installation/removal is complete and durable.  Replay
is driven by member next offsets, so a crash before the
`initial_recovery_snapshot` write leaves every member's durable `next ≤ X` and
the batch re-read. A raft snapshot ≥ X will reflect the control batch.

Once the control batch has applied, local-eviction ≥ X requires a local
snapshot of a newly installed STM.  A hydration trim skips that, but is instead
covered by a raft snapshot. Thus, §9A's start-offset routing should ensure
that we always apply the control batch.

Supporting invariants:

**I1 — replay closure.** Everything control batch application consumes must be
reconstructible at any crash point. The seed always has a home: the member's
own local snapshot (a local-eviction trim past `op_offset` forces one first),
the install batch (in the log until a trim), or the member's portion in a
covering `managed_snapshot` (what a hydration trim leaves behind). One of the
three exists whenever `initial_recovery_snapshot` durably lists the member, and
§9A's start-offset routing selects the right one — so the seed needs no
separate retention.

**I2 — atomic registration.** No suspension point between constructing an
installed STM and registering it in both `_machines` and `stm_hookset`.
`stm_hookset::ensure_snapshot_exists` builds its continuation chain
synchronously over `_stms`, so an STM added later is invisible to an in-flight
truncation. Joining the hookset before `start()` is safe:
`ensure_local_snapshot_exists` awaits `wait_for_snapshot_hydrated()`, so an
early truncation waits rather than skipping the new member.

**I3 — no trim floor from unseeded state.** `max_removable_local_log_offset()`
is a min over hookset members, and overrides may derive it from STM state
(`ctp_stm`: `_state.get_max_collectible_offset()`). Constructing from the
creation seed before registration allows the implementation to safely set
`get_max_collectible_offset()`.

**I4 — idempotent application.** Install and remove may run any number of times
for the same batch (replay, restart) and must
converge: install skips members whose `op_offset` matches and never moves it,
purge deletes what exists, `remove_floor` is monotone.

**I5 — deterministic seed.** An installed STM's replicated state is a
function of the creation seed alone (decisions 5–6).

Existing manager APIs needing attention under changing membership:

- `wait(offset)` materialises one future per member at call time; `stop()`
  completes outstanding waiters with `abort_requested_exception`,
  indistinguishable from shutdown. Removal needs a distinct completion — the
  migration driver synchronises on this aggregate wait.
- `_machines` must not be mutated across a suspension point mid-iteration; copy
  to a vector first. `stop()` and `remove_local_state()` currently hold map
  iterators across `parallel_for_each`.
- `get<T>()` returns a `shared_ptr` with no membership-lifetime contract;
  callers must tolerate absence and not cache across suspension points.

## 11. Compatibility and upgrade

- **Cluster feature activation gates the new behavior.** Pre-activation behavior
  is exactly today's; because the erase keeps `initial_recovery_snapshot` equal
  to the builder's output, flipping authority at activation is a no-op at the
  switch instant, and a leader activation sweep converges anything a config
  change moved before or during the upgrade.
- **Activation is the point of no return.** Old binaries can't tolerate the new
  control batches due to offset translation: a pre-support binary replaying the
  control batches counts them as data offsets, shifting Kafka offset translation.
  It would also erase `initial_recovery_snapshot` entries and drop installed
  members. Before activation, no control batch or new-format
  `initial_recovery_snapshot` exists, so every pre-activation artifact is
  old-binary-compatible and rollback before activation is clean.
- **The gate reads the locally-known feature table**, which trails
  the cluster on a node that was down across activation. Safe because:
  - control batch application is never gated
  - replay and hydration rebuild members regardless
  - on startup, behavior is determined by the format of `initial_recovery_snapshot`
    rather than by the local feature flag.  The replica may, technically,
    have seen control batches but not the feature flag.
  - old-format partitions get today's semantics from such a node, including
    the erase after a flag flip — dev-existing skew, converged by controller
    catch-up and hydration.
- An install naming an STM with no factory (or no `apply_creation_seed`) is
  invalid. Take the partition out of service with a loud error.
- Bundles, `initial_recovery_snapshot`, and `managed_snapshot` are versioned
  serde envelopes. `managed_snapshot` gains a membership flag, set on every
  snapshot written once activation is locally known — both `take_snapshot`
  overloads satisfy §9C's collection rules. This is the marker §9B requires
  before a key set may drive teardown. `initial_recovery_snapshot`'s `op_offset` and `remove_floor`
  fields appear only post-activation, with `compat_version` bumped so a
  rolled-back binary fails loudly rather than silently stripping them.
- STM names gain one more reader; they already key `managed_snapshot`
  portions, snapshot files, and kvstore entries.

## 12. Testing

Existing dynamic-membership tests use non-snapshotable test STMs that never
join `stm_hookset`, leaving the hookset and purge paths untested. The suite
needs real `persisted_stm`-derived test STMs, and:

1. Post-activation, predicate flips false → restart → member still constructed,
   resumes, gates truncation; `initial_recovery_snapshot` stable across further
   restarts.
2. Post-activation, predicate flips true after creation → restart → not added;
   the reconciler, not the builder, installs it.
3. Install: absent STM appears on every replica, seeded, applying only offsets
   `> X`; replaying the install batch is a no-op.
4. Restart before the `initial_recovery_snapshot` write: batch replays,
   everything rebuilt from the log.
5. Restart after the `initial_recovery_snapshot` write but before the member
   snapshots: member reconstructed via the targeted read of the install batch
   at `op_offset` (§9A).
6. Install → remove → re-install of a name: the second instance starts from the
   new seed, not the old local snapshot.
7. Remove of a static member survives restart: `initial_recovery_snapshot`
   entry gone, a flagged snapshot's key set does not re-add it.
8. Crash mid-teardown: restart reconstructs the member and redoes the removal
   (replay, or the reconcile for a hydrated replica).
9. Truncation pressure concurrent with an install: the new member loses no
   offsets (I1–I3); a local-eviction trim past a control batch forces the
   affected members' local snapshots.
10. Raft snapshot install onto a node that never saw the control batches; onto
    one whose incoming key set lacks a current member (static included) —
    teardown; a crash after the install but before members write local
    snapshots — restart recovers via `initial_recovery_snapshot`; a crash
    before the `initial_recovery_snapshot` update — `reconciled_through`
    re-runs the reconcile at restart.
11. `take_snapshot(T)` between two membership events yields the membership as
    of `T`.
12. `take_snapshot(T)` with `T < remove_floor` fails; truncation retries above
    it. An install enacted between `wait(T)` and portion collection is
    excluded — portions match membership at `T`; a remove enacted there fails
    the snapshot. The fixed-`T` learner on-demand snapshot falls back to a
    full snapshot instead of stalling.
13. `wait()` across an install batch and across a remove batch, including a
    wait outstanding when the removal lands.
14. Completion contract: waiting on a control batch's offset unblocks only
    after enactment; `get<T>()` succeeds immediately afterwards.
15. Install on a partition that starts with no STMs at all.
16. Reconciler: enabling a gated flag installs the STM on pre-existing
    partitions via the leader; a re-sweep is a no-op; followers never author.
17. An `initial_recovery_snapshot` entry naming an unknown factory takes the
    loud-failure path.
18. Duplicate install of a present name (author retry): `op_offset` does
    not move; replicas converge.
19. Crash injected between each §6/§7 enactment sub-step: the batch is re-read
    and enactment converges.
20. Hydrating a pre-activation snapshot (no membership flag): portions apply by
    name, key-set members absent locally are created, nothing is torn down.
21. Replica added to an existing group: no builder output; membership converges
    via hydration, and via full log replay the install batch's seed is
    consumed.
22. Missing or unreadable `initial_recovery_snapshot` with a non-empty log: the
    partition fails loudly instead of running the builder.

## 13. Implementation outline

Each phase is one commit.

1. `state_machine_factory::create` → `stm_name()` + `make_initial_stm(consensus*, cfg)`;
   builder gains `add_machine`. Mechanical across ~15 factories, pure refactor.
2. Registry records a per-name factory closure owning a copy of the topic
   config. Keeps `topic_configuration` out of a widely-included header; carries
   the §4 hazard.
3. `initial_recovery_snapshot`-sustained derivation with `op_offset` entries
   (§9A) behind the feature gate; the erase and predicate authority remain
   pre-activation; loud missing-factory path.
4. `apply_creation_seed` on installable types (seed → replicated state);
   implement for `ctp_stm` with a round-trip test. Default must fail loudly.
5. The two batch types + bundle envelopes + `offset_translator_batch_types()`.
6. Install: interception, §6, I1–I3; the replicate-and-wait contract. A
   zero-member manager still runs its apply loop (added replicas start
   empty).
7. Remove: synchronous teardown and purge, entry erase + `remove_floor`;
   `stm_hookset::remove_stm`.
8. Recovery: `op_offset`-driven derivation on `start()` including the targeted
   install-batch read; key-set reconcile on raft-snapshot install; as-of-`T`
   portion collection with the membership flag and `remove_floor` gate.
9. Removal breaks outstanding `wait()`s; `_machines` iteration audited for
   suspension points.
10. Config reconciler (§8): leader-resident diff-and-author, per-STM rules,
    feature declaration + activation sweep; `apply_creation_seed` for the
    STMs it authors.
11. Test suite per §12. Phases 6–8, 10 land with happy-path tests; this adds
    crash, truncation, and reconcile cases.
