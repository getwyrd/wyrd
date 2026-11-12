//! `UploadPart`'s **commit half** (#1027, the first slice of the part commit; staging is
//! [`super::stage`]): turn a staged attempt into a committed part, or into a compensated stop, and
//! compose the whole verb, reserve → stage → commit ([`upload_part`]). Proposal 0016's *Part commit*
//! and *Live-session compensation* rows (`0016:660`, `:673`), the post-staging refusal rule
//! (`0016:1420-1431`) and the quiesce rule (`0016:1720-1733`). A commit or a compensation
//! answered `Conflict` is classified from a re-read (#1028, the second slice; `0016:1584-1653`).
//! A commit answered `CommitUnknownResult` sends nothing more and is settled from reads alone
//! (#1029, the third slice; `0016:1630-1651`), and so is a compensation answered unknown (#1030,
//! the fourth).
//!
//! | Batch | Preconditions | Mutations |
//! |---|---|---|
//! | part commit | `require(mpu:<id> == Open@E)`, `require_absent(part:<id>:<n>)` or `require(part:<id>:<n> == prior)`, `require(slot:<id>:<k> == the bytes this attempt last wrote)`, and for a re-upload over a non-empty prior `require_absent(retire:bytes:s:<id>:<E>:<n>:<attempt>)` | put `part:`, put its `psum:` summary, delete the slot, delete every owned `sidx:` entry, and for that re-upload put the `{chunks}` obligation naming the **prior** part's chunks |
//! | compensation | `require(mpu:<id> == Open@E)`, where `E` is the epoch it compensates under (the commit's, or the newer `Open` epoch a rollback left), `require(slot:<id>:<k> == the bytes this attempt last wrote)`, and for a non-empty part `require_absent` of its obligation key | delete the slot, delete every owned `sidx:` entry, and for a non-empty part put the `{chunks}` obligation naming **this attempt's own** chunks, keyed under `E` |
//!
//! An obligation names at least one chunk (`RetireObligation` refuses an empty list), so an empty
//! part, or a re-upload over an empty prior, installs none, and its slot release and owned-entry
//! deletes ride in the same batch all the same.
//!
//! # Quiesce, then commit
//!
//! A part past a ceiling is refused first. Otherwise the attempt's last renewal
//! ([`StagedAttempt::renew`]) runs, with a lease covering [`DISPATCH_MARGIN_MILLIS`]; no renewal is
//! sent after it, so the only writer of the slot until the commit lands is the commit itself
//! (`0016:1720-1733`). Then two reads: the bounded `psum:<id>:` range, for the cumulative ceiling,
//! and the part key. Then one batch.
//!
//! The renewal can adopt a session it finds at other `Open` bytes: that is how a rollback before it
//! reaches the commit, which then runs under the live epoch. But it does not judge the epoch, so
//! every arm that would go on to send a batch compares the epochs here: an adopted record at a lower
//! epoch, or under other bytes at the same epoch, is damage ([`RecordError::SessionEpochNotRisen`]),
//! and no batch is sent under it. An arm that sends nothing keeps what the renewal said, its doubt
//! included.
//!
//! # What the lease check does and does not promise
//!
//! A renewal guarantees its cover only at the moment it returns, and the reads and each earlier
//! round spend from it. So the clock is read before every commit round, the first included, and a
//! round is sent only with [`DISPATCH_MARGIN_MILLIS`] left on the lease. The attempt never renews
//! again to buy time: that would rewrite the slot the commit requires.
//!
//! That is a promise about dispatch, and only about dispatch. The margin is `E_tx`'s five seconds,
//! not a deadline on `commit`: the store promises termination, not five seconds, and a commit
//! abandoned at a deadline may still apply (`crates/traits/src/lib.rs`, "Operational envelope"). So
//! no check here can stop a sent batch applying after its lease ran out. Safety rests on the
//! `require`s instead, since the store enforces bytes, never expiry (`0016:1851-1852`): the commit
//! requires `mpu == Open@E` and its own slot's bytes, and the idle fence requires the same session
//! bytes and pins that slot (`0016:1859-1863`), so whichever lands first makes the other fail. A
//! lapsed lease is therefore never evidence about whether a commit landed.
//!
//! # A commit that lost
//!
//! The store answers `Conflict` without naming the precondition that failed, so a commit answered
//! `Conflict` is never compensated blind (`0016:1792`): it is classified from a re-read
//! (`0016:1597-1647`). Each round reads, in this order, the part key, the session, the attempt's
//! own slot, then the session again, each read its own transaction. A commit answered unknown
//! makes the same four reads (*A commit in doubt*, below). Here, with nothing in doubt, only the
//! last session read decides the answer:
//!
//! | The session (last read) | The part key | Answer |
//! |---|---|---|
//! | still `Open@E` | what the commit requires | a conflict no read explains (on TiKV, a lock another writer held): the same batch again |
//! | still `Open@E` | changed: a same-number rival won, or a repair rewrote the prior | compensate under `Open@E`; [`Refusal::OperationAborted`] |
//! | back at a newer `Open@E'` | any | compensate under `Open@E'`; [`Refusal::OperationAborted`] |
//! | not `Open`, at a higher epoch (a fence) | any | [`Refusal::NoSuchUpload`], nothing written: the residue is the reaper's, whose walk covers every non-`Open` state |
//! | gone, and a slot read made after it finds the key absent | any | [`Refusal::NoSuchUpload`]: a terminal delete took the session and the slot in one batch (`0016:674`) |
//! | gone, and that later slot read finds any record there, this attempt's or another's | any | [`Stop::Record`] ([`RecordError::SessionRecordMissing`]): damage |
//!
//! The reaper does not walk an `Open` session (`0016:1600-1620`), so a commit that lost to a live
//! session compensates: nothing else would free its slot before `W_session`. That covers the
//! Complete that fenced the session and rolled it back between the last renewal and the commit
//! (`0016:1607-1620`): the session is `Open` again, at a newer epoch, and an `Open` session is
//! never answered `NoSuchUpload`. A repair that rewrote the prior is answered like a rival, with
//! no retry against the new record.
//!
//! A re-upload over a non-empty prior also requires its obligation key absent, and no read of a
//! round looks there. So when rounds that no read explained come to an end, spent or stopped by
//! the lease, the attempt reads that key before it compensates: a record there is
//! [`Stop::ObligationKeyTaken`], with no compensation, since the key names this attempt
//! (`0016:364`) and none of its batches landed. Without that read an empty re-upload, whose
//! compensation installs no obligation and so does not require that key, would land its
//! compensation and report the rounds spent, the damage unreported.
//!
//! A session read is evidence only once it is judged. It has **moved** when the record decodes at
//! a higher epoch than the bytes the batch required, `Open` (a rollback) or not (a fence): every
//! fence and every rollback bumps the epoch (`0016:364`, `:704-708`). Bytes that do not decode,
//! and a record under other bytes at the same or a lower epoch, whatever its state
//! ([`RecordError::SessionEpochNotRisen`]), are damage (ADR-0045), never a rollback or a fence.
//! At the round's last read damage stops the attempt with [`Stop::Record`]. The round's first
//! session read is passed over, damage and all: with nothing in doubt it decides nothing. After an
//! unknown outcome it can only show that the commit cannot land any more (*A commit in doubt*).
//!
//! A missing session record shows a terminal delete only together with the slot key found absent
//! by a read made **after** it. A slot read made before the read that found the change proves
//! nothing, since the delete can land between the two, so a round whose last session read finds
//! no record reads the slot key once more. That read tests the key itself: a terminal delete
//! removes every surviving slot in its one batch, and a reservation requires the session's `Open`
//! bytes ([`super::stage::reserve_slot`]), so any record there behind a missing session is damage,
//! another attempt's as much as this one's.
//!
//! The rest of the damage a round can find stops the attempt the same way, with no compensation:
//! bytes at the part key that do not decode, whatever the session (a rival's record must decode,
//! ADR-0045); and a prior part record gone while the session is still `Open@E`
//! ([`RecordError::PartRecordMissing`]), since a `part:` record's only remover is the retirement
//! drain, which reaches it only once the session has left `Open` (`0016:351`). Under a session that
//! moved, the moved session's row answers instead.
//!
//! A slot found **gone** is absent or holds another attempt's `attempt_id` (the index was released
//! and reserved again); one under this attempt's id at bytes it did not last write is
//! **rewritten**. With nothing in doubt neither has a cause while the session is live: only this
//! attempt writes its own slot, it has quiesced, and none of its batches landed. So a slot gone
//! under an `Open` session, and a slot rewritten under any session record, is [`Stop::SlotLost`],
//! claiming no release. A fenced session answers its refusal whatever else its slot holds, and a
//! missing one has its own two rows.
//!
//! # A commit in doubt
//!
//! A commit answered [`CommitUnknownResult`] may have landed, or may still land, so it is never
//! read as `Conflict` (`0016:1630-1651`). Once a round is answered unknown **the attempt sends no
//! further batch**: not the commit again, and not a compensation. Nothing a read can see tells
//! "did not land" from "landed, then rebuilt". The commit deletes the slot, and a late reserve,
//! intent and renewal of this attempt's own staging can then land and put back the very bytes the
//! commit required: every reserve round puts the same bytes, a held reserve round pins its index,
//! and retry rounds at one reading stamp the same value ([`super::stage`]). A same-number rival
//! over an empty prior writes that prior's bytes again, since a `PartRecord` names no attempt. So
//! a compensation could orphan-mark the chunks of a part that landed (`0016:1792`), and the same
//! batch sent again could land a second time, publishing chunks a rival's obligation already owes:
//! a [`WriteBatch`] is not guaranteed idempotent (`crates/traits/src/lib.rs`,
//! `CommitUnknownResult`).
//!
//! The attempt reads instead, in laps of the same four reads in the same order (the part key, the
//! session, its own slot, the session again), at most [`SETTLE_LAPS`] of them, and the clock plays
//! no part. A lap whose slot read finds the slot gone, under a commit that required the part key
//! absent, reads that key once more before its last session read.
//!
//! * **This attempt's own commit, checked first.** The slot read finds the slot gone (absent, or
//!   holding another attempt's `attempt_id`), and the session read made **after** it still finds
//!   the `Open@E` bytes the commit required: `Committed`. While the session is `Open@E` only this
//!   attempt's own commit and its own compensation delete its slot (no reaper touches an `Open`
//!   session, `0016:1625-1628`, and no staging batch deletes a slot), and it sent no compensation;
//!   once that index is free another attempt may reserve it, so another `attempt_id` there is the
//!   same proof. A session read made before the slot read proves nothing, since a teardown can take
//!   the slot between the two. The part's bytes prove nothing either: a `PartRecord` names no
//!   attempt, and an empty part names no chunk.
//! * **But no record at the part key disproves it.** A slot erased by damage looks the same as a
//!   released one. So a commit that required the part key absent also needs a record at that key
//!   when it is read again **after** that slot read and **before** that last session read. A
//!   landed commit left its record there, and a `part:` record's only remover is the retirement
//!   drain, once the session has left `Open` (`0016:351`); with `Open@E` read after it, no drain
//!   ran in between. So the key found absent there means no commit of this attempt landed: the
//!   doubt kept, with [`Stop::SlotLost`]. Any record there is a record, this attempt's or a
//!   rival's. Bytes there that do not decode are no record (ADR-0045) and prove nothing either way:
//!   the doubt kept, with [`Stop::Record`], the flag as it stood. A part read made before the slot
//!   read disproves nothing, since the commit may land between the two. This check only ever turns
//!   a `Committed` into the doubt.
//! * **Two limits of the proof, each reached only through damage**, a slot key erased while the
//!   session is `Open@E`, which no legal writer does. Both answer `Committed`, since no read can
//!   tell them from a commit of this attempt that landed. A re-upload's commit required the
//!   prior's bytes, which a rival or a repair can leave too, so for it the slot alone answers. And
//!   a same-number rival's record found at the part key is a record: a first upload whose commit
//!   never landed, a rival's first upload landing in its place and its slot then erased, reads
//!   exactly like its commit landing and then being replaced by that rival's re-upload, since a
//!   `PartRecord` names no attempt, and the obligation a re-upload installs is keyed under the
//!   rival's attempt, which this one cannot name (and over an empty prior there is none). In both
//!   no chunk is left unnamed: the commit never landed, so this attempt's owned `sidx:` entries
//!   still name its chunks, and a Complete checks the digest it names against the record that
//!   stands ([`super::complete::fence_complete`]).
//! * **Anything else keeps the doubt.** A slot still held at the bytes this attempt last wrote
//!   proves nothing, since late staging rebuilds them. With the slot held and the session
//!   unchanged the attempt reads one more lap, while the round may still land and laps are left:
//!   the round may have landed after the lap's slot read. Every other finding ends the reads.
//!
//! | What a lap found | Answer |
//! |---|---|
//! | the slot gone, the last session read still `Open@E`, and, for a commit that required the part key absent, a record at that key read between the two | `Committed` |
//! | the slot gone, the last session read still `Open@E`, and the part key the commit required absent found absent between the two | [`CommitError::InDoubt`], [`Stop::SlotLost`] |
//! | the slot gone, the last session read still `Open@E`, and bytes that do not decode at the part key the commit required absent, read between the two | [`CommitError::InDoubt`], [`Stop::Record`] |
//! | the slot held, the session still `Open@E` | another lap while one is left and the round may still land; then [`CommitError::InDoubt`], [`Stop::OutcomeUnknown`] |
//! | the session moved, the slot held or gone | [`CommitError::InDoubt`], [`Stop::SessionMoved`]: a teardown removes slots too, and an epoch only rises, so no later read can prove the commit |
//! | the session gone, and the slot key, read once more, absent | [`CommitError::InDoubt`], [`Stop::SessionMoved`]: the terminal delete took both, perhaps after the commit landed |
//! | the session gone, and any record at the slot key read once more | [`CommitError::InDoubt`], [`Stop::Record`] ([`RecordError::SessionRecordMissing`]) |
//! | the slot rewritten under this attempt's id | [`CommitError::InDoubt`], [`Stop::SlotLost`] |
//! | bytes that do not decode at the part key, the slot or the session; a session at an epoch that has not risen; a prior part record gone under `Open@E`; a read that fails | [`CommitError::InDoubt`], [`Stop::Record`] or [`Stop::Store`] |
//!
//! [`InDoubt::may_still_commit`] starts at the store's report and only ever falls: a round cannot
//! land later once a read found the session moved, or deleted with its slots, or found a record at
//! a part key the commit required absent (a `part:` record goes only after the session leaves
//! `Open`, `0016:351`). Each read is weighed as it returns, before the next is made, so a read that
//! fails later keeps what the earlier reads showed. A changed **prior** settles nothing: an empty rival writes an empty prior's
//! bytes again, and staged reconstruction can move a part's placement away and back while the
//! session is `Open@E` (`crates/custodian/src/reconstruction/staged.rs`, `0016:671`). Damage
//! settles nothing either, so a round that may still land keeps the flag however the damage is
//! later repaired, and a lapsed lease is no evidence either way (*What the lease check does and
//! does not promise*).
//!
//! The cost is availability, which 0016's refutation standard allows (`0016:2811-2813`): a commit
//! that did not land, or that landed and was then replaced while late staging rebuilt its slot, is
//! answered in doubt rather than `Committed`. Its slot stays held until the session ends, and the
//! client sends the part again as a new attempt.
//!
//! # The compensation, and the epoch chase
//!
//! A compensation answered `Conflict` is classified the same way, from a re-read in this order:
//! its obligation key (for a non-empty part), the attempt's own slot, then the session.
//!
//! | What the re-read found | Then |
//! |---|---|
//! | any record at the obligation key | [`Stop::ObligationKeyTaken`]: the key names this attempt (`0016:364`), and no batch of it put that record; the record is kept |
//! | the session at a newer `Open` epoch, the slot held | **the epoch chase**: the same compensation under that epoch, its obligation keyed under it (`0016:1607-1620`) |
//! | the session unchanged, the slot held | a conflict no read explains: the same batch again |
//! | the session fenced | its refusal, with no further write; the residue is the reaper's |
//! | the session gone | the slot key read once more, as for the commit: [`Refusal::NoSuchUpload`] or [`RecordError::SessionRecordMissing`] |
//! | a slot lost or rewritten, or damage | [`Stop::SlotLost`], [`Stop::Record`] |
//!
//! Every answer but the landed one is the compensation's own: a session it found fenced or deleted
//! is the refusal, whatever stopped the commit, and a stop of the compensation itself is its own
//! typed error and claims no release.
//!
//! # A compensation in doubt
//!
//! A compensation answered [`CommitUnknownResult`] is settled by the rule a commit in doubt
//! follows: it is never read as `Conflict`, and **the attempt sends no further batch**, not the
//! compensation again and nothing else. Nothing a read can see proves that a compensation did
//! **not** land. Its slot found held at the bytes it required proves nothing, since late staging
//! of this attempt rebuilds them after the compensation deleted them, just as after a landed
//! commit (*A commit in doubt*). Its obligation key found absent proves nothing either: the
//! retirement drain deletes a drained obligation whatever the session's state. And an empty
//! part's compensation, sent again after a landing that late staging then hid, would land twice.
//!
//! So the attempt reads, in laps of its re-read's reads in the same order (its obligation key, for
//! a non-empty part, its own slot, then the session), at most [`SETTLE_LAPS`] of them, then, for a
//! non-empty part, its obligation key once more. The clock plays no part. It takes the
//! compensation as landed, and gives the answer it would give had the store answered `Committed`,
//! only on evidence that no other batch can produce:
//!
//! * **A non-empty part: its own obligation, at the exact bytes the round put**, under the epoch
//!   that round required. The key names this attempt and that epoch (`0016:358-366`), and no
//!   staging batch writes a `retire:` key. The one other batch of the attempt with a key of this
//!   form is a re-upload's commit, which puts the prior's chunks and cannot land once a
//!   compensation is sent: every commit sent before it was answered `Conflict`. A session that
//!   moved does not erase an obligation, so the key is read again after the other reads.
//! * **An empty part: its slot found gone, and a session read made after that slot read still the
//!   bytes the round required.** While the session holds those bytes, only this attempt's commit
//!   and its compensation remove its slot, and no commit of it can land. A slot gone under a moved
//!   session proves nothing, since a teardown of a fenced session removes slots too.
//!
//! A non-empty part never settles on its slot alone: a slot erased by damage looks the same as a
//! released one, and the obligation is what tells them apart. An empty part's slot erased by
//! damage does look like a landed compensation; answering it so promises nothing, since an empty
//! part owes no chunk, and no legal writer erases a slot. That is a limit of the proof, reached
//! only through damage, as the own-commit proof's two are (*A commit in doubt*).
//!
//! Anything else keeps the doubt: [`CommitError::InDoubt`] for [`DoubtfulBatch::Compensation`],
//! whose cause is what ended the reads. Another lap is read only while the slot is held, the
//! session unchanged and the round may still land. [`InDoubt::may_still_commit`] falls by the rule
//! a commit's follows: once a read finds the session at a higher epoch than the round required,
//! `Open` or not, or finds no session record and then a slot read made after that read finds the
//! slot key absent (the terminal delete). Damage settles nothing, so the flag stays: bytes that do
//! not decode, a missing session record with a record still at the slot key, and other bytes at the
//! obligation key, which the round required absent but which are not there for good (the drain
//! and a repair both remove them). A fault or damage ends the reads, keeping what the reads before
//! it showed.
//!
//! | What the reads found | Answer |
//! |---|---|
//! | a non-empty part's obligation at the bytes the round put, at either read of it | the landed answer |
//! | an empty part's slot gone, the session read after it still the bytes the round required | the landed answer |
//! | other bytes at the obligation key | [`CommitError::InDoubt`], [`Stop::ObligationKeyTaken`], the flag kept |
//! | the session moved | [`CommitError::InDoubt`], [`Stop::SessionMoved`] |
//! | the session gone, and the slot key, read once more, absent | [`CommitError::InDoubt`], [`Stop::SessionMoved`] |
//! | the session gone, and any record at the slot key read once more | [`CommitError::InDoubt`], [`Stop::Record`] ([`RecordError::SessionRecordMissing`]) |
//! | the slot rewritten under this attempt's id | [`CommitError::InDoubt`], [`Stop::SlotLost`] |
//! | the slot held under the unchanged session, or a non-empty part's slot gone under it with no obligation found | [`CommitError::InDoubt`], [`Stop::OutcomeUnknown`] |
//! | bytes that do not decode at the slot or the session, a session at an epoch that has not risen, a read that fails | [`CommitError::InDoubt`], [`Stop::Record`] or [`Stop::Store`] |
//!
//! The cost is availability, which 0016's refutation standard allows (`0016:2811-2813`): a
//! compensation that did not land, or that landed and was then hidden (its obligation drained, or
//! an empty part's slot rebuilt), is answered in doubt, and its slot stays held until the session
//! ends.
//!
//! # The stops
//!
//! The reaper does not walk an `Open` session (`0016:1600-1620`), so a stop that left an attempt's
//! slot held under a live session would keep it until `W_session`, and repeated stops would fill
//! `MAX_INFLIGHT_PARTS` (`0016:1420-1431`). So a settled stop with no commit able to land
//! compensates:
//!
//! | The stop | Action |
//! |---|---|
//! | the quiesce renewal stopped by `CoverShort`, `RenewalRoundsSpent`, `LeaseLapsed` or `InvalidRequest` | compensate; the answer keeps the renewal's error |
//! | the lease short before a commit round | compensate; [`Stop::LeaseShort`] |
//! | over the per-part or the cumulative ceiling | compensate; [`Refusal::EntityTooLarge`] |
//! | a commit that lost to a live session | compensate; [`Refusal::OperationAborted`] |
//! | the commit rounds spent, every one answered `Conflict` with nothing a read explains | compensate; [`Stop::CommitRoundsSpent`] |
//! | the commit rounds spent, or the lease short before a later round, when a re-upload's commit's obligation key holds a record | no compensation; [`Stop::ObligationKeyTaken`] |
//! | the quiesce renewal in doubt | keep the doubt; no compensation |
//! | `SlotLost`, and staging's `EntryLost` | no compensation: the attempt cannot show the records are its own |
//! | the session has left `Open`, or was deleted with its slots | answer the refusal; the residue is the reaper's |
//! | a store fault or a damaged record | no compensation; the residue stays, as after a crash (`0016:1778`) |
//! | a sent commit answered unknown | no further batch and no compensation: the reads of *A commit in doubt*, then `Committed` on the own-commit proof, or else keep the doubt ([`CommitError::InDoubt`], its cause what ended the reads) |
//! | a compensation answered unknown | no further batch: the reads of *A compensation in doubt*, then the stop's own answer, the slot released, on the compensation's proof, or else keep the doubt ([`CommitError::InDoubt`], claiming no release, its cause what ended the reads) |
//! | a compensation's own stop: its rounds spent, its obligation key taken, the slot lost | that stop, claiming no release |
//!
//! An owned entry lost after staging has no stop of its own: the renewal requires that entry,
//! conflicts on every round and ends `RenewalRoundsSpent`, which compensates like any other.
//!
//! A lapsed lease does not block a compensation: renewal refuses to revive a lapsed lease
//! (`0016:1845`), but the compensation revives nothing and lands only if the session and the slot
//! are unchanged (`0016:1651-1653`). The caller gets the original stop's typed error, in its own
//! error class, and [`CommitError::slot_released`] says whether the slot was released.
//!
//! # Bounds
//!
//! | Loop | At most |
//! |---|---|
//! | the quiesce renewal | [`super::stage::RENEWAL_ROUNDS`] commits |
//! | the part commit, its retries included | [`COMMIT_ROUNDS`] commits, then [`Stop::CommitRoundsSpent`]; a round answered unknown is the last |
//! | the reads after a commit answered unknown | [`SETTLE_LAPS`] laps of four reads (five in a lap that reads the part key again), no batch |
//! | the compensation, the epoch chase included | [`COMPENSATION_ROUNDS`] commits, then [`Stop::CompensationRoundsSpent`]; a round answered unknown is the last |
//! | the reads after a compensation answered unknown | [`SETTLE_LAPS`] laps of three reads (two for an empty part), no batch |
//!
//! A commit round answered `Conflict` is followed by at most five reads: its four, then either the
//! slot key once more behind a missing session or, before the compensation that follows rounds no
//! read explained, a re-upload's obligation key. A commit round answered unknown is followed by at
//! most [`SETTLE_LAPS`] laps of four reads, a lap that finds the slot gone reading the part key once
//! more, and then the slot key once more behind a missing session. A compensation round answered
//! `Conflict` is followed by at most four reads. One answered unknown is followed by at most
//! [`SETTLE_LAPS`] laps of three reads (two for an empty part), then the slot key once more behind
//! a missing session and, for a non-empty part, its obligation key once more. Each store call is
//! bounded by the store's own contract (every operation terminates, `crates/traits/src/lib.rs`,
//! "Operational envelope").
//!
//! # Every batch requires the slot bytes this attempt last wrote
//!
//! Both builders take the slot bytes from the attempt's own state ([`StagedAttempt::slot_bytes`],
//! which its last renewal updated), never from a read of the key. So a slot index another attempt
//! has since reserved is never required or deleted by this one.
//!
//! # The cumulative ceiling is a read, not a fence
//!
//! The `psum:` read is unfenced on purpose: concurrent commits of different part numbers may both
//! read the same headroom and both commit, and the overshoot that leaves is bounded by the in-flight
//! cap, `MAX_INFLIGHT_PARTS × MAX_PART_CHUNKS`, and charged in `U_REF` (`0016:1441-1451`). No counter
//! or session-wide precondition joins the batch (`0016:1686-1742`).
//!
//! # One clock (ADR-0009)
//!
//! The quiesce renewal, the lease check before each commit round and the part's
//! `committed_at_millis` stamp (taken once, at the first round's reading, so every round sends the
//! same bytes) all read the caller's [`StageClock`], the clock that owns the attempt's lease
//! lifecycle and that staging ran on ([`super::stage`]'s "One clock"). Nothing else here reads a
//! clock: the re-reads and the compensation read none.
//!
//! No production path calls this module yet: the S3 `UploadPart` verb is #669, and the part records
//! it writes are consumed by Complete (#658). The tests drive every branch over a real store with
//! each interleaving forced by a scripted wrapper
//! (`crates/core/tests/multipart_upload_part_commit.rs`).

// deferred: #660 — seeded Tier-0 DST coverage of the commit and compensation races (a commit
// against a rival, an Abort, a Complete's rollback, the reaper), as for staging (`stage.rs`).

use std::fmt;

use bytes::Bytes;
use futures_util::Stream;
use wyrd_traits::{
    BoxError, CommitOutcome, CommitUnknownResult, MetadataStore, PlacementChunkStore, WriteBatch,
};

use super::stage::{
    stage_part, RenewOutcome, StageClock, StageError, StageOutcome, StageRequest, StageRuntime,
    StagedAttempt, Stop as StageStop,
};
use super::{
    decode_part_record, decode_part_summary, decode_session_record, decode_slot_record, mpu_key,
    parse_psum_key, part_key, psum_key, psum_range, upload_part_answer, PartRecord, RecordError,
    Refusal, RetireObligation, UploadId, UploadPartAnswer, UploadPartOutcome, E_TX_MILLIS,
    MAX_COMPLETE_ATTEMPTS, MAX_PART_CHUNKS, MAX_STAGED_CHUNKS,
};
use crate::metadata;

/// The lease a part commit must have left at the moment it is sent, and so the cover the quiesce
/// renewal asks for: `E_tx`'s five seconds, the number 0016 chose for how much lease a commit should
/// start with (`0016:1726-1728`). A **dispatch margin**, not a deadline: it says nothing about when
/// a sent batch applies (the module doc's *What the lease check does and does not promise*). A
/// request staged with a lease TTL not above it cannot be committed: the renewal refuses a cover it
/// could not give.
pub const DISPATCH_MARGIN_MILLIS: u64 = E_TX_MILLIS;

/// The most part-commit batches one [`commit_part`] sends: its first, and three more for a
/// conflict no read explains (on TiKV, a lock another writer held while the commit's locking reads
/// ran). Nothing else sends the batch again: a conflict a read explains is answered from that read
/// (the module doc's *A commit that lost*), and a commit shares no writable key with another part
/// number (`0016:1597-1610`), so no other part's progress spends a round. A round answered
/// `CommitUnknownResult` is the last one sent, whatever the budget has left (the module doc's *A
/// commit in doubt*). A budget for the work, not for the answer: a commit that spends it
/// compensates and returns [`Stop::CommitRoundsSpent`], unless a re-upload finds its commit's
/// obligation key holding a record ([`Stop::ObligationKeyTaken`]).
pub const COMMIT_ROUNDS: u32 = 4;

/// The most laps of reads one [`commit_part`] makes after a commit answered `CommitUnknownResult`,
/// each the part key, the session, the attempt's own slot and the session again, with no batch
/// (the module doc's *A commit in doubt*). A lap proves the commit only if it landed before the
/// lap's slot read, and the first lap starts as soon as the store answered, so a commit still in
/// flight then can land after it: the second lap reads the slot once that whole first lap is over.
/// Only a lap that found the slot held under the unchanged session, with the commit still able to
/// land, leads to another. A budget for the work, not for the answer: a commit that may land after
/// the last lap is answered [`CommitError::InDoubt`], as Abort returns its doubt rather than wait
/// for a fence that may land later ([`super::abort::abort_session`]).
///
/// The same budget, for the same reason, bounds the laps after a compensation answered
/// `CommitUnknownResult`, each its obligation key (for a non-empty part), the attempt's own slot and
/// the session (the module doc's *A compensation in doubt*).
pub const SETTLE_LAPS: u32 = 2;

/// The most compensation batches one [`commit_part`] sends: [`MAX_COMPLETE_ATTEMPTS`] `+ 1`, the
/// budget [`super::abort::ABORT_ROUNDS`] takes and for the same reason. A session goes back to a
/// newer `Open` epoch only when a Complete fence is released, and it can spend at most
/// [`MAX_COMPLETE_ATTEMPTS`] charged fences, so that many rounds cover the epoch chase after each
/// of them; the one more is the round that lands. A Complete rejected behind its fence gives its
/// attempt back when it releases that fence (#1007), so such Completes can move the session more
/// often than that, and each such move spends a round too, as does a conflict no read explains.
/// A budget for the work, not for the answer: a compensation that spends it returns
/// [`Stop::CompensationRoundsSpent`], claiming no release, and what it staged stays until
/// `W_session`.
pub const COMPENSATION_ROUNDS: u32 = MAX_COMPLETE_ATTEMPTS + 1;

/// Which of the attempt's batches a [`CommitError::InDoubt`] is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DoubtfulBatch {
    /// The part commit: the part may be committed, or may still be.
    Commit,
    /// The live-session compensation: the obligation may be installed and the slot released, or
    /// may still be.
    Compensation,
}

/// A part commit that ended with neither an outcome nor a refusal.
///
/// # Error class
///
/// [`wyrd_traits::classify`] reads [`Self::InDoubt`] as
/// [`ErrorClass::Indeterminate`](wyrd_traits::ErrorClass::Indeterminate): its
/// [`source`](std::error::Error::source) is the store's own [`CommitUnknownResult`].
/// [`Self::Stopped`] keeps its cause's class, and [`Self::Renewal`] its renewal error's, whether or
/// not the slot was then released.
#[derive(Debug)]
pub enum CommitError {
    /// The quiesce renewal ([`StagedAttempt::renew`]) did not cover the commit, so no commit was
    /// sent: its error, its doubt about its own rounds kept.
    Renewal {
        /// What stopped the renewal.
        error: StageError,
        /// Whether the attempt's live-session compensation then landed. When `false`, what it
        /// staged stays in the store.
        slot_released: bool,
    },
    /// A batch of this attempt may have landed, or may still land, and no read showed which
    /// ([`InDoubt::cause`] says what ended the reads). The attempt sent nothing after that batch.
    /// Never "nothing was written". The batch was re-read and not proven landed: a commit by the
    /// module doc's *A commit in doubt*, a compensation by its *A compensation in doubt*.
    InDoubt(Box<InDoubt>),
    /// The commit stopped, and no batch of it is in doubt.
    Stopped {
        /// The session.
        upload_id: UploadId,
        /// What stopped it.
        cause: Stop,
        /// Whether the attempt's live-session compensation then landed. When `false`, what it
        /// staged stays in the store.
        slot_released: bool,
    },
}

impl CommitError {
    /// Whether this error's stop was followed by a compensation that landed: the attempt's slot is
    /// released, its owned entries are deleted, and its chunks are owed to a `retire:bytes:`
    /// obligation (none for an empty part). A compensation the store answered
    /// `CommitUnknownResult` counts only on its own evidence, read back: its obligation at the
    /// bytes it put, or for an empty part its slot gone under the session bytes it required (the
    /// module doc's *A compensation in doubt*). Never `true` for [`Self::InDoubt`].
    pub const fn slot_released(&self) -> bool {
        match self {
            Self::Renewal { slot_released, .. } | Self::Stopped { slot_released, .. } => {
                *slot_released
            }
            Self::InDoubt(_) => false,
        }
    }
}

/// What a [`CommitError::InDoubt`] reports.
#[derive(Debug)]
pub struct InDoubt {
    /// The session.
    pub upload_id: UploadId,
    /// Which batch is in doubt.
    pub batch: DoubtfulBatch,
    /// Whether that batch may still be applied **after** the call returned: the store reported
    /// [`CommitUnknownResult::may_still_commit`], and no read since showed it unable to land (the
    /// module doc's *A commit in doubt* and *A compensation in doubt*). So never `true` where the
    /// store said `false`. With `false` it may still have landed.
    pub may_still_commit: bool,
    /// The store's report for that batch. It is the error's
    /// [`source`](std::error::Error::source).
    pub outcome: CommitUnknownResult,
    /// What ended the attempt with the doubt kept: the reads that could not prove the commit
    /// landed, or the fault or damage that stopped them.
    pub cause: Stop,
}

/// What stopped a part commit ([`CommitError::Stopped`], [`InDoubt::cause`]).
#[derive(Debug)]
pub enum Stop {
    /// All [`COMMIT_ROUNDS`] part commits were sent, each answered `Conflict` with nothing a read
    /// could see to explain it. The attempt then compensates.
    CommitRoundsSpent {
        /// Part commits sent.
        rounds: u32,
    },
    /// All [`COMPENSATION_ROUNDS`] compensations were sent without one landing, each lost to a
    /// rollback or to a conflict no read explains. It released nothing.
    CompensationRoundsSpent {
        /// Compensations sent.
        rounds: u32,
    },
    /// The attempt's slot is gone (absent, or another attempt's) under a live session, or
    /// rewritten under this attempt's id, and nothing explains it: the session's state does not
    /// account for it, and no batch of this attempt landed (as a stop) or none can be shown to
    /// have (as an [`InDoubt::cause`]). As the cause of a commit in doubt that required the part
    /// key absent, it also covers a slot gone under the unchanged session while that key, read
    /// after the slot, is still absent: no commit of this attempt landed (the module doc's *A
    /// commit in doubt*). Nothing is released.
    SlotLost {
        /// The slot key, as UTF-8 (lossy).
        key: String,
    },
    /// An obligation key of this attempt holds a record no batch of this attempt put: the key its
    /// compensation requires absent, or the one a re-upload's commit required absent in every
    /// round no read explained. The key names this attempt, and a token is minted once
    /// (`0016:364`), so this is damage a retry cannot clear. The record is kept, and nothing is
    /// released. As the cause of a compensation in doubt, those bytes are not its own obligation's,
    /// and the flag is kept: the drain or a repair can clear them, and the round may then land.
    ObligationKeyTaken {
        /// The `retire:` key, as UTF-8 (lossy).
        key: String,
    },
    /// The attempt's lease had less than [`DISPATCH_MARGIN_MILLIS`] left when a commit round came
    /// to be sent, so it was not.
    LeaseShort {
        /// The lease the quiesce renewal wrote.
        lease_expiry_millis: u64,
        /// The reading it was judged at.
        now_millis: u64,
    },
    /// With a batch in doubt, the session left the bytes the batch required, or was deleted with
    /// its slots. A teardown removes slots too, and an epoch only rises, so no read of the slot can
    /// show whether the batch landed before the session moved, and it cannot land after. A
    /// compensation's own obligation, found at the bytes it put, still proves it, so a
    /// compensation ends here only when that read did not find them. Only an [`InDoubt::cause`].
    SessionMoved,
    /// The store could not report the batch's outcome, and no read showed that it landed. For a
    /// commit, the slot was still held at the bytes it last wrote, which late staging can rebuild
    /// after the commit landed. For a compensation, under the unchanged session, its slot was still
    /// held, or, for a non-empty part, its obligation was not found at the bytes it puts, which
    /// the retirement drain can delete after it landed. Nothing more was sent. Only an
    /// [`InDoubt::cause`].
    OutcomeUnknown,
    /// A stored record is not valid (ADR-0045), or a value this attempt would write is not one its
    /// decoder accepts.
    Record(RecordError),
    /// A metadata store operation failed.
    Store(BoxError),
}

/// What a stop left in the store, for an error's message.
const fn residue(slot_released: bool) -> &'static str {
    if slot_released {
        "the attempt's slot is released and its chunks are retired"
    } else {
        "this attempt released nothing, and what it staged stays in the store"
    }
}

impl fmt::Display for CommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Renewal {
                error,
                slot_released,
            } => write!(
                f,
                "multipart part commit: the quiesce renewal did not cover the commit, so none was \
                 sent: {error}; {}",
                residue(*slot_released)
            ),
            Self::InDoubt(doubt) => write!(
                f,
                "multipart part commit: upload {} is in doubt: this attempt's {} {}, and nothing \
                 more was sent ({})",
                doubt.upload_id,
                match doubt.batch {
                    DoubtfulBatch::Commit => "part commit",
                    DoubtfulBatch::Compensation => "compensation",
                },
                if doubt.may_still_commit {
                    "may still be applied"
                } else {
                    "may have been applied"
                },
                doubt.cause,
            ),
            Self::Stopped {
                upload_id,
                cause,
                slot_released,
            } => write!(
                f,
                "multipart part commit: upload {upload_id}: {cause}; no batch of this part attempt \
                 is in doubt; {}",
                residue(*slot_released)
            ),
        }
    }
}

impl fmt::Display for Stop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CommitRoundsSpent { rounds } => write!(
                f,
                "all {rounds} part commits were answered Conflict with nothing a read could see \
                 to explain it"
            ),
            Self::CompensationRoundsSpent { rounds } => write!(
                f,
                "all {rounds} compensations were sent without one landing"
            ),
            Self::SlotLost { key } => write!(
                f,
                "the slot {key} was removed or rewritten by something other than this attempt"
            ),
            Self::ObligationKeyTaken { key } => write!(
                f,
                "the obligation key {key} holds a record no batch of this attempt put; this is \
                 damage a retry cannot clear"
            ),
            Self::LeaseShort {
                lease_expiry_millis,
                now_millis,
            } => write!(
                f,
                "the lease expiring at {lease_expiry_millis} had less than \
                 {DISPATCH_MARGIN_MILLIS} ms left at {now_millis}, so no further commit was sent"
            ),
            Self::SessionMoved => f.write_str(
                "the session left the bytes the batch required, or was deleted, so no read can \
                 say whether the batch landed before it moved",
            ),
            Self::OutcomeUnknown => f.write_str(
                "the store could not report the batch's outcome, and no read showed that it \
                 landed",
            ),
            Self::Record(err) => write!(f, "invalid record: {err}"),
            Self::Store(err) => write!(f, "store fault: {err}"),
        }
    }
}

impl std::error::Error for CommitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Renewal { error, .. } => Some(error),
            // The unknown outcome, never the cause: `classify` stops at the first seam type on
            // the chain, and a commit in doubt must read `Indeterminate` even when a store fault
            // is what ended its reads.
            Self::InDoubt(doubt) => Some(&doubt.outcome),
            Self::Stopped { cause, .. } => match cause {
                Stop::Record(err) => Some(err),
                Stop::Store(err) => Some(err.as_ref()),
                Stop::CommitRoundsSpent { .. }
                | Stop::CompensationRoundsSpent { .. }
                | Stop::SlotLost { .. }
                | Stop::ObligationKeyTaken { .. }
                | Stop::LeaseShort { .. }
                | Stop::SessionMoved
                | Stop::OutcomeUnknown => None,
            },
        }
    }
}

/// What the composed [`upload_part`] ended with, when it ended with neither an outcome nor a
/// refusal: staging's error, or the commit's. Each keeps its own class ([`StageError`],
/// [`CommitError`]).
#[derive(Debug)]
pub enum UploadPartError {
    /// Staging stopped ([`stage_part`]); no commit was attempted.
    Stage(StageError),
    /// The commit stopped ([`commit_part`]).
    Commit(CommitError),
}

impl fmt::Display for UploadPartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stage(err) => write!(f, "{err}"),
            Self::Commit(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for UploadPartError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Stage(err) => Some(err),
            Self::Commit(err) => Some(err),
        }
    }
}

/// The `mpu:` bytes a batch of this attempt requires, with the epoch they carry, decoded once.
#[derive(Debug, Clone)]
struct Session {
    bytes: Bytes,
    epoch: u64,
}

impl Session {
    fn of(bytes: &Bytes) -> Result<Self, RecordError> {
        Ok(Self {
            bytes: bytes.clone(),
            epoch: decode_session_record(bytes)?.epoch(),
        })
    }
}

/// What a read of the session found, judged against the session a batch of this attempt required
/// ([`judge_session`]).
#[derive(Debug)]
enum SessionSeen {
    /// Those very bytes.
    Same,
    /// An `Open` record at a higher epoch: a Complete fenced the session and rolled it back.
    Open(Session),
    /// A record at a higher epoch that is not `Open`: a fence, whose state gives the refusal.
    Left(Refusal),
    /// No record. A terminal delete only together with a slot read made after this one
    /// ([`session_gone`]).
    Gone,
}

/// Judge a read of the session against `required` (the module doc's *A commit that lost*). Bytes
/// that do not decode, and any other record at an epoch that has not risen, whatever its state, are
/// an error (ADR-0045), never a moved session.
fn judge_session(found: Option<&Bytes>, required: &Session) -> Result<SessionSeen, RecordError> {
    let Some(bytes) = found else {
        return Ok(SessionSeen::Gone);
    };
    if *bytes == required.bytes {
        return Ok(SessionSeen::Same);
    }
    let record = decode_session_record(bytes)?;
    if record.epoch() <= required.epoch {
        return Err(RecordError::SessionEpochNotRisen {
            found: record.epoch(),
            required: required.epoch,
        });
    }
    Ok(match upload_part_answer(Some(record.state())) {
        UploadPartAnswer::Accepted => SessionSeen::Open(Session {
            bytes: bytes.clone(),
            epoch: record.epoch(),
        }),
        UploadPartAnswer::Refused(refusal) => SessionSeen::Left(refusal),
    })
}

/// What a read of the attempt's own slot found ([`judge_slot`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotSeen {
    /// The bytes this attempt last wrote.
    Held,
    /// Absent, or another attempt's record (`SlotRecord`'s `attempt_id`): the index was released.
    Gone,
    /// This attempt's id, under bytes it did not last write.
    Rewritten,
}

/// Judge a read of the attempt's slot. A stored slot that does not decode is an error (ADR-0045),
/// never "another attempt's".
fn judge_slot(found: Option<&Bytes>, attempt: &StagedAttempt) -> Result<SlotSeen, RecordError> {
    let Some(bytes) = found else {
        return Ok(SlotSeen::Gone);
    };
    if bytes == attempt.slot_bytes() {
        return Ok(SlotSeen::Held);
    }
    Ok(
        if decode_slot_record(bytes)?.attempt_id() == attempt.attempt_id() {
            SlotSeen::Rewritten
        } else {
            SlotSeen::Gone
        },
    )
}

fn lossy(key: &[u8]) -> String {
    String::from_utf8_lossy(key).into_owned()
}

/// [`Stop::SlotLost`], naming the attempt's slot key.
fn slot_lost_at(attempt: &StagedAttempt) -> Stop {
    Stop::SlotLost {
        key: lossy(attempt.slot_key()),
    }
}

/// The attempt's slot is lost: [`Stop::SlotLost`], claiming no release.
fn slot_lost(attempt: &StagedAttempt) -> CommitError {
    stopped(attempt.upload_id(), slot_lost_at(attempt))
}

/// Read `obligation`'s key, which a batch of this attempt requires absent: any record there is
/// [`Stop::ObligationKeyTaken`]. The key names this attempt (`0016:364`), and with nothing in doubt
/// none of its batches landed, so none of them put that record. The record is kept.
async fn obligation_key_free(
    store: &impl MetadataStore,
    attempt: &StagedAttempt,
    obligation: &RetireObligation,
) -> Result<(), CommitError> {
    let upload_id = attempt.upload_id();
    let key = obligation.key();
    match store.get(&key).await {
        Ok(None) => Ok(()),
        Ok(Some(_)) => Err(stopped(
            upload_id,
            Stop::ObligationKeyTaken { key: lossy(&key) },
        )),
        Err(fault) => Err(stopped(upload_id, Stop::Store(fault))),
    }
}

/// The last session read found no record (the module doc's *A commit that lost*): read the slot key
/// once more, now, since a slot read made before that session read proves nothing. Absent, a
/// terminal delete took the session and the slot together (`0016:674`): [`Refusal::NoSuchUpload`].
/// Any record there, this attempt's or another's, is damage
/// ([`RecordError::SessionRecordMissing`]).
async fn session_gone(
    store: &impl MetadataStore,
    attempt: &StagedAttempt,
    required: &Session,
) -> Result<Refusal, CommitError> {
    let upload_id = attempt.upload_id();
    match store.get(attempt.slot_key()).await {
        Ok(None) => Ok(Refusal::NoSuchUpload),
        Ok(Some(_)) => Err(stopped(
            upload_id,
            Stop::Record(RecordError::SessionRecordMissing {
                required: required.epoch,
            }),
        )),
        Err(fault) => Err(stopped(upload_id, Stop::Store(fault))),
    }
}

/// A commit stopped with nothing in doubt and nothing released.
fn stopped(upload_id: &UploadId, cause: Stop) -> CommitError {
    CommitError::Stopped {
        upload_id: upload_id.clone(),
        cause,
        slot_released: false,
    }
}

/// **A batch in doubt** (the module doc's *A commit in doubt* and *A compensation in doubt*): the
/// round the store answered `CommitUnknownResult`, which is the last batch the attempt sends.
#[derive(Debug)]
struct Doubt {
    /// Which batch the round was.
    batch: DoubtfulBatch,
    /// The store's report.
    outcome: CommitUnknownResult,
    /// Whether the round may still land: the store said `may_still_commit`, and no read since
    /// found one of its preconditions gone for good.
    may_land: bool,
}

impl Doubt {
    fn of(batch: DoubtfulBatch, outcome: &CommitUnknownResult) -> Self {
        Self {
            batch,
            outcome: outcome.clone(),
            may_land: outcome.may_still_commit,
        }
    }

    /// A read found a precondition of the round gone for good: it cannot land from now on, though
    /// it may have already.
    fn cannot_land(&mut self) {
        self.may_land = false;
    }

    /// A read of the session, judged ([`judge_session`]). A record at a higher epoch than the
    /// round required, `Open` or not, is a session that moved, and an epoch only rises, so the
    /// round cannot land from now on. Damage and a missing record settle nothing: a repair can let
    /// the round land, and a missing record is judged by a later slot read ([`gone_in_doubt`]).
    fn seen_session(&mut self, seen: &Result<SessionSeen, RecordError>) {
        if matches!(seen, Ok(SessionSeen::Open(_) | SessionSeen::Left(_))) {
            self.cannot_land();
        }
    }

    /// The error for an attempt whose reads ended for `cause`, the doubt kept. It claims no
    /// release.
    fn end(&self, upload_id: &UploadId, cause: Stop) -> CommitError {
        CommitError::InDoubt(Box::new(InDoubt {
            upload_id: upload_id.clone(),
            batch: self.batch,
            may_still_commit: self.may_land,
            outcome: self.outcome.clone(),
            cause,
        }))
    }

    /// One read of a settle lap, its own transaction. A fault ends the reads in doubt, keeping
    /// what every read before it showed: each is folded in as it returns, before the next is made.
    async fn read(
        &self,
        store: &impl MetadataStore,
        upload_id: &UploadId,
        key: &[u8],
    ) -> Result<Option<Bytes>, CommitError> {
        store
            .get(key)
            .await
            .map_err(|fault| self.end(upload_id, Stop::Store(fault)))
    }

    /// With a compensation in doubt, read `obligation`'s key, which the round put under
    /// `require_absent`: whether it holds the exact bytes the round put. The key names this
    /// attempt and the epoch the round required (`0016:358-366`), and of this attempt's batches
    /// only that round can have put those bytes there, so they prove it landed. Absent proves
    /// nothing: the retirement drain deletes a drained obligation whatever the session's state.
    /// Other bytes there are damage, which ends the reads with [`Stop::ObligationKeyTaken`] and the
    /// flag kept, since they are not there for good.
    async fn owns(
        &self,
        store: &impl MetadataStore,
        upload_id: &UploadId,
        obligation: &RetireObligation,
    ) -> Result<bool, CommitError> {
        let key = obligation.key();
        match self.read(store, upload_id, &key).await? {
            None => Ok(false),
            Some(found) if found == metadata::encode(obligation.payload()) => Ok(true),
            Some(_) => Err(self.end(upload_id, Stop::ObligationKeyTaken { key: lossy(&key) })),
        }
    }
}

/// With a batch in doubt, a lap's last session read found no record: read the slot key once more,
/// now, since a slot read made before that session read proves nothing. Absent, a terminal delete
/// took the session and the slot together (`0016:674`), perhaps after the batch landed, and the
/// batch cannot land after it: [`Stop::SessionMoved`]. Any record there, this attempt's or
/// another's, is damage ([`RecordError::SessionRecordMissing`]), and the flag is kept. Returns the
/// cause the doubt is kept with; a fault ends the reads at once.
async fn gone_in_doubt(
    store: &impl MetadataStore,
    attempt: &StagedAttempt,
    required: &Session,
    doubt: &mut Doubt,
) -> Result<Stop, CommitError> {
    let found = doubt
        .read(store, attempt.upload_id(), attempt.slot_key())
        .await?;
    Ok(match found {
        None => {
            doubt.cannot_land();
            Stop::SessionMoved
        }
        Some(_) => Stop::Record(RecordError::SessionRecordMissing {
            required: required.epoch,
        }),
    })
}

/// **Settle a part commit answered `CommitUnknownResult`** (the module doc's *A commit in doubt*):
/// send nothing more, read at most [`SETTLE_LAPS`] laps, and answer
/// [`UploadPartOutcome::Committed`] only on the own-commit proof, the slot found gone and the
/// session read after it still the `Open@E` bytes the commit required, with, for a commit that
/// required the part key absent, a record at that key read between the two. Anything else keeps
/// the doubt ([`CommitError::InDoubt`]). The clock plays no part.
async fn settle(
    store: &impl MetadataStore,
    attempt: &StagedAttempt,
    session: &Session,
    required: Option<&Bytes>,
    mut doubt: Doubt,
) -> Result<UploadPartOutcome, CommitError> {
    let upload_id = attempt.upload_id();
    let session_key = mpu_key(upload_id);
    let part_at = part_key(upload_id, attempt.part_number());
    let mut laps = 0u32;
    loop {
        laps += 1;
        // The lap's four reads, in order. Each is folded into the doubt before the next is made,
        // so a read that fails later in the lap keeps what the earlier ones showed.
        let part = doubt.read(store, upload_id, &part_at).await?;
        // A changed part key is another writer's record only if it decodes (ADR-0045). Damage
        // there is answered below, once the own-commit proof has been tried.
        let mut torn = None;
        if part.as_ref() != required {
            match part.as_deref().map(decode_part_record).transpose() {
                // A record where the commit required none: it cannot land from now on, since a
                // `part:` record goes only after the session leaves `Open` (`0016:351`).
                Ok(Some(_)) if required.is_none() => doubt.cannot_land(),
                // A changed prior settles nothing, a rival's record or a repair's: the prior's
                // bytes can come back. A prior gone is judged below, by the session.
                Ok(_) => {}
                Err(err) => torn = Some(err),
            }
        }
        let first = doubt.read(store, upload_id, &session_key).await?;
        doubt.seen_session(&judge_session(first.as_ref(), session));
        let slot = doubt.read(store, upload_id, attempt.slot_key()).await?;
        let slot = judge_slot(slot.as_ref(), attempt);
        // The own-commit proof's part read, for a commit that required the part key absent and a
        // slot found gone: after that slot read and before the last session read, so with
        // `Open@E` read after it no drain ran in between, and the key found absent disproves the
        // commit (a `part:` record goes only after the session leaves `Open`, `0016:351`). A part
        // read made before the slot read disproves nothing: the commit may land between the two.
        // What it found that keeps the proof from answering, if anything, is the doubt's cause.
        let mut disproof = None;
        if required.is_none() && matches!(slot, Ok(SlotSeen::Gone)) {
            let found = doubt.read(store, upload_id, &part_at).await?;
            match found.as_deref().map(decode_part_record).transpose() {
                // No record: with `Open@E` read next, no commit of this attempt landed, and
                // damage took the slot.
                Ok(None) => disproof = Some(slot_lost_at(attempt)),
                // A record where the commit required none, this attempt's or a rival's: it cannot
                // land from now on. A rival's record here behind a slot erased by damage reads
                // exactly like this commit landing and then being replaced by a rival's
                // re-upload, so it proves the commit: a limit of the proof (the module doc's *A
                // commit in doubt*).
                Ok(Some(_)) => doubt.cannot_land(),
                // Bytes that do not decode are no record either (ADR-0045), so they prove no
                // landed commit. Damage settles nothing: the flag is kept, since a repair can
                // let the round land.
                Err(err) => disproof = Some(Stop::Record(err)),
            }
        }
        let last = doubt.read(store, upload_id, &session_key).await?;
        let last = judge_session(last.as_ref(), session);
        doubt.seen_session(&last);
        // The own-commit proof, before any damage the lap's first part read found: while the
        // session is `Open@E` only this attempt's own commit removes its slot, and it sent no
        // compensation.
        if let (Ok(SessionSeen::Same), Ok(SlotSeen::Gone)) = (&last, &slot) {
            if let Some(cause) = disproof {
                return Err(doubt.end(upload_id, cause));
            }
            return Ok(UploadPartOutcome::Committed {
                part_number: attempt.part_number(),
                digest: *attempt.digest(),
            });
        }
        if let Some(err) = torn {
            return Err(doubt.end(upload_id, Stop::Record(err)));
        }
        let slot = slot.map_err(|err| doubt.end(upload_id, Stop::Record(err)))?;
        let last = last.map_err(|err| doubt.end(upload_id, Stop::Record(err)))?;
        let cause = match (last, slot) {
            (SessionSeen::Gone, _) => gone_in_doubt(store, attempt, session, &mut doubt).await?,
            (_, SlotSeen::Rewritten) => slot_lost_at(attempt),
            // A teardown removes slots too, so a slot gone under a moved session proves nothing,
            // and no later session read can find the bytes the commit required.
            (SessionSeen::Open(_) | SessionSeen::Left(_), _) => Stop::SessionMoved,
            // From here the slot is held: gone under `Open@E` is the proof, answered above. A
            // `part:` record goes only after the session leaves `Open`: damage, not a rival.
            (SessionSeen::Same, _) if required.is_some() && part.is_none() => {
                Stop::Record(RecordError::PartRecordMissing {
                    part_number: attempt.part_number().get(),
                })
            }
            // The round may have landed after this lap's slot read: one more lap.
            (SessionSeen::Same, _) if doubt.may_land && laps < SETTLE_LAPS => continue,
            (SessionSeen::Same, _) => Stop::OutcomeUnknown,
        };
        return Err(doubt.end(upload_id, cause));
    }
}

/// The refusal of an attempt past the per-part ceiling, if it is. `stage_part` cannot enforce it:
/// the bound it stops at is the caller's, and only a body past that bound is marked
/// ([`StagedAttempt::chunk_bound_exceeded`]), so this looks at both. A truncated body had at least
/// one more chunk than it holds, and its [`StagedAttempt::len`] and [`StagedAttempt::digest`]
/// describe only the prefix.
fn past_part_ceiling(attempt: &StagedAttempt) -> Option<Refusal> {
    let held = attempt.chunks().len() as u64;
    let limit = u64::from(MAX_PART_CHUNKS);
    if attempt.chunk_bound_exceeded() {
        return Some(Refusal::EntityTooLarge {
            chunks: held.saturating_add(1),
            limit: held.min(limit),
        });
    }
    (held > limit).then_some(Refusal::EntityTooLarge {
        chunks: held,
        limit,
    })
}

/// The session the batches after the quiesce renewal require: what the renewal left, which is
/// `staged` unless it adopted a session it found at other `Open` bytes. An adoption the epoch does
/// not explain is damage.
fn renewed_session(attempt: &StagedAttempt, staged: &Session) -> Result<Session, RecordError> {
    // deferred: #1026 — a non-`Open` record at an epoch that has not risen is answered
    // `NoSuchUpload` inside the renewal, before this can judge it.
    let session = Session::of(attempt.session_bytes())?;
    if session.bytes != staged.bytes && session.epoch <= staged.epoch {
        return Err(RecordError::SessionEpochNotRisen {
            found: session.epoch,
            required: staged.epoch,
        });
    }
    Ok(session)
}

/// Whether a stopped quiesce renewal leaves the attempt to compensate (the module doc's *The
/// stops*): only a stop with nothing in doubt, whose records the attempt can still show are its own,
/// under a store that is answering.
const fn compensates_after(error: &StageError) -> bool {
    matches!(
        error,
        StageError::Stopped {
            cause: StageStop::CoverShort { .. }
                | StageStop::RenewalRoundsSpent { .. }
                | StageStop::LeaseLapsed { .. }
                | StageStop::InvalidRequest { .. },
            ..
        }
    )
}

/// The chunks the session's committed parts hold, this part number's own prior left out (a re-upload
/// replaces it): the sum over the bounded `psum:<id>:` range of summaries, never the chunk lists
/// (`0016:352`).
async fn staged_elsewhere(
    store: &impl MetadataStore,
    attempt: &StagedAttempt,
) -> Result<u64, Stop> {
    let mut total = 0u64;
    for (key, value) in store
        .scan(&psum_range(attempt.upload_id()))
        .await
        .map_err(Stop::Store)?
    {
        let (_, part_number) = parse_psum_key(&key).map_err(Stop::Record)?;
        if part_number == attempt.part_number() {
            continue;
        }
        let summary = decode_part_summary(&value).map_err(Stop::Record)?;
        total = total.saturating_add(u64::from(summary.chunks));
    }
    Ok(total)
}

/// The obligation an attempt's batch under `session` installs for `chunks`, none when there are
/// none.
fn obligation_for(
    attempt: &StagedAttempt,
    session: &Session,
    chunks: &[metadata::ChunkRef],
) -> Result<Option<RetireObligation>, RecordError> {
    if chunks.is_empty() {
        return Ok(None);
    }
    RetireObligation::part_chunks(
        attempt.upload_id(),
        session.epoch,
        attempt.part_number(),
        attempt.attempt_id(),
        chunks,
    )
    .map(Some)
}

/// The part commit's batch (`0016:660`).
fn commit_batch(
    attempt: &StagedAttempt,
    session: &Bytes,
    required: Option<&Bytes>,
    record: &PartRecord,
    retire: Option<&RetireObligation>,
) -> WriteBatch {
    let upload_id = attempt.upload_id();
    let part = part_key(upload_id, attempt.part_number());
    let mut batch = WriteBatch::new().require(mpu_key(upload_id), session.clone());
    batch = match required {
        Some(prior) => batch.require(part.clone(), prior.clone()),
        None => batch.require_absent(part.clone()),
    };
    // The bytes this attempt last wrote, from its own state: never re-read from the key.
    batch = batch.require(attempt.slot_key().to_vec(), attempt.slot_bytes().clone());
    if let Some(obligation) = retire {
        batch = batch.require_absent(obligation.key());
    }
    batch = batch
        .put(part, metadata::encode(record))
        .put(
            psum_key(upload_id, attempt.part_number()),
            metadata::encode(&record.summary()),
        )
        .delete(attempt.slot_key().to_vec());
    for owned in attempt.owned() {
        batch = batch.delete(owned.key().to_vec());
    }
    if let Some(obligation) = retire {
        batch = batch.put(obligation.key(), metadata::encode(obligation.payload()));
    }
    batch
}

/// The live-session compensation's batch (`0016:673`), under `session`.
fn compensation_batch(
    attempt: &StagedAttempt,
    session: &Bytes,
    obligation: Option<&RetireObligation>,
) -> WriteBatch {
    let mut batch = WriteBatch::new()
        .require(mpu_key(attempt.upload_id()), session.clone())
        // The bytes this attempt last wrote, from its own state: never re-read from the key.
        .require(attempt.slot_key().to_vec(), attempt.slot_bytes().clone());
    if let Some(obligation) = obligation {
        batch = batch.require_absent(obligation.key());
    }
    batch = batch.delete(attempt.slot_key().to_vec());
    for owned in attempt.owned() {
        batch = batch.delete(owned.key().to_vec());
    }
    if let Some(obligation) = obligation {
        batch = batch.put(obligation.key(), metadata::encode(obligation.payload()));
    }
    batch
}

/// What a compensation that ended without an error came to ([`compensate`]).
#[derive(Debug)]
enum Compensated {
    /// It landed: the slot released, the owned entries deleted, the chunks owed. Either the store
    /// answered `Committed`, or a round it answered `CommitUnknownResult` was read back on the
    /// compensation's own evidence ([`settle_compensation`]).
    Released,
    /// No round landed, and the session has left `Open` or was deleted with its slots: this
    /// refusal, with nothing more written. The residue is the reaper's.
    Left(Refusal),
}

/// **Settle a compensation answered `CommitUnknownResult`** (the module doc's *A compensation in
/// doubt*): send nothing more, read at most [`SETTLE_LAPS`] laps of `obligation`'s key (for a
/// non-empty part), the attempt's own slot and the session, then `obligation`'s key once more, and
/// answer [`Compensated::Released`] only on the compensation's own evidence: for a non-empty part,
/// `obligation` at the exact bytes the round put, under the epoch the round required; for an
/// empty part, its slot found gone and the session read after that slot read still the bytes
/// `session` holds. Anything else keeps the doubt ([`CommitError::InDoubt`],
/// [`DoubtfulBatch::Compensation`]). The clock plays no part.
async fn settle_compensation(
    store: &impl MetadataStore,
    attempt: &StagedAttempt,
    session: &Session,
    obligation: Option<&RetireObligation>,
    mut doubt: Doubt,
) -> Result<Compensated, CommitError> {
    let upload_id = attempt.upload_id();
    let session_key = mpu_key(upload_id);
    let mut laps = 0u32;
    let cause = loop {
        laps += 1;
        // The lap's reads, in the re-read's order. Each is folded into the doubt before the next
        // is made, so a read that fails later keeps what the earlier ones showed.
        if let Some(obligation) = obligation {
            if doubt.owns(store, upload_id, obligation).await? {
                return Ok(Compensated::Released);
            }
        }
        let slot = doubt.read(store, upload_id, attempt.slot_key()).await?;
        let slot = judge_slot(slot.as_ref(), attempt)
            .map_err(|err| doubt.end(upload_id, Stop::Record(err)))?;
        let now = doubt.read(store, upload_id, &session_key).await?;
        let now = judge_session(now.as_ref(), session);
        doubt.seen_session(&now);
        let now = now.map_err(|err| doubt.end(upload_id, Stop::Record(err)))?;
        match (now, slot) {
            // An empty part's proof: while the session holds the bytes the round required only
            // this attempt's commit and compensation remove its slot, and no commit of it can
            // land. A non-empty part never settles on its slot: damage can erase one too.
            (SessionSeen::Same, SlotSeen::Gone) if obligation.is_none() => {
                return Ok(Compensated::Released)
            }
            (SessionSeen::Gone, _) => {
                break gone_in_doubt(store, attempt, session, &mut doubt).await?;
            }
            (_, SlotSeen::Rewritten) => break slot_lost_at(attempt),
            // A teardown removes slots too, so a slot gone under a moved session proves nothing.
            (SessionSeen::Open(_) | SessionSeen::Left(_), _) => break Stop::SessionMoved,
            // The round may have landed after this lap's slot read: one more lap.
            (SessionSeen::Same, SlotSeen::Held) if doubt.may_land && laps < SETTLE_LAPS => {}
            (SessionSeen::Same, _) => break Stop::OutcomeUnknown,
        }
    };
    // A session that moved does not erase an obligation, and the round may have landed after the
    // first read of it: a non-empty part reads it once more, last.
    if let Some(obligation) = obligation {
        if doubt.owns(store, upload_id, obligation).await? {
            return Ok(Compensated::Released);
        }
    }
    Err(doubt.end(upload_id, cause))
}

/// **The live-session compensation** (`0016:673`, `:1607-1620`): one batch that releases the slot,
/// deletes the owned entries and, for a non-empty part, installs one `{chunks}` obligation naming
/// the attempt's own chunks, under `session`. Called only when no commit of the attempt can land:
/// none was sent, or each one sent was answered `Conflict`. A round answered `Conflict` is
/// classified from a re-read (the module doc's *The compensation, and the epoch chase*), within
/// [`COMPENSATION_ROUNDS`] rounds. A round answered `CommitUnknownResult` is the last batch the
/// attempt sends, settled from reads alone ([`settle_compensation`]). The clock plays no part.
async fn compensate(
    store: &impl MetadataStore,
    attempt: &StagedAttempt,
    session: &Session,
) -> Result<Compensated, CommitError> {
    let upload_id = attempt.upload_id();
    let fault = |fault| stopped(upload_id, Stop::Store(fault));
    let damage = |err| stopped(upload_id, Stop::Record(err));
    let session_key = mpu_key(upload_id);
    let mut session = session.clone();
    let mut rounds = 0u32;
    loop {
        // The attempt's own chunks, under the epoch this round requires; none for an empty part.
        let obligation = obligation_for(attempt, &session, attempt.chunks()).map_err(damage)?;
        let batch = compensation_batch(attempt, &session.bytes, obligation.as_ref());
        rounds += 1;
        match store.commit(batch).await {
            Ok(CommitOutcome::Committed) => return Ok(Compensated::Released),
            Ok(CommitOutcome::Conflict) => {}
            Err(fault) => {
                return match fault.downcast_ref::<CommitUnknownResult>() {
                    // Never read as `Conflict`, and never followed by another batch: the reads
                    // prove it landed, or keep the doubt.
                    Some(unknown) => {
                        let doubt = Doubt::of(DoubtfulBatch::Compensation, unknown);
                        settle_compensation(store, attempt, &session, obligation.as_ref(), doubt)
                            .await
                    }
                    None => Err(stopped(upload_id, Stop::Store(fault))),
                };
            }
        }
        // The re-read, in order: the obligation key, the slot, the session.
        if let Some(obligation) = &obligation {
            obligation_key_free(store, attempt, obligation).await?;
        }
        let slot = store.get(attempt.slot_key()).await.map_err(fault)?;
        let now = store.get(&session_key).await.map_err(fault)?;
        let slot = judge_slot(slot.as_ref(), attempt).map_err(damage)?;
        match (judge_session(now.as_ref(), &session).map_err(damage)?, slot) {
            (SessionSeen::Gone, _) => {
                return session_gone(store, attempt, &session)
                    .await
                    .map(Compensated::Left)
            }
            (_, SlotSeen::Rewritten)
            | (SessionSeen::Same | SessionSeen::Open(_), SlotSeen::Gone) => {
                return Err(slot_lost(attempt))
            }
            (SessionSeen::Left(refusal), _) => return Ok(Compensated::Left(refusal)),
            // The epoch chase: the same compensation, under the newer `Open` epoch.
            (SessionSeen::Open(newer), SlotSeen::Held) => session = newer,
            // A conflict no read explains: the same batch again.
            (SessionSeen::Same, SlotSeen::Held) => {}
        }
        if rounds == COMPENSATION_ROUNDS {
            return Err(stopped(upload_id, Stop::CompensationRoundsSpent { rounds }));
        }
    }
}

/// Refuse the part with `refusal`, once what it staged is compensated under `session`. A session
/// the compensation found fenced or deleted answers its own refusal instead.
async fn refuse(
    store: &impl MetadataStore,
    attempt: &StagedAttempt,
    session: &Session,
    refusal: Refusal,
) -> Result<UploadPartOutcome, CommitError> {
    Ok(UploadPartOutcome::Refused(
        match compensate(store, attempt, session).await? {
            Compensated::Released => refusal,
            Compensated::Left(left) => left,
        },
    ))
}

/// The commit stopped for `cause` with nothing in doubt: compensate under `session`, and return
/// `cause` reporting the slot released. A session the compensation found fenced or deleted answers
/// its refusal instead.
async fn compensated_stop(
    store: &impl MetadataStore,
    attempt: &StagedAttempt,
    session: &Session,
    cause: Stop,
) -> Result<UploadPartOutcome, CommitError> {
    match compensate(store, attempt, session).await? {
        Compensated::Released => Err(CommitError::Stopped {
            upload_id: attempt.upload_id().clone(),
            cause,
            slot_released: true,
        }),
        Compensated::Left(refusal) => Ok(UploadPartOutcome::Refused(refusal)),
    }
}

/// The commit rounds end for `cause` after a round no read explained, and the attempt
/// compensates. A re-upload over a non-empty prior required `retire`'s key absent in every round,
/// and no read of a round looks there, so it is read first: a record there is
/// [`Stop::ObligationKeyTaken`], with no compensation (the module doc's *A commit that lost*).
async fn unexplained_stop(
    store: &impl MetadataStore,
    attempt: &StagedAttempt,
    session: &Session,
    retire: Option<&RetireObligation>,
    cause: Stop,
) -> Result<UploadPartOutcome, CommitError> {
    if let Some(obligation) = retire {
        obligation_key_free(store, attempt, obligation).await?;
    }
    compensated_stop(store, attempt, session, cause).await
}

/// What the re-read after a part commit answered `Conflict` found ([`lost_commit`]).
#[derive(Debug)]
enum Lost {
    /// A conflict no read explains: the same batch again.
    Unexplained,
    /// The live session the commit lost to, the same one or the newer `Open` one a rollback
    /// left: compensate under it and answer [`Refusal::OperationAborted`].
    Compensate(Session),
    /// The session has left `Open`, or was deleted with its slots: this refusal, with nothing more
    /// written.
    Refused(Refusal),
}

/// **Classify a part commit answered `Conflict`** (the module doc's *A commit that lost*): read the
/// part key, the session, the attempt's own slot and the session again, in that order, then judge
/// them, damage first, and answer from the last session read.
async fn lost_commit(
    store: &impl MetadataStore,
    attempt: &StagedAttempt,
    session: &Session,
    required: Option<&Bytes>,
) -> Result<Lost, CommitError> {
    let upload_id = attempt.upload_id();
    let fault = |fault| stopped(upload_id, Stop::Store(fault));
    let damage = |err| stopped(upload_id, Stop::Record(err));
    let session_key = mpu_key(upload_id);
    let part_now = store
        .get(&part_key(upload_id, attempt.part_number()))
        .await
        .map_err(fault)?;
    // With nothing in doubt only the last session read decides, so what this first session read
    // found is passed over. After an unknown outcome `settle` makes the same four reads.
    store.get(&session_key).await.map_err(fault)?;
    let slot = store.get(attempt.slot_key()).await.map_err(fault)?;
    let last = store.get(&session_key).await.map_err(fault)?;
    // A changed part key is another writer's record only if it decodes (ADR-0045).
    if part_now.as_ref() != required {
        if let Some(bytes) = &part_now {
            decode_part_record(bytes).map_err(damage)?;
        }
    }
    let slot = judge_slot(slot.as_ref(), attempt).map_err(damage)?;
    match (judge_session(last.as_ref(), session).map_err(damage)?, slot) {
        (SessionSeen::Gone, _) => session_gone(store, attempt, session)
            .await
            .map(Lost::Refused),
        (_, SlotSeen::Rewritten) | (SessionSeen::Same | SessionSeen::Open(_), SlotSeen::Gone) => {
            Err(slot_lost(attempt))
        }
        // Not `Open` any more: the residue is the reaper's.
        (SessionSeen::Left(refusal), _) => Ok(Lost::Refused(refusal)),
        // A Complete fenced and rolled the session back: the reaper does not walk an `Open`
        // session, so this attempt compensates under the newer epoch.
        (SessionSeen::Open(newer), SlotSeen::Held) => Ok(Lost::Compensate(newer)),
        (SessionSeen::Same, SlotSeen::Held) => {
            if part_now.as_ref() == required {
                Ok(Lost::Unexplained)
            } else if required.is_some() && part_now.is_none() {
                Err(damage(RecordError::PartRecordMissing {
                    part_number: attempt.part_number().get(),
                }))
            } else {
                // A same-number rival won, or a repair rewrote the prior.
                Ok(Lost::Compensate(session.clone()))
            }
        }
    }
}

/// **The commit rounds**: read the clock before each round and send it only with
/// [`DISPATCH_MARGIN_MILLIS`] left on the lease; answer `Committed` only for a round the store
/// answered as landed; classify a `Conflict` from a re-read ([`lost_commit`]) and send the same
/// batch again only for a conflict no read explains, within [`COMMIT_ROUNDS`], reading a
/// re-upload's obligation key before the compensation that ends those rounds
/// ([`unexplained_stop`]); settle an unknown outcome from reads alone, sending nothing more
/// ([`settle`]).
async fn commit_rounds(
    store: &impl MetadataStore,
    clock: &impl StageClock,
    attempt: &StagedAttempt,
    session: &Session,
    lease_expiry_millis: u64,
    required: Option<&Bytes>,
    prior: Option<&PartRecord>,
) -> Result<UploadPartOutcome, CommitError> {
    let upload_id = attempt.upload_id();
    let damage = |err| stopped(upload_id, Stop::Record(err));
    let short = |now_millis: u64| {
        (lease_expiry_millis.saturating_sub(now_millis) < DISPATCH_MARGIN_MILLIS).then_some(
            Stop::LeaseShort {
                lease_expiry_millis,
                now_millis,
            },
        )
    };
    // One reading for the first round's check and for the stamp: the clock that owns the
    // attempt's leases. Every round sends this one batch, so a retry puts the same bytes.
    let now_millis = clock.now_millis();
    if let Some(cause) = short(now_millis) {
        return compensated_stop(store, attempt, session, cause).await;
    }
    let record = PartRecord::commit(
        attempt.chunks().to_vec(),
        *attempt.digest(),
        now_millis,
        session.epoch,
    )
    .map_err(damage)?;
    // The prior part's chunks, owed by a re-upload (none for an empty prior).
    let retire = obligation_for(attempt, session, prior.map_or(&[], |prior| prior.chunks()))
        .map_err(damage)?;
    let batch = commit_batch(attempt, &session.bytes, required, &record, retire.as_ref());
    let mut rounds = 0u32;
    loop {
        rounds += 1;
        match store.commit(batch.clone()).await {
            Ok(CommitOutcome::Committed) => {
                return Ok(UploadPartOutcome::Committed {
                    part_number: attempt.part_number(),
                    digest: *attempt.digest(),
                })
            }
            Ok(CommitOutcome::Conflict) => {}
            Err(fault) => {
                return match fault.downcast_ref::<CommitUnknownResult>() {
                    // Never read as `Conflict`, and never followed by another batch: the reads
                    // prove it landed, or keep the doubt.
                    Some(unknown) => {
                        let doubt = Doubt::of(DoubtfulBatch::Commit, unknown);
                        settle(store, attempt, session, required, doubt).await
                    }
                    None => Err(stopped(upload_id, Stop::Store(fault))),
                };
            }
        }
        match lost_commit(store, attempt, session, required).await? {
            Lost::Unexplained => {}
            Lost::Compensate(live) => {
                return refuse(store, attempt, &live, Refusal::OperationAborted).await
            }
            Lost::Refused(refusal) => return Ok(UploadPartOutcome::Refused(refusal)),
        }
        if rounds == COMMIT_ROUNDS {
            let cause = Stop::CommitRoundsSpent { rounds };
            return unexplained_stop(store, attempt, session, retire.as_ref(), cause).await;
        }
        // Each later round's own reading, on the same clock that owns the attempt's leases: the
        // reads and the rounds before it spent from the lease.
        if let Some(cause) = short(clock.now_millis()) {
            return unexplained_stop(store, attempt, session, retire.as_ref(), cause).await;
        }
    }
}

/// **Commit a staged part** (`0016:660`, `:1584-1653`, `:1720-1733`), answering in the landed
/// vocabulary ([`UploadPartOutcome`], [`Refusal`]). The module doc states the batches, the
/// classification of a lost commit, the stops and the bounds.
///
/// * A part past [`MAX_PART_CHUNKS`], however it was staged, is refused
///   [`Refusal::EntityTooLarge`] and compensated. So is a body truncated at the request's bound,
///   whose `chunks` is then one past what it holds, since staging did not read the rest.
/// * Otherwise the quiesce renewal runs ([`DISPATCH_MARGIN_MILLIS`]); a session it finds no longer
///   `Open` is [`Refusal::NoSuchUpload`], with nothing more written.
/// * A part that would take the session's committed chunks past [`MAX_STAGED_CHUNKS`], counted over
///   the `psum:` summaries of every other part number, is refused [`Refusal::EntityTooLarge`]
///   (`chunks` the total it would reach, `limit` the ceiling) and compensated.
/// * Otherwise the commit batch, each round sent only with [`DISPATCH_MARGIN_MILLIS`] left on the
///   lease: [`UploadPartOutcome::Committed`] once a round lands. A round answered `Conflict` is
///   classified from a re-read: a commit that lost to a live session (a same-number rival, a
///   repair of the prior, a rollback to a newer `Open` epoch) compensates and answers
///   [`Refusal::OperationAborted`]; one that lost to a fence, or to a terminal delete, answers
///   [`Refusal::NoSuchUpload`] with nothing written; a conflict no read explains sends the same
///   batch again, within [`COMMIT_ROUNDS`], and then compensates, unless a re-upload finds the
///   obligation key its commit required absent holding a record ([`Stop::ObligationKeyTaken`]).
/// * A round answered `CommitUnknownResult` is the last batch sent: no retry and no compensation.
///   Reads alone settle it (the module doc's *A commit in doubt*): [`UploadPartOutcome::Committed`]
///   when they prove this attempt's own commit landed, and otherwise [`CommitError::InDoubt`],
///   saying whether it may still land and what ended the reads.
///
/// `clock` must be the clock the attempt was staged on.
///
/// **Not cancel-safe once a batch is sent**: dropping the future can leave a batch that lands with
/// no answer naming it. The slot and every owned entry it could leave are still in reach of the
/// reaper's per-session ranges.
pub async fn commit_part(
    store: &impl MetadataStore,
    clock: &impl StageClock,
    mut attempt: StagedAttempt,
) -> Result<UploadPartOutcome, CommitError> {
    let upload_id = attempt.upload_id().clone();
    let stop = |cause| stopped(&upload_id, cause);
    let damage = |err| stopped(&upload_id, Stop::Record(err));
    let staged = Session::of(attempt.session_bytes()).map_err(damage)?;
    if let Some(refusal) = past_part_ceiling(&attempt) {
        return refuse(store, &attempt, &staged, refusal).await;
    }
    let renewed = attempt.renew(store, clock, DISPATCH_MARGIN_MILLIS).await;
    let (session, lease_expiry_millis) = match renewed {
        Ok(RenewOutcome::Covered {
            lease_expiry_millis,
            ..
        }) => (
            renewed_session(&attempt, &staged).map_err(damage)?,
            lease_expiry_millis,
        ),
        Ok(RenewOutcome::Refused(refusal)) => return Ok(UploadPartOutcome::Refused(refusal)),
        Err(error) if compensates_after(&error) => {
            let session = renewed_session(&attempt, &staged).map_err(damage)?;
            return match compensate(store, &attempt, &session).await? {
                Compensated::Released => Err(CommitError::Renewal {
                    error,
                    slot_released: true,
                }),
                Compensated::Left(refusal) => Ok(UploadPartOutcome::Refused(refusal)),
            };
        }
        // Nothing more is sent, so nothing is judged: a renewal in doubt keeps its doubt.
        Err(error) => {
            return Err(CommitError::Renewal {
                error,
                slot_released: false,
            })
        }
    };
    // The commit's reads, after the last renewal: no renewal is sent from here on.
    let total = staged_elsewhere(store, &attempt)
        .await
        .map_err(stop)?
        .saturating_add(attempt.chunks().len() as u64);
    if total > u64::from(MAX_STAGED_CHUNKS) {
        let refusal = Refusal::EntityTooLarge {
            chunks: total,
            limit: u64::from(MAX_STAGED_CHUNKS),
        };
        return refuse(store, &attempt, &session, refusal).await;
    }
    let required = store
        .get(&part_key(&upload_id, attempt.part_number()))
        .await
        .map_err(|fault| stop(Stop::Store(fault)))?;
    let prior = required
        .as_deref()
        .map(decode_part_record)
        .transpose()
        .map_err(damage)?;
    commit_rounds(
        store,
        clock,
        &attempt,
        &session,
        lease_expiry_millis,
        required.as_ref(),
        prior.as_ref(),
    )
    .await
}

/// **`UploadPart`, whole**: reserve a slot, stage the body ([`stage_part`]), then commit it
/// ([`commit_part`]), on one runtime whose clock owns the attempt's leases. A staging refusal is
/// answered as it is; a staging error is [`UploadPartError::Stage`] and no commit is attempted.
///
/// No production path calls this yet (the S3 verb is #669).
pub async fn upload_part<B>(
    store: &impl MetadataStore,
    chunk_store: &impl PlacementChunkStore,
    runtime: &mut impl StageRuntime,
    request: &StageRequest<'_>,
    body: B,
) -> Result<UploadPartOutcome, UploadPartError>
where
    B: Stream<Item = wyrd_traits::Result<Bytes>> + Unpin,
{
    match stage_part(store, chunk_store, runtime, request, body).await {
        Ok(StageOutcome::Staged(attempt)) => commit_part(store, &*runtime, *attempt)
            .await
            .map_err(UploadPartError::Commit),
        Ok(StageOutcome::Refused(refusal)) => Ok(UploadPartOutcome::Refused(refusal)),
        Err(err) => Err(UploadPartError::Stage(err)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multipart::{AttemptId, PartNumber};

    fn stopped_by(cause: StageStop) -> StageError {
        StageError::Stopped {
            upload_id: UploadId::new("0".repeat(32)).unwrap(),
            cause,
        }
    }

    /// The table in the module doc, read row by row. `EntryLost` has its own row here because
    /// `StagedAttempt::renew` cannot produce it (only a chunk's intent does, `stage.rs`), so no
    /// store history can drive that row through the renewal; the crate's test file drives the
    /// others, and `EntryLost` through the composed `upload_part`.
    #[test]
    fn a_stopped_renewal_compensates_only_on_the_rows_that_say_so() {
        let key = || "k".to_owned();
        for (cause, compensates) in [
            (
                StageStop::CoverShort {
                    rounds: 1,
                    cover_millis: 1,
                    lease_expiry_millis: 1,
                    checked_at_millis: 1,
                },
                true,
            ),
            (StageStop::RenewalRoundsSpent { rounds: 1 }, true),
            (
                StageStop::LeaseLapsed {
                    key: key(),
                    lease_expiry_millis: 1,
                    now_millis: 1,
                },
                true,
            ),
            (StageStop::InvalidRequest { reason: "r" }, true),
            (StageStop::SlotLost { key: key() }, false),
            (StageStop::EntryLost { key: key() }, false),
            (StageStop::Store("fault".into()), false),
        ] {
            let error = stopped_by(cause);
            assert_eq!(compensates_after(&error), compensates, "{error}");
        }
    }

    /// `RetireObligation` keeps refusing an empty list, on the writer's route as on decode's
    /// (`crates/core/tests/multipart_retire_obligation.rs`), which is why a batch with nothing to
    /// retire installs no obligation.
    #[test]
    fn the_per_part_obligation_refuses_an_empty_list() {
        let upload_id = UploadId::new("0".repeat(32)).unwrap();
        let attempt_id = AttemptId::new("1".repeat(32)).unwrap();
        let part_number = PartNumber::new(1).unwrap();
        assert!(matches!(
            RetireObligation::part_chunks(&upload_id, 0, part_number, &attempt_id, &[]),
            Err(RecordError::RetireObligationOwesNothing {
                component: "chunks"
            })
        ));
    }
}
