#![cfg(shuttle)]
//! ⚜ THE REPRODUCER OF THE PHANTOM PAGE ⚜
//! ================================================================
//! A rite to summon the corruption `Invalid page type 0` — first heard as a
//! single damning cry from the Antithesis proving-grounds whilst the
//! `turso-stress` engine laboured — into the clean light of the deterministic
//! `shuttle` scheduler, through the honest public `turso` API alone.
//!
//! # The nature of the fiend (PR #7658)
//!
//! The shared write-ahead-log frame index — `WalFileShared.runtime.frame_cache`,
//! a map of `page -> [frame slots]` common to every connection of one database —
//! could be made to point a page at a physical frame slot that held the bytes of
//! a *different* page (or a page never yet written: a leaf of pure zeros). The
//! pre-fix `cache_frame` only ever *appended*; it never purged a stale
//! `page -> frame` mapping when the WAL append position rewound and a slot was
//! reused — by an aborted/uncommitted append, or by another connection reusing
//! the slots after a rewind. `find_frame` then handed a cursor a frame whose
//! bytes belonged elsewhere, surfacing as
//! `non-index page` / `Invalid page type: 0` / `inconsistent overflow chain`.
//!
//! The fiend is a **cross-connection race**. Within one connection the spill
//! frames are never reused (they chain forward), so a lone thread cannot rouse
//! it. It wants concurrent writers churning the log — spilling over-large sigils
//! under a starved page-cache, then ROLLING BACK to rewind the append position —
//! and concurrent readers whose read-marks pin the log open, forbidding the
//! restart that would sweep `frame_cache` clean. Hence the Rust-and-shuttle road.
//!
//! # The rite
//!
//! We besiege one WAL-journalled archive whose page-cache is starved to the
//! floor (so a single over-large sigil spills uncommitted frames into the log):
//!   * **Scribes of Ruin** — writer connections that churn the archive with
//!     over-large sigils and, by the omen, ROLL BACK — rewinding the append
//!     position and abandoning frame slots for the next writer to reuse — and now
//!     and again wrench the log through a checkpoint.
//!   * **Wardens of the Gate** — reader connections that open a read transaction,
//!     pin a read-mark, and scour every leaf with `PRAGMA integrity_check`; it is
//!     their honest cursors the phantom betrays.
//!
//! Each besieger mints its connection ONCE, before the churn — never mid-siege —
//! so no two of `shuttle`'s green threads ever contend the `turso` binding's
//! (real, uninstrumented) database lock, which would otherwise deadlock the
//! single-threaded scheduler. The workload is wholly deterministic (every choice
//! from the scribe's number and the round); all nondeterminism is the
//! scheduler's, replayable to the last step via `SHUTTLE_RANDOM_SEED`.
//!
//! On the cursed (pre-fix) engine some interleaving hands a warden a void /
//! aliased page and the query dies `Corrupt(...)`; on the blessed (post-fix)
//! engine none can.

use shuttle::scheduler::PctScheduler;
use turso::Builder;
use turso_stress::sync::atomic::{AtomicBool, Ordering};
use turso_stress::sync::Arc;

/// The reserved chronicle-ids each Scribe of Ruin holds dominion over.
const CHRONICLES_PER_SCRIBE: i64 = 6;

/// The inscriptions by which a phantom (void / aliased) page betrays itself.
/// The honest clamour of `Busy` / `BusySnapshot` contention is NOT among them.
const THE_SIGILS_OF_THE_PHANTOM: &[&str] = &[
    "Invalid page type",
    "non-index page",
    "inconsistent overflow",
    "Corrupt",
    "not a b-tree",
    "Invalid page number",
];

fn shuttle_config() -> shuttle::Config {
    turso_stress::shuttle_config()
}

fn knob(name: &str, default: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse::<i64>().ok())
        .unwrap_or(default)
}

/// Does this lament bear the sigil of the phantom — the corruption we hunt — or
/// is it merely the honest, expected protest of concurrent contention?
fn bears_the_sigil_of_the_phantom(the_lament: &turso::Error) -> bool {
    if matches!(the_lament, turso::Error::Corrupt(_)) {
        return true;
    }
    let inscription = the_lament.to_string();
    THE_SIGILS_OF_THE_PHANTOM
        .iter()
        .any(|sigil| inscription.contains(sigil))
}

/// Intone an incantation that yields no rows (DDL / DML / transaction verbs).
async fn intone(conn: &turso::Connection, incantation: &str) -> turso::Result<()> {
    conn.execute(incantation, ()).await.map(|_| ())
}

/// Intone an incantation that may yield rows (PRAGMA / SELECT) and drain it to
/// the dregs, so every page the cursor must touch is truly touched.
async fn intone_and_drain(conn: &turso::Connection, incantation: &str) -> turso::Result<usize> {
    let mut rows = conn.query(incantation, ()).await?;
    let mut tally = 0usize;
    while let Some(row) = rows.next().await? {
        let mut column = 0;
        while let Ok(value) = row.get_value(column) {
            std::hint::black_box(&value);
            column += 1;
        }
        tally += 1;
    }
    Ok(tally)
}

/// Raise the besieged archive: one WAL-journalled database whose page-cache is
/// starved to its floor, so a single over-large sigil overflows it and SPILLS
/// uncommitted frames into the log — the frames whose slots, once rewound, the
/// phantom haunts.
async fn raise_the_besieged_archive(num_scribes: i64) -> (turso::Database, tempfile::TempDir) {
    let sanctum = tempfile::tempdir().unwrap();
    let reliquary = sanctum.path().join("the-besieged-archive.db");
    let the_archive = Builder::new_local(reliquary.to_str().unwrap())
        .build()
        .await
        .unwrap();

    let founder = the_archive.connect().unwrap();
    intone_and_drain(&founder, "PRAGMA journal_mode = WAL")
        .await
        .unwrap();
    intone(&founder, "PRAGMA cache_size = 1").await.unwrap();
    intone(
        &founder,
        "CREATE TABLE the_besieged_archive(chronicle_id INTEGER PRIMARY KEY, phantom_sigil BLOB)",
    )
    .await
    .unwrap();
    for chronicle_id in 0..(num_scribes * CHRONICLES_PER_SCRIBE + 2) {
        let sprawl = 200 + (chronicle_id as usize % 6) * 700;
        intone(
            &founder,
            &format!(
                "INSERT INTO the_besieged_archive(chronicle_id, phantom_sigil) \
                 VALUES({chronicle_id}, zeroblob({sprawl}))"
            ),
        )
        .await
        .unwrap();
    }
    // Fold the seed into the main file so the siege begins from a clean log.
    let _ = intone_and_drain(&founder, "PRAGMA wal_checkpoint(TRUNCATE)").await;

    (the_archive, sanctum)
}

/// A Scribe of Ruin: a writer that churns its reserved chronicles with
/// over-large sigils (which SPILL under the starved cache) and, by the omen,
/// ROLLS BACK — rewinding the append position and abandoning frame slots for the
/// next writer to reuse — and now and again wrenches the log through a
/// checkpoint. Its connection is minted once, before the siege.
async fn the_scribe_of_ruin(
    conn: turso::Connection,
    scribe_number: i64,
    rounds: i64,
    the_phantom_manifested: Arc<AtomicBool>,
) {
    let _ = intone(&conn, "PRAGMA cache_size = 1").await;
    let dominion_base = scribe_number * CHRONICLES_PER_SCRIBE;

    for round in 0..rounds {
        if the_phantom_manifested.load(Ordering::SeqCst) {
            return;
        }
        let omen = scribe_number.wrapping_mul(7).wrapping_add(round);
        let target = dominion_base + (round % CHRONICLES_PER_SCRIBE);
        let sprawl = 400 + ((omen as usize) % 6) * 700;
        let shall_rewind = omen % 2 == 0;
        let shall_wrench_the_log = round % 3 == 2;

        let outcome: turso::Result<()> = async {
            intone(&conn, "BEGIN").await?;
            intone(
                &conn,
                &format!(
                    "UPDATE the_besieged_archive SET phantom_sigil = zeroblob({sprawl}) \
                     WHERE chronicle_id = {target}"
                ),
            )
            .await?;
            intone(
                &conn,
                &format!(
                    "INSERT OR REPLACE INTO the_besieged_archive(chronicle_id, phantom_sigil) \
                     VALUES({}, zeroblob({}))",
                    target + 1,
                    sprawl + 200
                ),
            )
            .await?;
            if shall_rewind {
                intone(&conn, "ROLLBACK").await?;
            } else {
                intone(&conn, "COMMIT").await?;
            }
            if shall_wrench_the_log {
                let mode = if omen % 2 == 0 { "TRUNCATE" } else { "PASSIVE" };
                intone_and_drain(&conn, &format!("PRAGMA wal_checkpoint({mode})"))
                    .await
                    .map(|_| ())?;
            }
            Ok(())
        }
        .await;

        // Yield the loom between rounds so the scheduler weaves fairly and the
        // scribes' rewinds fall within the wardens' held snapshots.
        shuttle::future::yield_now().await;

        match outcome {
            Ok(()) => {}
            Err(ref lament) if bears_the_sigil_of_the_phantom(lament) => {
                eprintln!("⚜ THE PHANTOM MANIFESTED to a scribe: {lament}");
                the_phantom_manifested.store(true, Ordering::SeqCst);
                return;
            }
            Err(_honest_clamour) => {
                let _ = intone(&conn, "ROLLBACK").await;
            }
        }
    }
}

/// A Warden of the Gate: a reader that opens a read transaction, pins a
/// read-mark (forbidding the log restart that would sweep the frame index
/// clean), scours every leaf, and walks the whole b-tree with
/// `PRAGMA integrity_check` — the honest cursor the phantom betrays.
async fn the_warden_of_the_gate(
    conn: turso::Connection,
    rounds: i64,
    the_phantom_manifested: Arc<AtomicBool>,
) {
    for _round in 0..rounds {
        if the_phantom_manifested.load(Ordering::SeqCst) {
            return;
        }
        let vigil: turso::Result<()> = async {
            intone(&conn, "BEGIN").await?;
            let _ = intone_and_drain(
                &conn,
                "SELECT chronicle_id, phantom_sigil FROM the_besieged_archive",
            )
            .await?;
            shuttle::future::yield_now().await;
            let _ = intone_and_drain(&conn, "PRAGMA integrity_check").await?;
            let _ = intone(&conn, "COMMIT").await;
            Ok(())
        }
        .await;

        match vigil {
            Ok(()) => {}
            Err(ref lament) if bears_the_sigil_of_the_phantom(lament) => {
                eprintln!("⚜ THE PHANTOM MANIFESTED to a warden: {lament}");
                the_phantom_manifested.store(true, Ordering::SeqCst);
                return;
            }
            Err(_honest_clamour) => {
                let _ = intone(&conn, "ROLLBACK").await;
            }
        }
    }
}

/// The siege proper: raise the archive, loose the scribes and wardens (each with
/// its own connection, minted once), and at the last pronounce judgement.
async fn the_siege_of_the_archive(num_scribes: i64, num_wardens: i64, rounds: i64) {
    let (the_archive, _sanctum) = raise_the_besieged_archive(num_scribes).await;
    let the_phantom_manifested = Arc::new(AtomicBool::new(false));

    let mut the_besiegers = Vec::new();

    for scribe_number in 0..num_scribes {
        let conn = the_archive.connect().unwrap();
        let the_phantom_manifested = the_phantom_manifested.clone();
        the_besiegers.push(turso_stress::future::spawn(async move {
            the_scribe_of_ruin(conn, scribe_number, rounds, the_phantom_manifested).await;
        }));
    }
    for _warden in 0..num_wardens {
        let conn = the_archive.connect().unwrap();
        let the_phantom_manifested = the_phantom_manifested.clone();
        the_besiegers.push(turso_stress::future::spawn(async move {
            the_warden_of_the_gate(conn, rounds, the_phantom_manifested).await;
        }));
    }

    for besieger in the_besiegers {
        let _ = besieger.await;
    }

    assert!(
        !the_phantom_manifested.load(Ordering::SeqCst),
        "⚜ THE PHANTOM PAGE MANIFESTED: a warden's honest cursor was handed the bytes of a \
         void / aliased page (`Invalid page type: 0` or kin) through the shared WAL frame \
         index. The corruption of PR #7658 has been summoned forth."
    );
}

/// ⚜ THE GRAND SUMMONING ⚜
///
/// The `PctScheduler` is the surest hunter of a rare, few-preemption race: it
/// plants a bounded number of priority-change points, reaching deep interleavings
/// that pure random scheduling seldom finds. Every knob bows to an environment
/// override, that the hunter may widen the net without recompiling.
#[test]
fn shuttle_phantom_page_invalid_page_type_zero() {
    let preemptions = knob("PHANTOM_PREEMPTIONS", 5) as usize;
    let schedules = knob("PHANTOM_SCHEDULES", 1500) as usize;
    let scribes = knob("PHANTOM_SCRIBES", 3);
    let wardens = knob("PHANTOM_WARDENS", 2);
    let rounds = knob("PHANTOM_ROUNDS", 6);
    let scheduler = PctScheduler::new(preemptions, schedules);
    let runner = shuttle::Runner::new(scheduler, shuttle_config());
    runner
        .run(move || shuttle::future::block_on(the_siege_of_the_archive(scribes, wardens, rounds)));
}
