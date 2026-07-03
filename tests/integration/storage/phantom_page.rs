//! # The Reproduction of the Phantom Page
//!
//! A deterministic re-enactment of the WAL frame-cache slot-reuse corruption —
//! the dreaded **"Invalid page type: 0"** spectre — that first manifested in the
//! Antithesis proving-grounds while the `turso-stress` engine laboured through
//! its trials. Sealed by PR #7658 (commit `80f8cdd6`).
//!
//! ## The Nature of the Haunting
//!
//! Within a single WAL generation, frames are appended bearing strictly
//! ascending numbers. A shared ledger — the *frame-cache* — maps every
//! `page -> [frame slots]`. Three deeds, performed in sequence, summon the
//! phantom:
//!
//! 1. **The Doomed Scribe spills, then vanishes.** A connection labouring under
//!    a starved page-cache must SPILL its surplus dirtied leaves to the WAL
//!    mid-transaction. Each spilled leaf is APPENDED to the log and INSCRIBED
//!    into the shared frame-cache — yet it bears no commit marker. Should that
//!    scribe then perish WITHOUT a graceful rollback (a dropped connection
//!    merely surrenders its write-lock; it does NOT purge the cache), its
//!    `page -> frame` inscriptions linger like a phantom over slots the log no
//!    longer considers committed.
//!
//! 2. **The Sealing Scribe reuses the haunted slots.** A fresh connection,
//!    beginning from the still-committed watermark, appends its OWN pages into
//!    those very same frame-slots and COMMITS. Pre-fix, `cache_frame` neglected
//!    to evict the phantom inscriptions when a slot was overwritten — so a
//!    doomed leaf and a freshly-sealed overflow page now BOTH claim the slot.
//!
//! 3. **The Warden reads, and recoils.** A later reader, consulting `find_frame`
//!    for one of the vanished scribe's leaves, is handed a slot that now
//!    physically holds an OVERFLOW page. An overflow page's leading octet is
//!    `0x00`; misread as a b-tree page, the engine recoils —
//!    *"Invalid page type: 0"*.
//!
//! ## The Instruments of Proof
//!
//! This rite provokes the spectre through USER-FACING APIs alone: three
//! connections, plain SQL, and a single connection dropped mid-transaction to
//! serve as the ungraceful abort. No peering into the engine's inner chambers;
//! no yield injection; no summoning of many threads. A single thread suffices,
//! for the abort — a dropped connection that skips rollback — is expressible
//! only in Rust, yet demands no scheduler sorcery whatsoever.
//!
//! On the HEALED engine (HEAD), the Warden reads the chronicle entire and finds
//! it whole. On the AFFLICTED engine (pre-#7658), the Warden is handed phantom
//! overflow bytes and the corruption surfaces. The assertion therefore GUARDS
//! the fix: it passes upon healed stone and fails upon cursed.

use crate::common::{limbo_exec_rows, limbo_exec_rows_fallible, TempDatabase};
use std::sync::Arc;
use turso_core::{Connection, LimboError};

/// The page-cache is starved to the engine's very floor (200 leaves). Once 180
/// dirtied leaves crowd it (the 90% spill-threshold), the pager must SPILL its
/// surplus to the WAL — the deed that inscribes uncommitted frames into the
/// shared cache.
const THE_STARVED_CACHE_OF_LEAVES: i64 = 200;

/// The Victim Chronicle is sown with leaves beyond counting so that the Doomed
/// Scribe's spill floods hundreds of frame-slots — a broad haunting the Warden
/// cannot fail to tread upon.
const THE_MULTITUDE_OF_VICTIM_ROWS: i64 = 9_000;

/// Each inscription is padded to this width so that only a handful nest upon
/// each leaf — multiplying the leaves, and thus the spilled, haunted frames.
const THE_WIDTH_OF_EACH_INSCRIPTION: i64 = 180;

/// The Great Sigil: eight hundred thousand bytes of void, which the engine must
/// scatter across ~195 overflow pages. These are the phantom bytes (leading
/// octet `0x00`) that — misread as a b-tree page — become "Invalid page type: 0".
const THE_GREAT_SIGIL_BYTES: i64 = 800_000;

/// The litany of sigils by which we know the phantom's touch upon the stone.
/// Chief among them is "Invalid page type: 0", but the aliasing may equally
/// deliver a mangled overflow chain or a leaf mistaken for an index.
fn bears_the_sigil_of_the_phantom(lament: &LimboError) -> bool {
    let utterance = lament.to_string();
    const SIGILS: &[&str] = &[
        "Invalid page type",
        "non-index page",
        "inconsistent overflow",
        "not a b-tree",
        "Invalid page number",
        "Corrupt",
    ];
    matches!(lament, LimboError::Corrupt(_)) || SIGILS.iter().any(|sigil| utterance.contains(sigil))
}

/// Drive a decree to its conclusion, brooking no failure — for the raising of
/// the chronicle and the deeds of the scribes must all succeed; only the
/// Warden's final reading is permitted to unearth corruption.
fn decree(the_scribe: &Arc<Connection>, the_incantation: &str) {
    limbo_exec_rows(the_scribe, the_incantation);
}

/// The multitude is sown in batches of this many rows per incantation — for
/// Turso does not yet wield recursive CTEs, and a single colossal VALUES clause
/// would be unwieldy.
const THE_BATCH_OF_THE_SOWING: i64 = 1_000;

/// Forge the Victim Chronicle: a broad table of nine thousand slender rows,
/// then CHECKPOINT it wholly into the database file so the WAL begins the rite
/// pristine, its frame-numbering reset to a clean and predictable dawn.
fn raise_the_victim_chronicle(the_archive: &TempDatabase) {
    let the_founder = the_archive.connect_limbo();
    decree(
        &the_founder,
        "CREATE TABLE theVictimChronicle (theSeal INTEGER PRIMARY KEY, theInscription TEXT)",
    );
    // Sow every row in fixed batches; each inscription is padded to a fixed
    // width so the leaves multiply into the hundreds.
    let the_width = THE_WIDTH_OF_EACH_INSCRIPTION as usize;
    let mut the_seal: i64 = 1;
    while the_seal <= THE_MULTITUDE_OF_VICTIM_ROWS {
        let the_horizon =
            (the_seal + THE_BATCH_OF_THE_SOWING - 1).min(THE_MULTITUDE_OF_VICTIM_ROWS);
        let mut the_litany =
            String::from("INSERT INTO theVictimChronicle (theSeal, theInscription) VALUES ");
        let mut the_quill = the_seal;
        while the_quill <= the_horizon {
            if the_quill > the_seal {
                the_litany.push(',');
            }
            the_litany.push_str(&format!("({the_quill}, '{the_quill:0>the_width$}')"));
            the_quill += 1;
        }
        decree(&the_founder, &the_litany);
        the_seal = the_horizon + 1;
    }
    // The Phantom Hoard is raised empty and BEFORE the checkpoint, so that the
    // Sealing Scribe's later reuse is a single, clean commit — its overflow
    // pages falling upon the very slots the Doomed Scribe abandoned.
    decree(
        &the_founder,
        "CREATE TABLE thePhantomHoard (theSeal INTEGER PRIMARY KEY, theGreatSigil BLOB)",
    );
    // Fold the whole chronicle into the database file and TRUNCATE the WAL, so
    // the Doomed Scribe's spill will begin inscribing frames from slot 1.
    decree(&the_founder, "PRAGMA wal_checkpoint(TRUNCATE)");
}

/// **Deed the First — The Doomed Scribe.** Under a starved cache, open a
/// transaction and rewrite every inscription, forcing the pager to SPILL
/// hundreds of leaves into the WAL as uncommitted frames. Then let the scribe
/// VANISH — the connection is dropped mid-transaction, surrendering only its
/// write-lock and leaving its phantom inscriptions upon the shared cache.
fn the_doomed_scribe_spills_and_vanishes(the_archive: &TempDatabase) {
    let the_doomed_scribe = the_archive.connect_limbo();
    // Starve the cache to its floor so a mere 180 dirtied leaves provoke a spill.
    decree(
        &the_doomed_scribe,
        &format!("PRAGMA cache_size = {THE_STARVED_CACHE_OF_LEAVES}"),
    );
    // An EXPLICIT transaction — without it the rewrite would auto-commit and no
    // frame would ever be abandoned.
    decree(&the_doomed_scribe, "BEGIN");
    // Touch the header page (page 1) so it too joins the spill and claims frame
    // slot 1 with a phantom self-mapping. Thus when the Sealing Scribe later
    // rewrites page 1 into that same slot, page 1 aliases ITSELF (harmless) and
    // the victim leaves are shunted upward onto the OVERFLOW slots — where the
    // phantom's leading 0x00 becomes the true "Invalid page type: 0".
    decree(&the_doomed_scribe, "PRAGMA user_version = 777");
    // Rewrite every inscription (same width, fresh content) to dirty every leaf.
    // Once the dirtied leaves crowd past 180, the surplus SPILLS to the WAL.
    decree(
        &the_doomed_scribe,
        &format!(
            "UPDATE theVictimChronicle \
             SET theInscription = printf('%.*d', {THE_WIDTH_OF_EACH_INSCRIPTION}, theSeal + 1000000)"
        ),
    );
    // The scribe VANISHES with no COMMIT and no ROLLBACK. Connection::drop on the
    // WAL path calls only end_write_tx (releasing the write-lock); it does NOT
    // purge the frame-cache. The spilled leaves' inscriptions now haunt the
    // shared ledger, their slots deemed uncommitted by the log.
    drop(the_doomed_scribe);
}

/// **Deed the Second — The Sealing Scribe.** A fresh connection, beginning from
/// the still-committed watermark, appends a single colossal blob whose ~195
/// overflow pages REUSE the haunted frame-slots, and COMMITS. Pre-fix, the
/// phantom leaf-inscriptions are not evicted when their slots are overwritten,
/// so leaf and overflow page now both claim the same slots.
fn the_sealing_scribe_reuses_the_haunted_slots(the_archive: &TempDatabase) {
    let the_sealing_scribe = the_archive.connect_limbo();
    // A SINGLE commit reuses the haunted slots: page 1 falls upon slot 1 (where
    // it aliases the Doomed Scribe's own page-1 phantom), and the blob's ~195
    // overflow pages fall upon the slots the abandoned victim leaves still claim.
    decree(&the_sealing_scribe, "BEGIN");
    decree(
        &the_sealing_scribe,
        &format!(
            "INSERT INTO thePhantomHoard (theSeal, theGreatSigil) VALUES (1, zeroblob({THE_GREAT_SIGIL_BYTES}))"
        ),
    );
    // The commit publishes the overflow frames into the reused slots and advances
    // the committed watermark to embrace them — arming the phantom for the Warden.
    decree(&the_sealing_scribe, "COMMIT");
}

/// **Deed the Third — The Warden Reads.** A pristine reader, its snapshot now
/// embracing the reused slots, scans the Victim Chronicle entire. On healed
/// stone it reads every inscription whole; on cursed stone, `find_frame` hands
/// it a haunted slot bearing overflow bytes and the corruption is unearthed.
fn the_warden_reads_the_chronicle(the_archive: &TempDatabase) -> Result<usize, LimboError> {
    let the_warden = the_archive.connect_limbo();
    // The Warden reads from the chronicle's LAST leaf backward. The highest seals
    // dwell in leaves the Doomed Scribe spilled beyond the committed watermark —
    // never aliased, read whole from the database file. Descending, the Warden
    // reaches the leaves whose slots the Sealing Scribe overwrote with OVERFLOW
    // pages, and there, upon cursed stone, meets the phantom's leading `0x00`:
    // the true and dreaded "Invalid page type: 0".
    let the_tally = limbo_exec_rows_fallible(
        the_archive,
        &the_warden,
        "SELECT * FROM theVictimChronicle ORDER BY theSeal DESC",
    )?;
    // A second, deeper augury: the engine's own integrity_check must also find
    // the chronicle whole.
    let the_verdict = limbo_exec_rows_fallible(the_archive, &the_warden, "PRAGMA integrity_check")?;
    let the_pronouncement = the_verdict
        .first()
        .and_then(|row| row.first())
        .and_then(|value| match value {
            rusqlite::types::Value::Text(pronouncement) => Some(pronouncement.clone()),
            _ => None,
        })
        .unwrap_or_default();
    if the_pronouncement != "ok" {
        return Err(LimboError::Corrupt(format!(
            "integrity_check pronounced the chronicle unwhole: {the_pronouncement}"
        )));
    }
    Ok(the_tally.len())
}

/// # The Rite Entire
///
/// Guards the seal of PR #7658. Upon HEALED stone the Warden reads the chronicle
/// whole and this rite PASSES; upon AFFLICTED stone (the frame-cache aliasing
/// unfixed) the Warden is handed phantom overflow bytes and this rite FAILS —
/// exactly the regression contract the fix demands.
#[test]
fn the_phantom_page_shall_not_alias_a_reused_frame_slot() {
    let the_archive = TempDatabase::builder()
        // Vanilla settings (no encryption) so the phantom's bytes surface as a
        // raw "Invalid page type", not a decryption lament.
        .with_opts(turso_core::DatabaseOpts::new())
        .build();

    raise_the_victim_chronicle(&the_archive);
    the_doomed_scribe_spills_and_vanishes(&the_archive);
    the_sealing_scribe_reuses_the_haunted_slots(&the_archive);

    match the_warden_reads_the_chronicle(&the_archive) {
        Ok(the_tally) => {
            // The stone is healed: every inscription was read whole.
            assert_eq!(
                the_tally as i64, THE_MULTITUDE_OF_VICTIM_ROWS,
                "the Warden should read every inscription of the Victim Chronicle"
            );
        }
        Err(the_lament) => {
            assert!(
                bears_the_sigil_of_the_phantom(&the_lament),
                "the Warden was thwarted, yet by no phantom sigil we recognize: {the_lament}"
            );
            panic!(
                "THE PHANTOM PAGE HAS MANIFESTED — the WAL frame-cache aliased a reused slot \
                 and the Warden read overflow bytes as a b-tree page: {the_lament}"
            );
        }
    }
}
