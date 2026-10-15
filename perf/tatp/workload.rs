//! The TATP workload, as `upstream/doc/TATP_Description.pdf` and the
//! reference implementation in `upstream/source/src/` define it: the
//! schema, the rows that populate it, the seven transactions and the keys
//! they look up. Nothing here touches a database; the engines turn what this
//! module generates into statements.

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

/// `upstream/bin/targetDBSchema.sql` without its `DROP TABLE`s and the
/// `tps` table, which no transaction uses.
pub const SCHEMA: &str = "
CREATE TABLE subscriber
    (s_id INTEGER NOT NULL PRIMARY KEY, sub_nbr VARCHAR(15) NOT NULL UNIQUE,
    bit_1 TINYINT, bit_2 TINYINT, bit_3 TINYINT, bit_4 TINYINT, bit_5 TINYINT,
    bit_6 TINYINT, bit_7 TINYINT, bit_8 TINYINT, bit_9 TINYINT, bit_10 TINYINT,
    hex_1 TINYINT, hex_2 TINYINT, hex_3 TINYINT, hex_4 TINYINT, hex_5 TINYINT,
    hex_6 TINYINT, hex_7 TINYINT, hex_8 TINYINT, hex_9 TINYINT, hex_10 TINYINT,
    byte2_1 SMALLINT, byte2_2 SMALLINT, byte2_3 SMALLINT, byte2_4 SMALLINT, byte2_5 SMALLINT,
    byte2_6 SMALLINT, byte2_7 SMALLINT, byte2_8 SMALLINT, byte2_9 SMALLINT, byte2_10 SMALLINT,
    msc_location INTEGER, vlr_location INTEGER);
CREATE TABLE access_info
    (s_id INTEGER NOT NULL, ai_type TINYINT NOT NULL,
    data1 SMALLINT, data2 SMALLINT, data3 CHAR(3), data4 CHAR(5),
    PRIMARY KEY (s_id, ai_type),
    FOREIGN KEY (s_id) REFERENCES subscriber (s_id));
CREATE TABLE special_facility
    (s_id INTEGER NOT NULL, sf_type TINYINT NOT NULL, is_active TINYINT NOT NULL,
    error_cntrl SMALLINT, data_a SMALLINT, data_b CHAR(5),
    PRIMARY KEY (s_id, sf_type),
    FOREIGN KEY (s_id) REFERENCES subscriber (s_id));
CREATE TABLE call_forwarding
    (s_id INTEGER NOT NULL, sf_type TINYINT NOT NULL, start_time TINYINT NOT NULL,
    end_time TINYINT, numberx VARCHAR(15),
    PRIMARY KEY (s_id, sf_type, start_time),
    FOREIGN KEY (s_id, sf_type) REFERENCES special_facility(s_id, sf_type));
";

pub const INSERT_SUBSCRIBER: &str = "INSERT INTO subscriber VALUES \
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
pub const INSERT_ACCESS_INFO: &str = "INSERT INTO access_info VALUES (?, ?, ?, ?, ?, ?)";
pub const INSERT_SPECIAL_FACILITY: &str = "INSERT INTO special_facility VALUES (?, ?, ?, ?, ?, ?)";
pub const INSERT_CALL_FORWARDING: &str = "INSERT INTO call_forwarding VALUES (?, ?, ?, ?, ?)";

/// The statements of the seven transactions, from
/// `upstream/bin/tr_mix_generic.sql`, with the reference's placeholders
/// turned into parameters.
pub const GET_SUBSCRIBER_DATA: &str = "SELECT s_id, sub_nbr,
    bit_1, bit_2, bit_3, bit_4, bit_5, bit_6, bit_7, bit_8, bit_9, bit_10,
    hex_1, hex_2, hex_3, hex_4, hex_5, hex_6, hex_7, hex_8, hex_9, hex_10,
    byte2_1, byte2_2, byte2_3, byte2_4, byte2_5, byte2_6, byte2_7, byte2_8, byte2_9, byte2_10,
    msc_location, vlr_location
    FROM subscriber WHERE s_id = ?";
pub const GET_NEW_DESTINATION: &str = "SELECT cf.numberx
    FROM special_facility AS sf, call_forwarding AS cf
    WHERE (sf.s_id = ?1 AND sf.sf_type = ?2 AND sf.is_active = 1)
    AND (cf.s_id = sf.s_id AND cf.sf_type = sf.sf_type)
    AND (cf.start_time <= ?3 AND ?4 < cf.end_time)";
pub const GET_ACCESS_DATA: &str =
    "SELECT data1, data2, data3, data4 FROM access_info WHERE s_id = ? AND ai_type = ?";
pub const UPDATE_SUBSCRIBER_BIT: &str = "UPDATE subscriber SET bit_1 = ? WHERE s_id = ?";
pub const UPDATE_SPECIAL_FACILITY: &str =
    "UPDATE special_facility SET data_a = ? WHERE s_id = ? AND sf_type = ?";
pub const UPDATE_LOCATION: &str = "UPDATE subscriber SET vlr_location = ? WHERE sub_nbr = ?";
pub const SUBSCRIBER_BY_NUMBER: &str = "SELECT s_id FROM subscriber WHERE sub_nbr = ?";
pub const SPECIAL_FACILITY_TYPES: &str = "SELECT sf_type FROM special_facility WHERE s_id = ?";
pub const DELETE_CALL_FORWARDING: &str =
    "DELETE FROM call_forwarding WHERE s_id = ? AND sf_type = ? AND start_time = ?";

/// Subscribers inserted per transaction while populating, as in
/// `upstream/tdf/example.tdf`.
pub const POPULATION_COMMIT_BLOCK: u64 = 2000;

#[derive(Debug, Clone)]
pub enum Param {
    Int(i64),
    Text(String),
}

pub enum Table {
    Subscriber,
    AccessInfo,
    SpecialFacility,
    CallForwarding,
}

/// Generates the initial rows the way `populateDatabase` in
/// `upstream/source/src/targetdb.c` does: subscribers in a shuffled order,
/// each followed by its 1 to 4 access_info rows, its 1 to 4
/// special_facility rows and, under each of those, 0 to 3 call_forwarding
/// rows.
pub struct Population {
    rng: ChaCha8Rng,
    subscribers: u64,
    order: Vec<u64>,
}

impl Population {
    pub fn new(subscribers: u64, seed: u64) -> Self {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let mut order: Vec<u64> = (1..=subscribers).collect();
        for i in 0..order.len() {
            let other = rng.random_range(0..order.len());
            order.swap(i, other);
        }
        Self {
            rng,
            subscribers,
            order,
        }
    }

    /// The rows of the next `POPULATION_COMMIT_BLOCK` subscribers, in the
    /// order they are inserted, or `None` when every subscriber is in.
    pub fn next_block(&mut self) -> Option<Vec<(Table, Vec<Param>)>> {
        if self.order.is_empty() {
            return None;
        }
        let take = self.order.len().min(POPULATION_COMMIT_BLOCK as usize);
        let s_ids: Vec<u64> = self.order.drain(..take).collect();
        let mut rows = Vec::new();
        for s_id in s_ids {
            self.subscriber(s_id, &mut rows);
        }
        Some(rows)
    }

    fn subscriber(&mut self, s_id: u64, rows: &mut Vec<(Table, Vec<Param>)>) {
        let rng = &mut self.rng;
        let mut subscriber = vec![Param::Int(s_id as i64), Param::Text(sub_nbr(s_id))];
        subscriber.extend((0..10).map(|_| Param::Int(rng.random_range(0..=1))));
        subscriber.extend((0..10).map(|_| Param::Int(rng.random_range(0..=15))));
        subscriber.extend((0..10).map(|_| Param::Int(rng.random_range(0..=255))));
        subscriber.extend((0..2).map(|_| Param::Int(rng.random_range(1..=u32::MAX as i64))));
        rows.push((Table::Subscriber, subscriber));

        for ai_type in distinct_types(rng) {
            rows.push((
                Table::AccessInfo,
                vec![
                    Param::Int(s_id as i64),
                    Param::Int(ai_type),
                    Param::Int(rng.random_range(0..=255)),
                    Param::Int(rng.random_range(0..=255)),
                    Param::Text(letters(rng, 3)),
                    Param::Text(letters(rng, 5)),
                ],
            ));
        }

        for sf_type in distinct_types(rng) {
            let is_active = if rng.random_range(0..100) < 15 { 0 } else { 1 };
            rows.push((
                Table::SpecialFacility,
                vec![
                    Param::Int(s_id as i64),
                    Param::Int(sf_type),
                    Param::Int(is_active),
                    Param::Int(rng.random_range(0..=255)),
                    Param::Int(rng.random_range(0..=255)),
                    Param::Text(letters(rng, 5)),
                ],
            ));
            let mut start_times = [0, 8, 16];
            for i in 0..start_times.len() {
                let other = rng.random_range(i..start_times.len());
                start_times.swap(i, other);
            }
            let call_forwardings = rng.random_range(0..=3);
            for &start_time in &start_times[..call_forwardings] {
                let end_time = start_time + rng.random_range(1..=8);
                let numberx = sub_nbr(rng.random_range(1..=self.subscribers));
                rows.push((
                    Table::CallForwarding,
                    vec![
                        Param::Int(s_id as i64),
                        Param::Int(sf_type),
                        Param::Int(start_time),
                        Param::Int(end_time),
                        Param::Text(numberx),
                    ],
                ));
            }
        }
    }
}

/// Between 1 and 4 of the types 1 to 4, each at most once, in random order.
fn distinct_types(rng: &mut ChaCha8Rng) -> Vec<i64> {
    let mut types = vec![1, 2, 3, 4];
    for i in 0..types.len() {
        let other = rng.random_range(i..types.len());
        types.swap(i, other);
    }
    types.truncate(rng.random_range(1..=4));
    types
}

/// Upper case letters. The reference draws them from `A` to `Y` only
/// (`get_random(1, 25) + 64`), and so does this.
fn letters(rng: &mut ChaCha8Rng, len: usize) -> String {
    (0..len)
        .map(|_| (b'A' + rng.random_range(0..=24)) as char)
        .collect()
}

/// The subscriber number of a subscriber: its id as 15 digits.
pub fn sub_nbr(s_id: u64) -> String {
    format!("{s_id:015}")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Kind {
    GetSubscriberData,
    GetNewDestination,
    GetAccessData,
    UpdateSubscriberData,
    UpdateLocation,
    InsertCallForwarding,
    DeleteCallForwarding,
}

impl Kind {
    pub const ALL: [Kind; 7] = [
        Kind::GetSubscriberData,
        Kind::GetNewDestination,
        Kind::GetAccessData,
        Kind::UpdateSubscriberData,
        Kind::UpdateLocation,
        Kind::InsertCallForwarding,
        Kind::DeleteCallForwarding,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Kind::GetSubscriberData => "GET_SUBSCRIBER_DATA",
            Kind::GetNewDestination => "GET_NEW_DESTINATION",
            Kind::GetAccessData => "GET_ACCESS_DATA",
            Kind::UpdateSubscriberData => "UPDATE_SUBSCRIBER_DATA",
            Kind::UpdateLocation => "UPDATE_LOCATION",
            Kind::InsertCallForwarding => "INSERT_CALL_FORWARDING",
            Kind::DeleteCallForwarding => "DELETE_CALL_FORWARDING",
        }
    }

    pub fn writes(self) -> bool {
        matches!(
            self,
            Kind::UpdateSubscriberData
                | Kind::UpdateLocation
                | Kind::InsertCallForwarding
                | Kind::DeleteCallForwarding
        )
    }
}

/// One transaction with every key and value it uses already drawn.
#[derive(Debug, Clone)]
pub enum Txn {
    GetSubscriberData {
        s_id: i64,
    },
    GetNewDestination {
        s_id: i64,
        sf_type: i64,
        start_time: i64,
        end_time: i64,
    },
    GetAccessData {
        s_id: i64,
        ai_type: i64,
    },
    UpdateSubscriberData {
        s_id: i64,
        bit_1: i64,
        sf_type: i64,
        data_a: i64,
    },
    UpdateLocation {
        sub_nbr: String,
        vlr_location: i64,
    },
    InsertCallForwarding {
        sub_nbr: String,
        sf_type: i64,
        start_time: i64,
        end_time: i64,
        numberx: String,
    },
    DeleteCallForwarding {
        sub_nbr: String,
        sf_type: i64,
        start_time: i64,
    },
}

impl Txn {
    pub fn kind(&self) -> Kind {
        match self {
            Txn::GetSubscriberData { .. } => Kind::GetSubscriberData,
            Txn::GetNewDestination { .. } => Kind::GetNewDestination,
            Txn::GetAccessData { .. } => Kind::GetAccessData,
            Txn::UpdateSubscriberData { .. } => Kind::UpdateSubscriberData,
            Txn::UpdateLocation { .. } => Kind::UpdateLocation,
            Txn::InsertCallForwarding { .. } => Kind::InsertCallForwarding,
            Txn::DeleteCallForwarding { .. } => Kind::DeleteCallForwarding,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mix {
    /// 80% reads and 20% writes: the mix TATP results are quoted for.
    Standard,
    /// Only the three read transactions, `read_mix` in
    /// `upstream/tdf/example.tdf`.
    Read,
}

impl Mix {
    pub fn label(self) -> &'static str {
        match self {
            Mix::Standard => "standard",
            Mix::Read => "read",
        }
    }

    /// Percent of transactions of each kind.
    fn weights(self) -> [(Kind, u32); 7] {
        match self {
            Mix::Standard => [
                (Kind::GetSubscriberData, 35),
                (Kind::GetNewDestination, 10),
                (Kind::GetAccessData, 35),
                (Kind::UpdateSubscriberData, 2),
                (Kind::UpdateLocation, 14),
                (Kind::InsertCallForwarding, 2),
                (Kind::DeleteCallForwarding, 2),
            ],
            Mix::Read => [
                (Kind::GetSubscriberData, 33),
                (Kind::GetNewDestination, 33),
                (Kind::GetAccessData, 34),
                (Kind::UpdateSubscriberData, 0),
                (Kind::UpdateLocation, 0),
                (Kind::InsertCallForwarding, 0),
                (Kind::DeleteCallForwarding, 0),
            ],
        }
    }
}

/// Draws transactions for one connection: the kind from the mix, the
/// subscriber with the non-uniform distribution of the specification
/// (or a uniform one), and every other value uniformly.
pub struct Generator {
    rng: ChaCha8Rng,
    mix: Mix,
    subscribers: i64,
    /// The `A` of NURand, or `None` for uniform keys.
    nurand_a: Option<i64>,
}

impl Generator {
    pub fn new(seed: u64, mix: Mix, subscribers: u64, uniform: bool) -> Self {
        let nurand_a = match subscribers {
            _ if uniform => None,
            0..=1_000_000 => Some(65535),
            1_000_001..=10_000_000 => Some(1_048_575),
            _ => Some(2_097_151),
        };
        Self {
            rng: ChaCha8Rng::seed_from_u64(seed),
            mix,
            subscribers: subscribers as i64,
            nurand_a,
        }
    }

    pub fn next_txn(&mut self) -> Txn {
        let kind = self.kind();
        match kind {
            Kind::GetSubscriberData => Txn::GetSubscriberData { s_id: self.s_id() },
            Kind::GetNewDestination => Txn::GetNewDestination {
                s_id: self.s_id(),
                sf_type: self.rng.random_range(1..=4),
                start_time: self.start_time(),
                end_time: self.end_time(),
            },
            Kind::GetAccessData => Txn::GetAccessData {
                s_id: self.s_id(),
                ai_type: self.rng.random_range(1..=4),
            },
            Kind::UpdateSubscriberData => Txn::UpdateSubscriberData {
                s_id: self.s_id(),
                bit_1: self.rng.random_range(0..=1),
                sf_type: self.rng.random_range(1..=4),
                data_a: self.rng.random_range(0..=255),
            },
            Kind::UpdateLocation => Txn::UpdateLocation {
                sub_nbr: sub_nbr(self.s_id() as u64),
                vlr_location: self.rng.random_range(1..=u32::MAX as i64),
            },
            Kind::InsertCallForwarding => Txn::InsertCallForwarding {
                sub_nbr: sub_nbr(self.s_id() as u64),
                sf_type: self.rng.random_range(1..=4),
                start_time: self.start_time(),
                end_time: self.end_time(),
                numberx: sub_nbr(self.rng.random_range(1..=self.subscribers) as u64),
            },
            Kind::DeleteCallForwarding => Txn::DeleteCallForwarding {
                sub_nbr: sub_nbr(self.s_id() as u64),
                sf_type: self.rng.random_range(1..=4),
                start_time: self.start_time(),
            },
        }
    }

    fn kind(&mut self) -> Kind {
        let mut pick = self.rng.random_range(0..100);
        for (kind, weight) in self.mix.weights() {
            if pick < weight {
                return kind;
            }
            pick -= weight;
        }
        unreachable!("the weights of a mix add up to 100")
    }

    /// `NURand(A, 1, P) = ((random(0, A) | random(1, P)) % P) + 1`.
    fn s_id(&mut self) -> i64 {
        let p = self.subscribers;
        match self.nurand_a {
            None => self.rng.random_range(1..=p),
            Some(a) => ((self.rng.random_range(0..=a) | self.rng.random_range(1..=p)) % p) + 1,
        }
    }

    fn start_time(&mut self) -> i64 {
        self.rng.random_range(0..=2) * 8
    }

    /// From 1 to 24, drawn the way the reference does.
    fn end_time(&mut self) -> i64 {
        self.rng.random_range(0..=2) * 8 + self.rng.random_range(1..=8)
    }
}
