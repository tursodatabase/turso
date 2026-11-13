//! Reader for traces in the Informal Trace Format (ITF) that
//! `quint run --mbt --out-itf` writes.

use std::collections::BTreeMap;

use serde_json::Value as Json;

/// One value of a Quint trace.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Itf {
    Int(i64),
    Str(String),
    Bool(bool),
    List(Vec<Itf>),
    Set(Vec<Itf>),
    Tup(Vec<Itf>),
    Map(Vec<(Itf, Itf)>),
    Rec(BTreeMap<String, Itf>),
    Variant { tag: String, value: Box<Itf> },
}

/// One state of a trace with the action that reached it.
#[derive(Debug)]
pub(super) struct Step {
    pub(super) action: String,
    /// The values that the action picked, by nondet name. Names that the
    /// action did not use are left out.
    pub(super) picks: BTreeMap<String, Itf>,
    /// State variables by their name without the module prefix.
    pub(super) vars: BTreeMap<String, Itf>,
}

impl Step {
    pub(super) fn var(&self, name: &str) -> &Itf {
        self.vars
            .get(name)
            .unwrap_or_else(|| panic!("trace state has no variable {name}"))
    }

    pub(super) fn pick(&self, name: &str) -> &Itf {
        self.picks
            .get(name)
            .unwrap_or_else(|| panic!("action {} has no pick {name}", self.action))
    }
}

pub(super) fn parse_trace(text: &str) -> Vec<Step> {
    let json: Json = serde_json::from_str(text).expect("trace is not JSON");
    let states = json["states"].as_array().expect("trace has no states");
    states.iter().map(parse_step).collect()
}

fn parse_step(state: &Json) -> Step {
    let object = state.as_object().expect("a state is an object");
    let mut step = Step {
        action: String::new(),
        picks: BTreeMap::new(),
        vars: BTreeMap::new(),
    };
    for (name, value) in object {
        match name.as_str() {
            "#meta" => {}
            "mbt::actionTaken" => {
                step.action = value.as_str().expect("action is a string").to_string()
            }
            "mbt::nondetPicks" => {
                for (pick, option) in value.as_object().expect("picks are a record") {
                    if let Itf::Variant { tag, value } = parse_value(option) {
                        if tag == "Some" {
                            step.picks.insert(pick.clone(), *value);
                        }
                    }
                }
            }
            _ => {
                let short = name.rsplit("::").next().expect("rsplit yields one part");
                step.vars.insert(short.to_string(), parse_value(value));
            }
        }
    }
    step
}

fn parse_value(json: &Json) -> Itf {
    match json {
        Json::Bool(b) => Itf::Bool(*b),
        Json::Number(n) => Itf::Int(n.as_i64().expect("an ITF number is an integer")),
        Json::String(s) => Itf::Str(s.clone()),
        Json::Array(items) => Itf::List(items.iter().map(parse_value).collect()),
        Json::Object(fields) => {
            if let Some(n) = fields.get("#bigint") {
                let n = n.as_str().expect("#bigint is a string");
                return Itf::Int(n.parse().expect("#bigint fits in i64"));
            }
            if let Some(items) = fields.get("#set") {
                return Itf::Set(array(items).iter().map(parse_value).collect());
            }
            if let Some(items) = fields.get("#tup") {
                return Itf::Tup(array(items).iter().map(parse_value).collect());
            }
            if let Some(pairs) = fields.get("#map") {
                return Itf::Map(
                    array(pairs)
                        .iter()
                        .map(|pair| {
                            let pair = array(pair);
                            (parse_value(&pair[0]), parse_value(&pair[1]))
                        })
                        .collect(),
                );
            }
            if fields.len() == 2 {
                if let (Some(Json::String(tag)), Some(value)) =
                    (fields.get("tag"), fields.get("value"))
                {
                    return Itf::Variant {
                        tag: tag.clone(),
                        value: Box::new(parse_value(value)),
                    };
                }
            }
            Itf::Rec(
                fields
                    .iter()
                    .map(|(name, value)| (name.clone(), parse_value(value)))
                    .collect(),
            )
        }
        Json::Null => panic!("ITF has no null"),
    }
}

fn array(json: &Json) -> &Vec<Json> {
    json.as_array().expect("expected an ITF array")
}

impl Itf {
    pub(super) fn int(&self) -> i64 {
        match self {
            Itf::Int(n) => *n,
            other => panic!("expected an integer, got {other:?}"),
        }
    }

    pub(super) fn str(&self) -> &str {
        match self {
            Itf::Str(s) => s,
            other => panic!("expected a string, got {other:?}"),
        }
    }

    pub(super) fn bool(&self) -> bool {
        match self {
            Itf::Bool(b) => *b,
            other => panic!("expected a bool, got {other:?}"),
        }
    }

    pub(super) fn field(&self, name: &str) -> &Itf {
        match self {
            Itf::Rec(fields) => fields
                .get(name)
                .unwrap_or_else(|| panic!("record has no field {name}: {self:?}")),
            other => panic!("expected a record, got {other:?}"),
        }
    }

    /// The elements of a set or a list.
    pub(super) fn items(&self) -> &[Itf] {
        match self {
            Itf::Set(items) | Itf::List(items) => items,
            other => panic!("expected a set or a list, got {other:?}"),
        }
    }

    pub(super) fn get(&self, key: &Itf) -> &Itf {
        match self {
            Itf::Map(pairs) => pairs
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v)
                .unwrap_or_else(|| panic!("map has no key {key:?}")),
            other => panic!("expected a map, got {other:?}"),
        }
    }

    pub(super) fn get_str(&self, key: &str) -> &Itf {
        self.get(&Itf::Str(key.to_string()))
    }

    pub(super) fn entries(&self) -> &[(Itf, Itf)] {
        match self {
            Itf::Map(pairs) => pairs,
            other => panic!("expected a map, got {other:?}"),
        }
    }
}

#[test]
fn parses_quint_values() {
    let text = r##"{"#meta":{"format":"ITF"},"vars":["x"],"states":[
        {"#meta":{"index":0},"mbt::actionTaken":"Write",
         "mbt::nondetPicks":{"k":{"tag":"Some","value":"k1"},"d":{"tag":"None","value":{"#tup":[]}}},
         "main::M::x":{"#bigint":"-7"},
         "main::M::s":{"#set":[{"#bigint":"1"}]},
         "main::M::m":{"#map":[["a",{"b":true}]]},
         "main::M::l":[{"#bigint":"2"}]}]}"##;
    let steps = parse_trace(text);
    assert_eq!(steps.len(), 1);
    let step = &steps[0];
    assert_eq!(step.action, "Write");
    assert_eq!(step.pick("k").str(), "k1");
    assert!(!step.picks.contains_key("d"));
    assert_eq!(step.var("x").int(), -7);
    assert_eq!(step.var("s").items(), &[Itf::Int(1)]);
    assert!(step.var("m").get_str("a").field("b").bool());
    assert_eq!(step.var("l").items()[0].int(), 2);
}
