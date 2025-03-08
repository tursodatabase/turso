mod palette;

use nu_ansi_term::Color;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Default, JsonSchema)]
#[schemars(deny_unknown_fields)]
#[serde(default, deny_unknown_fields)]
pub struct Config {

}

#[derive(Serialize, Deserialize, Clone, Default, JsonSchema)]
#[schemars(deny_unknown_fields)]
#[serde(default, deny_unknown_fields)]
pub struct TableConfig<'a> {
    header_color: &'a str
}


