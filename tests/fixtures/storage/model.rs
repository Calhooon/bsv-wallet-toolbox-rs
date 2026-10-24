use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Fixture {
    pub name: String,
    pub issue: u64,
    pub inputs: Inputs,
    pub calls: Vec<Call>,
    pub expected: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Inputs {
    pub user: String,
    #[serde(default)]
    pub transactions: Vec<SeedTransaction>,
    #[serde(default)]
    pub baskets: Vec<Basket>,
    #[serde(default)]
    pub services: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Basket {
    pub name: String,
    pub desired: u32,
    pub minimum: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SeedTransaction {
    pub name: String,
    #[serde(default)]
    pub spends: Option<String>,
    #[serde(default)]
    pub unlocking: Option<String>,
    pub outputs: Vec<SeedOutput>,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub commission_satoshis: Option<u64>,
    #[serde(default = "yes")]
    pub store: bool,
    #[serde(default = "yes")]
    pub proven: bool,
    #[serde(default = "completed")]
    pub status: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SeedOutput {
    pub satoshis: u64,
    #[serde(default)]
    pub basket: Option<String>,
    #[serde(default)]
    pub change: bool,
    #[serde(default)]
    pub script: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub custom_instructions: Option<String>,
    #[serde(default = "one")]
    pub repeat: usize,
}

fn yes() -> bool {
    true
}
fn one() -> usize {
    1
}
fn completed() -> String {
    "completed".to_string()
}

#[derive(Deserialize)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Call {
    CreateAction {
        #[serde(rename = "as")]
        name: String,
        args: Value,
        #[serde(default)]
        sign: bool,
    },
    CreateActionOffering {
        #[serde(rename = "as")]
        name: String,
        args: Value,
        offer: Vec<String>,
    },
    ProcessAction {
        from: String,
        no_send: bool,
    },
    InternalizeAction {
        from: String,
        args: Value,
    },
    AbortAction {
        from: String,
    },
    ListOutputs {
        args: Value,
    },
    ListActions {
        args: Value,
    },
    GetUtxoLeads {
        script: String,
        #[serde(default)]
        format: Option<String>,
    },
    SendWaiting,
    AgeRequests {
        seconds: i64,
    },
}
