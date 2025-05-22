use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
struct ClientRequest {
    #[serde(flatten)]
    payload: Payload,
    id: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Payload {
    Run(RunParams),
    SendSignal(SendSignalParams),
}

#[derive(Debug, Clone, Deserialize)]
struct RunParams {
    executable: String,
    args: Vec<String>,
    working_directory: String,
}

#[derive(Debug, Clone, Deserialize)]
struct SendSignalParams {
    pid: u32,
    signal: Option<u32>,
}
