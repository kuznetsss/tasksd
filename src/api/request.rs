#[derive(Debug, serde::Deserialize)]
struct Request {
    jsonrpc: &'static str,

    method: String

    #[serde(flatten)]
    body: RequestBody,
}

#[derive(Debug, serde::Deserialize)]
enum RequestBody {
    Start(StartBody),
    Stop(StopBody),
    List(ListBody),
}

#[derive(Debug, serde::Deserialize)]
struct StartBody {}

#[derive(Debug, serde::Deserialize)]
struct StopBody {}

#[derive(Debug, serde::Deserialize)]
struct ListBody {}
