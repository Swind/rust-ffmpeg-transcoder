use std::{env, error::Error, io, path::PathBuf, time::Duration};

use futures_util::StreamExt;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use tokio::{fs, io::AsyncWriteExt, time};
use tokio_util::io::ReaderStream;

type Result<T, E = Box<dyn Error + Send + Sync>> = std::result::Result<T, E>;

struct Args {
    base: String,
    profile: String,
    input: PathBuf,
    output: PathBuf,
    deadline: Duration,
}

impl Args {
    fn parse() -> Result<Option<Self>> {
        let mut values = std::collections::HashMap::new();
        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            if arg == "--help" || arg == "-h" {
                println!(
                    "transcoder-client --base-url URL --profile PROFILE --input FILE --output FILE \
                     [--deadline-secs 7500]\n\
                     Token: TRANSCODER_API_TOKEN. Output must not exist."
                );
                return Ok(None);
            }
            if !matches!(
                arg.as_str(),
                "--base-url" | "--profile" | "--input" | "--output" | "--deadline-secs"
            ) {
                return Err(format!("unknown argument: {arg}").into());
            }
            let value = args.next().ok_or_else(|| format!("missing value: {arg}"))?;
            if values.insert(arg.clone(), value).is_some() {
                return Err(format!("duplicate argument: {arg}").into());
            }
        }
        let mut required = |key: &str| {
            values
                .remove(key)
                .ok_or_else(|| format!("required argument: {key}"))
        };
        let base = required("--base-url")?.trim_end_matches('/').to_owned();
        let profile = required("--profile")?;
        let input = PathBuf::from(required("--input")?);
        let output = PathBuf::from(required("--output")?);
        let seconds = values
            .remove("--deadline-secs")
            .unwrap_or_else(|| "7500".to_owned())
            .parse::<u64>()?;
        if seconds == 0 {
            return Err("--deadline-secs must be positive".into());
        }
        let url = reqwest::Url::parse(&base)?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(
                "--base-url must be an HTTP(S) URL without credentials/query/fragment".into(),
            );
        }
        Ok(Some(Self {
            base,
            profile,
            input,
            output,
            deadline: Duration::from_secs(seconds),
        }))
    }
}

#[derive(Deserialize)]
struct Created {
    id: String,
}

#[derive(Deserialize)]
struct Status {
    state: String,
    error: Option<serde_json::Value>,
}

async fn expect(response: reqwest::Response, status: StatusCode) -> Result<reqwest::Response> {
    if response.status() == status {
        return Ok(response);
    }
    let actual = response.status();
    let mut stream = response.bytes_stream();
    let mut detail = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        let take = chunk.len().min(8192 - detail.len());
        detail.extend_from_slice(&chunk[..take]);
        if detail.len() == 8192 {
            break;
        }
    }
    Err(format!("HTTP {actual}: {}", String::from_utf8_lossy(&detail)).into())
}

async fn status(client: &Client, url: &str) -> Result<Status> {
    Ok(expect(client.get(url).send().await?, StatusCode::OK)
        .await?
        .json()
        .await?)
}

fn terminal(state: &str) -> bool {
    matches!(state, "completed" | "failed" | "cancelled" | "expired")
}

async fn transfer(
    args: &Args,
    client: &Client,
    input: fs::File,
    length: u64,
    output: &mut fs::File,
    session_url: &mut Option<String>,
) -> Result<()> {
    let created: Created = expect(
        client
            .post(format!("{}/v1/sessions", args.base))
            .json(&serde_json::json!({"profile": args.profile}))
            .send()
            .await?,
        StatusCode::CREATED,
    )
    .await?
    .json()
    .await?;
    let url = format!("{}/v1/sessions/{}", args.base, created.id);
    *session_url = Some(url.clone());
    eprintln!("session {}", created.id);
    loop {
        let current = status(client, &url).await?;
        if current.state == "ready" {
            break;
        }
        if terminal(&current.state) {
            return Err(format!("session {}: {:?}", current.state, current.error).into());
        }
        time::sleep(Duration::from_millis(100)).await;
    }

    // Headers must arrive before PUT, but no output bytes are required yet.
    let response = expect(
        client.get(format!("{url}/output")).send().await?,
        StatusCode::OK,
    )
    .await?;
    let upload = async {
        expect(
            client
                .put(format!("{url}/input"))
                .header(reqwest::header::CONTENT_TYPE, "application/octet-stream")
                .header(reqwest::header::CONTENT_LENGTH, length)
                .body(reqwest::Body::wrap_stream(ReaderStream::with_capacity(
                    input,
                    64 * 1024,
                )))
                .send()
                .await?,
            StatusCode::NO_CONTENT,
        )
        .await?;
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    };
    let download = async {
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            output.write_all(&chunk?).await?;
        }
        output.flush().await?;
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    };
    tokio::try_join!(upload, download)?;
    loop {
        let current = status(client, &url).await?;
        if current.state == "completed" {
            return Ok(());
        }
        if terminal(&current.state) {
            return Err(format!("session {}: {:?}", current.state, current.error).into());
        }
        time::sleep(Duration::from_millis(100)).await;
    }
}

async fn interrupted() -> io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}

async fn run(args: Args) -> Result<()> {
    let mut headers = reqwest::header::HeaderMap::new();
    match env::var("TRANSCODER_API_TOKEN") {
        Ok(token) if !token.is_empty() => {
            let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))?;
            value.set_sensitive(true);
            headers.insert(reqwest::header::AUTHORIZATION, value);
        }
        Ok(_) | Err(env::VarError::NotPresent) => {}
        Err(error) => return Err(error.into()),
    }
    let client = Client::builder()
        .default_headers(headers)
        .connect_timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let input = fs::File::open(&args.input).await?;
    let metadata = input.metadata().await?;
    if !metadata.is_file() {
        return Err("--input must be a regular file with a known Content-Length".into());
    }
    // create_new prevents accidental truncation, including when input == output.
    let mut output = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&args.output)
        .await?;
    let mut session_url = None;
    let result = tokio::select! {
        result = time::timeout(args.deadline, transfer(
            &args, &client, input, metadata.len(), &mut output, &mut session_url,
        )) => match result {
            Ok(result) => result,
            Err(_) => Err("transcode deadline exceeded".into()),
        },
        signal = interrupted() => match signal {
            Ok(()) => Err("transcode interrupted".into()),
            Err(error) => Err(error.into()),
        },
    };
    drop(output);
    if result.is_err() {
        if let Some(url) = session_url {
            let _ = time::timeout(Duration::from_secs(5), client.delete(url).send()).await;
        }
        if let Err(error) = fs::remove_file(&args.output).await {
            eprintln!(
                "cannot remove partial output {}: {error}",
                args.output.display()
            );
        }
    }
    result
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let result = match Args::parse() {
        Ok(Some(args)) => run(args).await,
        Ok(None) => Ok(()),
        Err(error) => Err(error),
    };
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("transcoder-client: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
