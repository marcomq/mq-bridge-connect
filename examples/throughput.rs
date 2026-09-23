//! Throughput harness for the Rust↔Go boundary.
//!
//! Each scenario moves the same payloads through the same Benthos engine, so the
//! only difference from the matching `nativebench` run — an identical pipeline
//! that never leaves Go — is the boundary. Prints the elapsed seconds.
//!
//! ```text
//! throughput <scenario> <messages> <batch> <max_in_flight> [path]
//! ```

use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use mq_bridge::errors::ConsumerError;
use mq_bridge::traits::{CustomEndpointFactory, MessageDisposition};
use mq_bridge::CanonicalMessage;
use mq_bridge_connect::ConnectFactory;
use serde_json::{json, Value};

const PAYLOAD_BYTES: usize = 256;
const RUN_LIMIT: Duration = Duration::from_secs(300);

struct Options {
    scenario: String,
    messages: usize,
    batch: usize,
    max_in_flight: usize,
    path: Option<String>,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    let options = parse_arguments()?;
    let payload = "x".repeat(PAYLOAD_BYTES);
    let factory = ConnectFactory::default();

    let elapsed = match options.scenario.as_str() {
        "generate-consume" => consume(&factory, generated(&options, &payload), &options).await?,
        "file-consume" => consume(&factory, read_file(&options)?, &options).await?,
        "publish-drop" => {
            let dropped = json!({ "yaml": "output:\n  drop: {}\n" });
            publish(&factory, dropped, &options, &payload).await?
        }
        "publish-file" => publish(&factory, write_file(&options)?, &options, &payload).await?,
        other => bail!("unknown scenario {other:?}"),
    };
    println!("{:.6}", elapsed.as_secs_f64());
    Ok(())
}

fn parse_arguments() -> anyhow::Result<Options> {
    const USAGE: &str = "usage: throughput <scenario> <messages> <batch> <max_in_flight> [path]";
    let mut arguments = std::env::args().skip(1);
    let scenario = arguments.next().context(USAGE)?;
    let messages = arguments.next().context(USAGE)?.parse()?;
    let batch = arguments.next().context(USAGE)?.parse()?;
    let max_in_flight = arguments.next().context(USAGE)?.parse()?;
    if batch == 0 || messages == 0 {
        bail!("messages and batch must both be positive");
    }
    Ok(Options {
        scenario,
        messages,
        batch,
        max_in_flight,
        path: arguments.next(),
    })
}

/// Benthos fabricates the messages, so nothing but the boundary stands between
/// the source and mq-bridge.
fn generated(options: &Options, payload: &str) -> Value {
    json!({ "yaml": format!(
        "max_in_flight: {}\ninput:\n  generate:\n    count: {}\n    interval: \"\"\n\
         \x20   batch_size: {}\n    mapping: 'root = \"{payload}\"'\n",
        options.max_in_flight, options.messages, options.batch
    )})
}

fn read_file(options: &Options) -> anyhow::Result<Value> {
    let path = options
        .path
        .as_deref()
        .context("file-consume needs a path")?;
    Ok(json!({ "yaml": format!(
        "max_in_flight: {}\ninput:\n  file:\n    paths: [ \"{path}\" ]\n",
        options.max_in_flight
    )}))
}

fn write_file(options: &Options) -> anyhow::Result<Value> {
    let path = options
        .path
        .as_deref()
        .context("publish-file needs a path")?;
    Ok(json!({ "yaml": format!("output:\n  file:\n    path: {path}\n") }))
}

async fn consume(
    factory: &ConnectFactory,
    config: Value,
    options: &Options,
) -> anyhow::Result<Duration> {
    let mut consumer = factory.create_consumer("throughput", &config).await?;
    consumer.set_exit_on_empty(true);

    let start = Instant::now();
    let mut received = 0;
    while received < options.messages {
        if start.elapsed() > RUN_LIMIT {
            bail!("gave up after {received} of {} messages", options.messages);
        }
        let batch = match consumer.receive_batch(options.batch).await {
            Ok(batch) => batch,
            Err(ConsumerError::EndOfStream) => break,
            Err(error) => return Err(anyhow::Error::new(error).context("receive_batch failed")),
        };
        let count = batch.messages.len();
        if count == 0 {
            continue;
        }
        received += count;
        (batch.commit)(vec![MessageDisposition::Ack; count]).await?;
    }
    let elapsed = start.elapsed();

    consumer.close().await?;
    if received != options.messages {
        bail!(
            "expected {} messages, received {received}",
            options.messages
        );
    }
    Ok(elapsed)
}

async fn publish(
    factory: &ConnectFactory,
    config: Value,
    options: &Options,
    payload: &str,
) -> anyhow::Result<Duration> {
    let publisher = factory.create_publisher("throughput", &config).await?;
    // Built once so the timed loop measures delivery, not message construction.
    let template: Vec<CanonicalMessage> = (0..options.batch)
        .map(|_| CanonicalMessage::from(payload.to_owned()))
        .collect();

    let start = Instant::now();
    let mut sent = 0;
    while sent < options.messages {
        let count = options.batch.min(options.messages - sent);
        publisher.send_batch(template[..count].to_vec()).await?;
        sent += count;
    }
    let elapsed = start.elapsed();

    if let Some(hook) = publisher.on_disconnect_hook() {
        hook.await?;
    }
    Ok(elapsed)
}
