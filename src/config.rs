//! Endpoint configuration, in two forms that reach Benthos through one path.
//!
//! Form A names a connector and passes the rest of the object to it:
//!
//! ```yaml
//! custom: { name: redpanda, config: { connector: mqtt, urls: [...], topics: [...] } }
//! ```
//!
//! `input` and `output` inside form A carry the fields whose names differ
//! between the two directions. The one matching this end is merged in, the
//! other dropped:
//!
//! ```yaml
//! custom:
//!   name: redpanda
//!   config:
//!     connector: amqp_0_9
//!     urls: [ "amqp://localhost:5672/" ]
//!     input: { queue: jobs }
//!     output: { exchange: "", key: jobs }
//! ```
//!
//! Form B is a Redpanda Connect config, minus the end mq-bridge owns:
//!
//! ```yaml
//! custom:
//!   name: redpanda
//!   config:
//!     yaml: |
//!       input: { mqtt: { urls: [...], topics: [...] } }
//!       pipeline: { processors: [ { bloblang: 'root = this' } ] }
//! ```
//!
//! Form A is compiled into form B rather than into a separate code path, so the
//! two cannot drift apart. JSON is valid YAML, so the synthesis needs no YAML
//! writer.

use anyhow::{anyhow, bail, Result};
use serde_json::{Map, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Direction {
    Consumer,
    Publisher,
}

impl Direction {
    /// The Benthos section this endpoint owns. mq-bridge owns the other one.
    fn section(self) -> &'static str {
        match self {
            Direction::Consumer => "input",
            Direction::Publisher => "output",
        }
    }

    fn opposite(self) -> Self {
        match self {
            Direction::Consumer => Direction::Publisher,
            Direction::Publisher => Direction::Consumer,
        }
    }
}

/// Builds the Benthos configuration for one end of a stream. Go rejects a
/// configuration that also declares the end mq-bridge owns.
pub(crate) fn stream_config(direction: Direction, config: &Value) -> Result<String> {
    let Value::Object(fields) = config else {
        bail!("the redpanda endpoint needs a configuration object");
    };

    match (fields.get("yaml"), fields.get("connector")) {
        (Some(_), Some(_)) => bail!(
            "configuration sets both `connector` and `yaml`; use `connector` for a single \
             component or `yaml` for a Redpanda Connect configuration, not both"
        ),
        (Some(yaml), None) => raw_yaml(fields, yaml),
        (None, Some(connector)) => synthesize(direction, fields, connector),
        (None, None) => bail!(
            "configuration must set either `connector` (a Redpanda Connect component name) \
             or `yaml` (a Redpanda Connect configuration)"
        ),
    }
}

/// What this endpoint accepts, as the JSON Schema a host reads to render a form
/// for it and to give a URI's values their types (plugin ABI 1.1).
///
/// Only the keys the two forms own are described. Form A's remaining fields
/// belong to whichever Redpanda Connect component `connector` names, so
/// `additionalProperties` stays open and the exclusion of the two forms is left
/// to [`stream_config`], which reports it by name.
pub(crate) fn config_schema() -> Value {
    // `connector` claims the path so the pre-schema mapping -- the whole URI as
    // `url` -- does not fire: this endpoint has no `url` field, and form B
    // rejects any key beside `yaml`.
    serde_json::json!({
        "type": "object",
        "title": "Redpanda Connect connector",
        "properties": {
            "connector": {
                "type": "string",
                "title": "Component name",
                "description": "The Redpanda Connect component to run at this end, such as \
                                `mqtt`. The object's remaining fields are that component's own.",
                "x-mqb-uri": "path"
            },
            "yaml": {
                "type": "string",
                "title": "Redpanda Connect configuration",
                "description": "A configuration document, minus the end mq-bridge owns. \
                                Takes no sibling fields."
            },
            "input": {
                "type": "object",
                "title": "Source-only fields",
                "description": "What the component takes when it reads. The other direction's \
                                block is dropped."
            },
            "output": {
                "type": "object",
                "title": "Sink-only fields",
                "description": "What the component takes when it writes. The other direction's \
                                block is dropped."
            }
        }
    })
}

fn raw_yaml(fields: &Map<String, Value>, yaml: &Value) -> Result<String> {
    let Value::String(yaml) = yaml else {
        bail!("`yaml` must be a string holding a Redpanda Connect configuration");
    };
    // Silently ignoring siblings of `yaml` would strand configuration the user
    // believes is in effect.
    let stray = stray_keys(fields, &["yaml"]);
    if !stray.is_empty() {
        bail!(
            "`yaml` cannot be combined with {}; put everything in the YAML document",
            stray.join(", ")
        );
    }
    Ok(yaml.clone())
}

fn synthesize(
    direction: Direction,
    fields: &Map<String, Value>,
    connector: &Value,
) -> Result<String> {
    let Value::String(connector) = connector else {
        bail!("`connector` must be the name of a Redpanda Connect component");
    };
    if connector.trim().is_empty() {
        bail!("`connector` must not be empty");
    }

    let mut component = fields.clone();
    component.remove("connector");

    // Most connectors name the same field differently in each direction --
    // `mqtt` reads `topics` and writes `topic`, `amqp_0_9` reads a queue and
    // writes to an exchange -- and Benthos rejects a field the direction does
    // not define. Take this end's block, drop the other's.
    let mine = take_direction_block(&mut component, direction)?;
    take_direction_block(&mut component, direction.opposite())?;
    for (key, value) in mine {
        component.insert(key, value);
    }

    let document = serde_json::json!({
        direction.section(): { connector: Value::Object(component) },
    });
    serde_json::to_string(&document).map_err(|error| anyhow!("{error}"))
}

/// Removes one direction's block, checking it is an object. A connector that
/// genuinely has a field called `input` or `output` needs the `yaml` form.
fn take_direction_block(
    component: &mut Map<String, Value>,
    direction: Direction,
) -> Result<Map<String, Value>> {
    let section = direction.section();
    match component.remove(section) {
        None => Ok(Map::new()),
        Some(Value::Object(block)) => Ok(block),
        Some(_) => bail!(
            "`{section}` must be an object holding the fields this connector takes \
             when used as {}",
            if direction.section() == "input" {
                "a source"
            } else {
                "a sink"
            }
        ),
    }
}

fn stray_keys(fields: &Map<String, Value>, allowed: &[&str]) -> Vec<String> {
    let mut stray: Vec<String> = fields
        .keys()
        .filter(|key| !allowed.contains(&key.as_str()))
        .map(|key| format!("`{key}`"))
        .collect();
    stray.sort();
    stray
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn form_a_wraps_the_connector_in_the_direction_it_owns() {
        let config = json!({ "connector": "mqtt", "urls": ["tcp://localhost:1883"] });

        let consumer: Value =
            serde_json::from_str(&stream_config(Direction::Consumer, &config).unwrap()).unwrap();
        assert_eq!(consumer["input"]["mqtt"]["urls"][0], "tcp://localhost:1883");
        assert!(consumer["input"]["mqtt"].get("connector").is_none());

        let publisher: Value =
            serde_json::from_str(&stream_config(Direction::Publisher, &config).unwrap()).unwrap();
        assert!(publisher.get("output").is_some());
        assert!(publisher.get("input").is_none());
    }

    #[test]
    fn form_b_passes_the_document_through_untouched() {
        let yaml = "input:\n  generate:\n    mapping: root = \"x\"\n";
        let config = json!({ "yaml": yaml });
        assert_eq!(stream_config(Direction::Consumer, &config).unwrap(), yaml);
    }

    #[test]
    fn the_two_forms_are_mutually_exclusive() {
        let config = json!({ "connector": "mqtt", "yaml": "input: {}" });
        let error = stream_config(Direction::Consumer, &config).unwrap_err();
        assert!(error.to_string().contains("both"));
    }

    #[test]
    fn neither_form_is_an_error_naming_both() {
        let error = stream_config(Direction::Consumer, &json!({ "urls": [] })).unwrap_err();
        assert!(error.to_string().contains("connector"));
        assert!(error.to_string().contains("yaml"));
    }

    #[test]
    fn keys_beside_yaml_are_rejected_rather_than_ignored() {
        let config = json!({ "yaml": "input: {}", "urls": ["tcp://localhost:1883"] });
        let error = stream_config(Direction::Consumer, &config).unwrap_err();
        assert!(error.to_string().contains("`urls`"));
    }

    #[test]
    fn each_direction_takes_its_own_block_and_drops_the_others() {
        let config = json!({
            "connector": "amqp_0_9",
            "urls": ["amqp://localhost:5672/"],
            "input": { "queue": "jobs" },
            "output": { "exchange": "", "key": "jobs" },
        });

        let consumer: Value =
            serde_json::from_str(&stream_config(Direction::Consumer, &config).unwrap()).unwrap();
        let read = &consumer["input"]["amqp_0_9"];
        assert_eq!(read["queue"], "jobs");
        assert_eq!(read["urls"][0], "amqp://localhost:5672/");
        // Benthos rejects a field the direction does not define, so the other
        // end's fields have to be gone, not merely unused.
        assert!(read.get("exchange").is_none());
        assert!(read.get("input").is_none());
        assert!(read.get("output").is_none());

        let publisher: Value =
            serde_json::from_str(&stream_config(Direction::Publisher, &config).unwrap()).unwrap();
        let write = &publisher["output"]["amqp_0_9"];
        assert_eq!(write["exchange"], "");
        assert_eq!(write["key"], "jobs");
        assert!(write.get("queue").is_none());
    }

    #[test]
    fn a_direction_block_overrides_a_shared_field() {
        let config = json!({
            "connector": "mqtt",
            "client_id": "shared",
            "output": { "client_id": "writer" },
        });

        let consumer: Value =
            serde_json::from_str(&stream_config(Direction::Consumer, &config).unwrap()).unwrap();
        assert_eq!(consumer["input"]["mqtt"]["client_id"], "shared");

        let publisher: Value =
            serde_json::from_str(&stream_config(Direction::Publisher, &config).unwrap()).unwrap();
        assert_eq!(publisher["output"]["mqtt"]["client_id"], "writer");
    }

    #[test]
    fn a_direction_block_that_is_not_an_object_is_rejected() {
        let config = json!({ "connector": "mqtt", "input": "topics" });
        let error = stream_config(Direction::Consumer, &config).unwrap_err();
        assert!(error.to_string().contains("`input`"));
    }

    #[test]
    fn a_non_object_configuration_is_rejected() {
        assert!(stream_config(Direction::Consumer, &json!("mqtt")).is_err());
    }

    #[test]
    fn the_schema_is_one_the_host_can_load() {
        mq_bridge::support::config_schema::validate(&config_schema()).unwrap();
    }

    #[test]
    fn validation_accepts_both_forms_and_the_connector_fields_it_cannot_know() {
        let schema = config_schema();
        let check =
            |config: &Value| mq_bridge::support::config_schema::validate_config(&schema, config);

        check(&json!({
            "connector": "mqtt",
            "urls": ["tcp://localhost:1883"],
            "input": { "topics": ["jobs"] },
        }))
        .unwrap();
        check(&json!({ "yaml": "input: { generate: {} }" })).unwrap();
    }

    #[test]
    fn validation_names_a_field_that_is_the_wrong_type() {
        let error = mq_bridge::support::config_schema::validate_config(
            &config_schema(),
            &json!({ "yaml": 7 }),
        )
        .unwrap_err();
        assert!(error.to_string().contains("yaml"), "{error}");
    }

    /// The pre-schema mapping puts the whole URI in `url`, which form B rejects
    /// and form A hands to a component that has no such field.
    #[test]
    fn a_uri_names_the_connector_rather_than_a_url() {
        let schema = config_schema();
        let mapping = mq_bridge::support::config_schema::UriSchema::from_schema(&schema);

        let config = mapping
            .config_from_uri("redpanda:///mqtt?client_id=reader")
            .unwrap();
        assert_eq!(config["connector"], "mqtt");
        assert_eq!(config["client_id"], "reader");
        assert!(config.get("url").is_none());
    }
}
