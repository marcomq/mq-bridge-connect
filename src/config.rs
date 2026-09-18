//! Endpoint configuration, in two forms that reach Benthos through one path.
//!
//! Form A names a connector and passes the rest of the object to it:
//!
//! ```yaml
//! custom: { name: redpanda, config: { connector: mqtt, urls: [...], topics: [...] } }
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
    let document = serde_json::json!({
        direction.section(): { connector: Value::Object(component) },
    });
    serde_json::to_string(&document).map_err(|error| anyhow!("{error}"))
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
    fn a_non_object_configuration_is_rejected() {
        assert!(stream_config(Direction::Consumer, &json!("mqtt")).is_err());
    }
}
