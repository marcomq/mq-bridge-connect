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
            "configuration must set either `connector` (a Redpanda Connect component name, \
             which a URI spells `redpanda+<component>://`) or `yaml` (a Redpanda Connect \
             configuration)"
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
    // The three annotated fields spell out `redpanda+mqtt://host:1883/orders`.
    // Claiming a position also suppresses the pre-schema mapping -- the whole
    // URI as `url` -- which this endpoint could not accept: form B rejects any
    // key beside `yaml`, and form A would forward `url` to a component that
    // has no such field.
    serde_json::json!({
        "type": "object",
        "title": "Redpanda Connect connector",
        "properties": {
            "connector": {
                "type": "string",
                "title": "Component name",
                "description": "The Redpanda Connect component to run at this end, such as \
                                `mqtt`. The object's remaining fields are that component's own.",
                "x-mqb-uri": "subscheme"
            },
            "address": {
                "type": "string",
                "title": "Broker address",
                "description": "Where the component connects, as a URI. Written into whichever \
                                field the component names it by, for the components listed in \
                                the README.",
                "x-mqb-uri": "origin"
            },
            "topic": {
                "type": "string",
                "title": "Topic, queue or subject",
                "description": "What is read or written at that address. Written into whichever \
                                field the component names it by in this direction.",
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
    // A URI scheme cannot hold `_` (RFC 3986 section 3.1), so `amqp_0_9` is
    // spelled `redpanda+amqp-0-9://`. No component name contains a `-`, which
    // is what makes the way back unambiguous.
    let connector = connector.replace('-', "_");

    let mut component = fields.clone();
    component.remove("connector");
    place_uri_fields(direction, &connector, &mut component)?;

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

/// One field a piece of a URI is written into.
enum Slot {
    /// The value as it stands.
    Scalar(&'static str),
    /// The value as the one-element list the component expects.
    List(&'static str),
    /// A constant the URI's shape implies, whatever the value is.
    Fixed(&'static str, &'static str),
}

/// Where a URI's address and topic go in one component's own configuration.
struct UriFields {
    /// The scheme the address field expects. It is rarely the one that named
    /// the component: `mqtt` connects over `tcp://`.
    scheme: &'static str,
    address: Slot,
    consumer: &'static [Slot],
    publisher: &'static [Slot],
}

/// The components a URI can address.
///
/// Every component names its address and its topic differently, and differently
/// again in each direction, so this is a table rather than a rule. A component
/// outside it is configured by its own field names, which is always available
/// and is what the rest of the catalogue uses.
fn uri_fields(connector: &str) -> Option<UriFields> {
    use Slot::{Fixed, List, Scalar};
    let fields = match connector {
        "mqtt" => UriFields {
            scheme: "tcp",
            address: List("urls"),
            consumer: &[List("topics")],
            publisher: &[Scalar("topic")],
        },
        // A sink with no exchange publishes to the default one, where the
        // routing key is the queue's name -- the other half of what a URI's
        // path means here.
        "amqp_0_9" => UriFields {
            scheme: "amqp",
            address: List("urls"),
            consumer: &[Scalar("queue")],
            publisher: &[Scalar("key"), Fixed("exchange", "")],
        },
        "amqp_1" => UriFields {
            scheme: "amqp",
            address: List("urls"),
            consumer: &[Scalar("source_address")],
            publisher: &[Scalar("target_address")],
        },
        "nats" | "nats_jetstream" => UriFields {
            scheme: "nats",
            address: List("urls"),
            consumer: &[Scalar("subject")],
            publisher: &[Scalar("subject")],
        },
        "pulsar" => UriFields {
            scheme: "pulsar",
            address: Scalar("url"),
            consumer: &[List("topics")],
            publisher: &[Scalar("topic")],
        },
        "redis_streams" => UriFields {
            scheme: "redis",
            address: Scalar("url"),
            consumer: &[List("streams")],
            publisher: &[Scalar("stream")],
        },
        "redis_pubsub" => UriFields {
            scheme: "redis",
            address: Scalar("url"),
            consumer: &[List("channels")],
            publisher: &[Scalar("channel")],
        },
        _ => return None,
    };
    Some(fields)
}

/// Rewrites the two fields a URI fills into the names the component uses.
///
/// A field the configuration already sets by its own name wins: the URI is a
/// shorthand for those fields, never an override of them.
fn place_uri_fields(
    direction: Direction,
    connector: &str,
    component: &mut Map<String, Value>,
) -> Result<()> {
    let address = component.remove("address");
    let topic = component.remove("topic");
    if address.is_none() && topic.is_none() {
        return Ok(());
    }
    let Some(fields) = uri_fields(connector) else {
        // Both names belong to components too -- `beanstalkd` and `socket` take
        // an `address`, `nsq` a `topic` -- so outside the table they are the
        // component's own fields and are left exactly where they are.
        for (field, value) in [("address", address), ("topic", topic)] {
            if let Some(value) = value {
                component.insert(field.into(), value);
            }
        }
        return Ok(());
    };

    if let Some(address) = address {
        let Value::String(address) = address else {
            bail!("`address` must be the URI of the broker `{connector}` connects to");
        };
        let rest = address
            .split_once("://")
            .map_or(address.as_str(), |(_, rest)| rest);
        let address = Value::String(format!("{}://{rest}", fields.scheme));
        fill(component, &fields.address, address);
    }
    if let Some(topic) = topic {
        let slots = match direction {
            Direction::Consumer => fields.consumer,
            Direction::Publisher => fields.publisher,
        };
        for slot in slots {
            fill(component, slot, topic.clone());
        }
    }
    Ok(())
}

fn fill(component: &mut Map<String, Value>, slot: &Slot, value: Value) {
    let (field, value) = match slot {
        Slot::Scalar(field) => (*field, value),
        Slot::List(field) => (*field, Value::Array(vec![value])),
        Slot::Fixed(field, constant) => (*field, Value::String((*constant).to_string())),
    };
    component.entry(field).or_insert(value);
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
    fn a_uri_names_the_connector_in_its_scheme() {
        let config = from_uri("redpanda+mqtt://localhost:1883/orders?client_id=reader");

        assert_eq!(config["connector"], "mqtt");
        assert_eq!(config["address"], "mqtt://localhost:1883");
        assert_eq!(config["topic"], "orders");
        assert_eq!(config["client_id"], "reader");
        assert!(config.get("url").is_none());
    }

    fn from_uri(uri: &str) -> Value {
        let mapping = mq_bridge::support::config_schema::UriSchema::from_schema(&config_schema());
        Value::Object(mapping.config_from_uri(uri).expect("map the uri"))
    }

    fn component(direction: Direction, uri: &str) -> Value {
        let document: Value =
            serde_json::from_str(&stream_config(direction, &from_uri(uri)).expect("build the end"))
                .expect("the document is json");
        let section = match direction {
            Direction::Consumer => "input",
            Direction::Publisher => "output",
        };
        document[section].clone()
    }

    /// The whole point of the URI form: one line that a component's own field
    /// names would have taken four of.
    #[test]
    fn a_uri_reaches_a_connector_s_own_field_names_in_both_directions() {
        let read = component(Direction::Consumer, "redpanda+mqtt://localhost:1883/orders");
        assert_eq!(read["mqtt"]["urls"], json!(["tcp://localhost:1883"]));
        assert_eq!(read["mqtt"]["topics"], json!(["orders"]));

        let write = component(
            Direction::Publisher,
            "redpanda+mqtt://localhost:1883/orders",
        );
        assert_eq!(write["mqtt"]["urls"], json!(["tcp://localhost:1883"]));
        assert_eq!(write["mqtt"]["topic"], json!("orders"));
    }

    /// `amqp_0_9` cannot be written in a scheme, and a sink there needs a second
    /// field the URI only implies.
    #[test]
    fn a_hyphenated_scheme_reaches_the_component_whose_name_has_underscores() {
        let write = component(
            Direction::Publisher,
            "redpanda+amqp-0-9://localhost:5672/jobs",
        );

        assert_eq!(write["amqp_0_9"]["urls"], json!(["amqp://localhost:5672"]));
        assert_eq!(write["amqp_0_9"]["key"], json!("jobs"));
        assert_eq!(write["amqp_0_9"]["exchange"], json!(""));

        let read = component(
            Direction::Consumer,
            "redpanda+amqp-0-9://localhost:5672/jobs",
        );
        assert_eq!(read["amqp_0_9"]["queue"], json!("jobs"));
        assert!(read["amqp_0_9"].get("exchange").is_none());
    }

    #[test]
    fn a_field_given_by_its_own_name_is_what_the_uri_would_have_filled() {
        let config = json!({
            "connector": "mqtt",
            "address": "mqtt://localhost:1883",
            "topic": "orders",
            "urls": ["tcp://elsewhere:1883"],
        });
        let document: Value =
            serde_json::from_str(&stream_config(Direction::Consumer, &config).unwrap()).unwrap();

        assert_eq!(
            document["input"]["mqtt"]["urls"],
            json!(["tcp://elsewhere:1883"])
        );
        assert_eq!(document["input"]["mqtt"]["topics"], json!(["orders"]));
    }

    /// `address` and `topic` are fields components have themselves --
    /// `beanstalkd` takes an address, `nsq` a topic -- so outside the table they
    /// are the component's own and translating them would break a configuration
    /// that works today.
    #[test]
    fn a_connector_outside_the_table_keeps_both_names_as_its_own() {
        let config = json!({ "connector": "beanstalkd", "address": "localhost:11300" });
        let document: Value =
            serde_json::from_str(&stream_config(Direction::Consumer, &config).unwrap()).unwrap();
        assert_eq!(
            document["input"]["beanstalkd"]["address"],
            json!("localhost:11300")
        );

        let config = json!({ "connector": "nsq", "topic": "jobs", "channel": "c" });
        let document: Value =
            serde_json::from_str(&stream_config(Direction::Consumer, &config).unwrap()).unwrap();
        assert_eq!(document["input"]["nsq"]["topic"], json!("jobs"));
    }
}
