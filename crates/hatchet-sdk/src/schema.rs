//! Offline JSON Schema validation and Serde-compatible schema generation.
use crate::{Error, Result};
use serde_json::Value;
use std::sync::Arc;
pub(crate) const MAX_ERRORS: usize = 16;
const MAX_DIAGNOSTIC_BYTES: usize = 512;
// Keep only locations: nested dependency diagnostics can echo property names or
// values even through masked(). Also bound reports before sending failure RPCs.
struct Diagnostic {
    text: String,
    truncated: bool,
}
impl std::fmt::Write for Diagnostic {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        let available = MAX_DIAGNOSTIC_BYTES.saturating_sub(self.text.len());
        if value.len() <= available {
            self.text.push_str(value);
            Ok(())
        } else {
            let mut boundary = available;
            while !value.is_char_boundary(boundary) {
                boundary -= 1;
            }
            self.text.push_str(&value[..boundary]);
            self.truncated = true;
            Err(std::fmt::Error)
        }
    }
}
pub(crate) fn diagnostic(arguments: std::fmt::Arguments<'_>) -> String {
    let mut output = Diagnostic {
        text: String::new(),
        truncated: false,
    };
    let _ = std::fmt::write(&mut output, arguments);
    if output.truncated {
        output.text.push_str("...");
    }
    output.text
}
fn locations(error: &jsonschema::ValidationError<'_>) -> String {
    diagnostic(format_args!(
        "instance {} violates schema {}",
        error.instance_path(),
        error.schema_path()
    ))
}
struct Offline;
impl jsonschema::Retrieve for Offline {
    fn retrieve(
        &self,
        _uri: &jsonschema::Uri<String>,
    ) -> std::result::Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "external JSON Schema retrieval is disabled; use local definitions/references",
        )
        .into())
    }
}
#[derive(Clone)]
pub(crate) struct InputValidator(Arc<jsonschema::Validator>);
impl InputValidator {
    pub(crate) fn compile(schema: Option<&Value>) -> Result<Option<Self>> {
        schema
            .map(|schema| {
                jsonschema::options()
                    .with_retriever(Offline)
                    .should_validate_formats(true)
                    .should_ignore_unknown_formats(true)
                    .build(schema)
                    .map(|validator| Self(Arc::new(validator)))
                    .map_err(|error| {
                        Error::Config(format!("invalid or unsupported input schema ({}); external retrieval is disabled",locations(&error)))
                    })
            })
            .transpose()
    }
    pub(crate) fn errors(&self, input: &Value) -> Vec<String> {
        let mut errors = self.0.iter_errors(input);
        let mut messages = errors
            .by_ref()
            .take(MAX_ERRORS - 1)
            .map(|error| locations(&error))
            .collect::<Vec<_>>();
        if errors.next().is_some() {
            messages.push("additional input validation errors omitted".into());
        }
        messages
    }
}
pub(crate) fn errors(validator: Option<&InputValidator>, input: &Value) -> Vec<String> {
    validator.map_or_else(Vec::new, |validator| validator.errors(input))
}
pub(crate) fn generated<I: schemars::JsonSchema>() -> Value {
    schemars::generate::SchemaSettings::draft2020_12()
        .for_deserialize()
        .into_generator()
        .into_root_schema_for::<I>()
        .to_value()
}
