# Runtime input schemas and generation

Task input validation uses `jsonschema` 0.60.1 rather than the previous handwritten
subset. `schemars` 1.2.3 generates a draft 2020-12 schema for a type's Serde
deserialization form. Both selected releases support the SDK's Rust 1.85 floor;
Schemars is constrained to its current minor line because it may raise MSRV in a
minor release.

```rust
use another_hatchet_sdk::{schemars, TaskConfig};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, schemars::JsonSchema)]
#[schemars(crate = "another_hatchet_sdk::schemars")]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Input {
    user_id: u32,
    choice: Option<String>,
}

let config = TaskConfig::builder().input_schema_for::<Input>().build();
config.validate_input_schema().unwrap();
assert!(config.validate_input_value(&serde_json::json!({"userId": 7})).is_empty());
assert!(!config.validate_input_value(&serde_json::json!({"user_id": 7})).is_empty());
```

The SDK re-exports Schemars. The derive `crate` attribute makes the expansion use
that re-export, so applications need no additional Schemars dependency. A separate
consumer package with only the SDK/Serde dependencies verifies this API. Serde
renames, optional/default fields, enum representation and local definitions follow
the generator's deserialization contract. Custom Serde adapters may need explicit
Schemars annotations; generated schemas do not introspect arbitrary application
code.

Use `TaskConfig::with_input_schema_for::<T>()` or its builder method for runtime
validation. Existing raw JSON methods remain available. Worker registration
compiles a reusable validator once per handler. Both normal and durable tasks
validate before typed decoding/handler execution; batch tasks validate each
member input. Middleware transformations run before the task validator. Invalid
schema configuration stops worker startup or workflow registration before its
network mutation. Input decoding failures are non-retryable `InputValidation`
errors rather than retried worker faults.

The validator handles its supported JSON Schema drafts and keywords, including
numeric/string/array/object constraints, boolean schemas, composition,
conditionals, dependencies and recursive local references. Known `format` values
are checked; unknown formats remain annotations, including generated numeric
format hints. Diagnostics retain instance/schema locations and omit rendered
values, including nested property-name errors. Instance paths and batch member
identities remain visible. Error count and complete message size are bounded to
keep failure reports small. Schemas
are non-mutating: JSON Schema defaults/coercions do not rewrite payloads. Serde
still performs its own typed deserialization and defaults.

Validation is offline. File/HTTP resolution features are disabled and an explicit
retriever rejects all external resources even if dependency feature unification
enables them elsewhere. Schemas must be self-contained with local definitions or
references. Missing references and invalid schemas are errors, never a silent
validation bypass. This is not Zod callback/coercion/transform execution; application
middleware/Serde adapters remain separate mechanisms.

`WorkflowDeclaration::input_schema_for::<T>()` sets workflow input metadata sent
to the backend/dashboard. Runtime enforcement belongs to the task config and is
not inferred from that workflow annotation. The generated metadata is transmitted
without losing definitions or Serde names.

Evidence covers constraints/composition/conditionals/local recursion, generated
Serde schemas, value-free diagnostics, no network/file retrieval, pre-mutation schema
errors, normal/durable/member validation and exact workflow metadata. A real
engine test counts every invocation through middleware and proves invalid inputs
and typed decoding failures are rejected without consuming three configured
retries. The official TS/Go runtime suites and exhaustive JSON Schema conformance
suite are not run; semantics follow the selected validator's supported dialects,
not an assertion of all-language equivalence.

References: [official TS input declaration](https://github.com/hatchet-dev/hatchet/blob/af128ec1672adaf8e3cc4afd339ce5c2c6f08c35/sdks/typescript/src/v1/declaration.ts),
[jsonschema](https://docs.rs/jsonschema/0.60.1/jsonschema/),
[Schemars](https://docs.rs/schemars/1.2.3/schemars/).
