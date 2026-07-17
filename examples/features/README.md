# SDK feature examples

This workspace package contains focused, mostly single-worker demonstrations of
individual SDK capabilities. For the deployable two-worker topology, see
[`../multi-node`](../multi-node/README.md).

Run any feature example from the repository root with:

```bash
cargo run -p another-hatchet-sdk-examples --bin <name>
```

Available binaries:

- `basic`
- `child_workflows`
- `children_workflows`
- `concurrency`
- `conditions`
- `dag`
- `durable_execution`
- `macros`
- `rate_limits`
- `rate_limits_in_memory`
- `standalone_functions`
