# Hatchet Rust SDK

The official Hatchet Rust SDK. This library provides a native, idiomatic Rust client for declaring, dispatching, and executing Hatchet workflows, tasks, and durable functions.

## Features

- **Workflow Orchestration**: Define complex directed acyclic graphs (DAGs) and distributed task topologies natively in Rust.
- **Durable Execution**: Survive system crashes or pod evictions with `durable_task` pausing and state memoization.
- **Strict Typing**: Type-checked payload serialization via `serde_json` mapping seamlessly to typed handlers.
- **Procedural Macros**: Eliminate boilerplate safely mapping structs to background workers using `#[workflow]`, `#[task]`, and `#[durable_task]`.
- **System Parity**: 1:1 Behavioral parity with the TypeScript SDK backed by the native Hatchet Engine.

## Installation

Add the `hatchet-sdk` and its macros utility to your application:

```bash
cargo add hatchet-sdk hatchet-macros
```

### Protocol Buffers Requirement

Because the SDK natively compiles gRPC protocol buffers via `tonic-build`, you must have `protoc` installed on your system.
```bash
# macOS
brew install protobuf
# Ubuntu
apt install -y protobuf-compiler
```
*(If cargo is having trouble locating the installation, simply export `PROTOC=/path/to/protoc` prior to running `cargo build`.)*

## Quickstart

```rust
use hatchet_sdk::{Client, InMemoryContext, InMemoryTransport, Result, TaskConfig, WorkflowDeclaration};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::new(Arc::new(InMemoryTransport::new()));

    // 1. Declare the Task
    let echo_task = client.task::<serde_json::Value, String>("echo-task");

    // 2. Register to a Worker
    let worker = client.worker("example-worker")
        .register_task(&echo_task, |input, _ctx: InMemoryContext<_>| async move {
            println!("Task invoked with: {:?}", input);
            Ok("Success!".to_string())
        })
        .build();
        
    let _handle = worker.start()?;

    // 3. Declare Workflow and Run
    let decl = WorkflowDeclaration::new("echo-workflow")
        .step(&echo_task, TaskConfig::default());
    client.register_workflow(decl).await.unwrap();

    let result = client.workflow::<serde_json::Value, String>("echo-workflow")
        .run(serde_json::json!({ "message": "hello" }), hatchet_sdk::RunOpts::default())
        .await?;

    println!("Result: {}", result);

    Ok(())
}
```

## Exploring Examples

This repository contains a suite of isolated example projects demonstrating the SDK's advanced capabilities. To test them locally out of the box, clone the repository and run:

```bash
# 1. A streamlined quickstart
cargo run -p example --bin 01_basic

# 2. Directed Acyclic Graphs with concurrent and dependent tasks
cargo run -p example --bin 02_dag

# 3. Defining structural rate limits and concurrency locks
cargo run -p example --bin 03_concurrency

# 4. End-to-end durable sleeping and cache memoization
cargo run -p example --bin 04_durable_execution

# 5. Utilizing external event listeners and pause conditions
cargo run -p example --bin 05_conditions

# 6. Ergonomic macro integration using `#[workflow]`
cargo run -p example --bin 06_macros

# 7. Spawning and awaiting child workflows dynamically
cargo run -p example --bin 07_child_workflows

# 8. Declaring and executing completely freestanding functional tasks
cargo run -p example --bin 08_standalone_functions
```

## Development and Testing

If you'd like to extend or validate the internal implementation:

```bash
# Run strict formatting and lints
just fmt
just lint

# Execute all simulated bounds testing and the parity contract matrix
cargo test -p hatchet-sdk
```
