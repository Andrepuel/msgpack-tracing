# msgpack tracing

Compact storage for tracing using msgpack

## Installing Logger

### Single File

```rust
fn main() {
    msgpack_tracing::install_logger(
        File::create(path).unwrap(),
        msgpack_tracing::WithConsole::AnsiColors,
    )
}
```

### Log Rotate

```rust
fn main() {
    msgpack_tracing::install_rotate_logger(
        path,
        max_len,
        msgpack_tracing::WithConsole::AnsiColors,
    )
    .unwrap()
}
```

## Parsing file

Use the sub-crate `msgpack-tracing-printer` for parsing files.

```shell
cargo run -p msgpack-tracing-printer -- file.log
```