# Sequoia deployment

The `Dockerfile` here builds the `nmsr-aas` image served at `nmsr.seqwawa.com`.

Build it on the serving host. `TARGET_CPU=native` ties the binary to the CPU it is compiled on.

The build context is the host directory holding this `Dockerfile` and the production `config.toml`. The configuration stays on the host and is not committed.

## Pinned inputs

| Input | Pin |
|---|---|
| Application source | `NMSR_COMMIT`, fetched from `NMSR_REPOSITORY` and checked against its hash |
| Dependency versions | `cargo --locked` |
| Rust toolchain | `RUST_TOOLCHAIN` |
| Base image | `BASE` (digest) |

Both stages run `apt-get upgrade` before downloading anything, so `git`, `cargo` and `rustup` use the current Debian trust store. The build fails if `ca-certificates` is older than `20250419~deb12u2`.

To update the application, change `NMSR_COMMIT` to a reviewed commit on this branch. Rebuild with `--no-cache`, so the build cache cannot reuse an older package layer and both stages fetch current Debian packages.
