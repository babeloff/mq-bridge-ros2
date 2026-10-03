# Endpoint plugin template

A starting point for an mq-bridge endpoint that lives in its own repository and
ships as a Rust crate, an npm package, a Python wheel, a Homebrew formula and a
conda package, all from one compiled library. The endpoint in `src/lib.rs` is a
working example: it posts each batch as NDJSON to a URL.

It needs mq-bridge 0.4.18 or later.

## Start a plugin from it

```sh
cp -R examples/plugin-template ../mq-bridge-acme
cd ../mq-bridge-acme
python3 scripts/rename.py acme your-github-user
git init
cargo test
```

`rename.py` replaces `myendpoint` and `your-github-user` in every file and
renames the Python package folder. The endpoint name must be lowercase letters,
digits and underscores.

Then, by hand:

1. Add `LICENSE-MIT` and `LICENSE-APACHE` (or change the license in
   `Cargo.toml`, `node/package.json`, `python/pyproject.toml`,
   `packaging/conda/recipe.yaml`, `packaging/homebrew/render.sh` and the file
   list in `.github/workflows/release.yml`).
2. Replace the publisher in `src/lib.rs` and the tests in `tests/plugin.rs`.
3. Delete `scripts/rename.py` and rewrite this README.

## What is where

| Path | Purpose |
| --- | --- |
| `src/lib.rs` | Config struct, factory, `export_endpoint_plugin!`, `register()` |
| `tests/plugin.rs` | Runs the endpoint linked directly and loaded as a plugin, against a stub HTTP server |
| `node/` | npm package: loads the prebuilt library through `mq-bridge` |
| `python/` | Python package: the same, through `mq_bridge` |
| `packaging/conda/recipe.yaml` | Repackages the release archive; compiles nothing |
| `packaging/homebrew/render.sh` | Writes the formula from the release's checksums |
| `scripts/set_version.py` | `set_version.py 0.2.0` sets every package version; `--check` compares them |
| `.github/workflows/ci.yml` | Tests on push and pull request |
| `.github/workflows/release.yml` | On a version tag: builds five platforms, publishes everywhere |

## Before the first release

The release workflow fails until these exist. None of them can be created from
the repository:

- **crates.io**: a trusted publisher for this repository, workflow
  `release.yml`, environment `crates-io`. The first version of a new crate must
  be published by hand with `cargo publish`.
- **npm**: a trusted publisher for the package, environment `npm`. The first
  version must be published by hand too.
- **PyPI**: a pending trusted publisher for the project, environment `pypi`.
- **Homebrew**: a tap repository named `homebrew-tap` under your account, and a
  deploy key with write access to it stored as the secret
  `HOMEBREW_TAP_DEPLOY_KEY`.
- **conda**: an Anaconda.org account and its token stored as the secret
  `ANACONDA_API_TOKEN`.
- GitHub environments `crates-io`, `npm` and `pypi` in the repository settings.

Remove the jobs for channels you do not publish to; `github-release` lists the
publishing jobs it waits for in `needs`.

To release, run `python3 scripts/set_version.py X.Y.Z`, commit, and push the tag
`X.Y.Z`.

## Testing against an unreleased mq-bridge

Add `.cargo/config.toml` beside `Cargo.toml`:

```toml
[patch.crates-io]
mq-bridge = { path = "../mq-bridge" }
```

A `--config` flag on the command line is not enough: the plugin test starts a
second `cargo build`, which only reads the file.
