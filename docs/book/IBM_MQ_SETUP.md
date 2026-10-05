# IBM MQ Setup

How to build and install `mq-bridge-app` (CLI/server **and** the Tauri desktop app)
with IBM MQ support.

IBM MQ is included in the default `full` feature set. It loads IBM's native MQ
client library at runtime, only when an IBM MQ endpoint is first used, so the IBM
MQ SDK is not required to build or install `mq-bridge-app`. The UI auto-detects
whether the running backend was built with IBM MQ and shows or hides the IBM MQ
endpoint type accordingly (via the `/features` endpoint).

## 1. Install the IBM MQ client library

You need IBM's native MQ C client on each machine where an IBM MQ endpoint will
run. It is not needed on build-only machines.

1. Download a supported **IBM MQ C client** for your platform. The
   [`mqi` setup instructions](https://github.com/advantic-au/mqi#usage) link to
   the x86-64 redistributable clients and the additional Linux and macOS
   packages available for other architectures.

2. Extract or install it, set `MQ_HOME` to the installation directory, and add
   its native library directory to the platform's library search path:

   **Linux / macOS**
   ```bash
   mkdir -p ~/ibm-mq && tar -xzf IBM-MQC-Redist-*.tar.gz -C ~/ibm-mq
   export MQ_HOME=~/ibm-mq

   # Linux
   export LD_LIBRARY_PATH="$MQ_HOME/lib64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
   # macOS (use the library directory present in your MQ package)
   export DYLD_LIBRARY_PATH="$MQ_HOME/lib64${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
   ```

   **Windows (PowerShell)** — extract the zip, then:
   ```powershell
   $env:MQ_HOME = "C:\IBM\MQ"
   $env:Path = "$env:MQ_HOME\bin64;$env:Path"
   ```

If the client is installed in the platform's standard location and is already on
the library search path, the loader can find it without these variables.

## 2. Install mq-bridge-app

The normal/default build already includes IBM MQ support. No additional Cargo
feature or IBM MQ build environment is required.

### CLI / server (web UI)

```bash
cargo install mq-bridge-app
mqb --ui                                       # then open http://localhost:9091
```

The web UI is embedded directly in the binary, so a plain `cargo install` serves
the full UI — no `static/` folder or extra files to ship.

### Desktop app (Tauri)

The desktop crate is not published to crates.io, so install it straight from git.
The committed UI bundle is reused, so no `npm` build is required:

```bash
cargo install --git https://github.com/marcomq/mq-bridge \
  mq-bridge-app-desktop
mq-bridge-app-desktop
```

The desktop build also needs the usual [Tauri prerequisites](https://tauri.app/start/prerequisites/)
(WebKitGTK + build tools on Linux; Xcode command-line tools on macOS; WebView2 on
Windows).

### From a local checkout

```bash
git clone https://github.com/marcomq/mq-bridge
cd mq-bridge

# CLI / server
cargo install --path apps/mq-bridge-app/crates/cli
# or desktop
cargo install --path apps/mq-bridge-app/crates/desktop
```

### Docker (CLI / server, amd64 only)

A prebuilt IBM MQ image is published as the `ibm-mq` / `latest-ibm-mq` tags:

```bash
docker run --rm -p 9091:9091 ghcr.io/marcomq/mq-bridge-app:latest-ibm-mq
```

Or build it yourself (the Dockerfile downloads the MQ client automatically):

```bash
docker build -f apps/mq-bridge-app/Dockerfile \
  --build-arg ENABLE_IBM_MQ=true -t mq-bridge-app:ibm-mq .
```

## 3. Verify IBM MQ is enabled

```bash
curl http://localhost:9091/features
# => {"ibm_mq":true, "kafka":true, ...}
```

When `ibm_mq` is `true`, the IBM MQ endpoint type appears in the publisher and
consumer dropdowns in the UI.

## 4. Configure an IBM MQ endpoint

```yaml
publishers:
  - name: "IBM MQ Publisher"
    endpoint:
      ibmmq:
        connection_manager: "QM1"
        queue: "DEV.QUEUE.1"
        # ...or a topic instead of a queue:
        # topic: "topic://events"
        url: "mq-host(1414)"
        channel: "DEV.APP.SVRCONN"
        username: "app"
        password: "${MQ_PASSWORD}"

consumers:
  - name: "IBM MQ Consumer"
    endpoint:
      ibmmq:
        connection_manager: "QM1"
        queue: "DEV.QUEUE.1"
        url: "mq-host(1414)"
        channel: "DEV.APP.SVRCONN"
        username: "app"
        password: "${MQ_PASSWORD}"
```

## Troubleshooting

**An IBM MQ route says the client library is unavailable** — confirm `MQ_HOME`
points at the C client installation and its library directory is present in
`LD_LIBRARY_PATH` (Linux), `DYLD_LIBRARY_PATH` (macOS), or `PATH` (Windows).
Make sure you installed the C client, not only the Java client, and that it matches
the application's architecture.

As app-specific alternatives, set `MQ_INSTALLATION_PATH` to the client installation
directory or set `MQB_IBM_MQ_LIB` to the exact native library path. The explicit
path is useful for non-standard layouts.

**Runtime: `cannot open shared object file: libmqic_r.so` (Linux) /
`libmqic_r.dylib` (macOS)** — the loader needs the library at runtime:

```bash
# Linux
export LD_LIBRARY_PATH="$MQ_HOME/lib64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
# macOS
export DYLD_LIBRARY_PATH="$MQ_HOME/lib64${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"
```

On Linux you can make it permanent:
```bash
echo "$MQ_HOME/lib64" | sudo tee /etc/ld.so.conf.d/ibm-mq.conf
sudo ldconfig
```

**Connection errors** — verify the queue manager is running, the `channel`/`queue`
exist, the port (default 1414) is reachable, and credentials are correct. Check the
MQ error logs for detail.

## Platform notes

| Platform | Runtime search variable | Typical library directory |
|----------|-------------------------|---------------------------|
| Linux    | `LD_LIBRARY_PATH`       | `$MQ_HOME/lib64`           |
| macOS    | `DYLD_LIBRARY_PATH`     | `$MQ_HOME/lib64`           |
| Windows  | `PATH`                  | `%MQ_HOME%\bin64`          |

## License

The IBM MQ client library is redistributable under IBM's own license terms. If you
distribute binaries built with IBM MQ support, include IBM's license files (from
`$MQ_HOME/licenses`) and comply with IBM's redistribution terms.

## Further reading

- [IBM MQ documentation](https://www.ibm.com/docs/en/ibm-mq)
- [IBM MQ client downloads](https://www.ibm.com/support/pages/downloading-ibm-mq-94)
- [`mqi` setup instructions](https://github.com/advantic-au/mqi#usage)
