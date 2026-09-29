# WebSocket

Acts as a WebSocket **server** (consumer — listens for connections) or
**client** (publisher — connects to a target URL). As a consumer it receives
frames; as a publisher it sends them.

## URL format

```text
# Consumer (listen):   ws://<bind-address>
# Publisher (connect):  ws://host[:port]/path
```

`ws://` (plain) and `wss://` (TLS) are both accepted. For a consumer the URL is
the listen address (e.g. `ws://0.0.0.0:9000`); for a publisher it is the target
server URL.

## Config (YAML / library)

The same settings as a route endpoint in a config file, or in `Route.from_config` /
`fromConfig`. Every URL query parameter is a field of the same name under `websocket:`.

```yaml
input:
  websocket: { url: "0.0.0.0:9000" }                # listen address
output:
  websocket: { url: "ws://localhost:9000/events" }  # target server
```

## Examples

**Listen for WebSocket frames and forward them to Kafka, continuous:**

```bash
mqb copy \
  --from ws://0.0.0.0:9000 \
  --to kafka://kafka.local:9092?topic=ws-events
```

**Only accept a specific path:**

```bash
mqb copy \
  --from ws://0.0.0.0:9000?path=/ingest \
  --to file:///data/ws.jsonl?format=json
```

**Push a stream to a remote WebSocket server, continuous:**

```bash
mqb copy \
  --from redis://localhost:6379?stream=events \
  --to wss://feed.example.com/socket
```

## Key options

| Option | Purpose |
|---|---|
| `path` | Consumer-only: only upgrade requests whose URI path matches exactly are delivered. |
| `message_id_header` | Consumer-only: handshake header to read the message ID from (default `message-id`). |
| `execution_mode` | Consumer-only: `auto` (default), `direct_only`, or `routed`. |
| `backlog` | Consumer-only: TCP listen backlog for the accept socket (default 4096). |
| `routed_queue_capacity` | Consumer-only: queue capacity for the routed adapter (default 100). |

Full field list: [reference/websocket.md](../reference/websocket.md).
