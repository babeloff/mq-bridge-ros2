# HTTP

As a publisher, sends each message as an HTTP request to a target URL. As a
consumer, runs an embedded HTTP server and turns incoming requests into
messages.

## URL format

```text
http://host[:port][/path]?method=<verb>
```

`http://`/`https://` pass through unchanged — they're already the native
scheme the driver expects (the target path, if any, is just part of the URL,
not a separate query param).

## Config (YAML / library)

The same settings as a route endpoint in a config file, or in `Route.from_config` /
`fromConfig`. Every URL query parameter is a field of the same name under `http:`.

```yaml
input:
  http: { url: "0.0.0.0:8080" }                                  # listen address
output:
  http: { url: "https://api.example.com/orders", method: "POST" }  # target URL
```

## Examples

**Consume a RabbitMQ queue and POST each message to an API, continuous:**

```bash
mqb copy \
  --from rabbitmq://guest:guest@localhost:5672/%2f?queue=orders \
  --to http://internal-api.local/ingest?method=POST
```

**Run an HTTP listener as the source (webhook receiver), continuous:**

```bash
mqb copy \
  --from http://0.0.0.0:8080?method=POST \
  --to kafka://kafka.local:9092?topic=webhooks
```

**Non-blocking publisher (don't wait for the downstream response):**

```bash
mqb copy --drain \
  --from file:///data/events.jsonl?format=json \
  --to 'https://api.example.com/ingest?method=POST&request_timeout_ms=5000'
```

## Key options

| Option | Purpose |
|---|---|
| `method` | HTTP method. Publisher: request method (defaults to POST). Consumer: restrict to this method. |
| `request_timeout_ms` | Per-request timeout. Defaults to 30000ms. |
| `workers` | Consumer-only: worker thread count. Defaults to unlimited. |
| `fire_and_forget` | Consumer-only: respond 202 immediately, don't wait for downstream processing. |
| `message_id_header` | Header to extract the message ID from. Defaults to `message-id`. |

Full field list: [reference/http.md](../reference/http.md).
