// Headless `--config` runs through `aggregate` and the middlewares it is combined with:
// `deduplication`, `transform` and `lookup`. Each route drains a file into a file.
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "mq-bridge-app-middleware-test-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir(&path).expect("create test directory");
        Self(path)
    }

    fn file(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// One drained route `cards` from a file to a file. Middlewares are JSON, which is YAML too.
struct Run<'a> {
    dir: &'a TestDir,
    name: &'a str,
    format: &'a str,
    source: &'a str,
    /// A fixed consumer id; without one the config is a `routes:` map.
    id: Option<&'a str>,
    input: Value,
    output: Value,
}

impl<'a> Run<'a> {
    fn new(dir: &'a TestDir, name: &'a str, source: &'a str) -> Self {
        Self {
            dir,
            name,
            format: "raw",
            source,
            id: None,
            input: json!([]),
            output: json!([]),
        }
    }

    fn input(mut self, middlewares: Value) -> Self {
        self.input = middlewares;
        self
    }

    fn output(mut self, middlewares: Value) -> Self {
        self.output = middlewares;
        self
    }

    fn format(mut self, format: &'a str) -> Self {
        self.format = format;
        self
    }

    /// Runs the CLI until the route drained and returns the rows it wrote.
    fn rows(self) -> Vec<Value> {
        let source = self.dir.file(&format!("{}.in", self.name));
        let target = self.dir.file(&format!("{}.out", self.name));
        let config = self.dir.file(&format!("{}.yaml", self.name));
        std::fs::write(&source, self.source).expect("seed source");
        let input = json!({
            "file": { "path": source, "format": self.format },
            "middlewares": self.input,
        });
        let output = json!({
            "file": { "path": target, "format": "raw" },
            "middlewares": self.output,
        });
        let routes = match self.id {
            None => json!({ "routes": { "cards": {
                "exit_on_empty": true, "concurrency": 1, "input": input, "output": output,
            }}}),
            Some(id) => json!({
                "publishers": [{ "id": "sink", "name": "sink", "endpoint": output }],
                "consumers": [{
                    "id": id,
                    "name": id,
                    "exit_on_empty": true,
                    "concurrency": 1,
                    "endpoint": input,
                    "output": { "mode": "publisher", "publisher": "sink" },
                    "message_capture": { "enabled": false },
                }],
            }),
        };
        std::fs::write(&config, routes.to_string()).expect("write config");

        let mut child = Command::new(env!("CARGO_BIN_EXE_mq-bridge-app"))
            .args(["--no-ui", "--no-metrics", "--config"])
            .arg(&config)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start headless CLI");
        let deadline = Instant::now() + Duration::from_secs(60);
        while child.try_wait().expect("poll CLI").is_none() {
            if Instant::now() > deadline {
                let _ = child.kill();
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let output = child.wait_with_output().expect("collect CLI output");
        assert!(
            output.status.success(),
            "{}: exited with {}: {}",
            self.name,
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        read_rows(&target)
    }
}

fn read_rows(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|line| serde_json::from_str(line).expect("output row is JSON"))
        .collect()
}

fn number(value: &Value) -> f64 {
    value
        .as_f64()
        .unwrap_or_else(|| panic!("{value} is not a number"))
}

fn count_per_card() -> Value {
    json!({ "aggregate": {
        "key": "${payload:card}", "into": "stats", "fields": { "n": "count" },
    }})
}

#[test]
fn aggregate_fields_keep_one_state_per_key() {
    let dir = TestDir::new();
    let rows = Run::new(
        &dir,
        "fields",
        "{\"card\":\"a\",\"amount\":10}\n{\"card\":\"b\",\"amount\":5}\n{\"card\":\"a\",\"amount\":\"30\"}\n",
    )
    .input(json!([{ "aggregate": {
        "key": "${payload:card}",
        "into": "stats",
        "fields": {
            "n": "count", "total": "sum(amount)", "high": "max(amount)",
            "avg": "mean(amount)", "ema": "ema(amount, 0.5)",
        },
    }}]))
    .rows();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["amount"], json!(10));
    assert_eq!(number(&rows[0]["stats"]["ema"]), 10.0);
    assert_eq!(number(&rows[1]["stats"]["n"]), 1.0);
    let stats = &rows[2]["stats"];
    assert_eq!(number(&stats["n"]), 2.0);
    assert_eq!(number(&stats["total"]), 40.0);
    assert_eq!(number(&stats["high"]), 30.0);
    assert_eq!(number(&stats["avg"]), 20.0);
    // Weights 0.5 and 1: (10 * 0.5 + 30) / 1.5.
    assert!((number(&stats["ema"]) - 35.0 / 1.5).abs() < 1e-9);
}

#[test]
fn aggregate_expressions_run_per_entry_with_output_and_previous() {
    let dir = TestDir::new();
    let rows = Run::new(
        &dir,
        "expressions",
        "{\"card\":1,\"shop\":\"x\",\"amount\":20}\n{\"card\":1,\"shop\":\"x\",\"amount\":50}\n",
    )
    .input(json!([{ "aggregate": { "entries": [
        {
            "key": "${payload:card}",
            "into": "features.card",
            "expression": "{ s: (state.s ?? 0) + amount, n: (state.n ?? 0) + 1 }",
            "output": "state.s / state.n",
        },
        {
            "key": "${payload:shop}",
            "into": "features.shop",
            "emit": "previous",
            "expression": "{ max: max([state.max ?? amount, amount]) }",
        },
    ]}}]))
    .rows();

    assert_eq!(rows.len(), 2);
    assert_eq!(number(&rows[0]["features"]["card"]), 20.0);
    assert_eq!(rows[0]["features"]["shop"], Value::Null);
    assert_eq!(number(&rows[1]["features"]["card"]), 35.0);
    assert_eq!(number(&rows[1]["features"]["shop"]["max"]), 20.0);
}

#[test]
fn aggregate_drops_a_message_it_cannot_fold_and_keeps_the_state() {
    let dir = TestDir::new();
    let rows = Run::new(
        &dir,
        "unfoldable",
        "{\"card\":\"a\",\"amount\":1}\n{\"card\":\"a\"}\nnot json\n{\"card\":\"a\",\"amount\":2}\n",
    )
    .input(json!([{ "aggregate": {
        "key": "${payload:card}", "into": "stats",
        "fields": { "n": "count", "total": "sum(amount)" },
    }}]))
    .rows();

    assert_eq!(rows.len(), 2);
    assert_eq!(number(&rows[1]["stats"]["n"]), 2.0);
    assert_eq!(number(&rows[1]["stats"]["total"]), 3.0);
}

#[test]
fn aggregate_runs_on_an_output() {
    let dir = TestDir::new();
    let rows = Run::new(&dir, "output", "{\"card\":\"a\"}\n{\"card\":\"a\"}\n")
        .output(json!([count_per_card()]))
        .rows();

    assert_eq!(rows.len(), 2);
    assert_eq!(number(&rows[1]["stats"]["n"]), 2.0);
}

// On an input the last middleware sees a message first, so `deduplication` goes after
// `aggregate` to keep a replayed message out of the count.
#[test]
fn deduplication_listed_after_aggregate_keeps_a_replay_out_of_the_state() {
    let dir = TestDir::new();
    let source = "{\"tx\":1,\"card\":\"a\"}\n{\"tx\":2,\"card\":\"a\"}\n\
                  {\"tx\":1,\"card\":\"a\"}\n{\"tx\":3,\"card\":\"a\"}\n";
    let dedup = |name: &str| {
        json!({ "deduplication": {
            "store": format!("memory://{name}"), "ttl_seconds": 60, "key": "${payload:tx}",
        }})
    };

    let rows = Run::new(&dir, "dedup-first", source)
        .input(json!([count_per_card(), dedup("first")]))
        .rows();
    let counts: Vec<f64> = rows.iter().map(|r| number(&r["stats"]["n"])).collect();
    assert_eq!(counts, [1.0, 2.0, 3.0]);

    // The other way round the replay is counted before it is dropped.
    let rows = Run::new(&dir, "dedup-last", source)
        .input(json!([dedup("last"), count_per_card()]))
        .rows();
    let counts: Vec<f64> = rows.iter().map(|r| number(&r["stats"]["n"])).collect();
    assert_eq!(counts, [1.0, 2.0, 4.0]);
}

#[test]
fn transform_shapes_csv_rows_before_aggregate_reads_them() {
    let dir = TestDir::new();
    let rows = Run::new(&dir, "transform", "card,amount,note\n7,10.5,x\n7,4.5,y\n")
        .format("csv")
        .input(json!([
            { "aggregate": {
                "key": "${payload:card_id}", "into": "card",
                "fields": { "n": "count", "total": "sum(amount)" },
            }},
            { "transform": {
                "mapping": { "card_id": "$.card", "amount": "$.amount" },
                "schema": { "type": "object", "properties": {
                    "card_id": { "type": "integer" }, "amount": { "type": "number" },
                }},
            }},
        ]))
        .rows();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1]["card_id"], json!(7));
    assert_eq!(rows[1]["amount"], json!(4.5));
    assert_eq!(rows[1].get("note"), None);
    assert_eq!(number(&rows[1]["card"]["n"]), 2.0);
    assert_eq!(number(&rows[1]["card"]["total"]), 15.0);
}

#[test]
fn aggregate_keys_on_a_field_a_lookup_added() {
    let dir = TestDir::new();
    let rows = Run::new(
        &dir,
        "lookup",
        "{\"id\":1,\"amount\":2}\n{\"id\":2,\"amount\":3}\n{\"id\":1,\"amount\":4}\n",
    )
    .input(json!([
        { "aggregate": {
            "key": "${payload:user.name}", "into": "stats",
            "fields": { "total": "sum(amount)" },
        }},
        { "lookup": {
            "from": { "static": { "body": "{\"name\":\"user-${payload:id}\"}", "raw": true } },
            "into": "user",
        }},
    ]))
    .rows();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2]["user"]["name"], json!("user-1"));
    let totals: Vec<f64> = rows.iter().map(|r| number(&r["stats"]["total"])).collect();
    assert_eq!(totals, [2.0, 3.0, 6.0]);
}

#[test]
fn deduplication_transform_lookup_and_aggregate_combine_on_one_input() {
    let dir = TestDir::new();
    let rows = Run::new(
        &dir,
        "chain",
        "tx,id,amount\n1,7,10\n2,7,5\n1,7,10\n3,7,1\n",
    )
    .format("csv")
    .input(json!([
        { "aggregate": {
            "key": "${payload:user.name}", "into": "stats",
            "fields": { "n": "count", "total": "sum(amount)" },
        }},
        { "lookup": {
            "from": { "static": { "body": "{\"name\":\"user-${payload:id}\"}", "raw": true } },
            "into": "user",
        }},
        { "transform": { "schema": { "type": "object", "properties": {
            "amount": { "type": "number" },
        }}}},
        { "deduplication": {
            "store": "memory://chain", "ttl_seconds": 60, "key": "${payload:tx}",
        }},
    ]))
    .rows();

    assert_eq!(rows.len(), 3);
    assert_eq!(number(&rows[2]["amount"]), 1.0);
    assert_eq!(rows[2]["user"]["name"], json!("user-7"));
    assert_eq!(number(&rows[2]["stats"]["n"]), 3.0);
    assert_eq!(number(&rows[2]["stats"]["total"]), 16.0);
}

// The table is named after the route, which the app derives from the consumer id; a route
// without one gets an id derived from its name.
#[cfg(feature = "sqlx")]
#[test]
fn aggregate_states_in_a_sqlite_store_survive_a_restart() {
    for (consistency, id) in [("shared", None), ("single_writer", Some("cards"))] {
        let dir = TestDir::new();
        let database = dir.file("states.db");
        std::fs::File::create(&database).expect("create database file");
        let aggregate = json!([{ "aggregate": {
            "store": format!("sqlite://{}", database.display()),
            "consistency": consistency,
            "key": "${payload:card}",
            "into": "stats",
            "fields": { "n": "count", "total": "sum(amount)" },
        }}]);
        let source = "{\"card\":\"a\",\"amount\":1}\n{\"card\":\"b\",\"amount\":5}\n\
                      {\"card\":\"a\",\"amount\":2}\n";

        let mut first = Run::new(&dir, "first", source).input(aggregate.clone());
        first.id = id;
        let first = first.rows();
        assert_eq!(number(&first[2]["stats"]["total"]), 3.0, "{consistency}");

        let mut second = Run::new(&dir, "second", source).input(aggregate);
        second.id = id;
        let second = second.rows();
        assert_eq!(second.len(), 3, "{consistency}");
        assert_eq!(number(&second[0]["stats"]["n"]), 3.0, "{consistency}");
        assert_eq!(number(&second[1]["stats"]["total"]), 10.0, "{consistency}");
        assert_eq!(number(&second[2]["stats"]["total"]), 6.0, "{consistency}");
    }
}
