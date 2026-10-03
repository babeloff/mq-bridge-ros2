//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT OR Apache-2.0, see LICENSE file for more details
//  git clone https://github.com/marcomq/mq-bridge

//! Keeps one state document per key in this process and updates it with every message.

use crate::middleware::raw_json::RawPairs;
use crate::models::{AggregateConsistency, AggregateEmit, AggregateMiddleware};
use crate::support::interpolation::CompiledTemplate;
use crate::traits::{
    BatchCommitFunc, BoxFuture, ConsumerError, EndpointStatus, MessageConsumer, MessageDisposition,
    MessagePublisher, PublisherError, ReceivedBatch, Sent, SentBatch,
};
use crate::CanonicalMessage;
use anyhow::Context;
use async_trait::async_trait;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use serde_json::{Map, Value};
use states::States;
use std::any::Any;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use store::{StateStore, StateWrite};
use writer::Writer;
use zen_expression::compiler::{FetchFastTarget, Opcode};
use zen_expression::variable::{Symbol, VariableMap};
use zen_expression::vm::VM;
use zen_expression::{compile_expression, expression::Standard, Expression, Variable};

mod states;
pub(crate) mod store;
mod writer;

const STATE: &str = "state";
const META: &str = "meta";

type Folded = Result<CanonicalMessage, (CanonicalMessage, PublisherError)>;
/// One entry's result for a message: entry index, key, new state, JSON to write.
type Update = (usize, String, Stored, Vec<u8>);
/// Per message, the JSON each entry writes; `None` until that entry ran.
type Emitted = anyhow::Result<Vec<Option<Vec<u8>>>>;

/// Longest key of one state; with the `into` prefix it fits the store's key column.
const MAX_KEY_LEN: usize = 384;
/// Rounds of reload-and-refold before a contended batch is given back to the source.
const MAX_ROUNDS: usize = 32;
/// States one entry keeps in memory unless `max_keys` says otherwise.
const DEFAULT_MAX_KEYS: usize = 1_000_000;

struct Entry {
    key: CompiledTemplate,
    /// Set when `key` is one top-level payload field, which the parsed payload answers.
    key_field: Option<String>,
    fold: Fold,
    into: Vec<String>,
    /// `,"<into>":` when `into` is one top-level field, to append the result to a payload.
    label: Option<Vec<u8>>,
    emit: AggregateEmit,
}

/// How an entry computes its next state.
enum Fold {
    Zen {
        expression: Expression<Standard>,
        output: Option<Expression<Standard>>,
    },
    /// Built-in aggregates over `f64`; `width` is the number of state slots.
    Fields { fields: Vec<Field>, width: usize },
}

/// What one entry is configured with.
struct EntryConfig<'a> {
    key: &'a str,
    expression: Option<&'a str>,
    fields: Option<&'a BTreeMap<String, String>>,
    output: Option<&'a str>,
    into: &'a str,
    emit: AggregateEmit,
}

enum Op {
    Count,
    Sum,
    Min,
    Max,
    Last,
    Mean,
    /// Holds the decay `1 - alpha`.
    Ema(f64),
    /// Sample variance, or its root, of values weighted by `decay`; 1 weights all equally.
    Spread {
        decay: f64,
        root: bool,
    },
}

/// One built-in aggregate: reads the value at `source` and owns the state slots from `at`.
struct Field {
    /// `"<name>":`, as written into the result.
    label: Vec<u8>,
    op: Op,
    /// Index of the value it reads; unused by `count`.
    source: usize,
    at: usize,
}

impl Field {
    /// Parses `count`, `sum(path)`, `min`, `max`, `last`, `mean`, `stddev`, `variance`, or
    /// `ema(path, alpha)`, `ema_stddev`, `ema_variance`.
    fn new(
        name: &str,
        spec: &str,
        at: usize,
        sources: &mut Vec<Vec<String>>,
    ) -> anyhow::Result<Self> {
        let invalid = || anyhow::anyhow!("aggregate: invalid field '{name}: {spec}'");
        let mut label = serde_json::to_vec(name)?;
        label.push(b':');
        let spec = spec.trim();
        if spec == "count" {
            return Ok(Self {
                label,
                op: Op::Count,
                source: 0,
                at,
            });
        }
        let (function, args) = spec
            .strip_suffix(')')
            .and_then(|s| s.split_once('('))
            .ok_or_else(invalid)?;
        let mut args = args.split(',').map(str::trim);
        let path: Vec<String> = args
            .next()
            .unwrap_or_default()
            .split('.')
            .map(str::to_string)
            .collect();
        if path.iter().any(String::is_empty) {
            return Err(invalid());
        }
        let mut decay = || {
            let alpha: f64 = args
                .next()
                .and_then(|a| a.parse().ok())
                .ok_or_else(invalid)?;
            if !(alpha > 0.0 && alpha <= 1.0) {
                anyhow::bail!("aggregate: field '{name}': alpha must be in (0, 1]");
            }
            Ok(1.0 - alpha)
        };
        let spread = |decay, root| Op::Spread { decay, root };
        let op = match function.trim() {
            "sum" => Op::Sum,
            "min" => Op::Min,
            "max" => Op::Max,
            "last" => Op::Last,
            "mean" => Op::Mean,
            "stddev" => spread(1.0, true),
            "variance" => spread(1.0, false),
            "ema" => Op::Ema(decay()?),
            "ema_stddev" => spread(decay()?, true),
            "ema_variance" => spread(decay()?, false),
            _ => return Err(invalid()),
        };
        if args.next().is_some() {
            return Err(invalid());
        }
        let source = match sources.iter().position(|s| *s == path) {
            Some(source) => source,
            None => {
                sources.push(path);
                sources.len() - 1
            }
        };
        Ok(Self {
            label,
            op,
            source,
            at,
        })
    }

    fn width(&self) -> usize {
        match self.op {
            Op::Mean | Op::Ema(_) => 2,
            Op::Spread { .. } => 4,
            _ => 1,
        }
    }

    /// Folds `x` into the slots; `first` on a zeroed state. A result that is no longer
    /// finite is not kept.
    fn apply(&self, slots: &mut [f64], x: f64, first: bool) {
        let a = self.at;
        let keep = |slot: &mut f64| {
            let next = *slot + x;
            if next.is_finite() {
                *slot = next;
            }
        };
        match self.op {
            Op::Count => slots[a] += 1.0,
            Op::Sum => keep(&mut slots[a]),
            Op::Min => slots[a] = if first { x } else { slots[a].min(x) },
            Op::Max => slots[a] = if first { x } else { slots[a].max(x) },
            Op::Last => slots[a] = x,
            Op::Mean => {
                keep(&mut slots[a]);
                slots[a + 1] += 1.0;
            }
            // Weighted sum and weight: the mean of the values seen, without a start bias.
            Op::Ema(decay) => {
                slots[a] = slots[a] * decay + x;
                slots[a + 1] = slots[a + 1] * decay + 1.0;
            }
            // Weight, mean, weighted squared deviations, sum of squared weights (West 1979).
            Op::Spread { decay, .. } => {
                let weight = slots[a] * decay + 1.0;
                let mean = slots[a + 1] + (x - slots[a + 1]) / weight;
                let squares = slots[a + 2] * decay + (x - slots[a + 1]) * (x - mean);
                if squares.is_finite() {
                    slots[a] = weight;
                    slots[a + 1] = mean;
                    slots[a + 2] = squares;
                    slots[a + 3] = slots[a + 3] * decay * decay + 1.0;
                }
            }
        }
    }

    fn write(&self, slots: &[f64], out: &mut Vec<u8>) -> anyhow::Result<()> {
        let a = self.at;
        out.extend_from_slice(&self.label);
        match self.op {
            Op::Count => write!(out, "{}", slots[a] as u64)?,
            Op::Mean | Op::Ema(_) => serde_json::to_writer(out, &(slots[a] / slots[a + 1]))?,
            Op::Spread { root, .. } => {
                // Unbiased for weighted values; `n - 1` when all weigh the same.
                let freedom = slots[a] - slots[a + 3] / slots[a];
                if freedom.is_nan() || freedom <= 1e-9 {
                    out.extend_from_slice(b"null");
                    return Ok(());
                }
                let variance = (slots[a + 2] / freedom).max(0.0);
                serde_json::to_writer(out, &if root { variance.sqrt() } else { variance })?
            }
            _ => serde_json::to_writer(out, &slots[a])?,
        }
        Ok(())
    }
}

/// Writes the result object of a `fields` entry.
fn write_fields(fields: &[Field], slots: &[f64], out: &mut Vec<u8>) -> anyhow::Result<()> {
    out.push(b'{');
    for (i, field) in fields.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        field.write(slots, out)?;
    }
    out.push(b'}');
    Ok(())
}

/// A payload whose top-level fields are read without building a JSON tree.
enum Doc<'a> {
    Raw(Vec<(&'a str, &'a serde_json::value::RawValue)>),
    /// Keys with escapes cannot be borrowed; those payloads take the full parse.
    Tree(Value),
}

impl<'a> Doc<'a> {
    fn parse(payload: &'a [u8]) -> anyhow::Result<Self> {
        if let Ok(RawPairs(pairs)) = serde_json::from_slice::<RawPairs>(payload) {
            return Ok(Self::Raw(pairs));
        }
        match serde_json::from_slice(payload).context("aggregate: payload is not JSON")? {
            tree @ Value::Object(_) => Ok(Self::Tree(tree)),
            _ => anyhow::bail!("aggregate: payload must be a JSON object"),
        }
    }

    /// The number at `path`; numeric strings count, as a CSV source delivers them.
    fn number(&self, path: &[String]) -> Option<f64> {
        let text = |text: &str| text.trim().parse::<f64>().ok().filter(|x| x.is_finite());
        match self {
            Self::Raw(pairs) => {
                let (top, rest) = path.split_first()?;
                let mut raw = pairs.iter().rev().find(|(key, _)| key == top)?.1;
                for name in rest {
                    let RawPairs(inner) = serde_json::from_str(raw.get()).ok()?;
                    raw = inner.iter().rev().find(|(key, _)| key == name)?.1;
                }
                let raw = raw.get();
                let unquoted = raw.strip_prefix('"').and_then(|t| t.strip_suffix('"'));
                text(unquoted.unwrap_or(raw))
            }
            Self::Tree(tree) => match path.iter().try_fold(tree, |at, name| at.get(name))? {
                Value::Number(n) => n.as_f64(),
                Value::String(s) => text(s),
                _ => None,
            },
        }
    }

    /// The text of the top-level `field`, when it is a plain string, number or boolean.
    fn text(&self, field: &str) -> Option<&'a str> {
        let Self::Raw(pairs) = self else { return None };
        let raw = pairs.iter().rev().find(|(key, _)| *key == field)?.1.get();
        match raw.as_bytes().first()? {
            b'"' => Some(&raw[1..raw.len() - 1]).filter(|inner| !inner.contains('\\')),
            b'{' | b'[' | b'n' => None,
            _ => Some(raw),
        }
    }
}

impl Entry {
    fn new(config: EntryConfig<'_>, sources: &mut Vec<Vec<String>>) -> anyhow::Result<Self> {
        let EntryConfig {
            key,
            expression,
            fields,
            output,
            into,
            emit,
        } = config;
        let fold = match (expression, fields) {
            (Some(expression), None) => Fold::Zen {
                expression: compile(expression, "expression")?,
                output: output.map(|o| compile(o, "output")).transpose()?,
            },
            (None, Some(specs)) if output.is_none() && !specs.is_empty() => {
                let mut fields = Vec::with_capacity(specs.len());
                let mut width = 0;
                for (name, spec) in specs {
                    let field = Field::new(name, spec, width, sources)?;
                    width += field.width();
                    fields.push(field);
                }
                Fold::Fields { fields, width }
            }
            (None, Some(_)) => {
                anyhow::bail!("aggregate: `fields` must not be empty and takes no `output`")
            }
            _ => anyhow::bail!("aggregate: set either `expression` or `fields`"),
        };
        let into: Vec<String> = into
            .trim()
            .trim_start_matches('$')
            .trim_start_matches('.')
            .split('.')
            .map(str::to_string)
            .collect();
        if into.iter().any(String::is_empty) {
            anyhow::bail!("aggregate: invalid `into` path '{}'", into.join("."));
        }
        let key = CompiledTemplate::compile(key, None)?;
        if !key.is_dynamic() {
            anyhow::bail!("aggregate: `key` must read the message, e.g. `${{payload:sensor_id}}`");
        }
        let key_field = key
            .sole_payload_path()
            .filter(|path| !path.is_empty() && !path.contains('.'))
            .map(str::to_string);
        let label = match into.as_slice() {
            [top] => {
                let mut label = vec![b','];
                serde_json::to_writer(&mut label, top)?;
                label.push(b':');
                Some(label)
            }
            _ => None,
        };
        Ok(Self {
            key,
            key_field,
            fold,
            into,
            label,
            emit,
        })
    }

    /// Like `key`, for a payload that was not parsed into a tree.
    fn raw_key<'a>(&self, msg: &CanonicalMessage, doc: &Doc<'a>) -> Option<Cow<'a, str>> {
        let fast = self.key_field.as_deref().and_then(|field| doc.text(field));
        let rendered = || String::from_utf8(self.key.render_resolved(Some(msg))?).ok();
        fast.map(Cow::Borrowed)
            .or_else(|| rendered().map(Cow::Owned))
            .filter(|key| !key.is_empty())
    }

    /// Reads a state of this entry from its JSON text in the store.
    fn parse_state(&self, json: &str) -> anyhow::Result<Stored> {
        let Fold::Fields { width, .. } = &self.fold else {
            return Stored::parse(json);
        };
        let slots: Vec<f64> = serde_json::from_str(json)
            .ok()
            .filter(|slots: &Vec<f64>| slots.len() == *width)
            .context("aggregate: a stored state does not match the configured `fields`")?;
        Ok(Stored::Floats(slots.into()))
    }

    /// The next state and the JSON to emit for a `fields` entry.
    fn fold_fields(
        &self,
        previous: Option<&Stored>,
        values: &[f64],
    ) -> anyhow::Result<(Stored, Vec<u8>)> {
        let Fold::Fields { fields, width } = &self.fold else {
            unreachable!("only called for a `fields` entry")
        };
        let mut json = Vec::with_capacity(96);
        let (mut slots, first) = match previous {
            Some(Stored::Floats(slots)) => (slots.clone(), false),
            _ => (vec![0.0; *width].into_boxed_slice(), true),
        };
        match (self.emit, first) {
            (AggregateEmit::Previous, true) => json.extend_from_slice(b"null"),
            (AggregateEmit::Previous, false) => write_fields(fields, &slots, &mut json)?,
            (AggregateEmit::Updated, _) => {}
        }
        for field in fields {
            field.apply(
                &mut slots,
                values.get(field.source).copied().unwrap_or(0.0),
                first,
            );
        }
        if self.emit == AggregateEmit::Updated {
            write_fields(fields, &slots, &mut json)?;
        }
        Ok((Stored::Floats(slots), json))
    }

    /// The state key for this message; `None` when it is missing or empty.
    fn key(&self, msg: &CanonicalMessage, fields: &VariableMap) -> Option<String> {
        let fast = self.key_field.as_deref().and_then(|field| {
            Some(match fields.get_str(field)? {
                Variable::String(s) => s.as_str().to_string(),
                Variable::Number(n) => n.normalize().to_string(),
                Variable::Bool(b) => b.to_string(),
                _ => return None,
            })
        });
        fast.or_else(|| String::from_utf8(self.key.render_resolved(Some(msg))?).ok())
            .filter(|key| !key.is_empty())
    }

    /// The key of a state in the store: entries of one middleware share a table.
    fn store_key(&self, key: &str) -> String {
        format!("{}:{key}", self.into.join("."))
    }

    fn reads_meta(&self) -> bool {
        match &self.fold {
            Fold::Zen { expression, output } => {
                reads(expression, META) || output.as_ref().is_some_and(|o| reads(o, META))
            }
            Fold::Fields { .. } => false,
        }
    }
}

fn compile(expression: &str, field: &str) -> anyhow::Result<Expression<Standard>> {
    compile_expression(expression).map_err(|e| anyhow::anyhow!("aggregate: invalid `{field}`: {e}"))
}

/// Whether the expression reads the top-level name `root`.
fn reads(expression: &Expression<Standard>, root: &str) -> bool {
    expression.bytecode().iter().any(|opcode| match opcode {
        Opcode::FetchEnv(name) => name.as_ref() == root,
        Opcode::FetchFast(targets) => {
            targets.iter().find_map(|target| match target {
                FetchFastTarget::String(name) => Some(name.as_ref()),
                _ => None,
            }) == Some(root)
        }
        _ => false,
    })
}

/// The entries of one `aggregate` middleware and their states, one map per entry.
struct Aggregate {
    entries: Vec<Entry>,
    reads_meta: bool,
    /// Payload paths the `fields` entries read, each once per message.
    sources: Vec<Vec<String>>,
    /// No entry is an expression: the payload is never parsed into a tree.
    plain: bool,
    /// Plain, and every result is one distinct top-level field appended to the payload.
    append: bool,
    states: Mutex<Vec<States>>,
    /// States one entry keeps in memory; 0 is unlimited.
    max_keys: usize,
    /// The first forgotten state was logged.
    warned: AtomicBool,
    /// With a store the states live there and are loaded per batch.
    store: Option<Arc<dyn StateStore>>,
    /// One batch at a time against the store, so this instance does not race itself.
    gate: tokio::sync::Mutex<()>,
    /// Set for `consistency: single_writer`; it then owns the states.
    writer: Option<Arc<Writer>>,
}

impl Drop for Aggregate {
    fn drop(&mut self) {
        if let Some(writer) = &self.writer {
            writer.close();
        }
    }
}

/// Resolves once the states a batch changed are stored; at once without `single_writer`.
struct Flush(Option<(Arc<Writer>, u64)>);

impl Flush {
    async fn wait(self) -> anyhow::Result<()> {
        match self.0 {
            Some((writer, seq)) => writer.flushed(seq).await,
            None => Ok(()),
        }
    }
}

/// Buffers reused across the messages of a batch.
#[derive(Default)]
struct Scratch {
    values: Vec<f64>,
}

impl Aggregate {
    fn new(config: &AggregateMiddleware) -> anyhow::Result<Self> {
        let mut entries = Vec::with_capacity(config.entries.len() + 1);
        let mut sources = Vec::new();
        let folds = config.expression.is_some() || config.fields.is_some();
        match (&config.key, &config.into) {
            (Some(key), Some(into)) if folds => entries.push(Entry::new(
                EntryConfig {
                    key,
                    expression: config.expression.as_deref(),
                    fields: config.fields.as_ref(),
                    output: config.output.as_deref(),
                    into,
                    emit: config.emit,
                },
                &mut sources,
            )?),
            (None, None) if !folds && config.output.is_none() => {}
            _ => anyhow::bail!(
                "aggregate: `key`, `into` and `expression` or `fields` must be set together"
            ),
        }
        for e in &config.entries {
            entries.push(Entry::new(
                EntryConfig {
                    key: &e.key,
                    expression: e.expression.as_deref(),
                    fields: e.fields.as_ref(),
                    output: e.output.as_deref(),
                    into: &e.into,
                    emit: e.emit,
                },
                &mut sources,
            )?);
        }
        if entries.is_empty() {
            return Err(crate::errors::InvalidConfig(anyhow::anyhow!(
                "aggregate: set `key`, `expression` and `into`, or list `entries`"
            ))
            .into());
        }
        // `into` names an entry's states in the store, so two entries cannot share one.
        let paths: HashSet<&[String]> = entries.iter().map(|e| e.into.as_slice()).collect();
        if paths.len() != entries.len() {
            anyhow::bail!("aggregate: two entries write to the same `into`");
        }
        let plain = (entries.iter()).all(|e| matches!(e.fold, Fold::Fields { .. }));
        let max_keys = config.max_keys.unwrap_or(DEFAULT_MAX_KEYS);
        let tops: HashSet<&str> = entries.iter().map(|e| e.into[0].as_str()).collect();
        Ok(Self {
            reads_meta: entries.iter().any(Entry::reads_meta),
            append: plain
                && tops.len() == entries.len()
                && entries.iter().all(|e| e.label.is_some()),
            plain,
            sources,
            states: Mutex::new(entries.iter().map(|_| States::new(max_keys)).collect()),
            max_keys,
            warned: AtomicBool::new(false),
            entries,
            store: None,
            gate: tokio::sync::Mutex::new(()),
            writer: None,
        })
    }

    /// Like `new`, and opens the configured `store`.
    async fn connect(config: &AggregateMiddleware, route_name: &str) -> anyhow::Result<Self> {
        let mut aggregate = Self::new(config)?;
        if let Some(spec) = &config.store {
            let longest = aggregate.entries.iter().map(|e| e.store_key("").len());
            if longest.max().unwrap_or(0) + MAX_KEY_LEN > store::MAX_KEY_LEN {
                anyhow::bail!("aggregate: an `into` path is too long to key a stored state");
            }
            let store = store::build_store(spec, route_name).await?;
            match config.consistency {
                AggregateConsistency::Shared => aggregate.store = Some(store),
                AggregateConsistency::SingleWriter => aggregate.write_behind(store),
            }
        }
        Ok(aggregate)
    }

    /// Keeps the states in memory and writes them to `store` behind the batches.
    fn write_behind(&mut self, store: Arc<dyn StateStore>) {
        let prefixes = self.entries.iter().map(|e| e.into.join(".")).collect();
        self.writer = Some(Writer::start(store, prefixes, self.max_keys));
    }

    /// Folds a batch. The outer error is a store failure: nothing was folded, retry the batch.
    async fn fold_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> anyhow::Result<(Vec<Folded>, Flush)> {
        if let Some(writer) = &self.writer {
            let (folded, seq) = self.fold_behind(writer, messages).await?;
            return Ok((folded, Flush(Some((writer.clone(), seq)))));
        }
        let folded = match &self.store {
            Some(store) => self.fold_shared(store.as_ref(), messages).await?,
            None => self.fold(messages),
        };
        Ok((folded, Flush(None)))
    }

    /// Loads the states the batch needs and memory lacks, then folds in memory. Returns the
    /// batch number its commit waits on.
    async fn fold_behind(
        &self,
        writer: &Writer,
        messages: Vec<CanonicalMessage>,
    ) -> anyhow::Result<(Vec<Folded>, u64)> {
        let _gate = writer.gate.lock().await;
        let n = self.entries.len();
        let mut missing = vec![HashSet::new(); n];
        {
            let inner = writer.lock();
            for keys in messages.iter().filter_map(|m| self.keys(m)) {
                for (i, key) in keys.into_iter().enumerate() {
                    if !inner.states[i].contains_key(&key) {
                        missing[i].insert(key);
                    }
                }
            }
        }
        let ids: Vec<String> = (missing.iter().zip(&self.entries))
            .flat_map(|(keys, entry)| keys.iter().map(|key| entry.store_key(key)))
            .collect();
        let mut loaded = match ids.is_empty() {
            true => HashMap::new(),
            false => writer.store.load_many(&ids).await?,
        };
        let mut inner = writer.lock();
        for (i, keys) in missing.into_iter().enumerate() {
            let entry = &self.entries[i];
            for key in keys {
                if let Some((json, version)) = loaded.remove(&entry.store_key(&key)) {
                    inner.states[i].insert(key.clone(), entry.parse_state(&json)?);
                    inner.versions[i].insert(key, version);
                }
            }
        }
        let writer::Inner { states, dirty, .. } = &mut *inner;
        let folded = self.fold_into(messages, states, Some(dirty));
        Ok((folded, writer.folded(&mut inner)))
    }

    /// Loads the states of the batch, folds, and writes them back guarded by their versions.
    /// Keys another instance changed in between are reloaded and only they are folded again.
    async fn fold_shared(
        &self,
        store: &dyn StateStore,
        messages: Vec<CanonicalMessage>,
    ) -> anyhow::Result<Vec<Folded>> {
        let _gate = self.gate.lock().await;
        let n = self.entries.len();
        let keys: Vec<Option<Vec<String>>> = messages.iter().map(|m| self.keys(m)).collect();
        let mut pending = vec![HashSet::new(); n];
        for keys in keys.iter().flatten() {
            for (i, key) in keys.iter().enumerate() {
                pending[i].insert(key.clone());
            }
        }
        let mut states: Vec<States> = (0..n).map(|_| States::new(0)).collect();
        let mut versions: Vec<HashMap<String, i64>> = vec![HashMap::new(); n];
        let mut slots: Vec<Emitted> = messages.iter().map(|_| Ok(vec![None; n])).collect();
        let mut settled = false;
        for round in 0..MAX_ROUNDS {
            let ids: Vec<String> = (pending.iter().zip(&self.entries))
                .flat_map(|(keys, entry)| keys.iter().map(|key| entry.store_key(key)))
                .collect();
            let mut loaded = store.load_many(&ids).await?;
            for (i, keys) in pending.iter().enumerate() {
                for key in keys {
                    let version = match loaded.remove(&self.entries[i].store_key(key)) {
                        Some((json, version)) => {
                            states[i].insert(key.clone(), self.entries[i].parse_state(&json)?);
                            version
                        }
                        None => {
                            states[i].remove(key);
                            0
                        }
                    };
                    versions[i].insert(key.clone(), version);
                }
            }
            let only = (round > 0).then_some(pending.as_slice());
            let touched = self.fold_round(&messages, &keys, &mut states, only, &mut slots);
            let mut writes = Vec::new();
            let mut owners = Vec::new();
            for (i, keys) in touched.into_iter().enumerate() {
                for key in keys {
                    let mut state = Vec::new();
                    let folded = states[i]
                        .get(&key)
                        .context("aggregate: a folded state is gone")?;
                    folded.write_json(&mut state)?;
                    writes.push(StateWrite {
                        key: self.entries[i].store_key(&key),
                        state: String::from_utf8(state)?,
                        expected: versions[i][&key],
                    });
                    owners.push((i, key));
                }
            }
            let lost = match writes.is_empty() {
                true => Vec::new(),
                false => store.store_many(&writes).await?,
            };
            if lost.is_empty() {
                settled = true;
                break;
            }
            pending = vec![HashSet::new(); n];
            for at in lost {
                let (i, key) = owners
                    .get(at)
                    .context("aggregate: the store reported a write it was not given")?;
                pending[*i].insert(key.clone());
            }
        }
        if !settled {
            anyhow::bail!("aggregate: states kept changing in the store for {MAX_ROUNDS} rounds");
        }
        Ok(messages
            .into_iter()
            .zip(slots)
            .map(
                |(mut msg, slot)| match slot.and_then(|emitted| self.payload(&msg, emitted)) {
                    Ok(payload) => {
                        msg.payload = payload.into();
                        Ok(msg)
                    }
                    Err(e) => Err((msg, PublisherError::NonRetryable(e))),
                },
            )
            .collect())
    }

    /// The key of every entry for this message; `None` when one cannot be read.
    fn keys(&self, msg: &CanonicalMessage) -> Option<Vec<String>> {
        if self.plain {
            let doc = Doc::parse(&msg.payload).ok()?;
            return (self.entries.iter())
                .map(|e| e.raw_key(msg, &doc).map(Cow::into_owned))
                .collect();
        }
        let doc: Variable = serde_json::from_slice(&msg.payload).ok()?;
        let object = doc.as_object()?;
        let fields = object.borrow();
        self.entries.iter().map(|e| e.key(msg, &fields)).collect()
    }

    /// Folds the messages that still have a key in `only` (all when `None`) and returns the
    /// keys whose state changed. A message that fails keeps its error and is skipped later.
    fn fold_round(
        &self,
        messages: &[CanonicalMessage],
        keys: &[Option<Vec<String>>],
        states: &mut [States],
        only: Option<&[HashSet<String>]>,
        slots: &mut [Emitted],
    ) -> Vec<HashSet<String>> {
        let mut vm = VM::new();
        let mut touched = vec![HashSet::new(); self.entries.len()];
        for ((msg, keys), slot) in messages.iter().zip(keys).zip(slots) {
            let affected = match (only, keys) {
                (Some(only), Some(keys)) => keys.iter().zip(only).any(|(k, set)| set.contains(k)),
                _ => true,
            };
            let Ok(emitted) = slot else { continue };
            if !affected {
                continue;
            }
            match self.compute(msg, states, &mut vm, only) {
                Ok(updates) => {
                    for (i, key, next, json) in updates {
                        touched[i].insert(key.clone());
                        states[i].insert(key, next);
                        emitted[i] = Some(json);
                    }
                }
                Err(e) => *slot = Err(e),
            }
        }
        touched
    }

    /// Writes what the entries emitted into the message's payload.
    fn payload(
        &self,
        msg: &CanonicalMessage,
        emitted: Vec<Option<Vec<u8>>>,
    ) -> anyhow::Result<Vec<u8>> {
        let values = (self.entries.iter().zip(emitted))
            .filter_map(|(entry, json)| Some((entry.into.as_slice(), json?)))
            .collect();
        write_payload(&msg.payload, values)
    }

    /// Updates the states message by message, in order. A failing message changes no state.
    fn fold(&self, messages: Vec<CanonicalMessage>) -> Vec<Folded> {
        let mut states = self.states.lock().unwrap_or_else(|e| e.into_inner());
        let folded = self.fold_into(messages, &mut states, None);
        // Without a store a dropped state is gone, so say so once.
        let dropped: usize = states.iter_mut().map(|s| s.turn(|_| false)).sum();
        if dropped > 0 && !self.warned.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                "aggregate: more than `max_keys` ({}) states; the least recently used are \
                 forgotten and start again when their key returns",
                self.max_keys
            );
        }
        folded
    }

    /// Folds into `states`, noting every key it changed in `dirty`.
    fn fold_into(
        &self,
        messages: Vec<CanonicalMessage>,
        states: &mut [States],
        mut dirty: Option<&mut Vec<HashSet<String>>>,
    ) -> Vec<Folded> {
        let mut vm = VM::new();
        let mut scratch = Scratch::default();
        messages
            .into_iter()
            .map(|mut msg| {
                let dirty = dirty.as_deref_mut();
                match self.update(&msg, states, &mut vm, &mut scratch, dirty) {
                    Ok(payload) => {
                        msg.payload = payload.into();
                        Ok(msg)
                    }
                    Err(e) => Err((msg, PublisherError::NonRetryable(e))),
                }
            })
            .collect()
    }

    /// Runs every entry for one message and returns its new payload.
    fn update(
        &self,
        msg: &CanonicalMessage,
        states: &mut [States],
        vm: &mut VM,
        scratch: &mut Scratch,
        mut dirty: Option<&mut Vec<HashSet<String>>>,
    ) -> anyhow::Result<Vec<u8>> {
        if self.append {
            let dirty = dirty.as_deref_mut();
            if let Some(payload) = self.update_appending(msg, states, scratch, dirty)? {
                return Ok(payload);
            }
        }
        let mut updates = self.compute(msg, states, vm, None)?;
        let emitted = updates
            .iter_mut()
            .map(|update| Some(std::mem::take(&mut update.3)))
            .collect();
        let payload = self.payload(msg, emitted)?;
        for (i, key, next, _) in updates {
            if let Some(dirty) = dirty.as_deref_mut() {
                dirty[i].insert(key.clone());
            }
            states[i].insert(key, next);
        }
        Ok(payload)
    }

    /// The value of every source path; fails when one is missing or not a number.
    fn read_values(&self, doc: &Doc<'_>, values: &mut Vec<f64>) -> anyhow::Result<()> {
        values.clear();
        for path in &self.sources {
            let value = doc.number(path).with_context(|| {
                format!("aggregate: field '{}' is not a number", path.join("."))
            })?;
            values.push(value);
        }
        Ok(())
    }

    /// The fast path of `fields`: reads only what it needs, updates the states in place and
    /// appends the results to the payload bytes. `None` when the results cannot be appended.
    fn update_appending(
        &self,
        msg: &CanonicalMessage,
        states: &mut [States],
        scratch: &mut Scratch,
        mut dirty: Option<&mut Vec<HashSet<String>>>,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let doc = Doc::parse(&msg.payload)?;
        let Doc::Raw(pairs) = &doc else {
            return Ok(None);
        };
        let end = msg.payload.iter().rposition(|b| !b.is_ascii_whitespace());
        let taken = |entry: &Entry| pairs.iter().any(|(key, _)| *key == entry.into[0]);
        let Some(end) = end.filter(|_| !pairs.is_empty() && !self.entries.iter().any(taken)) else {
            return Ok(None);
        };
        self.read_values(&doc, &mut scratch.values)?;
        let mut keys = Vec::with_capacity(self.entries.len());
        for entry in &self.entries {
            let key = entry
                .raw_key(msg, &doc)
                .context("aggregate: `key` has no value for this message")?;
            if key.len() > MAX_KEY_LEN {
                anyhow::bail!("aggregate: `key` is longer than {MAX_KEY_LEN} bytes");
            }
            keys.push(key);
        }

        let mut out = Vec::with_capacity(msg.payload.len() + 96 * self.entries.len());
        out.extend_from_slice(&msg.payload[..end]);
        for (i, (entry, key)) in self.entries.iter().zip(keys).enumerate() {
            let (Fold::Fields { fields, width }, Some(label)) = (&entry.fold, &entry.label) else {
                unreachable!("`append` is set for top-level `fields` entries only")
            };
            if let Some(dirty) = dirty.as_deref_mut() {
                if !dirty[i].contains(key.as_ref()) {
                    dirty[i].insert(key.to_string());
                }
            }
            let (state, mut first) = states[i].slot(key.as_ref());
            if !matches!(state, Stored::Floats(slots) if slots.len() == *width) {
                *state = Stored::Floats(vec![0.0; *width].into());
                first = true;
            }
            let Stored::Floats(slots) = state else {
                unreachable!("the state was just made a float state")
            };
            out.extend_from_slice(label);
            match (entry.emit, first) {
                (AggregateEmit::Previous, true) => out.extend_from_slice(b"null"),
                (AggregateEmit::Previous, false) => write_fields(fields, slots, &mut out)?,
                (AggregateEmit::Updated, _) => {}
            }
            for field in fields {
                let x = scratch.values.get(field.source).copied().unwrap_or(0.0);
                field.apply(slots, x, first);
            }
            if entry.emit == AggregateEmit::Updated {
                write_fields(fields, slots, &mut out)?;
            }
        }
        out.push(b'}');
        Ok(Some(out))
    }

    /// Evaluates the entries of one message whose key is in `only` (every entry when
    /// `None`) against `states`, without changing them.
    fn compute(
        &self,
        msg: &CanonicalMessage,
        states: &[States],
        vm: &mut VM,
        only: Option<&[HashSet<String>]>,
    ) -> anyhow::Result<Vec<Update>> {
        let mut values = Vec::new();
        let raw = match self.plain || !self.sources.is_empty() {
            true => Some(Doc::parse(&msg.payload)?),
            false => None,
        };
        if let Some(raw) = &raw {
            self.read_values(raw, &mut values)?;
        }
        let tree = match self.plain {
            true => None,
            false => Some(self.tree(msg)?),
        };
        let state = Symbol::from(STATE);
        let mut evaluate = |expression: &Expression<Standard>, with: Variable| {
            let doc = tree.as_ref().expect("an expression entry has a tree");
            if let Some(object) = doc.as_object() {
                object.borrow_mut().insert(state.clone(), with);
            }
            expression
                .evaluate_with(doc.clone(), vm)
                .map_err(|e| anyhow::anyhow!("aggregate: expression failed: {e}"))
        };

        let mut updates = Vec::with_capacity(self.entries.len());
        for (i, (entry, states)) in self.entries.iter().zip(states.iter()).enumerate() {
            let key = match (tree.as_ref().and_then(Variable::as_object), &raw) {
                (Some(object), _) => entry.key(msg, &object.borrow()),
                (None, Some(raw)) => entry.raw_key(msg, raw).map(Cow::into_owned),
                (None, None) => None,
            };
            let key = key.context("aggregate: `key` has no value for this message")?;
            if key.len() > MAX_KEY_LEN {
                anyhow::bail!("aggregate: `key` is longer than {MAX_KEY_LEN} bytes");
            }
            if only.is_some_and(|only| !only[i].contains(&key)) {
                continue;
            }
            let previous = states.get(&key);
            let Fold::Zen { expression, output } = &entry.fold else {
                let (next, json) = entry.fold_fields(previous, &values)?;
                updates.push((i, key, next, json));
                continue;
            };
            let before = previous.map_or(Variable::Null, Stored::to_variable);
            let next = evaluate(expression, before.clone())?;
            let shown = match (entry.emit, previous) {
                (AggregateEmit::Updated, _) => next.clone(),
                (AggregateEmit::Previous, Some(_)) => before,
                (AggregateEmit::Previous, None) => Variable::Null,
            };
            let shown = match output {
                Some(output) if !matches!(shown, Variable::Null) => evaluate(output, shown)?,
                _ => shown,
            };
            let mut json = Vec::with_capacity(96);
            write_json(&shown, &mut json)?;
            updates.push((i, key, Stored::from_variable(&next), json));
        }
        Ok(updates)
    }

    /// The payload as the document expressions read, with `meta` when one of them uses it.
    fn tree(&self, msg: &CanonicalMessage) -> anyhow::Result<Variable> {
        let doc: Variable =
            serde_json::from_slice(&msg.payload).context("aggregate: payload is not JSON")?;
        let Some(object) = doc.as_object() else {
            anyhow::bail!("aggregate: payload must be a JSON object");
        };
        if self.reads_meta {
            let meta = msg
                .metadata
                .iter()
                .map(|(k, v)| {
                    (
                        Symbol::from(k.as_str()),
                        Variable::String(v.as_str().into()),
                    )
                })
                .collect();
            object
                .borrow_mut()
                .insert(Symbol::from(META), Variable::from_object(meta));
        }
        Ok(doc)
    }
}

/// A state document that can cross threads; `Variable` is `Rc`-based. Numbers stay
/// `Decimal`, so a state is not rounded through `f64` between two messages.
enum Stored {
    Null,
    Bool(bool),
    Number(Decimal),
    String(Box<str>),
    Array(Vec<Stored>),
    Object(Vec<(Box<str>, Stored)>),
    /// The slots of a `fields` entry.
    Floats(Box<[f64]>),
}

impl Stored {
    fn from_variable(variable: &Variable) -> Self {
        match variable {
            Variable::Null => Self::Null,
            Variable::Bool(b) => Self::Bool(*b),
            Variable::Number(n) => Self::Number(*n),
            Variable::String(s) => Self::String(s.as_str().into()),
            Variable::Array(items) => {
                Self::Array(items.borrow().iter().map(Self::from_variable).collect())
            }
            Variable::Object(fields) => Self::Object(
                fields
                    .borrow()
                    .iter()
                    .map(|(k, v)| (k.as_str().into(), Self::from_variable(v)))
                    .collect(),
            ),
            dynamic => Self::from_variable(&Variable::from(Value::from(dynamic.clone()))),
        }
    }

    fn to_variable(&self) -> Variable {
        match self {
            Self::Null => Variable::Null,
            Self::Bool(b) => Variable::Bool(*b),
            Self::Number(n) => Variable::Number(*n),
            Self::String(s) => Variable::String(Symbol::from(s.as_ref())),
            Self::Array(items) => {
                Variable::from_array(items.iter().map(Self::to_variable).collect())
            }
            Self::Object(fields) => Variable::from_object(
                fields
                    .iter()
                    .map(|(k, v)| (Symbol::from(k.as_ref()), v.to_variable()))
                    .collect(),
            ),
            // An expression never reads a `fields` state.
            Self::Floats(_) => Variable::Null,
        }
    }

    /// JSON text for the store. Numbers are written as exact decimals.
    fn write_json(&self, out: &mut Vec<u8>) -> anyhow::Result<()> {
        match self {
            Self::Null => out.extend_from_slice(b"null"),
            Self::Bool(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
            Self::Number(n) => write!(out, "{n}")?,
            Self::String(s) => serde_json::to_writer(out, s.as_ref())?,
            Self::Array(items) => {
                out.push(b'[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(b',');
                    }
                    item.write_json(out)?;
                }
                out.push(b']');
            }
            Self::Object(fields) => {
                out.push(b'{');
                for (i, (key, value)) in fields.iter().enumerate() {
                    if i > 0 {
                        out.push(b',');
                    }
                    serde_json::to_writer(&mut *out, key.as_ref())?;
                    out.push(b':');
                    value.write_json(out)?;
                }
                out.push(b'}');
            }
            Self::Floats(slots) => serde_json::to_writer(out, slots)?,
        }
        Ok(())
    }

    /// Reads what `write_json` wrote, keeping every digit of a number.
    fn parse(json: &str) -> anyhow::Result<Self> {
        use serde_json::value::RawValue;
        let json = json.trim();
        Ok(match json.as_bytes().first() {
            Some(b'{') => match serde_json::from_str::<RawPairs>(json) {
                Ok(RawPairs(pairs)) => Self::Object(
                    pairs
                        .into_iter()
                        .map(|(k, v)| Ok((k.into(), Self::parse(v.get())?)))
                        .collect::<anyhow::Result<_>>()?,
                ),
                // Keys with escapes cannot be borrowed; numbers then pass through `f64`.
                Err(_) => {
                    Self::from_variable(&Variable::from(serde_json::from_str::<Value>(json)?))
                }
            },
            Some(b'[') => Self::Array(
                serde_json::from_str::<Vec<&RawValue>>(json)?
                    .into_iter()
                    .map(|item| Self::parse(item.get()))
                    .collect::<anyhow::Result<_>>()?,
            ),
            Some(b'"') => Self::String(serde_json::from_str::<String>(json)?.into()),
            Some(b't') | Some(b'f') => Self::Bool(serde_json::from_str(json)?),
            Some(b'n') => Self::Null,
            _ => Self::Number(
                Decimal::from_str(json)
                    .or_else(|_| Decimal::from_scientific(json))
                    .with_context(|| {
                        format!("aggregate: stored state has an invalid number '{json}'")
                    })?,
            ),
        })
    }
}

/// Serializes a `Variable`. Its own `Serialize` truncates fractions, and the detour
/// through `Value` parses every number back from text.
fn write_json(variable: &Variable, out: &mut Vec<u8>) -> anyhow::Result<()> {
    match variable {
        Variable::Null => out.extend_from_slice(b"null"),
        Variable::Bool(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
        // Integers exactly; fractions as the shortest `f64`, like `Value::from(Variable)`.
        Variable::Number(n) => match n.is_integer().then(|| n.to_i128()).flatten() {
            Some(integer) => write!(out, "{integer}")?,
            None => serde_json::to_writer(out, &n.as_f64())?,
        },
        Variable::String(s) => serde_json::to_writer(out, s.as_str())?,
        Variable::Array(items) => {
            out.push(b'[');
            for (i, item) in items.borrow().iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_json(item, out)?;
            }
            out.push(b']');
        }
        Variable::Object(fields) => {
            out.push(b'{');
            for (i, (key, value)) in fields.borrow().iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                serde_json::to_writer(&mut *out, key.as_str())?;
                out.push(b':');
                write_json(value, out)?;
            }
            out.push(b'}');
        }
        dynamic => serde_json::to_writer(out, &Value::from(dynamic.clone()))?,
    }
    Ok(())
}

/// JSON being assembled: text copied as is, or an object whose fields are set by path.
enum Node {
    Raw(Vec<u8>),
    Object(Vec<(String, Node)>),
}

impl Node {
    /// Sets `json` at `path`. `Ok(false)` when an object on the way has escaped keys.
    fn insert(&mut self, path: &[String], json: Vec<u8>) -> anyhow::Result<bool> {
        let Some((key, rest)) = path.split_first() else {
            *self = Node::Raw(json);
            return Ok(true);
        };
        if let Node::Raw(raw) = self {
            if raw.first() != Some(&b'{') {
                anyhow::bail!("aggregate: cannot set '{key}': its parent is not an object");
            }
            let Ok(RawPairs(pairs)) = serde_json::from_slice::<RawPairs>(raw) else {
                return Ok(false);
            };
            let fields = pairs
                .into_iter()
                .map(|(k, v)| (k.to_string(), Node::Raw(v.get().as_bytes().to_vec())))
                .collect();
            *self = Node::Object(fields);
        }
        let Node::Object(fields) = self else {
            unreachable!("raw text was just expanded")
        };
        let at = match fields.iter().position(|(k, _)| k == key) {
            Some(at) => at,
            None => {
                fields.push((key.clone(), Node::Object(Vec::new())));
                fields.len() - 1
            }
        };
        fields[at].1.insert(rest, json)
    }

    fn write(&self, out: &mut Vec<u8>) -> anyhow::Result<()> {
        match self {
            Node::Raw(raw) => out.extend_from_slice(raw),
            Node::Object(fields) => {
                out.push(b'{');
                for (i, (key, node)) in fields.iter().enumerate() {
                    if i > 0 {
                        out.push(b',');
                    }
                    serde_json::to_writer(&mut *out, key)?;
                    out.push(b':');
                    node.write(out)?;
                }
                out.push(b'}');
            }
        }
        Ok(())
    }
}

/// Writes each JSON value at its path, copying every untouched field verbatim.
fn write_payload(payload: &[u8], values: Vec<(&[String], Vec<u8>)>) -> anyhow::Result<Vec<u8>> {
    if let Some(spliced) = splice(payload, &values)? {
        return Ok(spliced);
    }
    // Keys with escapes cannot be borrowed; those payloads take the full parse.
    let mut doc: Value = serde_json::from_slice(payload)?;
    for (path, json) in values {
        insert_at(&mut doc, path, serde_json::from_slice(&json)?)?;
    }
    Ok(serde_json::to_vec(&doc)?)
}

fn splice(payload: &[u8], values: &[(&[String], Vec<u8>)]) -> anyhow::Result<Option<Vec<u8>>> {
    let Ok(RawPairs(pairs)) = serde_json::from_slice::<RawPairs>(payload) else {
        return Ok(None);
    };
    let mut written: Vec<(&str, Node)> = Vec::with_capacity(values.len());
    for (path, json) in values {
        let (top, rest) = path.split_first().expect("path is non-empty");
        let at = match written.iter().position(|(key, _)| key == top) {
            Some(at) => at,
            None => {
                let node = match pairs.iter().rev().find(|(key, _)| key == top) {
                    Some((_, raw)) if !rest.is_empty() => Node::Raw(raw.get().as_bytes().to_vec()),
                    _ => Node::Object(Vec::new()),
                };
                written.push((top, node));
                written.len() - 1
            }
        };
        if !written[at].1.insert(rest, json.clone())? {
            return Ok(None);
        }
    }

    let mut out = Vec::with_capacity(payload.len() + 128 * written.len());
    out.push(b'{');
    for (key, raw) in &pairs {
        if written.iter().any(|(top, _)| top == key) {
            continue;
        }
        out.push(b'"');
        out.extend_from_slice(key.as_bytes());
        out.extend_from_slice(b"\":");
        out.extend_from_slice(raw.get().as_bytes());
        out.push(b',');
    }
    for (key, node) in &written {
        serde_json::to_writer(&mut out, key)?;
        out.push(b':');
        node.write(&mut out)?;
        out.push(b',');
    }
    out.pop();
    out.push(b'}');
    Ok(Some(out))
}

/// Writes `value` at the dotted `path`, creating objects along the way.
fn insert_at(root: &mut Value, path: &[String], value: Value) -> anyhow::Result<()> {
    let (last, parents) = path.split_last().expect("path is non-empty");
    let mut cur = root;
    for key in parents {
        let Value::Object(map) = cur else {
            anyhow::bail!("aggregate: cannot nest under '{key}': it is not an object");
        };
        cur = map
            .entry(key.as_str())
            .or_insert_with(|| Value::Object(Map::new()));
    }
    let Value::Object(map) = cur else {
        anyhow::bail!("aggregate: cannot set '{last}': its parent is not an object");
    };
    map.insert(last.clone(), value);
    Ok(())
}

pub struct AggregatePublisher {
    inner: Box<dyn MessagePublisher>,
    aggregate: Aggregate,
}

impl AggregatePublisher {
    pub async fn new(
        inner: Box<dyn MessagePublisher>,
        config: &AggregateMiddleware,
        route_name: &str,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            inner,
            aggregate: Aggregate::connect(config, route_name).await?,
        })
    }

    async fn fold(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<(Vec<Folded>, Flush), PublisherError> {
        let folded = self.aggregate.fold_batch(messages).await;
        folded.map_err(PublisherError::Retryable)
    }
}

#[async_trait]
impl MessagePublisher for AggregatePublisher {
    fn on_connect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_connect_hook()
    }

    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_disconnect_hook()
    }

    async fn send(&self, message: CanonicalMessage) -> Result<Sent, PublisherError> {
        let (mut folded, flush) = self.fold(vec![message]).await?;
        let message = folded
            .pop()
            .expect("one result per message")
            .map_err(|(_, e)| e)?;
        let sent = self.inner.send(message).await?;
        flush.wait().await.map_err(PublisherError::Retryable)?;
        Ok(sent)
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        let mut folded = Vec::with_capacity(messages.len());
        let mut failed = Vec::new();
        let (results, flush) = self.fold(messages).await?;
        for result in results {
            match result {
                Ok(m) => folded.push(m),
                Err(f) => failed.push(f),
            }
        }
        if folded.is_empty() {
            return Ok(SentBatch::Partial {
                responses: None,
                failed,
            });
        }
        let sent = self.inner.send_batch(folded).await?;
        // The batch is not done before the states it changed are stored.
        flush.wait().await.map_err(PublisherError::Retryable)?;
        if failed.is_empty() {
            return Ok(sent);
        }
        match sent {
            SentBatch::Ack => Ok(SentBatch::Partial {
                responses: None,
                failed,
            }),
            SentBatch::Partial {
                responses,
                failed: mut inner_failed,
            } => {
                inner_failed.extend(failed);
                Ok(SentBatch::Partial {
                    responses,
                    failed: inner_failed,
                })
            }
        }
    }

    async fn flush(&self) -> anyhow::Result<()> {
        self.inner.flush().await
    }

    async fn status(&self) -> EndpointStatus {
        self.inner.status().await
    }

    fn requires_ordered_publish(&self) -> bool {
        self.inner.requires_ordered_publish()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Folds each received batch before the handler sees it. A message that cannot be
/// folded is logged, acked and dropped.
pub struct AggregateConsumer {
    inner: Box<dyn MessageConsumer>,
    aggregate: Aggregate,
}

impl AggregateConsumer {
    pub async fn new(
        inner: Box<dyn MessageConsumer>,
        config: &AggregateMiddleware,
        route_name: &str,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            inner,
            aggregate: Aggregate::connect(config, route_name).await?,
        })
    }
}

#[async_trait]
impl MessageConsumer for AggregateConsumer {
    fn set_exit_on_empty(&mut self, exit_on_empty: bool) {
        self.inner.set_exit_on_empty(exit_on_empty);
    }

    fn commit_requires_order(&self) -> bool {
        self.inner.commit_requires_order()
    }

    fn on_connect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_connect_hook()
    }

    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        self.inner.on_disconnect_hook()
    }

    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        loop {
            let ReceivedBatch { messages, commit } = self.inner.receive_batch(max_messages).await?;
            if messages.is_empty() {
                return Ok(ReceivedBatch { messages, commit });
            }
            let len = messages.len();
            let (results, flush) = match self.aggregate.fold_batch(messages).await {
                Ok(results) => results,
                Err(e) => {
                    if let Err(nack) = commit(vec![MessageDisposition::Nack; len]).await {
                        tracing::warn!("aggregate: failed to nack the batch: {nack}");
                    }
                    return Err(ConsumerError::Connection(e));
                }
            };
            let mut folded = Vec::with_capacity(len);
            let mut kept = Vec::with_capacity(len);
            for (i, result) in results.into_iter().enumerate() {
                match result {
                    Ok(m) => {
                        folded.push(m);
                        kept.push(i);
                    }
                    Err((m, e)) => tracing::error!(
                        message_id = format_args!("{:032x}", m.message_id),
                        "aggregate: dropping input message: {e:#}"
                    ),
                }
            }
            if folded.is_empty() {
                commit(vec![MessageDisposition::Ack; len])
                    .await
                    .map_err(ConsumerError::Connection)?;
                continue;
            }
            // Dropped messages are acked; the kept ones take the route's dispositions.
            let commit: BatchCommitFunc = match kept.len() == len {
                true => commit,
                false => Box::new(move |dispositions| {
                    let mut all = vec![MessageDisposition::Ack; len];
                    for (i, d) in kept.into_iter().zip(dispositions) {
                        all[i] = d;
                    }
                    commit(all)
                }),
            };
            // The ack waits until the states this batch changed are stored.
            let commit: BatchCommitFunc = match flush.0.is_some() {
                false => commit,
                true => Box::new(move |dispositions| {
                    Box::pin(async move {
                        if let Err(e) = flush.wait().await {
                            let nacks = vec![MessageDisposition::Nack; dispositions.len()];
                            if let Err(nack) = commit(nacks).await {
                                tracing::warn!("aggregate: failed to nack the batch: {nack}");
                            }
                            return Err(e);
                        }
                        commit(dispositions).await
                    })
                }),
            };
            return Ok(ReceivedBatch {
                messages: folded,
                commit,
            });
        }
    }

    async fn status(&self) -> EndpointStatus {
        self.inner.status().await
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests;
