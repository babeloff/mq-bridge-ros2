//  mq-bridge
//  © Copyright 2026, by Marco Mengelkoch
//  Licensed under MIT OR Apache-2.0, see LICENSE file for more details
//  git clone https://github.com/marcomq/mq-bridge

use super::*;
use crate::support::lookup_batch::{self, ListQuery};
use mongodb::options::UpdateModifications;

pub struct MongoDbPublisher {
    collection: Collection<Document>,
    meta_collection: Collection<Document>,
    db: Database,
    // Retains the shared registry entry so concurrent publishers reuse this client/pool.
    _shared_client: std::sync::Arc<Client>,
    collection_name: String,
    request_reply: bool,
    request_timeout: Duration,
    reply_polling_interval: Duration,
    format: MongoDbFormat,
    id_field: Option<String>,
    id_template: Option<CompiledTemplate>,
    report_outcome: bool,
    find: Option<CompiledTemplate>,
    /// Set when `find` holds `"field": {"$in": [${…}]}`: one query answers a whole batch.
    find_list: Option<(ListQuery, CompiledTemplate)>,
    /// Opt-in write: upsert this update on the `find` match and answer with the result.
    update: Option<CompiledTemplate>,
}

/// Metadata key carrying the insert outcome when `report_outcome` is enabled.
pub(crate) const OUTCOME_KEY: &str = "mongodb.outcome";
pub(crate) const OUTCOME_INSERTED: &str = "inserted";
pub(crate) const OUTCOME_EXISTED: &str = "existed";
/// Metadata key telling whether a `find` matched a document.
const FOUND_KEY: &str = "mongodb.found";
/// Distinct `update` keys written in parallel per batch; one key's messages run in order.
const UPDATE_CONCURRENCY: usize = 16;

fn mongodb_uses_sequencer(request_reply: bool, format: &MongoDbFormat) -> bool {
    !request_reply && !matches!(format, MongoDbFormat::Raw)
}

pub(crate) fn namespaced_sequencer_id(collection_name: &str) -> String {
    format!("{}:sequencer", collection_name)
}

impl MongoDbPublisher {
    fn uses_sequencer(&self) -> bool {
        mongodb_uses_sequencer(self.request_reply, &self.format)
    }

    pub async fn new(config: &MongoDbConfig) -> anyhow::Result<Self> {
        let id_template = config
            .id_field
            .as_deref()
            .filter(|value| value.contains("${"))
            .map(|template| {
                let compiled = CompiledTemplate::compile(template, None)
                    .context("invalid MongoDB `id_field` template")?;
                if !compiled.is_dynamic() || !compiled.has_only_replay_stable_tokens() {
                    anyhow::bail!(
                        "MongoDB `id_field` template must contain only replay-stable payload or metadata tokens"
                    );
                }
                Ok(compiled)
            })
            .transpose()?;
        let id_field = config
            .id_field
            .as_ref()
            .filter(|value| !value.contains("${"))
            .cloned();
        let collection_name = config
            .collection
            .as_deref()
            .ok_or_else(|| anyhow!("Collection name is required for MongoDB publisher"))?;
        let find = config
            .find
            .as_deref()
            .map(|t| {
                CompiledTemplate::compile(t, Some("application/json"))
                    .context("invalid MongoDB `find` template")
            })
            .transpose()?;
        let find_list = match config.find.as_deref().map(lookup_batch::parse_mongodb) {
            Some(parsed) => parsed?.map(|list| {
                let element = format!("{}{}{}", list.prefix, list.token, list.suffix);
                CompiledTemplate::compile(&element, Some("application/json"))
                    .context("invalid MongoDB `find` template")
                    .map(|element| (list, element))
            }),
            None => None,
        }
        .transpose()?;
        let update = config
            .update
            .as_deref()
            .map(|t| {
                CompiledTemplate::compile(t, Some("application/json"))
                    .context("invalid MongoDB `update` template")
            })
            .transpose()?;
        if update.is_some() && find.is_none() {
            anyhow::bail!("MongoDB `update` requires `find`");
        }
        if update.is_some() && find_list.is_some() {
            anyhow::bail!("MongoDB `update` needs one key per message; `find` must not use `$in`");
        }
        let shared_client = create_shared_client(config).await?;
        let client = (*shared_client).clone();
        let db = client.database(&config.database);
        if find.is_some() {
            if update.is_some() {
                info!(database = %config.database, collection = %collection_name, "MongoDB find publisher connected; `update` writes on every lookup");
            } else {
                info!(database = %config.database, collection = %collection_name, "MongoDB find publisher connected");
            }
            return Ok(Self {
                collection: db.collection(collection_name),
                meta_collection: db.collection(collection_name),
                db,
                _shared_client: shared_client,
                collection_name: collection_name.to_string(),
                request_reply: false,
                request_timeout: Duration::ZERO,
                reply_polling_interval: Duration::ZERO,
                format: config.format.clone(),
                id_field: None,
                id_template: None,
                report_outcome: false,
                find,
                find_list,
                update,
            });
        }

        if let Some(capped_size) = config.capped_size_bytes {
            let collections = db
                .list_collection_names()
                .filter(doc! { "name": collection_name })
                .await?;
            if collections.is_empty() {
                info!(collection = %collection_name, size = %capped_size, "Creating capped collection");
                db.create_collection(collection_name)
                    .capped(true)
                    .size(capped_size as u64)
                    .await?;
            }
        }

        let collection = db.collection(collection_name);
        let meta_collection_name = config
            .meta_collection
            .clone()
            .unwrap_or_else(|| collection_name.to_string());
        let meta_collection = db.collection(&meta_collection_name);

        if mongodb_uses_sequencer(config.request_reply, &config.format) {
            // Ensure unique index on seq. The sequencer doc has 'seq_counter', so it won't conflict.
            let index_options = mongodb::options::IndexOptions::builder()
                .unique(true)
                .sparse(true) // Only index documents that have the seq field
                .build();
            let index_model = IndexModel::builder()
                .keys(doc! { "seq": 1 })
                .options(index_options)
                .build();
            if let Err(e) = collection.create_index(index_model).await {
                warn!(
                    "Failed to create seq index on collection {}: {}",
                    collection_name, e
                );
            }
        }
        info!(database = %config.database, collection = %collection_name, request_reply = %config.request_reply, "MongoDB publisher connected");

        if let Some(ttl) = config.ttl_seconds {
            let options = mongodb::options::IndexOptions::builder()
                .expire_after(Duration::from_secs(ttl))
                .build();
            let model = IndexModel::builder()
                .keys(doc! { "created_at": 1 })
                .options(options)
                .build();
            if let Err(e) = collection.create_index(model).await {
                warn!(
                    "Failed to create TTL index on publisher collection {} : {}",
                    collection_name, e
                );
            }
        }

        if config.request_reply {
            let reply_collection_name = format!("{}_replies", collection_name);
            let reply_collection = db.collection::<Document>(&reply_collection_name);
            let index_model = IndexModel::builder()
                .keys(doc! { "metadata.correlation_id": 1 })
                .build();
            if let Err(e) = reply_collection.create_index(index_model).await {
                warn!(
                    "Failed to create correlation_id index on reply collection {} : {}",
                    reply_collection_name, e
                );
            }
            // Also apply TTL to the reply collection if configured, to clean up unconsumed replies.
            if let Some(ttl) = config.ttl_seconds {
                let options = mongodb::options::IndexOptions::builder()
                    .expire_after(Duration::from_secs(ttl))
                    .build();
                let model = IndexModel::builder()
                    .keys(doc! { "created_at": 1 })
                    .options(options)
                    .build();
                if let Err(e) = reply_collection.create_index(model).await {
                    warn!(
                        "Failed to create TTL index on reply collection {} : {}",
                        reply_collection_name, e
                    );
                }
            }
        }
        Ok(Self {
            collection,
            meta_collection,
            db,
            _shared_client: shared_client,
            collection_name: collection_name.to_string(),
            request_reply: config.request_reply,
            request_timeout: Duration::from_millis(config.request_timeout_ms.unwrap_or(30000)),
            reply_polling_interval: Duration::from_millis(config.reply_polling_ms.unwrap_or(50)),
            format: config.format.clone(),
            id_field,
            id_template,
            report_outcome: config.report_outcome,
            find: None,
            find_list: None,
            update: None,
        })
    }

    /// Runs the rendered `find` filter and answers with the first match, or an empty payload.
    async fn find_one(
        &self,
        template: &CompiledTemplate,
        message: &CanonicalMessage,
    ) -> Result<Sent, PublisherError> {
        if let Some((list, element)) = &self.find_list {
            let found = self
                .find_many(list, element, std::slice::from_ref(message))
                .await?
                .pop()
                .flatten();
            let payload = match &found {
                Some(doc) => {
                    serde_json::to_vec(doc).map_err(|e| PublisherError::NonRetryable(e.into()))?
                }
                None => Vec::new(),
            };
            let mut response = CanonicalMessage::new(payload, Some(message.message_id));
            response
                .metadata
                .insert(FOUND_KEY.to_string(), found.is_some().to_string());
            return Ok(Sent::Response(response));
        }
        let found = match &self.update {
            // A message without a key writes nothing: it would share one counter with all such.
            Some(update) => match template.render_resolved(Some(message)) {
                Some(rendered) => {
                    self.find_and_update(find_filter(&rendered)?, update, message)
                        .await?
                }
                None => None,
            },
            None => self
                .collection
                .find_one(find_filter(&template.render(Some(message)))?)
                .await
                .map_err(|e| PublisherError::Retryable(e.into()))?,
        };
        let payload = match &found {
            Some(doc) => serde_json::to_vec(&Bson::Document(doc.clone()).into_relaxed_extjson())
                .map_err(|e| PublisherError::NonRetryable(e.into()))?,
            None => Vec::new(),
        };
        let mut response = CanonicalMessage::new(payload, Some(message.message_id));
        response
            .metadata
            .insert(FOUND_KEY.to_string(), found.is_some().to_string());
        Ok(Sent::Response(response))
    }

    /// Upserts the rendered `update` on `filter` and returns the document after the update.
    async fn find_and_update(
        &self,
        filter: Document,
        update: &CompiledTemplate,
        message: &CanonicalMessage,
    ) -> Result<Option<Document>, PublisherError> {
        let update = update_modifications(&update.render(Some(message)))?;
        let options = FindOneAndUpdateOptions::builder()
            .upsert(true)
            .return_document(ReturnDocument::After)
            .build();
        self.collection
            .find_one_and_update(filter, update)
            .with_options(options)
            .await
            .map_err(update_error)
    }

    /// Answers every message with its updated document: distinct keys in parallel, the
    /// messages of one key in order, so each sees the state its predecessors left.
    async fn update_many(
        &self,
        find: &CompiledTemplate,
        update: &CompiledTemplate,
        messages: &[CanonicalMessage],
    ) -> Result<Vec<Option<serde_json::Value>>, PublisherError> {
        let mut groups: Vec<(Vec<u8>, Vec<usize>)> = Vec::new();
        let mut by_filter: HashMap<Vec<u8>, usize> = HashMap::new();
        // Keyless messages join no group and stay unanswered, like `find_many`'s.
        for (i, message) in messages.iter().enumerate() {
            let Some(rendered) = find.render_resolved(Some(message)) else {
                continue;
            };
            let group = *by_filter.entry(rendered.clone()).or_insert_with(|| {
                groups.push((rendered, Vec::new()));
                groups.len() - 1
            });
            groups[group].1.push(i);
        }
        let mut answers = futures::stream::iter(groups)
            .map(|(rendered, indices)| async move {
                let mut out = Vec::with_capacity(indices.len());
                let filter = match find_filter(&rendered) {
                    Ok(filter) => filter,
                    Err(e) => {
                        let e = rejected_in_batch(e)?;
                        warn!(error = %e, "MongoDB `update` lookup: invalid filter; answering not found");
                        out.extend(indices.into_iter().map(|i| (i, None)));
                        return Ok(out);
                    }
                };
                for i in indices {
                    let doc = match self.find_and_update(filter.clone(), update, &messages[i]).await {
                        Ok(doc) => doc,
                        // One rejected message must neither stall nor drop the rest of the batch.
                        Err(e) => {
                            let e = rejected_in_batch(e)?;
                            warn!(error = %e, "MongoDB `update` lookup rejected; answering not found");
                            None
                        }
                    };
                    out.push((i, doc.map(|d| Bson::Document(d).into_relaxed_extjson())));
                }
                Ok::<_, PublisherError>(out)
            })
            .buffer_unordered(UPDATE_CONCURRENCY);
        let mut results = vec![None; messages.len()];
        while let Some(group) = answers.next().await {
            for (i, doc) in group? {
                results[i] = doc;
            }
        }
        Ok(results)
    }

    /// Answers every message with one `$in` query per chunk of distinct keys.
    async fn find_many(
        &self,
        list: &ListQuery,
        element: &CompiledTemplate,
        messages: &[CanonicalMessage],
    ) -> Result<Vec<Option<serde_json::Value>>, PublisherError> {
        // A token without a value renders no key: that message is simply not found.
        let elements: Vec<Option<String>> = messages
            .iter()
            .map(|m| {
                element
                    .render_resolved(Some(m))
                    .and_then(|b| String::from_utf8(b).ok())
            })
            .collect();
        let keys: Vec<Option<String>> = elements
            .iter()
            .map(|e| {
                e.as_deref()
                    .and_then(|e| serde_json::from_str::<serde_json::Value>(e).ok())
                    .and_then(|v| lookup_batch::key_of(&v))
            })
            .collect();
        let mut records = Vec::new();
        for chunk in lookup_batch::distinct(&keys).chunks(lookup_batch::MAX_KEYS_PER_QUERY) {
            let listed: Vec<&str> = chunk
                .iter()
                .filter_map(|&i| elements[i].as_deref())
                .collect();
            let rendered = format!("{}{}{}", list.before, listed.join(", "), list.after);
            let filter = find_filter(rendered.as_bytes())?;
            let mut cursor = self
                .collection
                .find(filter)
                .await
                .map_err(|e| PublisherError::Retryable(e.into()))?;
            let wanted: std::collections::HashSet<&str> =
                chunk.iter().filter_map(|&i| keys[i].as_deref()).collect();
            let mut matched = std::collections::HashSet::with_capacity(wanted.len());
            // Keep the first document per key and stop once every key has one.
            while let Some(doc) = cursor.next().await {
                let doc = doc.map_err(|e| PublisherError::Retryable(e.into()))?;
                let record = Bson::Document(doc).into_relaxed_extjson();
                match lookup_batch::field(&record, &list.key) {
                    // `answer` reports the missing key field.
                    None => records.push(record),
                    Some(v) => {
                        if let Some(k) = lookup_batch::key_of(v) {
                            if wanted.contains(k.as_str()) && matched.insert(k) {
                                records.push(record);
                            }
                        }
                    }
                }
                if matched.len() == wanted.len() {
                    break;
                }
            }
        }
        lookup_batch::answer(&keys, records, &list.key).map_err(PublisherError::NonRetryable)
    }

    async fn recover_correlation_id_from_duplicate(
        &self,
        message: &mut CanonicalMessage,
    ) -> Result<(), PublisherError> {
        // Look up by the same `_id` message_to_document wrote: the id_field value when
        // configured, else the message_id UUID. Otherwise an explicit-id duplicate is
        // never found and the request retries forever.
        let id_bson =
            match explicit_id_bson(message, self.id_field.as_deref(), self.id_template.as_ref())
                .map_err(PublisherError::NonRetryable)?
            {
                Some(id) => id,
                None => Bson::from(mongodb::bson::Uuid::from_bytes(
                    message.message_id.to_be_bytes(),
                )),
            };
        let filter = doc! { "_id": id_bson };
        match self.collection.find_one(filter).await {
            Ok(Some(existing_doc)) => {
                let existing_msg = parse_mongodb_document(existing_doc).map_err(|e| {
                    PublisherError::NonRetryable(anyhow::anyhow!(
                        "Failed to parse existing document: {}",
                        e
                    ))
                })?;

                if let Some(cid) = existing_msg.metadata.get("correlation_id") {
                    message
                        .metadata
                        .insert("correlation_id".to_string(), cid.clone());
                }
                if let Some(rt) = existing_msg.metadata.get("reply_to") {
                    message.metadata.insert("reply_to".to_string(), rt.clone());
                }
                Ok(())
            }
            Ok(None) => Err(PublisherError::Retryable(anyhow::anyhow!(
                "Duplicate key error but document not found"
            ))),
            Err(e) => Err(PublisherError::Retryable(anyhow::anyhow!(
                "Failed to fetch existing document: {}",
                e
            ))),
        }
    }

    fn outcome_or_ack(&self, message: CanonicalMessage, outcome: &str) -> Sent {
        tag_outcome(self.report_outcome, message, outcome)
    }
}

/// With `report_outcome`, tag the message with `mongodb.outcome` and return it as a
/// `Sent::Response` so a downstream `switch` can branch; otherwise a plain `Ack`.
pub(crate) fn tag_outcome(
    report_outcome: bool,
    mut message: CanonicalMessage,
    outcome: &str,
) -> Sent {
    if report_outcome {
        message
            .metadata
            .insert(OUTCOME_KEY.to_string(), outcome.to_string());
        Sent::Response(message)
    } else {
        Sent::Ack
    }
}

/// A server-rejected update (bad data for the pipeline) cannot succeed on retry; a network or
/// failover error can, and so can E11000 from two upserts racing to insert the same key.
fn update_error(e: mongodb::error::Error) -> PublisherError {
    let duplicate = match &*e.kind {
        ErrorKind::Write(mongodb::error::WriteFailure::WriteError(w)) => w.code == 11000,
        ErrorKind::Command(c) => c.code == 11000,
        _ => false,
    };
    let rejected = matches!(*e.kind, ErrorKind::Command(_) | ErrorKind::Write(_))
        && !duplicate
        && !e.contains_label(mongodb::error::RETRYABLE_WRITE_ERROR);
    if rejected {
        PublisherError::NonRetryable(anyhow!(e).context("MongoDB `update` rejected"))
    } else {
        PublisherError::Retryable(e.into())
    }
}

/// In a batch, a non-retryable error is the message's own and is answered as not found;
/// a retryable one still fails the batch.
fn rejected_in_batch(e: PublisherError) -> Result<anyhow::Error, PublisherError> {
    match e {
        PublisherError::NonRetryable(e) => Ok(e),
        other => Err(other),
    }
}

/// Parses a rendered `update` into an update document or an aggregation pipeline.
pub(super) fn update_modifications(rendered: &[u8]) -> Result<UpdateModifications, PublisherError> {
    let parsed = serde_json::from_slice::<serde_json::Value>(rendered)
        .map_err(anyhow::Error::from)
        .and_then(|v| Ok(Bson::try_from(v)?));
    let invalid =
        |e: anyhow::Error| PublisherError::NonRetryable(e.context("invalid MongoDB `update`"));
    match parsed.map_err(invalid)? {
        Bson::Document(d) => Ok(UpdateModifications::Document(d)),
        Bson::Array(stages) => stages
            .into_iter()
            .map(|stage| match stage {
                Bson::Document(d) => Ok(d),
                other => Err(anyhow!("pipeline stage is a {:?}", other.element_type())),
            })
            .collect::<anyhow::Result<Vec<_>>>()
            .map(UpdateModifications::Pipeline)
            .map_err(invalid),
        other => Err(invalid(anyhow!(
            "update is a {:?}, not a document or pipeline",
            other.element_type()
        ))),
    }
}

/// Parses a rendered `find` filter into a BSON document.
fn find_filter(rendered: &[u8]) -> Result<Document, PublisherError> {
    serde_json::from_slice::<serde_json::Value>(rendered)
        .map_err(anyhow::Error::from)
        .and_then(|v| Ok(Bson::try_from(v)?))
        .and_then(|b| match b {
            Bson::Document(d) => Ok(d),
            other => Err(anyhow!(
                "filter is a {:?}, not a document",
                other.element_type()
            )),
        })
        .map_err(|e| PublisherError::NonRetryable(e.context("invalid MongoDB `find` filter")))
}

#[async_trait]
impl MessagePublisher for MongoDbPublisher {
    async fn send(&self, mut message: CanonicalMessage) -> Result<Sent, PublisherError> {
        if let Some(template) = &self.find {
            return self.find_one(template, &message).await;
        }
        if !self.request_reply {
            trace!(message_id = %format!("{:032x}", message.message_id), collection = %self.collection_name, uses_sequencer = self.uses_sequencer(), "Publishing document to MongoDB");
            let mut doc = message_to_document(
                &message,
                &self.format,
                self.id_field.as_deref(),
                self.id_template.as_ref(),
            )
            .map_err(PublisherError::NonRetryable)?;

            if self.uses_sequencer() {
                // Atomically increment a sequence counter. This is safe without a transaction for just getting a sequence number.
                // If the subsequent insert fails, a sequence number might be "lost", creating a gap.
                let filter = doc! {
                    "_id": namespaced_sequencer_id(&self.collection_name)
                };
                let update = doc! { "$inc": { "seq_counter": 1_i64 } };
                let options = FindOneAndUpdateOptions::builder()
                    .upsert(true)
                    .return_document(ReturnDocument::After)
                    .build();

                let counter_doc = self
                    .meta_collection
                    .find_one_and_update(filter, update)
                    .with_options(options)
                    .await
                    .map_err(|e| PublisherError::Retryable(anyhow!(e)))?;
                let seq = counter_doc
                    .ok_or_else(|| {
                        PublisherError::Retryable(anyhow!(
                            "Sequencer document not returned after upsert"
                        ))
                    })?
                    .get_i64("seq_counter")
                    .map_err(|e| {
                        PublisherError::Retryable(anyhow!(
                            "Invalid seq_counter in sequencer: {}",
                            e
                        ))
                    })?;
                doc.insert("seq", seq);
            }

            match self.collection.insert_one(doc).await {
                Ok(_) => {}
                Err(e) => {
                    if let ErrorKind::Write(mongodb::error::WriteFailure::WriteError(ref w)) =
                        *e.kind
                    {
                        if w.code == 11000 {
                            warn!(message_id = %format!("{:032x}", message.message_id), "Duplicate key error inserting into MongoDB. Treating as idempotent success.");
                            return Ok(self.outcome_or_ack(message, OUTCOME_EXISTED));
                        }
                    }
                    return Err(PublisherError::Retryable(
                        anyhow::anyhow!(e).context("Failed to insert document into MongoDB"),
                    ));
                }
            }

            return Ok(self.outcome_or_ack(message, OUTCOME_INSERTED));
        }

        // --- Request-Reply Logic ---
        let mut correlation_id = if let Some(cid) = message.metadata.get("correlation_id") {
            cid.clone()
        } else {
            fast_uuid_v7::gen_id_string()
        };
        // Convention: reply collection is named <request_collection>_replies
        let reply_collection_name = format!("{}_replies", self.collection_name);

        message
            .metadata
            .insert("correlation_id".to_string(), correlation_id.clone());
        message
            .metadata
            .insert("reply_to".to_string(), reply_collection_name.clone());

        trace!(message_id = %format!("{:032x}", message.message_id), correlation_id = %correlation_id, collection = %self.collection_name, "Publishing request document to MongoDB");
        let doc = message_to_document(
            &message,
            &self.format,
            self.id_field.as_deref(),
            self.id_template.as_ref(),
        )
        .map_err(PublisherError::NonRetryable)?;
        match self.collection.insert_one(doc).await {
            Ok(_) => {}
            Err(e) => {
                let is_duplicate = matches!(&*e.kind, ErrorKind::Write(mongodb::error::WriteFailure::WriteError(w)) if w.code == 11000);
                if is_duplicate {
                    warn!(message_id = %format!("{:032x}", message.message_id), "Duplicate key error inserting request into MongoDB. Treating as idempotent success.");
                    self.recover_correlation_id_from_duplicate(&mut message)
                        .await?;
                    if let Some(cid) = message.metadata.get("correlation_id") {
                        correlation_id = cid.clone();
                    }
                } else {
                    return Err(PublisherError::Retryable(
                        anyhow::anyhow!(e)
                            .context("Failed to insert request document into MongoDB"),
                    ));
                }
            }
        }

        // Now, wait for the response by polling the reply collection.
        let reply_collection = self.db.collection::<Document>(&reply_collection_name);
        let filter = doc! { "metadata.correlation_id": correlation_id.clone() };

        let timeout = self.request_timeout;
        let start = Instant::now();
        let mut current_sleep = self.reply_polling_interval;

        loop {
            if start.elapsed() > timeout {
                return Err(PublisherError::NonRetryable(anyhow!(
                    "Request timed out waiting for MongoDB response"
                )));
            }

            match reply_collection.find_one_and_delete(filter.clone()).await {
                Ok(Some(doc)) => {
                    trace!(correlation_id = %correlation_id, "Received MongoDB response");
                    let response_msg = parse_mongodb_document(doc).map_err(|e| {
                        PublisherError::NonRetryable(anyhow!("Failed to parse response: {}", e))
                    })?;
                    return Ok(Sent::Response(response_msg));
                }
                Ok(None) => {
                    tokio::time::sleep(current_sleep).await;
                    current_sleep = std::cmp::min(
                        current_sleep + current_sleep / 2,
                        Duration::from_millis(500),
                    );
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Error polling for MongoDB reply. Retrying...");
                    tokio::time::sleep(current_sleep).await;
                }
            }
        }
    }

    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        if messages.is_empty() {
            return Ok(SentBatch::Ack);
        }

        if self.request_reply || self.report_outcome || self.find.is_some() {
            // report_outcome needs a per-message Response, so fan out through single send.
            return crate::traits::send_batch_helper(self, messages, |p, m| Box::pin(p.send(m)))
                .await;
        }

        trace!(count = messages.len(), collection = %self.collection_name, message_ids = ?LazyMessageIds(&messages), "Publishing batch of documents to MongoDB");
        let mut docs = Vec::with_capacity(messages.len());
        let mut failed_messages = Vec::new();
        let mut valid_messages = Vec::with_capacity(messages.len());

        for message in messages {
            match message_to_document(
                &message,
                &self.format,
                self.id_field.as_deref(),
                self.id_template.as_ref(),
            ) {
                Ok(doc) => {
                    docs.push(doc);
                    valid_messages.push(message);
                }
                Err(e) => {
                    failed_messages.push((message, PublisherError::NonRetryable(e)));
                }
            }
        }

        if docs.is_empty() {
            if failed_messages.is_empty() {
                return Ok(SentBatch::Ack);
            } else {
                return Ok(SentBatch::Partial {
                    responses: None,
                    failed: failed_messages,
                });
            }
        }

        if self.uses_sequencer() {
            // Atomically increment a sequence counter for the batch. This is safe without a transaction.
            // If the subsequent insert fails, sequence numbers might be "lost", creating gaps.
            let filter = doc! {
                "_id": namespaced_sequencer_id(&self.collection_name)
            };
            let update = doc! { "$inc": { "seq_counter": docs.len() as i64 } };
            let options = FindOneAndUpdateOptions::builder()
                .upsert(true)
                .return_document(ReturnDocument::After)
                .write_concern(
                    mongodb::options::WriteConcern::builder()
                        .w(mongodb::options::Acknowledgment::Majority)
                        .build(),
                )
                .build();
            let counter_doc = self
                .meta_collection
                .find_one_and_update(filter, update)
                .with_options(options)
                .await
                .map_err(|e| PublisherError::Retryable(anyhow!(e)))?;
            let end_seq = counter_doc
                .ok_or_else(|| {
                    PublisherError::Retryable(anyhow!(
                        "Sequencer document not returned after upsert"
                    ))
                })?
                .get_i64("seq_counter")
                .map_err(|e| {
                    PublisherError::Retryable(anyhow!("Invalid seq_counter in sequencer: {}", e))
                })?;
            let start_seq = end_seq - docs.len() as i64 + 1;

            for (i, doc) in docs.iter_mut().enumerate() {
                doc.insert("seq", start_seq + i as i64);
            }
        }

        // Unordered: an ordered insert stops at the first error, so one duplicate
        // `_id` would leave the rest uninserted and indistinguishable from a
        // failure. Capped collections take unordered inserts too and still store
        // in $natural order, so `seq` and insertion order both survive.
        match self.collection.insert_many(docs).ordered(false).await {
            Ok(_) => Ok(SentBatch::from_failures(failed_messages)),
            Err(e) => {
                if let ErrorKind::InsertMany(ref err) = *e.kind {
                    let mut errors_by_index = HashMap::new();
                    if let Some(write_errors) = &err.write_errors {
                        for we in write_errors {
                            errors_by_index.insert(we.index, we);
                        }
                    }

                    // If we have a write concern error, assume all failed to be safe (potential rollback).
                    // Since we have unique indexes, retrying is idempotent.
                    if err.write_concern_error.is_some() {
                        warn!("MongoDB write concern error detected. Retrying entire batch.");
                        for msg in valid_messages {
                            failed_messages.push((
                                msg,
                                PublisherError::Retryable(anyhow::anyhow!(
                                    "MongoDB write concern error"
                                )),
                            ));
                        }
                        return Ok(SentBatch::Partial {
                            responses: None,
                            failed: failed_messages,
                        });
                    }

                    // Every document was attempted, so an index with no write
                    // error was inserted and each error stands on its own.
                    for (i, msg) in valid_messages.into_iter().enumerate() {
                        if let Some(w) = errors_by_index.get(&i) {
                            // Duplicate `_id`: the document is already stored, which
                            // is what `id_field` uses to make a re-run idempotent.
                            if w.code != 11000 {
                                failed_messages.push((
                                    msg,
                                    PublisherError::Retryable(anyhow::anyhow!(
                                        "MongoDB write error: {:?}",
                                        w
                                    )),
                                ));
                            }
                        }
                    }

                    Ok(SentBatch::Partial {
                        responses: None,
                        failed: failed_messages,
                    })
                } else {
                    Err(PublisherError::Retryable(anyhow!(e)))
                }
            }
        }
    }

    async fn status(&self) -> EndpointStatus {
        let (healthy, error) = match self.db.run_command(doc! { "ping": 1 }).await {
            Ok(_) => (true, None),
            Err(e) => (false, Some(e.to_string())),
        };
        EndpointStatus {
            healthy,
            target: self.collection_name.clone(),
            error,
            details: serde_json::json!({ "database": self.db.name(), "request_reply": self.request_reply }),
            ..Default::default()
        }
    }

    async fn lookup_batch(
        &self,
        requests: &[CanonicalMessage],
    ) -> Option<Result<Vec<Option<serde_json::Value>>, PublisherError>> {
        if let (Some(find), Some(update)) = (&self.find, &self.update) {
            return Some(self.update_many(find, update, requests).await);
        }
        let (list, element) = self.find_list.as_ref()?;
        Some(self.find_many(list, element, requests).await)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
