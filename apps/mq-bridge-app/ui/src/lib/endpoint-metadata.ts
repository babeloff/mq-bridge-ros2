import type { FeatureAvailabilityResponse } from './generated/ui-types';

export type RequestBarFieldDescriptor = {
  inputId: "pub-extra-1" | "pub-extra-2" | "pub-url";
  field: string;
  label: string;
  placeholder?: string;
};

export type RequestBarLayout = {
  showMethod?: boolean;
  fields: readonly RequestBarFieldDescriptor[];
};

/**
 * Presentation tweaks for a single endpoint kind. Everything here is optional:
 * kinds are discovered from the backend JSON schema (see
 * `registerEndpointKindsFromSchema`), and anything not listed below is
 * registered with values derived from the schema itself. Only add an entry when
 * the derived defaults are not good enough.
 */
type EndpointKindOverride = {
  /** Position in the add menu. Kinds without an order are appended in schema order. */
  order?: number;
  basicFields?: readonly string[];
  requestBar?: RequestBarLayout;
  publisher?: boolean;
  consumer?: boolean;
  responseCapable?: boolean;
  requiresFeature?: string;
  /** Fallback for the schema's `format: "structural_endpoint"` marker. */
  structural?: boolean;
};

type EndpointKindMetadata = {
  kind: string;
  structural: boolean;
  order: number;
  basicFields: readonly string[];
  requestBar: RequestBarLayout;
  publisher: boolean;
  consumer: boolean;
  responseCapable: boolean;
  requiresFeature?: string;
};

const ENDPOINT_KIND_OVERRIDES = {
  http: {
    order: 0,
    basicFields: ["method", "url", "path"],
    requestBar: {
      showMethod: true,
      fields: [{ inputId: "pub-url", field: "url", label: "URL", placeholder: "https://example.com/api" }],
    },
    responseCapable: true,
  },
  websocket: {
    order: 1,
    basicFields: ["url"],
    requestBar: {
      showMethod: true,
      fields: [{ inputId: "pub-url", field: "url", label: "URL", placeholder: "ws://localhost:8080" }],
    },
  },
  grpc: {
    order: 2,
    basicFields: ["url", "topic"],
    requestBar: {
      fields: [{ inputId: "pub-url", field: "url", label: "URL", placeholder: "http://localhost:50051" }],
    },
  },
  nats: {
    order: 3,
    basicFields: ["url", "subject", "stream"],
    requestBar: {
      fields: [
        { inputId: "pub-extra-1", field: "subject", label: "SUBJECT", placeholder: "events.created" },
        { inputId: "pub-url", field: "url", label: "SERVERS", placeholder: "nats://localhost:4222" },
      ],
    },
    responseCapable: true,
  },
  memory: {
    order: 4,
    basicFields: ["topic"],
    requestBar: {
      fields: [{ inputId: "pub-url", field: "topic", label: "TOPIC", placeholder: "events" }],
    },
    responseCapable: true,
  },
  amqp: {
    order: 5,
    basicFields: ["url", "queue", "subscribe_mode", "exchange"],
    requestBar: {
      fields: [
        { inputId: "pub-extra-1", field: "queue", label: "QUEUE", placeholder: "jobs" },
        { inputId: "pub-url", field: "url", label: "URL", placeholder: "amqp://guest:guest@localhost:5672/%2f" },
      ],
    },
    responseCapable: true,
  },
  kafka: {
    order: 6,
    basicFields: ["url", "topic", "group_id"],
    requestBar: {
      fields: [
        { inputId: "pub-extra-1", field: "topic", label: "TOPIC", placeholder: "events" },
        { inputId: "pub-url", field: "url", label: "BROKERS", placeholder: "kafka:9092" },
      ],
    },
    responseCapable: true,
  },
  pulsar: {
    order: 7,
    basicFields: ["url", "topic", "subscription"],
    requestBar: {
      fields: [
        { inputId: "pub-extra-1", field: "topic", label: "TOPIC", placeholder: "persistent://public/default/events" },
        { inputId: "pub-url", field: "url", label: "BROKER", placeholder: "pulsar://localhost:6650" },
      ],
    },
  },
  mqtt: {
    order: 8,
    basicFields: ["url", "topic"],
    requestBar: {
      fields: [
        { inputId: "pub-extra-1", field: "topic", label: "TOPIC", placeholder: "events/updates" },
        { inputId: "pub-url", field: "url", label: "BROKER", placeholder: "tcp://localhost:1883" },
      ],
    },
    responseCapable: true,
  },
  mongodb: {
    order: 9,
    basicFields: ["url", "database", "collection", "change_stream"],
    requestBar: {
      fields: [
        { inputId: "pub-extra-1", field: "database", label: "DATABASE", placeholder: "app" },
        { inputId: "pub-extra-2", field: "collection", label: "COLLECTION", placeholder: "messages" },
        { inputId: "pub-url", field: "url", label: "URL", placeholder: "mongodb://localhost:27017" },
      ],
    },
    responseCapable: true,
  },
  sqlx: {
    order: 10,
    basicFields: ["url", "table"],
    requestBar: {
      fields: [
        { inputId: "pub-extra-1", field: "table", label: "TABLE", placeholder: "events" },
        { inputId: "pub-url", field: "url", label: "URL", placeholder: "postgres://user:pass@localhost/db" },
      ],
    },
  },
  zeromq: {
    order: 11,
    basicFields: ["url", "topic"],
    requestBar: {
      fields: [
        { inputId: "pub-extra-1", field: "topic", label: "TOPIC", placeholder: "events" },
        { inputId: "pub-url", field: "url", label: "URL", placeholder: "tcp://127.0.0.1:5555" },
      ],
    },
    responseCapable: true,
  },
  file: {
    order: 12,
    basicFields: ["path", "mode"],
    requestBar: {
      fields: [{ inputId: "pub-url", field: "path", label: "PATH", placeholder: "/tmp/messages.jsonl" }],
    },
  },
  dir_spool: {
    // Between `file` (12) and `static` (13); the integer ranks either side are taken.
    order: 12.5,
    basicFields: ["path", "payload_extension", "drain_on_read"],
    requestBar: {
      fields: [{ inputId: "pub-url", field: "path", label: "PATH", placeholder: "/tmp/spool" }],
    },
  },
  sled: {
    order: 15,
    basicFields: ["path", "tree"],
    requestBar: {
      fields: [
        { inputId: "pub-extra-1", field: "tree", label: "TREE", placeholder: "default" },
        { inputId: "pub-url", field: "path", label: "PATH", placeholder: "./data/sled" },
      ],
    },
  },
  ibmmq: {
    order: 16,
    basicFields: ["url", "queue_manager", "channel", "queue", "topic", "username", "password"],
    requestBar: {
      fields: [
        { inputId: "pub-extra-1", field: "queue", label: "QUEUE", placeholder: "DEV.QUEUE.1" },
        { inputId: "pub-extra-2", field: "topic", label: "TOPIC", placeholder: "topic://events" },
        { inputId: "pub-url", field: "url", label: "HOST", placeholder: "mq-host(1414)" },
      ],
    },
    requiresFeature: "ibm_mq",
  },
  aws: {
    order: 23,
    basicFields: ["region", "queue_url", "topic_arn", "access_key", "secret_key"],
  },
  redis_streams: {
    order: 24,
    basicFields: ["url", "stream", "group"],
    requestBar: {
      fields: [
        { inputId: "pub-extra-1", field: "stream", label: "STREAM", placeholder: "events" },
        { inputId: "pub-url", field: "url", label: "URL", placeholder: "redis://localhost:6379" },
      ],
    },
    requiresFeature: "redis_streams",
  },
  object_store: {
    order: 25,
    basicFields: ["url", "format"],
    requestBar: {
      fields: [{ inputId: "pub-url", field: "url", label: "URL", placeholder: "s3://bucket/prefix" }],
    },
    requiresFeature: "object_store",
  },
  postgres_cdc: {
    order: 26,
    basicFields: ["url", "publication", "slot_name", "publication_tables"],
    // Source only: mq-bridge has no postgres_cdc publisher.
    publisher: false,
  },
  clickhouse: {
    order: 27,
    basicFields: ["url", "table", "database"],
    requestBar: {
      fields: [
        { inputId: "pub-extra-1", field: "table", label: "TABLE", placeholder: "events" },
        { inputId: "pub-url", field: "url", label: "URL", placeholder: "http://localhost:8123" },
      ],
    },
  },
  http_bulk: {
    order: 28,
    basicFields: ["url", "headers", "operation", "upsert", "delete", "read"],
    requestBar: {
      fields: [{ inputId: "pub-url", field: "url", label: "URL", placeholder: "http://localhost:7700" }],
    },
  },

  // Structural kinds. `structural` here is only the offline fallback for the
  // schema's `format: "structural_endpoint"` marker.
  static: { order: 13, basicFields: ["static"], structural: true, consumer: true },
  ref: { order: 14, basicFields: ["ref"], structural: true, publisher: false, consumer: false },
  switch: { order: 17, basicFields: ["metadata_key", "default", "cases", "when"], structural: true, consumer: false },
  fanout: { order: 18, basicFields: ["endpoints"], structural: true, consumer: false },
  // Usable in both directions: publishers omit `correlation_id`, consumers require it.
  stream_buffer: { order: 19, basicFields: ["topic", "correlation_id", "capacity"], structural: true, consumer: true },
  request: { order: 20, basicFields: ["to", "forward_to"], structural: true, consumer: false },
  reader: { order: 21, basicFields: [], structural: true, publisher: false, consumer: false },
  response: { order: 22, basicFields: [], structural: true, publisher: false, consumer: false },
  custom: { order: 28, basicFields: [], structural: true, publisher: false, consumer: false },
  // `null` carries no structural marker in the schema, but it is one.
  null: { order: 29, basicFields: [], structural: true, publisher: false, consumer: false },
} as const satisfies Record<string, EndpointKindOverride>;

/** The curated kinds only — schema-derived kinds are appended at runtime and
 * are not part of this union, so this is not the exhaustive set. */
export type CuratedEndpointKind = keyof typeof ENDPOINT_KIND_OVERRIDES;

/** Kinds discovered from the schema are appended after the curated ones. */
const DERIVED_ORDER_BASE = 1000;
const MAX_DERIVED_BASIC_FIELDS = 4;
const URL_LIKE_FIELDS = new Set(["url", "path", "uri", "address", "servers", "brokers"]);
const EXTRA_INPUT_IDS = ["pub-extra-1", "pub-extra-2"] as const;

const registry = new Map<string, EndpointKindMetadata>();

/**
 * Derived collections. These keep a stable identity and are refilled in place
 * whenever the registry changes, so importers never hold a stale reference.
 */
export const KNOWN_ENDPOINT_ROOT_KEYS: string[] = [];
export const PUBLISHER_TYPE_OPTIONS: string[] = [];
export const CONSUMER_TYPE_OPTIONS: string[] = [];
export const STRUCTURAL_ENDPOINT_KINDS: Set<string> = new Set();
export const RESPONSE_CAPABLE_CONSUMER_TYPES: Set<string> = new Set();
export const BASIC_ENDPOINT_FIELDS: Record<string, readonly string[]> = {};
export const REQUEST_BAR_LAYOUTS: Record<string, RequestBarLayout> = {};

function resolveMetadata(
  kind: string,
  structural: boolean,
  derived: { order: number; basicFields: readonly string[]; requestBar: RequestBarLayout },
): EndpointKindMetadata {
  const override = (ENDPOINT_KIND_OVERRIDES as Record<string, EndpointKindOverride>)[kind] || {};
  return {
    kind,
    structural,
    order: override.order ?? derived.order,
    basicFields: override.basicFields ?? derived.basicFields,
    requestBar: override.requestBar ?? derived.requestBar,
    // Transports work in both directions unless stated otherwise; structural
    // kinds have to opt in, since most of them are output-only helpers.
    publisher: override.publisher ?? true,
    consumer: override.consumer ?? !structural,
    responseCapable: override.responseCapable ?? false,
    requiresFeature: override.requiresFeature,
  };
}

function byOrderThenKind(left: EndpointKindMetadata, right: EndpointKindMetadata): number {
  return left.order - right.order || left.kind.localeCompare(right.kind);
}

function rebuildDerivedCollections(): void {
  const entries = [...registry.values()].sort(byOrderThenKind);

  KNOWN_ENDPOINT_ROOT_KEYS.length = 0;
  PUBLISHER_TYPE_OPTIONS.length = 0;
  CONSUMER_TYPE_OPTIONS.length = 0;
  STRUCTURAL_ENDPOINT_KINDS.clear();
  RESPONSE_CAPABLE_CONSUMER_TYPES.clear();
  for (const key of Object.keys(BASIC_ENDPOINT_FIELDS)) delete BASIC_ENDPOINT_FIELDS[key];
  for (const key of Object.keys(REQUEST_BAR_LAYOUTS)) delete REQUEST_BAR_LAYOUTS[key];

  for (const entry of entries) {
    KNOWN_ENDPOINT_ROOT_KEYS.push(entry.kind);
    BASIC_ENDPOINT_FIELDS[entry.kind] = entry.basicFields;
    if (entry.publisher) {
      PUBLISHER_TYPE_OPTIONS.push(entry.kind);
      REQUEST_BAR_LAYOUTS[entry.kind] = entry.requestBar;
    }
    if (entry.consumer) CONSUMER_TYPE_OPTIONS.push(entry.kind);
    if (entry.structural) STRUCTURAL_ENDPOINT_KINDS.add(entry.kind);
    if (entry.responseCapable) RESPONSE_CAPABLE_CONSUMER_TYPES.add(entry.kind);
  }
}

function registerKind(
  kind: string,
  structural: boolean,
  derived: { order: number; basicFields: readonly string[]; requestBar: RequestBarLayout },
): void {
  registry.set(kind, resolveMetadata(kind, structural, derived));
}

function seedFromOverrides(): void {
  registry.clear();
  let index = 0;
  for (const [kind, override] of Object.entries(ENDPOINT_KIND_OVERRIDES as Record<string, EndpointKindOverride>)) {
    registerKind(kind, override.structural === true, {
      order: DERIVED_ORDER_BASE + index++,
      basicFields: [],
      requestBar: { fields: [] },
    });
  }
  rebuildDerivedCollections();
}

seedFromOverrides();

type SchemaNode = {
  $defs?: Record<string, SchemaNode>;
  $ref?: string;
  type?: string;
  format?: string;
  properties?: Record<string, SchemaNode>;
  required?: string[];
  oneOf?: SchemaNode[];
};

function resolveRef(node: SchemaNode | undefined, defs: Record<string, SchemaNode>): SchemaNode | undefined {
  if (!node?.$ref?.startsWith("#/$defs/")) return node;
  return defs[node.$ref.slice("#/$defs/".length)];
}

function isScalarField(node: SchemaNode | undefined): boolean {
  return !!node && (node.type === "string" || node.type === "number" || node.type === "integer" || node.type === "boolean");
}

/**
 * Basic fields for a kind we have no override for: the config's required
 * properties, which are the ones a user cannot leave out anyway.
 */
function deriveBasicFields(config: SchemaNode | undefined, kind: string): readonly string[] {
  const required = config?.required;
  if (!required?.length) return config?.properties ? [] : [kind];
  return required.slice(0, MAX_DERIVED_BASIC_FIELDS);
}

/**
 * Request bar for a kind we have no override for: its required scalar fields,
 * with the url-ish one in the wide input and up to two others beside it.
 */
function deriveRequestBar(config: SchemaNode | undefined, basicFields: readonly string[]): RequestBarLayout {
  const scalars = basicFields.filter((field) => isScalarField(config?.properties?.[field]));
  const urlField = scalars.find((field) => URL_LIKE_FIELDS.has(field));
  const fields: RequestBarFieldDescriptor[] = [];
  scalars
    .filter((field) => field !== urlField)
    .slice(0, EXTRA_INPUT_IDS.length)
    .forEach((field, index) => {
      fields.push({ inputId: EXTRA_INPUT_IDS[index], field, label: formatEndpointTypeLabel(field) });
    });
  if (urlField) {
    fields.push({ inputId: "pub-url", field: urlField, label: formatEndpointTypeLabel(urlField) });
  }
  return { fields };
}

/**
 * Rebuild the registry from the backend's JSON schema. Every `Endpoint` variant
 * becomes a kind, and variants tagged `format: "structural_endpoint"` are
 * grouped separately in the add menus. New transports therefore show up without
 * a UI change; `ENDPOINT_KIND_OVERRIDES` only refines how they are presented.
 *
 * Returns the registered kinds, or an empty list when the schema has no
 * recognisable `Endpoint` definition (in which case the curated list stands).
 */
export function registerEndpointKindsFromSchema(schema: unknown): string[] {
  const root = (schema || {}) as SchemaNode;
  const defs = root.$defs || {};
  const variants = defs.Endpoint?.oneOf;
  if (!Array.isArray(variants) || variants.length === 0) return [];

  const seeded = new Map(registry);
  registry.clear();
  variants.forEach((variant, index) => {
    const kind = variant.required?.[0] || Object.keys(variant.properties || {})[0];
    if (!kind) return;
    const config = resolveRef(variant.properties?.[kind], defs);
    const basicFields = deriveBasicFields(config, kind);
    registerKind(kind, variant.format === "structural_endpoint" || seeded.get(kind)?.structural === true, {
      order: DERIVED_ORDER_BASE + index,
      basicFields,
      requestBar: deriveRequestBar(config, basicFields),
    });
  });

  if (registry.size === 0) {
    registry.clear();
    for (const [kind, entry] of seeded) registry.set(kind, entry);
    return [];
  }

  rebuildDerivedCollections();
  return [...KNOWN_ENDPOINT_ROOT_KEYS];
}

/** Restore the curated defaults. Used by tests. */
export function resetEndpointKinds(): void {
  seedFromOverrides();
}

export type EndpointTypeGroup = {
  /** Empty for the primary group, which is rendered without a heading. */
  label: string;
  kinds: string[];
};

/**
 * Add-menu contents for a role: transports first and unlabelled so they stay one
 * click away, structural endpoints below under a heading.
 */
export function getEndpointTypeGroups(
  role: "publisher" | "consumer",
  features?: FeatureAvailabilityResponse,
): EndpointTypeGroup[] {
  const kinds = role === "publisher" ? PUBLISHER_TYPE_OPTIONS : CONSUMER_TYPE_OPTIONS;
  const available = features ? filterEndpointsByFeatures(kinds, features) : [...kinds];
  const groups: EndpointTypeGroup[] = [
    { label: "", kinds: available.filter((kind) => !STRUCTURAL_ENDPOINT_KINDS.has(kind)) },
    { label: "Routing", kinds: available.filter((kind) => STRUCTURAL_ENDPOINT_KINDS.has(kind)) },
  ];
  return groups.filter((group) => group.kinds.length > 0);
}

/**
 * Filter endpoint types based on available backend features.
 * This allows the UI to dynamically show/hide endpoint types like IBM MQ
 * based on what the backend was compiled with.
 */
export function filterEndpointsByFeatures(
  endpoints: readonly string[],
  features: FeatureAvailabilityResponse
): string[] {
  return endpoints.filter((kind) => {
    const requiresFeature = registry.get(kind)?.requiresFeature;
    if (!requiresFeature) return true;

    // Check if the required feature is available
    const featureKey = requiresFeature as keyof FeatureAvailabilityResponse;
    return features[featureKey] === true;
  });
}

export function getPublisherTypeOptions(features: FeatureAvailabilityResponse): string[] {
  return filterEndpointsByFeatures(PUBLISHER_TYPE_OPTIONS, features);
}

export function getConsumerTypeOptions(features: FeatureAvailabilityResponse): string[] {
  return filterEndpointsByFeatures(CONSUMER_TYPE_OPTIONS, features);
}

/**
 * Human-facing label for an endpoint kind: uppercased with underscores rendered
 * as spaces (e.g. "redis_streams" -> "REDIS STREAMS").
 */
export function formatEndpointTypeLabel(type: string | null | undefined): string {
  return String(type ?? "").replace(/_/g, " ").toUpperCase();
}

/**
 * Unique, sorted list of publisher ids (falling back to name) used as datalist
 * suggestions when referencing a publisher from an endpoint ref field.
 */
export function collectPublisherIdSuggestions(
  publishers: ReadonlyArray<{ id?: string; name?: string }> | null | undefined,
): string[] {
  return Array.from(
    new Set(
      (publishers || [])
        .map((publisher) => String(publisher?.id || publisher?.name || "").trim())
        .filter(Boolean),
    ),
  ).sort((a, b) => a.localeCompare(b));
}
