import type { InstanceStatus, StatusSummary } from "./generated/ui-types";
import type { PeerStatus, RuntimeStatus } from "./runtime-status";

export type StatusState =
  | "running"
  | "unhealthy"
  | "completed"
  | "completed (error)"
  | "stopped"
  | "failed"
  | "idle";

export interface StatusRow {
  id: string;
  instance: string;
  /** True for rows of the instance serving this page. */
  current: boolean;
  name: string;
  flow: string;
  state: StatusState;
  rate: number;
  average: number;
  total: number;
  pending: string;
  startedAtMs: number | null;
}

export const SPARKLINE_SAMPLES = 60;

const KIND_LABELS: Record<string, string> = {
  cli: "CLI",
  mcp: "MCP",
  tauri: "Desktop",
  "web-ui": "Web UI",
};

function instanceLabel(instance: InstanceStatus): string {
  const kind = KIND_LABELS[instance.kind] ?? instance.kind;
  return `${kind} · ${instance.workspace_label || "workspace"} [${instance.pid}]`;
}

// Mirrors `state_label` in the CLI's `status_cmd.rs`.
export function stateLabel(summary: Partial<StatusSummary>): StatusState {
  if (summary.outcome === "completed") return summary.error ? "completed (error)" : "completed";
  if (summary.outcome === "stopped") return "stopped";
  if (summary.outcome === "failed") return "failed";
  if (!summary.running) return "stopped";
  if (summary.healthy === false || summary.error) return "unhealthy";
  return "running";
}

function pendingLabel(pending?: number | null, capacity?: number | null): string {
  if (typeof pending !== "number") return "-";
  return typeof capacity === "number" ? `${pending}/${capacity}` : String(pending);
}

function row(
  instance: InstanceStatus,
  current: boolean,
  id: string,
  name: string,
  flow: string,
  summary: Partial<StatusSummary>,
): StatusRow {
  return {
    id: `${instance.instance_id}:${id}`,
    instance: instanceLabel(instance),
    current,
    name,
    flow,
    state: stateLabel(summary),
    rate: Number(summary.throughput || 0),
    average: Number(summary.average_throughput || 0),
    total: Number(summary.message_sequence || 0),
    pending: pendingLabel(summary.pending, summary.capacity),
    startedAtMs: typeof summary.started_at_ms === "number" ? summary.started_at_ms : null,
  };
}

function instanceRows(instance: InstanceStatus, current: boolean): StatusRow[] {
  const routes = instance.routes || [];
  const rows = routes.map((route) =>
    row(
      instance,
      current,
      `route:${route.id}`,
      route.label || route.id,
      `${route.input.endpoint} → ${route.output.endpoint}`,
      route.summary || {},
    ),
  );
  for (const consumer of instance.consumers || []) {
    // A route and its consumer twin share an id; show the route only.
    if (routes.some((route) => route.id === consumer.id)) continue;
    rows.push(
      row(
        instance,
        current,
        `consumer:${consumer.id}`,
        consumer.label || consumer.id,
        consumer.endpoint || "unknown",
        consumer.summary || {},
      ),
    );
  }
  if (rows.length === 0) {
    rows.push({ ...row(instance, current, "idle", "-", "-", {}), state: "idle" });
  }
  return rows;
}

// Used when the registry is not readable from this browser (non-loopback).
function localRows(runtime: RuntimeStatus): StatusRow[] {
  return Object.entries(runtime.consumers || {}).map(([name, consumer]) => ({
    id: `local:consumer:${name}`,
    instance: "This instance",
    current: true,
    name,
    flow: consumer.status?.target || "unknown",
    state: stateLabel({
      running: consumer.running,
      healthy: consumer.status?.healthy,
      error: consumer.status?.error,
      outcome: consumer.outcome,
    }),
    rate: Number(consumer.throughput || 0),
    average: 0,
    total: Number(consumer.message_sequence || 0),
    pending: pendingLabel(consumer.status?.pending, consumer.status?.capacity),
    startedAtMs: null,
  }));
}

/** Every route and consumer on this machine, this instance's rows first. */
export function statusRows(peers: PeerStatus, runtime: RuntimeStatus): StatusRow[] {
  if (peers.instances.length === 0) return localRows(runtime);
  const isCurrent = (instance: InstanceStatus) =>
    instance.instance_id === peers.current_instance_id;
  // This instance is not in the registry: its consumers still come first.
  const local = peers.instances.some(isCurrent) ? [] : localRows(runtime);
  return local.concat(
    [
      ...peers.instances.filter(isCurrent),
      ...peers.instances.filter((instance) => !isCurrent(instance)),
    ].flatMap((instance) => instanceRows(instance, isCurrent(instance))),
  );
}

/** Appends one rate sample per row and forgets rows that are gone. */
export function recordSamples(
  history: ReadonlyMap<string, number[]>,
  rows: StatusRow[],
  limit = SPARKLINE_SAMPLES,
): Map<string, number[]> {
  const next = new Map<string, number[]>();
  for (const { id, rate } of rows) {
    next.set(id, [...(history.get(id) || []), rate].slice(-limit));
  }
  return next;
}

/** SVG polyline points for `samples`, scaled to the largest sample. */
export function sparklinePoints(samples: number[], width: number, height: number): string {
  if (samples.length < 2) return "";
  const max = Math.max(...samples);
  const step = width / (samples.length - 1);
  return samples
    .map((sample, index) => {
      const y = max > 0 ? height - (sample / max) * height : height;
      return `${(index * step).toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
}

export function rateLabel(value: number): string {
  return value > 0 ? `${value.toFixed(1)}/s` : "-";
}

export function uptimeLabel(startedAtMs: number | null, nowMs: number): string {
  if (startedAtMs === null) return "-";
  const secs = Math.max(0, Math.floor((nowMs - startedAtMs) / 1000));
  const pad = (value: number) => String(value).padStart(2, "0");
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m${pad(secs % 60)}s`;
  return `${Math.floor(secs / 3600)}h${pad(Math.floor((secs % 3600) / 60))}m`;
}
