import { describe, expect, test } from "vitest";
import {
  recordSamples,
  sparklinePoints,
  stateLabel,
  statusRows,
  uptimeLabel,
} from "../../ui/src/lib/status-view";
import { EMPTY_PEER_STATUS, EMPTY_RUNTIME_STATUS } from "../../ui/src/lib/runtime-status";

const summary = { running: true, healthy: true, throughput: 12.5, message_sequence: 40 };

function instance(id: string, kind: "cli" | "web-ui", extra: object = {}) {
  return {
    schema_version: 1,
    instance_id: id,
    pid: 7,
    kind,
    application_version: "1",
    started_at_ms: 1,
    last_seen_at_ms: 1,
    workspace_id: id,
    workspace_label: "work",
    ...extra,
  };
}

const entity = (id: string, endpoint: string) => ({ id, label: id, endpoint, summary });

describe("statusRows", () => {
  test("lists this instance first and marks it", () => {
    const rows = statusRows(
      {
        current_instance_id: "local",
        instances: [
          instance("peer", "cli", {
            consumers: [entity("orders", "kafka")],
            routes: [
              {
                id: "orders",
                label: "orders",
                input: entity("orders:input", "kafka"),
                output: entity("orders:output", "postgres"),
                summary: { ...summary, average_throughput: 10, started_at_ms: 5000 },
              },
            ],
          }),
          instance("local", "web-ui", { consumers: [entity("inbox", "memory")] }),
        ],
      },
      EMPTY_RUNTIME_STATUS,
    );

    expect(rows.map((row) => [row.name, row.current])).toEqual([
      ["inbox", true],
      ["orders", false],
    ]);
    expect(rows[1]).toMatchObject({
      instance: "CLI · work [7]",
      flow: "kafka → postgres",
      state: "running",
      rate: 12.5,
      average: 10,
      total: 40,
      startedAtMs: 5000,
    });
  });

  test("an instance running nothing gets an idle row", () => {
    const rows = statusRows(
      { current_instance_id: "local", instances: [instance("local", "web-ui")] },
      EMPTY_RUNTIME_STATUS,
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].state).toBe("idle");
  });

  test("falls back to local runtime status without registry access", () => {
    const rows = statusRows(EMPTY_PEER_STATUS, {
      ...EMPTY_RUNTIME_STATUS,
      consumers: {
        inbox: {
          running: true,
          status: { healthy: false, target: "nats", pending: 3, capacity: 10 },
          throughput: 2,
          message_sequence: 9,
          capture_enabled: false,
          capture_keep_last: 0,
        },
      },
    });
    expect(rows).toEqual([
      expect.objectContaining({
        instance: "This instance",
        current: true,
        name: "inbox",
        flow: "nats",
        state: "unhealthy",
        pending: "3/10",
        total: 9,
      }),
    ]);
  });
});

test("state follows outcome, then health", () => {
  expect(stateLabel({ outcome: "failed", running: true })).toBe("failed");
  expect(stateLabel({ outcome: "completed", error: "error" })).toBe("completed (error)");
  expect(stateLabel({ running: true, healthy: true })).toBe("running");
  expect(stateLabel({})).toBe("stopped");
});

test("samples are capped and dropped with their row", () => {
  const rows = statusRows(
    {
      current_instance_id: "local",
      instances: [instance("local", "web-ui", { consumers: [entity("inbox", "memory")] })],
    },
    EMPTY_RUNTIME_STATUS,
  );
  let history = new Map<string, number[]>([["gone", [1, 2]]]);
  for (let i = 0; i < 5; i += 1) history = recordSamples(history, rows, 3);
  expect([...history.entries()]).toEqual([[rows[0].id, [12.5, 12.5, 12.5]]]);
});

test("sparkline scales to the largest sample", () => {
  expect(sparklinePoints([5], 10, 10)).toBe("");
  expect(sparklinePoints([0, 5, 10], 10, 10)).toBe("0.0,10.0 5.0,5.0 10.0,0.0");
  expect(sparklinePoints([0, 0], 10, 10)).toBe("0.0,10.0 10.0,10.0");
});

test("uptime uses the same units as the CLI", () => {
  expect(uptimeLabel(null, 0)).toBe("-");
  expect(uptimeLabel(10_000, 75_000)).toBe("1m05s");
  expect(uptimeLabel(0, 3_720_000)).toBe("1h02m");
});
