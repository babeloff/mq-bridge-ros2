<script lang="ts">
  import { onMount } from "svelte";
  import { activeMainTab, runtimeStatusStore } from "../lib/stores";
  import { getPeerStatus } from "../lib/state/peer-status.svelte";
  import {
    rateLabel,
    recordSamples,
    sparklinePoints,
    statusRows,
    uptimeLabel,
  } from "../lib/status-view";

  const SPARK_WIDTH = 96;
  const SPARK_HEIGHT = 18;
  const SAMPLE_INTERVAL_MS = 1000;

  const rows = $derived(statusRows(getPeerStatus(), $runtimeStatusStore));

  // Browser memory only: the samples are gone on reload and never leave the page.
  let history = $state.raw(new Map<string, number[]>());
  let now = $state(Date.now());

  onMount(() => {
    const timer = setInterval(() => {
      history = recordSamples(history, rows);
      now = Date.now();
    }, SAMPLE_INTERVAL_MS);
    return () => clearInterval(timer);
  });
</script>

<div class:active={$activeMainTab === "status"} class="tab-content-panel" id="tab-status">
  {#if rows.length === 0}
    <div class="empty-state">No running routes or consumers.</div>
  {:else}
    <div class="status-scroll">
      <table class="status-table">
        <thead>
          <tr>
            <th>Instance</th>
            <th>Name</th>
            <th>Flow</th>
            <th>State</th>
            <th class="num">Rate</th>
            <th>Last 60 s</th>
            <th class="num">Avg</th>
            <th class="num">Total</th>
            <th class="num">Pending</th>
            <th class="num">Uptime</th>
          </tr>
        </thead>
        <tbody>
          {#each rows as row (row.id)}
            <tr class:current={row.current}>
              <td>{row.instance}{row.current ? " (this)" : ""}</td>
              <td>{row.name}</td>
              <td>{row.flow}</td>
              <td><span class="state state--{row.state === 'completed (error)' ? 'failed' : row.state}">{row.state}</span></td>
              <td class="num">{rateLabel(row.rate)}</td>
              <td>
                <svg
                  class="sparkline"
                  width={SPARK_WIDTH}
                  height={SPARK_HEIGHT}
                  viewBox="0 0 {SPARK_WIDTH} {SPARK_HEIGHT}"
                  aria-hidden="true"
                >
                  <polyline
                    points={sparklinePoints(history.get(row.id) ?? [], SPARK_WIDTH, SPARK_HEIGHT)}
                  />
                </svg>
              </td>
              <td class="num">{rateLabel(row.average)}</td>
              <td class="num">{row.total}</td>
              <td class="num">{row.pending}</td>
              <td class="num">{uptimeLabel(row.startedAtMs, now)}</td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>
  {/if}
</div>

<style>
  #tab-status {
    padding: 12px;
  }

  .status-scroll {
    flex: 1;
    min-height: 0;
    overflow: auto;
  }

  .status-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 11px;
  }

  .status-table th,
  .status-table td {
    padding: 5px 10px;
    border-bottom: 1px solid var(--border);
    text-align: left;
    white-space: nowrap;
  }

  .status-table th {
    position: sticky;
    top: 0;
    background: var(--bg-panel);
    color: var(--text-muted);
    font-weight: 500;
  }

  .status-table .num {
    text-align: right;
    font-variant-numeric: tabular-nums;
  }

  .status-table tr.current td:first-child {
    color: var(--accent-blue);
  }

  .state {
    color: var(--text-muted);
  }

  .state--running,
  .state--completed {
    color: var(--accent-http);
  }

  .state--failed,
  .state--unhealthy {
    color: var(--accent-kafka);
  }

  .sparkline {
    display: block;
  }

  .sparkline polyline {
    fill: none;
    stroke: var(--accent-blue);
    stroke-width: 1.2;
  }
</style>
