import type { Bootstrap } from "./types";

export type PerfCause = "full-refresh" | "message-upsert" | "activity-flush";

export type PerfPhaseMetrics = {
  backendMs?: number;
  transportMs?: number;
  parseMs?: number;
  applyMs?: number;
  parseApplyMs?: number;
  commitMs?: number;
  renderActualMs?: number;
  totalMs?: number;
};

export type PerfCounts = {
  messages?: number;
  agentRuns?: number;
  agentActivities?: number;
  artifacts?: number;
  tasks?: number;
  channels?: number;
  updatedMessages?: number;
  updatedActivities?: number;
  updatedRuns?: number;
  longTasks?: number;
};

export type PerfSample = {
  id: number;
  cause: PerfCause;
  runtime: "tauri" | "web";
  startedAt: number;
  recordedAt: number;
  phases: PerfPhaseMetrics;
  counts: PerfCounts;
  backend?: Bootstrap["__perf"];
  payloadBytes?: number;
  transportPayloadBytes?: number;
  labels?: Record<string, string | number | boolean | null>;
  longTasks: Array<{ startTime: number; durationMs: number }>;
};

type PendingCommit = Omit<PerfSample, "recordedAt" | "longTasks"> & {
  commitFrom: number;
};

export type PerfSnapshot = {
  samples: PerfSample[];
  summary: Record<PerfCause, {
    count: number;
    p50: PerfPhaseMetrics;
    p95: PerfPhaseMetrics;
  }>;
};

const MAX_SAMPLES = 50;
const PENDING_COMMIT_MAX_AGE_MS = 2000;
const PERF_STORAGE_KEY = "lantor:perf";
let nextSampleId = 1;
let samples: PerfSample[] = [];
let pendingCommits: PendingCommit[] = [];
let longTasks: Array<{ startTime: number; durationMs: number }> = [];

function runtime(): "tauri" | "web" {
  return typeof window !== "undefined" && Boolean(window.__TAURI_INTERNALS__) ? "tauri" : "web";
}

export function shouldEnablePerfTelemetry() {
  if (typeof window === "undefined") return false;
  const params = new URLSearchParams(window.location.search);
  return params.has("lantorPerf")
    || window.localStorage.getItem(PERF_STORAGE_KEY) === "1";
}

function percentile(values: number[], percentileValue: number) {
  if (values.length === 0) return undefined;
  const sorted = [...values].sort((left, right) => left - right);
  const index = Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * percentileValue));
  return sorted[index];
}

function summarizePhases(items: PerfSample[], percentileValue: number): PerfPhaseMetrics {
  const keys: Array<keyof PerfPhaseMetrics> = [
    "backendMs",
    "transportMs",
    "parseMs",
    "applyMs",
    "parseApplyMs",
    "commitMs",
    "renderActualMs",
    "totalMs",
  ];
  const summary: PerfPhaseMetrics = {};
  for (const key of keys) {
    const values = items
      .map((sample) => sample.phases[key])
      .filter((value): value is number => typeof value === "number" && Number.isFinite(value));
    const value = percentile(values, percentileValue);
    if (value !== undefined) summary[key] = value;
  }
  return summary;
}

function snapshot(): PerfSnapshot {
  const summary = {} as PerfSnapshot["summary"];
  for (const cause of ["full-refresh", "message-upsert", "activity-flush"] as const) {
    const items = samples.filter((sample) => sample.cause === cause);
    summary[cause] = {
      count: items.length,
      p50: summarizePhases(items, 0.5),
      p95: summarizePhases(items, 0.95),
    };
  }
  return { samples, summary };
}

function publishSample(sample: PerfSample) {
  samples = [sample, ...samples].slice(0, MAX_SAMPLES);
}

export function createPerfDraft(cause: PerfCause): PendingCommit {
  return {
    id: nextSampleId++,
    cause,
    runtime: runtime(),
    startedAt: performance.now(),
    commitFrom: performance.now(),
    phases: {},
    counts: {},
  };
}

export function completePerfWithoutCommit(draft: PendingCommit) {
  const now = performance.now();
  publishSample({
    ...draft,
    recordedAt: now,
    phases: {
      ...draft.phases,
      totalMs: draft.phases.totalMs ?? now - draft.startedAt,
    },
    longTasks: recentLongTasks(draft.startedAt, now),
  });
}

export function waitForPerfCommit(draft: PendingCommit, commitFrom = performance.now()) {
  discardExpiredPendingCommits(commitFrom);
  pendingCommits = [...pendingCommits, { ...draft, commitFrom }].slice(-MAX_SAMPLES);
}

export function recordPerfCommit(actualDuration: number, commitTime: number) {
  discardExpiredPendingCommits(commitTime);
  if (pendingCommits.length === 0) return;
  const drafts = pendingCommits;
  pendingCommits = [];
  for (const draft of drafts) {
    publishSample({
      ...draft,
      recordedAt: commitTime,
      phases: {
        ...draft.phases,
        commitMs: Math.max(0, commitTime - draft.commitFrom),
        renderActualMs: actualDuration,
        totalMs: commitTime - draft.startedAt,
      },
      counts: {
        ...draft.counts,
        longTasks: recentLongTasks(draft.startedAt, commitTime).length,
      },
      longTasks: recentLongTasks(draft.startedAt, commitTime),
    });
  }
}

function discardExpiredPendingCommits(now = performance.now()) {
  pendingCommits = pendingCommits.filter((draft) => now - draft.commitFrom <= PENDING_COMMIT_MAX_AGE_MS);
}

function recentLongTasks(startedAt: number, endedAt: number) {
  return longTasks.filter((task) => task.startTime >= startedAt - 5 && task.startTime <= endedAt + 5);
}

export function initPerfTelemetry() {
  if (typeof window === "undefined") return;
  if (!window.__LANTOR_PERF__) {
    window.__LANTOR_PERF__ = {
      samples: () => samples,
      latest: () => samples[0] ?? null,
      summary: () => snapshot().summary,
      reset: () => {
        samples = [];
        pendingCommits = [];
        longTasks = [];
      },
    };
  }
  if (!shouldEnablePerfTelemetry()) return;
  if (!("PerformanceObserver" in window)) return;
  try {
    const observer = new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        longTasks = [
          { startTime: entry.startTime, durationMs: entry.duration },
          ...longTasks,
        ].slice(0, 100);
      }
    });
    observer.observe({ entryTypes: ["longtask"] });
  } catch {
    // Some runtimes expose PerformanceObserver but not longtask.
  }
}

declare global {
  interface Window {
    __TAURI_INTERNALS__?: unknown;
    __LANTOR_PERF__?: {
      samples: () => PerfSample[];
      latest: () => PerfSample | null;
      summary: () => PerfSnapshot["summary"];
      reset: () => void;
    };
  }
}
