import React from "react";
import { renderToString } from "react-dom/server";
import { performance } from "node:perf_hooks";

const LONG_TASK_MS = 50;
const DEFAULTS = {
  runs: 5,
  warmup: 5,
  sampleRepeats: 3,
  seed: 17,
  profile: "all",
};

const DATASETS = {
  typical: {
    messages: 140,
    bodyLines: [2, 5],
    attachmentsEvery: 14,
    activeRuns: 4,
    agents: 40,
    channels: 40,
  },
  stress: {
    messages: 360,
    bodyLines: [4, 9],
    attachmentsEvery: 6,
    activeRuns: 16,
    agents: 160,
    channels: 160,
  },
};

const PROFILES = [
  {
    name: "main-plain-stress",
    description: "main composer, plain typing, stress channel",
    surface: "main",
    typingMode: "plain",
    dataset: "stress",
    keystrokes: 40,
  },
  {
    name: "thread-plain-stress",
    description: "thread composer, plain typing, stress thread",
    surface: "thread",
    typingMode: "plain",
    dataset: "stress",
    keystrokes: 40,
  },
  {
    name: "main-agent-mention-stress",
    description: "main composer, @mention search, stress roster",
    surface: "main",
    typingMode: "agent-mention",
    dataset: "stress",
    keystrokes: 24,
  },
  {
    name: "main-channel-autocomplete-stress",
    description: "main composer, #channel autocomplete, stress channel list",
    surface: "main",
    typingMode: "channel-mention",
    dataset: "stress",
    keystrokes: 24,
  },
];

function optionString(name, fallback) {
  const index = process.argv.indexOf(`--${name}`);
  if (index === -1) return fallback;
  return process.argv[index + 1] || fallback;
}

function optionNumber(name, fallback) {
  const index = process.argv.indexOf(`--${name}`);
  if (index === -1) return fallback;
  const value = Number(process.argv[index + 1]);
  return Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback;
}

function hasFlag(name) {
  return process.argv.includes(`--${name}`);
}

const config = {
  runs: optionNumber("runs", DEFAULTS.runs),
  warmup: optionNumber("warmup", DEFAULTS.warmup),
  sampleRepeats: optionNumber("sample-repeats", DEFAULTS.sampleRepeats),
  seed: optionNumber("seed", DEFAULTS.seed),
  profile: optionString("profile", DEFAULTS.profile),
  json: hasFlag("json"),
};

function selectedProfiles() {
  if (config.profile === "all") return PROFILES;
  const profile = PROFILES.find((candidate) => candidate.name === config.profile);
  if (!profile) {
    throw new Error(`Unknown profile "${config.profile}". Use one of: all, ${PROFILES.map((item) => item.name).join(", ")}`);
  }
  return [profile];
}

function seededRandom(seed) {
  let state = seed >>> 0;
  return () => {
    state = (state * 1664525 + 1013904223) >>> 0;
    return state / 0x100000000;
  };
}

function makeAgents(count) {
  return Array.from({ length: count }, (_, index) => ({
    handle: `agent-${index}`,
    displayName: `Agent ${index}`,
    runtime: index % 2 === 0 ? "codex" : "claude",
    role: index % 3 === 0 ? "reviewer" : "worker",
  }));
}

function makeChannels(count) {
  return Array.from({ length: count }, (_, index) => ({
    name: `channel-${index}`,
    description: index % 4 === 0 ? "runtime retry benchmark discussion" : "general project work",
  }));
}

function makeMessages(dataset, seed) {
  const random = seededRandom(seed);
  const names = ["Martin", "admin", "Dylan", "Bugen", "Codex"];
  const phrases = [
    "runtime activity should remain visible while the provider is retrying",
    "typing into the composer should not rerender the full timeline",
    "markdown previews and message metadata dominate repeated render cost",
    "queued work needs an observable state so users know what is happening",
    "attachments, artifacts, and reply summaries stay stable during input",
    "thread summaries should remain scannable while long messages stream",
  ];
  const [minLines, maxLines] = dataset.bodyLines;

  return Array.from({ length: dataset.messages }, (_, index) => {
    const lineCount = minLines + Math.floor(random() * (maxLines - minLines + 1));
    const body = Array.from({ length: lineCount }, (_, line) => {
      const phrase = phrases[Math.floor(random() * phrases.length)];
      return `${phrase} ${index}:${line} **${names[(index + line) % names.length]}** ` +
        "`status` #lantor @agent [link](https://example.invalid)";
    }).join("\n");
    return {
      id: `message-${index}`,
      sender: names[index % names.length],
      role: index % 5 === 0 ? "owner" : "agent",
      body,
      replyCount: Math.floor(random() * 12),
      saved: random() > 0.82,
      attachments: dataset.attachmentsEvery > 0 && index % dataset.attachmentsEvery === 0
        ? 1 + Math.floor(random() * 3)
        : 0,
    };
  });
}

function makeActiveRuns(count) {
  return Array.from({ length: count }, (_, index) => ({
    id: `run-${index}`,
    agent: ["admin", "Dylan", "Bugen", "Theo"][index % 4],
    title: index % 3 === 0 ? "Claude provider retrying" : "Thinking",
    detail: index % 3 === 0
      ? "Lantor will retry automatically; no action needed · attempt 2/5 · status 529 (overloaded)"
      : "Inspecting channel context and current thread state",
  }));
}

function nextDraft(typingMode, index) {
  if (typingMode === "agent-mention") return `@agent-${index % 20}`;
  if (typingMode === "channel-mention") return `#channel-${index % 20}`;
  return "typing benchmark ".slice(0, 1 + (index % "typing benchmark ".length));
}

function filterAgents(agents, query) {
  const lowered = query.toLowerCase();
  return agents
    .filter((agent) => `${agent.handle} ${agent.displayName} ${agent.runtime} ${agent.role}`.toLowerCase().includes(lowered))
    .slice(0, 8);
}

function filterChannels(channels, query) {
  const lowered = query.toLowerCase();
  return channels
    .filter((channel) => `${channel.name} ${channel.description}`.toLowerCase().includes(lowered))
    .sort((left, right) => left.name.localeCompare(right.name))
    .slice(0, 8);
}

function tokenizeMarkdownLike(text) {
  return text
    .split(/(\s+|`[^`]*`|\*\*[^*]+\*\*|\[[^\]]+\]\([^)]+\)|#[A-Za-z0-9_-]+|@[A-Za-z0-9_-]+)/g)
    .filter(Boolean)
    .map((token, index) => {
      if (token.startsWith("**")) return React.createElement("strong", { key: index }, token.slice(2, -2));
      if (token.startsWith("`")) return React.createElement("code", { key: index }, token.slice(1, -1));
      if (token.startsWith("[")) return React.createElement("a", { key: index, href: "#" }, token.slice(1, token.indexOf("]")));
      if (token.startsWith("#") || token.startsWith("@")) return React.createElement("span", { key: index, className: "mention" }, token);
      return token;
    });
}

function MessageCard({ message }) {
  return React.createElement(
    "article",
    { className: `message-card ${message.saved ? "saved" : ""}` },
    React.createElement("header", null,
      React.createElement("strong", null, message.sender),
      React.createElement("span", null, message.role),
      React.createElement("time", null, "14:57"),
    ),
    React.createElement("section", null,
      message.body.split("\n").map((line, index) => React.createElement("p", { key: index }, tokenizeMarkdownLike(line))),
    ),
    message.attachments > 0
      ? React.createElement("div", { className: "attachments" },
        Array.from({ length: message.attachments }, (_, index) => (
          React.createElement("figure", { key: index },
            React.createElement("div", { className: "thumbnail" }),
            React.createElement("figcaption", null, `attachment-${index}.png`),
          )
        )),
      )
      : null,
    React.createElement("footer", null,
      React.createElement("button", null, `${message.replyCount} replies`),
      React.createElement("button", null, message.saved ? "Saved" : "Save"),
    ),
  );
}

function ActivityDock({ activeRuns }) {
  if (activeRuns.length === 0) return null;
  return React.createElement(
    "aside",
    { className: "activity-progress-dock" },
    activeRuns.map((run) => React.createElement("section", { key: run.id },
      React.createElement("strong", null, run.agent),
      React.createElement("span", null, run.title),
      React.createElement("p", null, run.detail),
    )),
  );
}

function Composer({ draft, typingMode, agents, channels }) {
  const query = draft.replace(/^[@#]/, "");
  const candidates = typingMode === "agent-mention"
    ? filterAgents(agents, query)
    : typingMode === "channel-mention"
      ? filterChannels(channels, query)
      : [];

  return React.createElement(
    "footer",
    { className: "composer" },
    candidates.length > 0
      ? React.createElement("div", { className: "mention-picker" },
        candidates.map((candidate) => React.createElement("button", { key: candidate.handle ?? candidate.name }, candidate.handle ?? candidate.name)),
      )
      : null,
    React.createElement("textarea", { value: draft, readOnly: true }),
    React.createElement("button", { disabled: draft.trim().length === 0 }, "Send"),
  );
}

function SurfaceTree({ profile, messages, activeRuns, agents, channels, draft }) {
  const composer = React.createElement(Composer, { draft, typingMode: profile.typingMode, agents, channels });
  if (profile.surface === "thread") {
    return React.createElement(
      "aside",
      { className: "thread-panel" },
      React.createElement("section", { className: "thread-list" },
        React.createElement(ActivityDock, { activeRuns }),
        messages.slice(0, Math.max(12, Math.floor(messages.length / 2))).map((message) => React.createElement(MessageCard, { key: message.id, message })),
      ),
      composer,
    );
  }

  return React.createElement(
    "main",
    { className: "conversation" },
    React.createElement("section", { className: "message-list" },
      React.createElement(ActivityDock, { activeRuns }),
      messages.map((message) => React.createElement(MessageCard, { key: message.id, message })),
    ),
    composer,
  );
}

function percentile(values, fraction) {
  const sorted = [...values].sort((left, right) => left - right);
  const index = Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * fraction));
  return sorted[index] ?? 0;
}

function summarize(samples) {
  const longTasks = samples.filter((value) => value >= LONG_TASK_MS);
  return {
    mean: samples.reduce((sum, value) => sum + value, 0) / samples.length,
    p50: percentile(samples, 0.5),
    p95: percentile(samples, 0.95),
    p99: percentile(samples, 0.99),
    max: Math.max(...samples),
    longTaskCount: longTasks.length,
    longTaskMs: longTasks.reduce((sum, value) => sum + value, 0),
  };
}

function relativeStdDev(values) {
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  const variance = values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / values.length;
  return Math.sqrt(variance) / Math.max(mean, 1);
}

function measureScenario({ profile, messages, activeRuns, agents, channels, strategy }) {
  const samples = [];
  let largeTreeCommitCount = 0;

  if (strategy === "buffered") {
    renderToString(React.createElement(SurfaceTree, { profile, messages, activeRuns, agents, channels, draft: "" }));
  }

  for (let index = 0; index < profile.keystrokes; index += 1) {
    const draft = nextDraft(profile.typingMode, index);
    const started = performance.now();
    for (let repeat = 0; repeat < config.sampleRepeats; repeat += 1) {
      if (strategy === "root-coupled") {
        renderToString(React.createElement(SurfaceTree, { profile, messages, activeRuns, agents, channels, draft }));
      } else {
        renderToString(React.createElement(Composer, { draft, typingMode: profile.typingMode, agents, channels }));
      }
    }
    if (strategy === "root-coupled") largeTreeCommitCount += 1;
    samples.push((performance.now() - started) / config.sampleRepeats);
  }

  return {
    ...summarize(samples),
    largeTreeCommitCount,
  };
}

function aggregateRuns(results) {
  const p95s = results.map((result) => result.p95);
  return {
    p50: percentile(results.map((result) => result.p50), 0.5),
    p95: percentile(p95s, 0.5),
    p99: percentile(results.map((result) => result.p99), 0.5),
    max: Math.max(...results.map((result) => result.max)),
    longTaskCount: Math.round(percentile(results.map((result) => result.longTaskCount), 0.5)),
    longTaskMs: percentile(results.map((result) => result.longTaskMs), 0.5),
    largeTreeCommitCount: Math.round(percentile(results.map((result) => result.largeTreeCommitCount), 0.5)),
    relativeStdDev: relativeStdDev(p95s),
  };
}

function measureProfile(profile) {
  const dataset = DATASETS[profile.dataset];
  const messages = makeMessages(dataset, config.seed);
  const activeRuns = makeActiveRuns(dataset.activeRuns);
  const agents = makeAgents(dataset.agents);
  const channels = makeChannels(dataset.channels);
  const strategies = ["root-coupled", "buffered"];
  const measurements = Object.fromEntries(strategies.map((name) => [name, []]));

  for (let run = 0; run < config.warmup + config.runs; run += 1) {
    for (const strategy of strategies) {
      const result = measureScenario({ profile, messages, activeRuns, agents, channels, strategy });
      if (run >= config.warmup) measurements[strategy].push(result);
    }
  }

  const root = aggregateRuns(measurements["root-coupled"]);
  const buffered = aggregateRuns(measurements.buffered);
  return {
    profile,
    dataset,
    perRun: measurements,
    summary: {
      "root-coupled": root,
      buffered,
      speedup: root.p95 / buffered.p95,
      savedP95MsPerKey: root.p95 - buffered.p95,
    },
  };
}

function formatMs(value) {
  return value.toFixed(3);
}

function runBenchmark() {
  const results = selectedProfiles().map(measureProfile);
  const output = {
    config: {
      ...config,
      longTaskMs: LONG_TASK_MS,
    },
    results,
  };

  if (config.json) {
    console.log(JSON.stringify(output, null, 2));
    return;
  }

  console.log("Composer input latency benchmark");
  console.log(`runs=${config.runs} warmup=${config.warmup} sampleRepeats=${config.sampleRepeats} seed=${config.seed} profile=${config.profile}`);
  console.log(`Metric is per-keystroke render cost; long task threshold is ${LONG_TASK_MS}ms.`);
  console.log("");
  console.table(Object.fromEntries(results.map((result) => {
    const root = result.summary["root-coupled"];
    const buffered = result.summary.buffered;
    return [result.profile.name, {
      surface: result.profile.surface,
      mode: result.profile.typingMode,
      dataset: result.profile.dataset,
      "root p95 ms/key": formatMs(root.p95),
      "buffered p95 ms/key": formatMs(buffered.p95),
      "root p99": formatMs(root.p99),
      "buffered p99": formatMs(buffered.p99),
      "speedup": `${result.summary.speedup.toFixed(1)}x`,
      "saved p95": formatMs(result.summary.savedP95MsPerKey),
      "root long tasks": root.longTaskCount,
      "buffered long tasks": buffered.longTaskCount,
      "root tree commits": root.largeTreeCommitCount,
      "buffered tree commits": buffered.largeTreeCommitCount,
      "root rstd": `${(root.relativeStdDev * 100).toFixed(1)}%`,
      "buffered rstd": `${(buffered.relativeStdDev * 100).toFixed(1)}%`,
    }];
  })));
}

runBenchmark();
