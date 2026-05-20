import {
  AlertCircle,
  ChevronDown,
  CircleDashed,
  LoaderCircle,
  Pencil,
  Terminal,
  Wrench,
} from "lucide-react";
import type { LucideIcon } from "lucide-react";
import type { Agent, AgentActivity, AgentRun, AgentWorkItem, Message } from "../types";
import { messageRunId } from "../message-grouping";
import { formatClockTime } from "../ui-utils";
import { AgentAvatar } from "./AgentAvatar";

type ActivityProgressDockProps = {
  messages: Message[];
  activities: AgentActivity[];
  runs: AgentRun[];
  workItems: AgentWorkItem[];
  agents: Agent[];
  channelId: string | null;
  threadRootId: string | null;
};

export type ActiveAgentProgress = {
  key: string;
  agent: Pick<Agent, "handle" | "display_name" | "status"> &
    Partial<Pick<Agent, "id" | "runtime" | "model" | "role" | "avatar" | "description">>;
  workItem: AgentWorkItem | null;
  queuedItems: AgentWorkItem[];
  latestActivity: AgentActivity | null;
  history: AgentActivity[];
  latestAt: number;
};

type ProgressCandidate = {
  message: Message | null;
  workItem: AgentWorkItem | null;
  latestAt: number;
};

const HIDDEN_ACTIVITY_TITLES = new Set([
  "Request acknowledged",
  "Stream event accepted",
]);
const MAX_PROGRESS_HISTORY_ITEMS = 20;
const ACTIVE_RUN_STATUSES = new Set(["starting", "running", "stopping"]);
const ACTIVE_WORK_ITEM_STATUSES = new Set(["queued", "running", "cancelling"]);
const SETTLING_WORK_ITEM_STATUSES = new Set(["done", "failed", "cancelled", "silent"]);
const COMPLETION_SETTLE_WINDOW_MS = 15_000;

function activityTitle(activity: AgentActivity) {
  return (activity.summary || activity.title || phaseLabel(activity.phase || activity.kind)).trim();
}

function phaseLabel(phase: string) {
  switch (phase) {
    case "thinking":
      return "Thinking";
    case "command":
      return "Running command";
    case "file_edit":
      return "Editing file";
    case "tools":
      return "Using tools";
    case "runtime":
      return "Runtime";
    case "run_retry":
      return "Provider retrying";
    case "work":
      return "Request";
    case "error":
    case "event_error":
    case "run_error":
      return "Error";
    default:
      return "Working";
  }
}

function progressIcon(activity: AgentActivity | null): LucideIcon {
  if (activity?.status === "error") return AlertCircle;
  switch (activity?.phase || activity?.kind) {
    case "command":
      return Terminal;
    case "file_edit":
      return Pencil;
    case "tools":
      return Wrench;
    case "thinking":
      return CircleDashed;
    default:
      return LoaderCircle;
  }
}

function isProviderRetryActivity(activity: AgentActivity | null) {
  return activity?.kind === "run_retry" || activity?.phase === "run_retry";
}

function activityDetail(activity: AgentActivity) {
  const metadata = activity.metadata ?? {};
  const preferred = [
    metadata.command,
    metadata.file,
    metadata.tool,
    metadata.operation,
    metadata.reason,
  ].find((value) => typeof value === "string" && value.trim());
  if (typeof preferred === "string") return preferred.trim();

  const detail = activity.detail.trim();
  if (!detail || detail.startsWith("{") || detail.startsWith("[")) return "";
  return detail;
}

function compact(value: string, limit: number) {
  const normalized = value.replace(/\s+/g, " ").trim();
  if (normalized.length <= limit) return normalized;
  return `${normalized.slice(0, Math.max(0, limit - 1)).trim()}...`;
}

function isUsefulActivity(activity: AgentActivity) {
  const title = activityTitle(activity);
  if (!title || HIDDEN_ACTIVITY_TITLES.has(title)) return false;
  if (activity.kind === "event" && title === "Activity accepted") return false;
  return true;
}

function compactProgressActivities(activities: AgentActivity[]) {
  const seen = new Set<string>();
  let lastSignature = "";
  return activities.filter((activity) => {
    if (seen.has(activity.id)) return false;
    seen.add(activity.id);
    const signature = `${activity.phase || activity.kind || ""}|${activityTitle(activity)}|${activity.detail.trim()}`;
    if (signature === lastSignature) return false;
    lastSignature = signature;
    return true;
  });
}

function timestamp(value: string) {
  const parsed = new Date(value).getTime();
  return Number.isFinite(parsed) ? parsed : 0;
}

function recentlyUpdated(value: string) {
  const updatedAt = timestamp(value);
  return updatedAt > 0 && Date.now() - updatedAt <= COMPLETION_SETTLE_WINDOW_MS;
}

function senderHandle(message: Message) {
  return message.sender_name.replace(/^@/, "").trim();
}

function activityAgentHandle(activity: AgentActivity | null) {
  return activity?.agent_handle?.replace(/^@/, "").trim() || "";
}

function isTerminalProgressActivity(activity: AgentActivity | null) {
  if (!activity) return false;
  const title = activityTitle(activity).toLowerCase();
  if (activity.phase === "runtime") {
    return title === "completed" || title === "failed" || title === "stopped";
  }
  if (activity.phase === "work") {
    return title === "request completed"
      || title === "request failed"
      || title === "request cancelled"
      || title === "no visible reply needed";
  }
  return false;
}

function workItemMatchesSurface(
  workItem: AgentWorkItem,
  channelId: string | null,
  threadRootId: string | null,
) {
  if (!channelId || workItem.channel_id !== channelId) return false;
  return (workItem.thread_root_id ?? null) === (threadRootId ?? null);
}

function agentForProgress(
  handle: string,
  activity: AgentActivity | null,
  agents: Agent[],
): ActiveAgentProgress["agent"] {
  const activityHandle = activity?.agent_handle?.replace(/^@/, "").trim();
  const lookup = activityHandle || handle;
  const agent = agents.find((candidate) => candidate.handle === lookup);
  if (agent) return agent;
  const displayName = lookup ? `@${lookup}` : "Agent";
  return {
    handle: lookup || "agent",
    display_name: displayName,
    status: "running",
  };
}

export function activeProgressByAgent(
  messages: Message[],
  activities: AgentActivity[],
  runs: AgentRun[],
  workItems: AgentWorkItem[],
  agents: Agent[],
  channelId: string | null,
  threadRootId: string | null,
) {
  const streamingMessages = messages
    .map((message) => ({ message, runId: messageRunId(message) }))
    .filter(({ message, runId }) =>
      runId
      && message.sender_role !== "owner"
      && message.sender_role !== "system"
      && message.delivery_state === "streaming");

  const activitiesByRun = new Map<string, AgentActivity[]>();
  activities
    .filter(isUsefulActivity)
    .sort((left, right) => timestamp(right.created_at) - timestamp(left.created_at))
    .forEach((activity) => {
      if (!activity.run_id) return;
      const current = activitiesByRun.get(activity.run_id) ?? [];
      current.push(activity);
      activitiesByRun.set(activity.run_id, current);
    });

  const runsById = new Map(runs.map((run) => [run.id, run]));
  const surfaceWorkItems = workItems.filter((workItem) => workItemMatchesSurface(workItem, channelId, threadRootId));
  const candidatesByRun = new Map<string, ProgressCandidate>();
  const addCandidate = (runId: string, candidate: ProgressCandidate) => {
    const current = candidatesByRun.get(runId);
    if (!current || candidate.latestAt > current.latestAt) {
      candidatesByRun.set(runId, candidate);
    }
  };

  streamingMessages.forEach(({ message, runId }) => {
    if (!runId) return;
    addCandidate(runId, {
      message,
      workItem: null,
      latestAt: timestamp(message.updated_at),
    });
  });

  surfaceWorkItems
    .filter((workItem) => workItem.run_id)
    .forEach((workItem) => {
      const runId = workItem.run_id;
      if (!runId) return;
      const run = runsById.get(runId);
      const latestActivity = activitiesByRun.get(runId)?.[0] ?? null;
      const activeRun = Boolean(run && ACTIVE_RUN_STATUSES.has(run.status));
      const activeWorkItem = ACTIVE_WORK_ITEM_STATUSES.has(workItem.status);
      const settlingWorkItem = SETTLING_WORK_ITEM_STATUSES.has(workItem.status)
        && !isTerminalProgressActivity(latestActivity)
        && recentlyUpdated(workItem.updated_at);

      if (!activeRun && !activeWorkItem && !settlingWorkItem) return;
      addCandidate(runId, {
        message: null,
        workItem,
        latestAt: Math.max(
          timestamp(workItem.updated_at),
          timestamp(run?.started_at ?? ""),
          timestamp(run?.stopped_at ?? ""),
        ),
      });
    });

  const progressByAgent = new Map<string, ActiveAgentProgress>();
  candidatesByRun.forEach((candidate, runId) => {
    const runActivities = compactProgressActivities(activitiesByRun.get(runId) ?? []);
    const latestActivity = runActivities[0] ?? null;
    if (isTerminalProgressActivity(latestActivity)) return;

    const handle = activityAgentHandle(latestActivity)
      || candidate.workItem?.agent_handle
      || (candidate.message ? senderHandle(candidate.message) : "");
    const key = handle || candidate.message?.sender_name || runId;
    const latestAt = Math.max(timestamp(latestActivity?.created_at ?? ""), candidate.latestAt);
    const existing = progressByAgent.get(key);
    const history = [...runActivities, ...(existing?.history ?? [])]
      .sort((left, right) => timestamp(right.created_at) - timestamp(left.created_at));
    const compactHistory = compactProgressActivities(history).slice(0, MAX_PROGRESS_HISTORY_ITEMS);
    progressByAgent.set(key, {
      key,
      agent: existing?.agent ?? agentForProgress(handle, latestActivity, agents),
      workItem: candidate.workItem ?? existing?.workItem ?? null,
      queuedItems: existing?.queuedItems ?? [],
      latestActivity: existing && existing.latestAt > latestAt ? existing.latestActivity : latestActivity,
      history: compactHistory,
      latestAt: Math.max(existing?.latestAt ?? 0, latestAt),
    });
  });

  surfaceWorkItems
    .filter((workItem) => workItem.status === "queued")
    .forEach((workItem) => {
      const key = workItem.agent_handle || workItem.agent_id;
      const existing = progressByAgent.get(key);
      const queuedItems = [...(existing?.queuedItems ?? [])];
      if (!queuedItems.some((item) => item.id === workItem.id)) queuedItems.push(workItem);
      const agent = existing?.agent
        ?? agents.find((candidate) => candidate.id === workItem.agent_id)
        ?? {
          id: workItem.agent_id,
          handle: workItem.agent_handle || "agent",
          display_name: workItem.agent_handle ? `@${workItem.agent_handle}` : "Agent",
          status: "idle",
        };

      progressByAgent.set(key, {
        key,
        agent,
        workItem: existing?.workItem ?? null,
        queuedItems,
        latestActivity: existing?.latestActivity ?? null,
        history: existing?.history ?? [],
        latestAt: Math.max(existing?.latestAt ?? 0, timestamp(workItem.updated_at)),
      });
    });

  return Array.from(progressByAgent.values())
    .sort((left, right) => right.latestAt - left.latestAt);
}

export function ActivityProgressDock({
  messages,
  activities,
  runs,
  workItems,
  agents,
  channelId,
  threadRootId,
}: ActivityProgressDockProps) {
  const progress = activeProgressByAgent(messages, activities, runs, workItems, agents, channelId, threadRootId);
  if (progress.length === 0) return null;

  const latest = progress[0];
  const latestActivity = latest.latestActivity;
  const Icon = progressIcon(latestActivity);
  const providerRetrying = isProviderRetryActivity(latestActivity);
  const queuedCount = progress.reduce((count, item) => count + item.queuedItems.length, 0);
  const title = progress.length === 1
    ? providerRetrying
      ? `${latest.agent.display_name} is waiting on provider`
      : latestActivity
        ? `${latest.agent.display_name} is working`
        : `${latest.agent.display_name} has queued work`
    : `${progress.length} agents are working`;
  const latestTitle = latestActivity ? activityTitle(latestActivity) : "Queued";
  const latestDetail = latestActivity ? activityDetail(latestActivity) : "";
  const history = progress
    .flatMap((item) => item.history.map((activity) => ({ activity, agent: item.agent })))
    .sort((left, right) => timestamp(right.activity.created_at) - timestamp(left.activity.created_at))
    .slice(0, MAX_PROGRESS_HISTORY_ITEMS);

  return (
    <details className="activity-progress-dock">
      <summary data-state={providerRetrying ? "provider-retrying" : latestActivity ? "working" : "queued"}>
        <span className="activity-progress-avatar-stack" aria-hidden="true">
          {progress.slice(0, 3).map((item) => (
            <AgentAvatar key={item.key} agent={item.agent} size="sm" showStatus={false} />
          ))}
        </span>
        <span className="activity-progress-copy">
          <strong>{title}</strong>
          <small>
            <Icon size={13} />
            <span>{latestTitle}</span>
            {latestDetail && <em>{compact(latestDetail, 80)}</em>}
            {queuedCount > 0 && <em>{queuedCount} queued on this surface</em>}
          </small>
        </span>
        <ChevronDown className="activity-progress-chevron" size={14} />
      </summary>
      {history.length > 0 && (
        <ol className="activity-progress-history">
          {history.map(({ activity, agent }) => {
            const detail = activityDetail(activity);
            return (
              <li key={activity.id} data-status={activity.status}>
                <time>{formatClockTime(activity.created_at)}</time>
                <span className="activity-progress-source-chip" data-kind={activity.kind || activity.phase}>
                  {phaseLabel(activity.phase || activity.kind)}
                </span>
                <span>
                  <strong>{agent.display_name}</strong>
                  <b>{activityTitle(activity)}</b>
                  {detail && <small>{compact(detail, 132)}</small>}
                </span>
              </li>
            );
          })}
        </ol>
      )}
    </details>
  );
}
