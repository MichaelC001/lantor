import {
  Bot,
  ExternalLink,
  GitPullRequest,
  Github,
  LoaderCircle,
  RefreshCw,
  Settings2,
} from "lucide-react";
import { useCallback, useEffect, useRef, useState } from "react";

import { apiInvoke, openExternalUrl } from "../apiClient";
import type {
  Agent,
  Channel,
  GithubChannelOverview,
  GithubPullRequest,
  GithubReviewTaskResult,
} from "../types";
import { Modal } from "./Modal";
import { TaskAssigneePicker } from "./TaskAssigneePicker";

type GithubPanelProps = {
  channel: Channel;
  agents: Agent[];
  onCreateReviewTask: (pullNumber: number, agentId: string) => Promise<GithubReviewTaskResult>;
  onOpenThread: (threadRootId: string) => void;
};

const GITHUB_AUTO_REFRESH_INTERVAL_MS = 60_000;
const githubOverviewCache = new Map<string, GithubChannelOverview>();
const githubRefreshRequests = new Map<string, Promise<GithubChannelOverview>>();

function cacheGithubOverview(channelId: string, overview: GithubChannelOverview) {
  githubOverviewCache.set(channelId, overview);
  return overview;
}

function githubQueueNeedsRefresh(overview: GithubChannelOverview) {
  const refreshedAt = overview.binding?.review_queue_synced_at;
  if (!overview.binding || !refreshedAt) return Boolean(overview.binding);
  const refreshedAtMs = new Date(refreshedAt).getTime();
  return (
    !Number.isFinite(refreshedAtMs) ||
    Date.now() - refreshedAtMs >= GITHUB_AUTO_REFRESH_INTERVAL_MS
  );
}

function requestGithubRefresh(channelId: string) {
  const existing = githubRefreshRequests.get(channelId);
  if (existing) return existing;
  const request = apiInvoke("refresh_github_review_queue", { channelId }).finally(() => {
    if (githubRefreshRequests.get(channelId) === request) {
      githubRefreshRequests.delete(channelId);
    }
  });
  githubRefreshRequests.set(channelId, request);
  return request;
}

function errorMessage(error: unknown, fallback: string) {
  if (error instanceof Error && error.message) return error.message;
  if (typeof error === "string" && error.trim()) return error;
  return fallback;
}

function formattedUpdate(value: string) {
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return value;
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date);
}

function statusLabel(value: string | null) {
  return value ? value.replace(/_/g, " ") : "";
}

export function GithubPanel({
  channel,
  agents,
  onCreateReviewTask,
  onOpenThread,
}: GithubPanelProps) {
  const [overview, setOverview] = useState<GithubChannelOverview | null>(
    () => githubOverviewCache.get(channel.id) ?? null,
  );
  const [loading, setLoading] = useState(() => !githubOverviewCache.has(channel.id));
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [showBindingModal, setShowBindingModal] = useState(false);
  const [repositoryDraft, setRepositoryDraft] = useState("");
  const [localPathDraft, setLocalPathDraft] = useState("");
  const [reviewLoginDraft, setReviewLoginDraft] = useState("");
  const [bindingBusy, setBindingBusy] = useState(false);
  const [bindingError, setBindingError] = useState<string | null>(null);
  const [reviewTarget, setReviewTarget] = useState<GithubPullRequest | null>(null);
  const [selectedAgentId, setSelectedAgentId] = useState("");
  const [reviewBusy, setReviewBusy] = useState(false);
  const [reviewError, setReviewError] = useState<string | null>(null);
  const requestEpochRef = useRef(0);

  const refreshOverview = useCallback(async () => {
    const requestEpoch = requestEpochRef.current + 1;
    requestEpochRef.current = requestEpoch;
    setRefreshing(true);
    setError(null);
    try {
      const next = await requestGithubRefresh(channel.id);
      if (requestEpochRef.current !== requestEpoch) return;
      cacheGithubOverview(channel.id, next);
      setOverview(next);
    } catch (loadError) {
      if (requestEpochRef.current !== requestEpoch) return;
      setError(errorMessage(loadError, "Failed to refresh the GitHub review queue"));
    } finally {
      if (requestEpochRef.current === requestEpoch) setRefreshing(false);
    }
  }, [channel.id]);

  const loadOverview = useCallback(async () => {
    const requestEpoch = requestEpochRef.current + 1;
    requestEpochRef.current = requestEpoch;
    const cached = githubOverviewCache.get(channel.id) ?? null;
    setOverview(cached);
    setLoading(!cached);
    setRefreshing(false);
    setError(null);
    try {
      const next = await apiInvoke("load_github_review_queue", {
        channelId: channel.id,
      });
      if (requestEpochRef.current !== requestEpoch) return;
      cacheGithubOverview(channel.id, next);
      setOverview(next);
      setLoading(false);
      if (githubQueueNeedsRefresh(next)) {
        void refreshOverview();
      }
    } catch (loadError) {
      if (requestEpochRef.current !== requestEpoch) return;
      if (!cached) setOverview(null);
      setError(errorMessage(loadError, "Failed to load the cached GitHub review queue"));
      setLoading(false);
    }
  }, [channel.id, refreshOverview]);

  useEffect(() => {
    void loadOverview();
    return () => {
      requestEpochRef.current += 1;
    };
  }, [loadOverview]);

  function openBindingModal() {
    setRepositoryDraft(overview?.binding?.name_with_owner ?? "");
    setLocalPathDraft(overview?.binding?.local_path ?? "");
    setReviewLoginDraft(overview?.binding?.review_login ?? overview?.account.login ?? "");
    setBindingError(null);
    setShowBindingModal(true);
  }

  async function saveBinding() {
    const repository = repositoryDraft.trim();
    if (!repository || bindingBusy) return;
    setBindingBusy(true);
    setBindingError(null);
    try {
      await apiInvoke("bind_github_repository", {
        channelId: channel.id,
        repository,
        localPath: localPathDraft.trim() || null,
        reviewLogin: reviewLoginDraft.trim() || null,
      });
      setShowBindingModal(false);
      githubOverviewCache.delete(channel.id);
      await loadOverview();
    } catch (saveError) {
      setBindingError(errorMessage(saveError, "Failed to bind the GitHub repository"));
    } finally {
      setBindingBusy(false);
    }
  }

  function chooseReviewAgent(pullRequest: GithubPullRequest) {
    setReviewTarget(pullRequest);
    setSelectedAgentId(agents[0]?.id ?? "");
    setReviewError(null);
  }

  async function createReviewTask() {
    if (!reviewTarget || !selectedAgentId || reviewBusy) return;
    setReviewBusy(true);
    setReviewError(null);
    try {
      await onCreateReviewTask(reviewTarget.number, selectedAgentId);
      setReviewTarget(null);
    } catch (createError) {
      setReviewError(errorMessage(createError, "Failed to create the GitHub review task"));
    } finally {
      setReviewBusy(false);
    }
  }

  async function openGithubUrl(url: string) {
    try {
      await openExternalUrl(url);
    } catch (openError) {
      setError(errorMessage(openError, "Failed to open GitHub"));
    }
  }

  if (loading && !overview) {
    return (
      <div className="github-panel github-panel-loading" aria-live="polite">
        <LoaderCircle className="spin" size={24} />
        <span>Loading GitHub review queue…</span>
      </div>
    );
  }

  if (error && !overview) {
    return (
      <div className="github-panel">
        <div className="empty-state github-empty-state" role="alert">
          <Github size={36} />
          <h2>GitHub connection unavailable</h2>
          <p>{error}</p>
          <button type="button" className="empty-state-action" onClick={() => void loadOverview()}>
            <RefreshCw size={15} />
            Retry
          </button>
        </div>
      </div>
    );
  }

  if (!overview) return null;

  const binding = overview.binding;
  const selectedAgent = agents.find((agent) => agent.id === selectedAgentId) ?? null;
  return (
    <div className="github-panel">
      {binding ? (
        <>
          <header className="github-panel-toolbar">
            <div className="github-repository-summary">
              <button
                type="button"
                className="github-repository-link"
                onClick={() => void openGithubUrl(binding.url)}
                title="Open repository on GitHub"
              >
                <Github size={18} />
                <strong>{binding.name_with_owner}</strong>
                <ExternalLink size={14} />
              </button>
              <span>
                Review requests for <b>@{binding.review_login}</b>
                {binding.local_path ? ` · ${binding.local_path}` : ""}
              </span>
              <span className="github-sync-status">
                {refreshing && <LoaderCircle className="spin" size={12} />}
                {binding.review_queue_synced_at
                  ? `${refreshing ? "Refreshing · " : ""}Last synced ${formattedUpdate(
                      binding.review_queue_synced_at,
                    )}`
                  : refreshing
                    ? "Syncing review requests…"
                    : "Not synced yet"}
              </span>
            </div>
            <div className="github-toolbar-actions">
              <button
                type="button"
                disabled={refreshing}
                onClick={() => void refreshOverview()}
                title="Refresh review queue"
              >
                <RefreshCw className={refreshing ? "spin" : ""} size={16} />
                Refresh
              </button>
              <button type="button" onClick={openBindingModal}>
                <Settings2 size={16} />
                Binding
              </button>
            </div>
          </header>

          <section className="github-queue" aria-label="GitHub review requested pull requests">
            <div className="github-queue-heading">
              <div>
                <span>Pull requests</span>
                <strong>Review requested</strong>
              </div>
              <mark>{overview.review_requests.length}</mark>
            </div>
            {error && <div className="github-inline-error" role="alert">{error}</div>}
            {overview.review_requests.length === 0 &&
            refreshing &&
            !binding.review_queue_synced_at ? (
              <div className="empty-state compact github-empty-state" aria-live="polite">
                <LoaderCircle className="spin" size={28} />
                <h2>Loading review requests…</h2>
                <p>The repository is ready while the first queue snapshot syncs.</p>
              </div>
            ) : overview.review_requests.length === 0 ? (
              <div className="empty-state compact github-empty-state">
                <GitPullRequest size={32} />
                <h2>Review queue is clear</h2>
                <p>No open pull requests currently request @{binding.review_login}&apos;s review.</p>
              </div>
            ) : (
              <div className="github-pull-list">
                {overview.review_requests.map((pullRequest) => {
                  const linked = Boolean(pullRequest.linked_thread_root_id);
                  return (
                    <article className={`github-pull-card ${linked ? "linked" : ""}`} key={pullRequest.number}>
                      <div className="github-pull-main">
                        <div className="github-pull-eyebrow">
                          <span>PR #{pullRequest.number}</span>
                          {pullRequest.is_draft && <mark>Draft</mark>}
                          {linked && <mark className="linked">Linked</mark>}
                        </div>
                        <button
                          type="button"
                          className="github-pull-title"
                          onClick={() => void openGithubUrl(pullRequest.url)}
                        >
                          {pullRequest.title}
                          <ExternalLink size={14} />
                        </button>
                        <p>
                          @{pullRequest.author_login}
                          <span aria-hidden="true"> · </span>
                          Updated {formattedUpdate(pullRequest.updated_at)}
                        </p>
                      </div>
                      <div className="github-pull-actions">
                        {linked ? (
                          <>
                            <div className="github-linked-task">
                              <strong>Task #{pullRequest.linked_task_number}</strong>
                              <span>
                                {statusLabel(pullRequest.linked_task_status)}
                                {pullRequest.linked_assignee_name
                                  ? ` · ${pullRequest.linked_assignee_name}`
                                  : ""}
                              </span>
                            </div>
                            <button
                              type="button"
                              className="github-primary-action"
                              onClick={() => {
                                if (pullRequest.linked_thread_root_id) {
                                  onOpenThread(pullRequest.linked_thread_root_id);
                                }
                              }}
                            >
                              Open thread
                            </button>
                          </>
                        ) : (
                          <button
                            type="button"
                            className="github-primary-action"
                            disabled={agents.length === 0}
                            onClick={() => chooseReviewAgent(pullRequest)}
                            title={agents.length === 0 ? "Add an agent to review this pull request" : undefined}
                          >
                            <Bot size={16} />
                            Review with agent
                          </button>
                        )}
                      </div>
                    </article>
                  );
                })}
              </div>
            )}
          </section>
        </>
      ) : (
        <div className="empty-state github-empty-state">
          <Github size={38} />
          <h2>Bind a GitHub repository</h2>
          <p>
            Signed in through GitHub CLI as @{overview.account.login}. Bind one repository to
            show pull requests requesting your review.
          </p>
          <button type="button" className="empty-state-action" onClick={openBindingModal}>
            <Github size={15} />
            Bind repository
          </button>
        </div>
      )}

      <Modal
        open={showBindingModal}
        title={binding ? "GitHub Repository Binding" : "Bind GitHub Repository"}
        onClose={() => {
          if (!bindingBusy) setShowBindingModal(false);
        }}
        closeOnBackdrop={!bindingBusy}
        closeOnEscape={!bindingBusy}
        width={580}
      >
        <div className="modal-form">
          <label>
            <span>Repository</span>
            <input
              autoFocus
              value={repositoryDraft}
              onChange={(event) => setRepositoryDraft(event.target.value)}
              placeholder="owner/repository or GitHub URL"
            />
          </label>
          <label>
            <span>GitHub login for “My review requests”</span>
            <input
              value={reviewLoginDraft}
              onChange={(event) => setReviewLoginDraft(event.target.value)}
              placeholder={overview.account.login}
            />
          </label>
          <label>
            <span>Local checkout (optional)</span>
            <input
              value={localPathDraft}
              onChange={(event) => setLocalPathDraft(event.target.value)}
              placeholder="/absolute/path/to/repository"
            />
          </label>
          <p className="github-modal-note">
            Authentication uses the active <code>gh</code> account @{overview.account.login}.
            The local checkout is included in Agent review tasks.
          </p>
          {bindingError && <div className="github-inline-error" role="alert">{bindingError}</div>}
          <div className="modal-actions">
            <button type="button" disabled={bindingBusy} onClick={() => setShowBindingModal(false)}>
              Cancel
            </button>
            <button
              type="button"
              className="primary"
              disabled={!repositoryDraft.trim() || bindingBusy}
              onClick={() => void saveBinding()}
            >
              {bindingBusy ? "Binding…" : binding ? "Update binding" : "Bind repository"}
            </button>
          </div>
        </div>
      </Modal>

      <Modal
        open={Boolean(reviewTarget)}
        title={reviewTarget ? `Review ${binding?.name_with_owner}#${reviewTarget.number}` : "Review Pull Request"}
        onClose={() => {
          if (!reviewBusy) setReviewTarget(null);
        }}
        closeOnBackdrop={!reviewBusy}
        closeOnEscape={!reviewBusy}
        width={540}
      >
        {reviewTarget && (
          <div className="modal-form">
            <div className="github-review-target">
              <GitPullRequest size={20} />
              <div>
                <strong>{reviewTarget.title}</strong>
                <span>by @{reviewTarget.author_login}</span>
              </div>
            </div>
            <div className="modal-field github-agent-assignment">
              <span className="modal-field-label">Assign agent</span>
              <TaskAssigneePicker
                agents={agents}
                assignee={selectedAgent}
                allowUnassigned={false}
                ariaLabel={`Assign review agent for ${binding?.name_with_owner}#${reviewTarget.number}`}
                onChange={setSelectedAgentId}
              />
            </div>
            {selectedAgent && (
              <p className="github-modal-note">
                A Task and canonical thread will be created and dispatched to @{selectedAgent.handle}.
              </p>
            )}
            <p className="github-modal-note">
              The task is anchored to the latest PR head SHA. The Agent reports in Lantor and
              will not publish a GitHub review.
            </p>
            {reviewError && <div className="github-inline-error" role="alert">{reviewError}</div>}
            <div className="modal-actions">
              <button type="button" disabled={reviewBusy} onClick={() => setReviewTarget(null)}>
                Cancel
              </button>
              <button
                type="button"
                className="primary"
                disabled={!selectedAgentId || reviewBusy}
                onClick={() => void createReviewTask()}
              >
                {reviewBusy ? "Creating…" : "Create task and start review"}
              </button>
            </div>
          </div>
        )}
      </Modal>
    </div>
  );
}
