// Resolves `#123`, `rw#123` and `owner/repo#123` in message text to GitHub
// links, and looks up the state of PRs/issues that are already in the synced
// GitHub queues. Data comes from one local read (`load_github_references`);
// nothing here calls GitHub.
import { apiInvoke } from "./apiClient";
import type { GithubReferenceItem, GithubReferences } from "./types";

export type GithubReferenceIndex = {
  /** Lower-cased `owner/repo` by channel id. */
  byChannel: ReadonlyMap<string, string>;
  /** Lower-cased alias or repo name → lower-cased `owner/repo`; `null` when ambiguous. */
  byName: ReadonlyMap<string, string | null>;
  /** Lower-cased `owner/repo` → canonical `owner/repo` and its short display prefix. */
  repositories: ReadonlyMap<string, { nameWithOwner: string; shortName: string }>;
  /** `owner/repo#number` (lower-cased repo) → cached item. */
  items: ReadonlyMap<string, GithubReferenceItem>;
  /** Explicit short name set on each channel binding. */
  aliasByChannel: ReadonlyMap<string, string>;
};

export type GithubIssueLink = {
  nameWithOwner: string;
  number: number;
};

const EMPTY_INDEX: GithubReferenceIndex = {
  byChannel: new Map(),
  byName: new Map(),
  repositories: new Map(),
  items: new Map(),
  aliasByChannel: new Map(),
};

const REPOSITORY_PART = /^[A-Za-z0-9][A-Za-z0-9_.-]*$/;
const GITHUB_ISSUE_URL = /^https:\/\/github\.com\/([A-Za-z0-9][A-Za-z0-9_.-]*)\/([A-Za-z0-9_.-]+)\/(?:pull|issues)\/([0-9]+)(?:[/?#].*)?$/;

let index = EMPTY_INDEX;
let loading: Promise<void> | null = null;
const listeners = new Set<() => void>();

/** `iceberg-rust` → `ir`; single-word names get no derived alias. */
export function derivedReferenceAlias(repositoryName: string) {
  const parts = repositoryName.toLowerCase().split(/[-_.]+/).filter(Boolean);
  if (parts.length < 2) return "";
  return parts.map((part) => part[0]).join("");
}

export function buildGithubReferenceIndex(data: GithubReferences): GithubReferenceIndex {
  const byChannel = new Map<string, string>();
  const aliasByChannel = new Map<string, string>();
  const repositories = new Map<string, { nameWithOwner: string; shortName: string }>();
  const candidates = new Map<string, Set<string>>();
  const explicit = new Map<string, Set<string>>();
  const add = (target: Map<string, Set<string>>, name: string, repository: string) => {
    if (!name) return;
    const key = name.toLowerCase();
    target.set(key, (target.get(key) ?? new Set()).add(repository));
  };

  for (const binding of data.repositories ?? []) {
    const key = binding.name_with_owner.toLowerCase();
    byChannel.set(binding.channel_id, key);
    const repositoryName = binding.name_with_owner.split("/")[1] ?? binding.name_with_owner;
    const alias = binding.reference_alias.trim().toLowerCase();
    aliasByChannel.set(binding.channel_id, alias);
    const previous = repositories.get(key);
    repositories.set(key, {
      nameWithOwner: binding.name_with_owner,
      shortName: alias || previous?.shortName || repositoryName,
    });
    add(explicit, alias, key);
    add(candidates, repositoryName, key);
    add(candidates, derivedReferenceAlias(repositoryName), key);
  }

  const byName = new Map<string, string | null>();
  // Explicit aliases win over derived names; anything that still maps to two
  // repositories stays unresolved instead of guessing.
  for (const source of [candidates, explicit]) {
    for (const [name, targets] of source) {
      byName.set(name, targets.size === 1 ? [...targets][0] : null);
    }
  }
  byName.delete("task");

  const items = new Map<string, GithubReferenceItem>();
  for (const item of data.items ?? []) {
    items.set(`${item.name_with_owner.toLowerCase()}#${item.number}`, item);
  }
  return { byChannel, byName, repositories, items, aliasByChannel };
}

export function getGithubReferenceIndex() {
  return index;
}

export function subscribeGithubReferences(listener: () => void) {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

export function setGithubReferences(data: GithubReferences) {
  index = buildGithubReferenceIndex(data);
  listeners.forEach((listener) => listener());
}

export function loadGithubReferences() {
  loading ??= apiInvoke("load_github_references")
    .then(setGithubReferences)
    .catch((error) => {
      console.warn("Failed to load GitHub references", error);
    })
    .finally(() => {
      loading = null;
    });
  return loading;
}

/** Resolves a reference prefix (`null` for a bare `#123`) to `owner/repo`. */
export function resolveGithubRepository(
  references: GithubReferenceIndex,
  prefix: string | null,
  channelId: string | undefined,
) {
  if (prefix === null) return channelId ? references.byChannel.get(channelId) ?? null : null;
  if (prefix.includes("/")) {
    const [owner, repository, extra] = prefix.split("/");
    if (extra !== undefined || !REPOSITORY_PART.test(owner) || !REPOSITORY_PART.test(repository ?? "")) return null;
    return prefix.toLowerCase();
  }
  return references.byName.get(prefix.toLowerCase()) ?? null;
}

export function githubReferenceUrl(references: GithubReferenceIndex, repository: string, number: number) {
  const item = references.items.get(`${repository}#${number}`);
  if (item) return item.url;
  const nameWithOwner = references.repositories.get(repository)?.nameWithOwner ?? repository;
  // GitHub redirects /issues/N to /pull/N when N is a pull request.
  return `https://github.com/${nameWithOwner}/issues/${number}`;
}

export function parseGithubIssueUrl(href: string | undefined): GithubIssueLink | null {
  const match = href?.match(GITHUB_ISSUE_URL);
  if (!match) return null;
  return { nameWithOwner: `${match[1]}/${match[2]}`, number: Number(match[3]) };
}

export function githubReferenceItem(references: GithubReferenceIndex, link: GithubIssueLink) {
  return references.items.get(`${link.nameWithOwner.toLowerCase()}#${link.number}`) ?? null;
}

/** `rw#27495` for bound repositories, `owner/repo#27495` otherwise. */
export function githubReferenceLabel(references: GithubReferenceIndex, link: GithubIssueLink) {
  const shortName = references.repositories.get(link.nameWithOwner.toLowerCase())?.shortName;
  return `${shortName ?? link.nameWithOwner}#${link.number}`;
}

export type GithubReferenceStatus = "open" | "draft" | "merged" | "closed";

export function githubReferenceStatus(item: GithubReferenceItem): GithubReferenceStatus {
  if (item.state === "merged") return "merged";
  if (item.state === "closed") return "closed";
  return item.kind === "pull" && item.is_draft ? "draft" : "open";
}
