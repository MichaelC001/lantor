import {
  Check,
  CircleDot,
  Clock3,
  GitMerge,
  GitPullRequest,
  GitPullRequestClosed,
  GitPullRequestDraft,
  X,
} from "lucide-react";
import type { AnchorHTMLAttributes, ReactNode } from "react";
import {
  type GithubIssueLink,
  type GithubReferenceIndex,
  githubReferenceItem,
  githubReferenceLabel,
  githubReferenceStatus,
} from "../github-references";

type GithubReferenceLinkProps = AnchorHTMLAttributes<HTMLAnchorElement> & {
  href: string;
  link: GithubIssueLink;
  references: GithubReferenceIndex;
  /** Replace the children (a bare URL) with `rw#123`. */
  compactLabel?: boolean;
  children?: ReactNode;
};

const STATUS_LABELS = { open: "Open", draft: "Draft", merged: "Merged", closed: "Closed" } as const;
const CHECK_LABELS: Record<string, string> = { success: "checks passed", failure: "checks failing", pending: "checks running" };

function StatusIcon({ kind, status }: { kind: "pull" | "issue"; status: keyof typeof STATUS_LABELS }) {
  const props = { size: 12, "aria-hidden": true, className: "github-reference-icon" } as const;
  if (kind === "issue") return <CircleDot {...props} />;
  if (status === "draft") return <GitPullRequestDraft {...props} />;
  if (status === "merged") return <GitMerge {...props} />;
  if (status === "closed") return <GitPullRequestClosed {...props} />;
  return <GitPullRequest {...props} />;
}

function ChecksIcon({ status }: { status: string }) {
  const props = { size: 11, "aria-hidden": true, className: `github-reference-checks ${status}` } as const;
  if (status === "success") return <Check {...props} />;
  if (status === "failure") return <X {...props} />;
  if (status === "pending") return <Clock3 {...props} />;
  return null;
}

/**
 * A link to a GitHub PR or issue. Items already in the synced GitHub queues
 * get a state icon, a CI mark and a title tooltip; others stay plain links.
 */
export function GithubReferenceLink({
  href,
  link,
  references,
  compactLabel = false,
  children,
  className,
  ...props
}: GithubReferenceLinkProps) {
  const item = githubReferenceItem(references, link);
  const label = compactLabel ? githubReferenceLabel(references, link) : children;
  const status = item ? githubReferenceStatus(item) : null;
  const checks = item?.kind === "pull" ? CHECK_LABELS[item.checks_status] : undefined;
  const title = item && status
    ? `${item.kind === "pull" ? "Pull request" : "Issue"} ${githubReferenceLabel(references, link)}: ${item.title}\n${STATUS_LABELS[status]}${checks ? ` · ${checks}` : ""}`
    : undefined;

  return (
    <a
      {...props}
      href={href}
      className={["github-reference", className].filter(Boolean).join(" ")}
      data-status={status ?? undefined}
      title={title ?? props.title}
      target="_blank"
      rel="noreferrer"
    >
      {item && status && <StatusIcon kind={item.kind} status={status} />}
      {label}
      {item && checks && <ChecksIcon status={item.checks_status} />}
    </a>
  );
}
