import assert from "node:assert/strict";
import test from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { MarkdownRenderer } from "../src/components/MarkdownRenderer";
import {
  buildGithubReferenceIndex,
  derivedReferenceAlias,
  githubReferenceLabel,
  parseGithubIssueUrl,
  resolveGithubRepository,
  setGithubReferences,
} from "../src/github-references";
import { markdownPreviewText } from "../src/markdown-preview";

process.env.TZ = "Asia/Singapore";

const RW_CHANNEL = "00000000-0000-4000-8000-000000000001";
const IR_CHANNEL = "00000000-0000-4000-8000-000000000002";
const references = {
  repositories: [
    { channel_id: RW_CHANNEL, name_with_owner: "risingwavelabs/risingwave", url: "https://github.com/risingwavelabs/risingwave", reference_alias: "rw" },
    { channel_id: IR_CHANNEL, name_with_owner: "risingwavelabs/iceberg-rust", url: "https://github.com/risingwavelabs/iceberg-rust", reference_alias: "" },
  ],
  items: [{
    name_with_owner: "risingwavelabs/risingwave", number: 27429, kind: "pull" as const, title: "fix(sum): keep NULL",
    url: "https://github.com/risingwavelabs/risingwave/pull/27429", state: "open", is_draft: false,
    checks_status: "failure", updated_at: "2026-10-14T00:00:00Z",
  }],
};

const render = (body: string, channelId?: string) =>
  renderToStaticMarkup(createElement(MarkdownRenderer, { body, channelId }));

test("reference index resolves bare, aliased, derived and full references", () => {
  const index = buildGithubReferenceIndex(references);
  assert.equal(derivedReferenceAlias("iceberg-compaction"), "ic");
  assert.equal(derivedReferenceAlias("risingwave"), "");
  assert.equal(resolveGithubRepository(index, null, RW_CHANNEL), "risingwavelabs/risingwave");
  assert.equal(resolveGithubRepository(index, null, undefined), null);
  assert.equal(resolveGithubRepository(index, "RW", undefined), "risingwavelabs/risingwave");
  assert.equal(resolveGithubRepository(index, "ir", undefined), "risingwavelabs/iceberg-rust");
  assert.equal(resolveGithubRepository(index, "iceberg-rust", undefined), "risingwavelabs/iceberg-rust");
  assert.equal(resolveGithubRepository(index, "nimtable/iceberg-compaction", undefined), "nimtable/iceberg-compaction");
  assert.equal(resolveGithubRepository(index, "task", undefined), null);
  assert.equal(resolveGithubRepository(index, "a/b/c", undefined), null);
  const link = parseGithubIssueUrl("https://github.com/risingwavelabs/iceberg-rust/pull/244/files");
  assert.deepEqual(link, { nameWithOwner: "risingwavelabs/iceberg-rust", number: 244 });
  assert.equal(githubReferenceLabel(index, link!), "iceberg-rust#244");
  assert.equal(parseGithubIssueUrl("https://github.com/o/r/blob/main/x.rs"), null);
});

test("messages link references and show cached state", () => {
  setGithubReferences(references);
  const html = render("See #27429, rw#27500 and ir#244. Skip `#27429`, task #99, #7, a#b and [#1](https://x.dev).", RW_CHANNEL);
  assert.match(html, /<a href="https:\/\/github.com\/risingwavelabs\/risingwave\/pull\/27429" class="github-reference" data-status="open" title="Pull request rw#27429: fix\(sum\): keep NULL\nOpen · checks failing"/);
  assert.match(html, /github-reference-checks failure/);
  assert.match(html, /href="https:\/\/github.com\/risingwavelabs\/risingwave\/issues\/27500" class="github-reference"[^>]*>rw#27500<\/a>/);
  assert.match(html, /href="https:\/\/github.com\/risingwavelabs\/iceberg-rust\/issues\/244"[^>]*>ir#244<\/a>/);
  assert.match(html, /<code>#27429<\/code>/);
  assert.match(html, /\/lantor\/task\/99/);
  assert.doesNotMatch(html, /issues\/7"/);
  assert.match(html, /href="https:\/\/x.dev"/);
  // Without a bound channel only prefixed references resolve.
  const dm = render("#27429 and rw#27429");
  assert.equal(dm.match(/github-reference"/g)?.length, 1);
  // A bare PR URL renders as a compact label.
  assert.match(render("https://github.com/risingwavelabs/risingwave/pull/27429"), /<\/svg>rw#27429<svg/);
  assert.match(render("https://github.com/risingwavelabs/iceberg-rust/pull/9"), />iceberg-rust#9<\/a>/);
  setGithubReferences({ repositories: [], items: [] });
});

test("UTC timestamps render in local time with the UTC value in the tooltip", () => {
  const html = render("Next reminder: 2026-10-13T19:00:00+00:00, fired 19:00Z, ran 10-14 07:46Z, code `07:46Z`.");
  assert.match(html, /<time dateTime="2026-10-13T19:00:00.000Z" title="2026-10-13 19:00 UTC" class="lantor-time">10-14 03:00<\/time>/);
  assert.match(html, /title="19:00 UTC" class="lantor-time">03:00 \+1d<\/time>/);
  assert.match(html, />10-14 15:46<\/time>/);
  assert.match(html, /<code>07:46Z<\/code>/);
  assert.match(render("at 2025-01-02T03:04:05Z"), />2025-01-02 11:04<\/time>/);
});

test("previews drop Markdown syntax", () => {
  assert.equal(
    markdownPreviewText("**结论**：`rw#1` 见 [PR](https://x.dev) ~~old~~\n\n- item *one*\n> quote\n\n```rust\nfn main() {}\n```\n\n| a | b |\n|---|---|\n| 1 | 2 |"),
    "结论：rw#1 见 PR old\nitem one\nquote\nfn main() {}\na · b\n1 · 2",
  );
  assert.equal(markdownPreviewText("plain snake_case_name and a_b_c"), "plain snake_case_name and a_b_c");
  assert.equal(markdownPreviewText("**unclosed"), "**unclosed");
});
