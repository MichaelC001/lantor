// Production-bundle regression for message text enrichment: GitHub references,
// local times and Markdown-free previews. All API calls are mocked; no user data
// is read or changed. Run after `npm run build` with `npm run test:message-text`.
import assert from "node:assert/strict";
import { chromium } from "playwright";
import { preview } from "vite";

const channelId = "00000000-0000-4000-8000-000000000001";
const body = [
  "**结论**：#27429 CI 挂了，rw#27500 还没 review，见 https://github.com/risingwavelabs/risingwave/pull/27429",
  "",
  "Next reminder: 2026-10-13T19:00:00+00:00，代码里 `#27429` 不链接。",
].join("\n");
const message = {
  id: "00000000-0000-4000-8000-000000000002", seq: 1,
  channel_id: channelId, thread_root_id: null, sender_agent_id: null,
  sender_name: "Test owner", sender_role: "owner", body, is_task: false,
  thread_followed: false, delivery_state: "complete", stream_key: "",
  task_number: null, task_status: null, attachments: [], artifacts: [],
  created_at: "2026-01-01T00:00:00Z", updated_at: "2026-01-01T00:00:00Z",
};
const references = {
  repositories: [{
    channel_id: channelId, name_with_owner: "risingwavelabs/risingwave",
    url: "https://github.com/risingwavelabs/risingwave", reference_alias: "rw",
  }],
  items: [{
    name_with_owner: "risingwavelabs/risingwave", number: 27429, kind: "pull", title: "fix(sum): keep NULL",
    url: "https://github.com/risingwavelabs/risingwave/pull/27429", state: "open", is_draft: false,
    checks_status: "failure", updated_at: "2026-10-14T00:00:00Z",
  }],
};
const bootstrap = {
  db_url: "synthetic://message-text", web_base_url: null,
  owner_profile: { display_name: "Test owner", avatar: "T", description: "" },
  channels: [{ id: channelId, name: "risingwave", description: "", kind: "channel",
    dm_agent_id: null, unread_count: 0, github_unread_count: 0, github_review_synced_at: null }],
  messages: [message], channel_message_history: [{ channel_id: channelId, before_seq: null, has_more: false }],
  thread_activities: [], channel_members: [], agents: [],
  saved_messages: [{
    id: "00000000-0000-4000-8000-000000000003", message_id: message.id, channel_id: channelId,
    channel_name: "risingwave", thread_root_id: null, sender_name: "Test owner", sender_role: "owner",
    body, message_created_at: message.created_at, created_at: message.created_at,
  }],
  dismissed_inbox_items: {}, read_inbox_items: {}, artifacts: [], tasks: [], reminders: [],
  agent_schedules: [], agent_runs: [], agent_work_items: [], agent_activities: [],
  supervisor: { pid: null, status: "stopped", updated_at: null },
  launch_agent: { label: "", plist_path: "", installed: false, loaded: false }, ui_event_cursor: 0,
};

const server = await preview({ preview: { host: "127.0.0.1", port: 0, strictPort: true } });
const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
let browser;
try {
  browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    serviceWorkers: "block", viewport: { width: 1100, height: 900 }, timezoneId: "Asia/Singapore",
  });
  const page = await context.newPage();
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.addInitScript(() => {
    window.EventSource = class extends EventTarget { close() {} };
  });
  await page.route("**/*", async (route) => {
    const url = new URL(route.request().url());
    if (url.origin !== origin) return route.abort();
    if (!url.pathname.startsWith("/api/")) return route.continue();
    let response = { ok: true };
    if (url.pathname === "/api/bootstrap") response = bootstrap;
    if (url.pathname === "/api/load_channel_messages") response = { messages: [message], next_before_seq: null, has_more: false };
    if (url.pathname === "/api/load_github_references") response = references;
    return route.fulfill({ contentType: "application/json", body: JSON.stringify(response) });
  });

  await page.goto(origin, { waitUntil: "networkidle" });
  const markdown = page.locator(".message-card .markdown-body").first();
  const queued = markdown.locator("a.github-reference[data-status='open']");
  await queued.first().waitFor();
  assert.equal(await queued.count(), 2, "bare #27429 and the PR URL both show the queued PR state");
  assert.equal(await queued.first().getAttribute("href"), "https://github.com/risingwavelabs/risingwave/pull/27429");
  assert.match(await queued.first().getAttribute("title"), /fix\(sum\): keep NULL\nOpen · checks failing/);
  assert.equal(await queued.first().locator(".github-reference-checks.failure").count(), 1);
  assert.equal((await queued.nth(1).innerText()).trim(), "rw#27429", "bare PR URL is shortened");
  const unknown = markdown.locator("a.github-reference:not([data-status])");
  assert.equal(await unknown.count(), 1);
  assert.equal(await unknown.getAttribute("href"), "https://github.com/risingwavelabs/risingwave/issues/27500");
  assert.equal(await markdown.locator("code").innerText(), "#27429");
  const time = markdown.locator("time.lantor-time");
  assert.equal(await time.innerText(), "10-14 03:00");
  assert.equal(await time.getAttribute("title"), "2026-10-13 19:00 UTC");
  console.log("PASS message body: GitHub references, cached PR state and local time");
  if (process.env.MESSAGE_TEXT_SCREENSHOT) {
    await markdown.screenshot({ path: process.env.MESSAGE_TEXT_SCREENSHOT });
    await page.evaluate(() => { document.documentElement.dataset.theme = "dark"; });
    await page.waitForTimeout(500);
    await markdown.screenshot({ path: process.env.MESSAGE_TEXT_SCREENSHOT.replace(/(\.png)?$/, "-dark.png") });
    await page.evaluate(() => { document.documentElement.dataset.theme = "light"; });
  }

  await page.getByRole("button", { name: "Saved" }).first().click();
  const saved = page.locator(".saved-row p").first();
  await saved.waitFor();
  const savedText = await saved.innerText();
  assert.ok(savedText.startsWith("结论：#27429"), savedText);
  assert.ok(!/[*`]/.test(savedText), `preview keeps Markdown syntax: ${savedText}`);
  console.log("PASS saved preview: Markdown syntax removed");
  assert.deepEqual(errors, []);
  await context.close();
} finally {
  await browser?.close();
  await new Promise((resolve, reject) => server.httpServer.close((error) => error ? reject(error) : resolve()));
}
