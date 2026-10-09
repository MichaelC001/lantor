import assert from "node:assert/strict";
import test from "node:test";
import { attachmentPreviewKind, displaySavedPath, revealActionLabel, TEXT_PREVIEW_LIMIT_BYTES } from "../src/attachment-sheet";

const kind = (mime_type: string, original_name: string, size_bytes = 1024) =>
  attachmentPreviewKind({ mime_type, original_name, size_bytes });

test("home screen file sheet previews only what iOS renders reliably in-app", () => {
  assert.equal(kind("text/markdown", "implementation.md"), "markdown");
  assert.equal(kind("application/octet-stream", "notes.MD"), "markdown");
  assert.equal(kind("text/plain; charset=utf-8", "run.log"), "text");
  assert.equal(kind("application/json", "data.json"), "text");
  assert.equal(kind("", "server.rs"), "text");
  // Served as attachments for safety; the sheet shows their source as text.
  assert.equal(kind("text/html", "report.html"), "text");
  assert.equal(kind("image/png", "chart.png"), "image");
  assert.equal(kind("video/mp4", "demo.mp4"), "video");
  assert.equal(kind("audio/mpeg", "memo.mp3"), "audio");
  // iOS shows only the first page of an embedded PDF; Office files never render.
  assert.equal(kind("application/pdf", "report.pdf"), "none");
  assert.equal(kind("application/vnd.openxmlformats-officedocument.presentationml.presentation", "deck.pptx"), "none");
  assert.equal(kind("application/octet-stream", "archive.zip"), "none");
});

test("large text files skip the in-memory preview but stay shareable", () => {
  assert.equal(kind("text/markdown", "big.md", TEXT_PREVIEW_LIMIT_BYTES), "markdown");
  assert.equal(kind("text/markdown", "big.md", TEXT_PREVIEW_LIMIT_BYTES + 1), "none");
  assert.equal(kind("image/png", "huge.png", 50 * 1024 * 1024), "image");
});

test("desktop saves report the full location and how to reveal it", () => {
  assert.equal(displaySavedPath("/Users/dylan/Downloads/report (1).pdf"), "~/Downloads/report (1).pdf");
  assert.equal(displaySavedPath("/home/dylan/Downloads/chart.png"), "~/Downloads/chart.png");
  assert.equal(displaySavedPath("C:\\Users\\dylan\\Downloads\\chart.png"), "~\\Downloads\\chart.png");
  // Only a leading home folder is shortened.
  assert.equal(displaySavedPath("/Volumes/Data/Users/x/chart.png"), "/Volumes/Data/Users/x/chart.png");
  assert.equal(revealActionLabel("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"), "Show in Finder");
  assert.equal(revealActionLabel("Mozilla/5.0 (Windows NT 10.0; Win64; x64)"), "Show in Explorer");
  assert.equal(revealActionLabel("Mozilla/5.0 (X11; Linux x86_64)"), "Show in folder");
});
