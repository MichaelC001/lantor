import assert from "node:assert/strict";
import { test } from "node:test";

const { createMessagePreview } = await import("../.test-build/message-preview.js");

test("createMessagePreview returns the full message when it fits", () => {
  const preview = createMessagePreview("one\ntwo", 3);

  assert.deepEqual(preview, {
    body: "one\ntwo",
    truncated: false,
  });
});

test("createMessagePreview reports truncated bodies without appending ellipsis text", () => {
  const preview = createMessagePreview("one\ntwo\nthree\nfour", 3);

  assert.deepEqual(preview, {
    body: "one\ntwo\nthree",
    truncated: true,
  });
});
