import assert from "node:assert/strict";
import test from "node:test";

import { modelLabel, modelOptionsForRuntime } from "../src/types";

test("Claude Fable preset targets the current 5.1 model", () => {
  assert.ok(modelOptionsForRuntime("claude").includes("fable"));
  assert.equal(modelLabel("fable"), "Claude Fable 5.1");
  assert.equal(modelLabel("claude-fable-5-1"), "Claude Fable 5.1");
});
