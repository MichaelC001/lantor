import assert from "node:assert/strict";
import test from "node:test";

import {
  shouldPopAppModalHistory,
  shouldReplaceAppModalHistory,
} from "../src/app-modal-history";
import { shouldDismissOnEscape, type EscapeDismissEvent } from "../src/escape-dismiss";

function escapeEvent(overrides: Partial<EscapeDismissEvent> = {}): EscapeDismissEvent {
  return {
    key: "Escape",
    defaultPrevented: false,
    isComposing: false,
    keyCode: 27,
    repeat: false,
    ...overrides,
  };
}

test("switching between app modals replaces the current history entry", () => {
  assert.equal(shouldReplaceAppModalHistory("activity", "search"), true);
  assert.equal(shouldReplaceAppModalHistory("search", "search"), false);
  assert.equal(shouldReplaceAppModalHistory(null, "search"), false);
});

test("closing an app modal only pops its matching current history entry", () => {
  const input = {
    activeModal: "search" as const,
    expectedModal: "search" as const,
    canNavigateBack: true,
    currentIndex: 3,
    historyState: { index: 3, activeModal: "search" as const },
  };

  assert.equal(shouldPopAppModalHistory(input), true);
  assert.equal(
    shouldPopAppModalHistory({
      ...input,
      historyState: { index: 3, activeModal: "activity" },
    }),
    false,
  );
  assert.equal(
    shouldPopAppModalHistory({
      ...input,
      historyState: { index: 2, activeModal: "search" },
    }),
    false,
  );
  assert.equal(
    shouldPopAppModalHistory({ ...input, activeModal: "activity" }),
    false,
  );
  assert.equal(shouldPopAppModalHistory({ ...input, canNavigateBack: false }), false);
});

test("Escape dismisses one surface only for an unhandled initial keydown", () => {
  assert.equal(shouldDismissOnEscape(escapeEvent()), true);
  assert.equal(shouldDismissOnEscape(escapeEvent({ repeat: true })), false);
  assert.equal(shouldDismissOnEscape(escapeEvent({ defaultPrevented: true })), false);
  assert.equal(shouldDismissOnEscape(escapeEvent({ isComposing: true })), false);
  assert.equal(shouldDismissOnEscape(escapeEvent({ keyCode: 229 })), false);
  assert.equal(shouldDismissOnEscape(escapeEvent({ key: "Enter" })), false);
});
