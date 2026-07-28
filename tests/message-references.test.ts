import assert from "node:assert/strict";
import test from "node:test";

import { messageReferenceLocation } from "../src/message-references";
import type { Message } from "../src/types";

function message(overrides: Partial<Message> = {}): Message {
  return {
    id: "message-id",
    seq: 1,
    channel_id: "channel-id",
    thread_root_id: null,
    sender_agent_id: null,
    sender_name: "Dylan",
    sender_role: "owner",
    body: "",
    is_task: false,
    thread_followed: false,
    delivery_state: "complete",
    stream_key: "",
    task_number: null,
    task_status: null,
    attachments: [],
    artifacts: [],
    created_at: "2026-07-28T00:00:00.000Z",
    updated_at: "2026-07-28T00:00:00.000Z",
    ...overrides,
  };
}

test("message references to channel roots stay on the channel surface", () => {
  assert.deepEqual(messageReferenceLocation("message", message()), {
    channelId: "channel-id",
    threadId: null,
    focusedMessageId: "message-id",
    showThread: false,
  });
});

test("message references to replies open the owning thread", () => {
  assert.deepEqual(
    messageReferenceLocation("message", message({ thread_root_id: "thread-id" })),
    {
      channelId: "channel-id",
      threadId: "thread-id",
      focusedMessageId: "message-id",
      showThread: true,
    },
  );
});

test("thread references open the referenced root", () => {
  assert.deepEqual(messageReferenceLocation("thread", message()), {
    channelId: "channel-id",
    threadId: "message-id",
    focusedMessageId: "message-id",
    showThread: true,
  });
});
