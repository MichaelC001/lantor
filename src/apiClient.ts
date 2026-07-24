import { convertFileSrc, invoke as tauriInvoke } from "@tauri-apps/api/core";
import { listen as tauriListen, type UnlistenFn } from "@tauri-apps/api/event";

const UI_REFRESH_EVENT = "lantor://refresh";

declare global {
  interface Window {
    __TAURI_INTERNALS__?: unknown;
  }
}

export function isTauriRuntime() {
  return typeof window !== "undefined" && Boolean(window.__TAURI_INTERNALS__);
}

export async function openExternalUrl(url: string): Promise<void> {
  if (isTauriRuntime()) {
    await tauriInvoke("open_external_url", { url });
    return;
  }
  window.open(url, "_blank", "noopener,noreferrer");
}

export async function downloadAttachment(storagePath: string, originalName: string): Promise<string> {
  if (!isTauriRuntime()) {
    throw new Error("downloadAttachment is only available in the desktop app");
  }
  return tauriInvoke<string>("download_attachment", { storagePath, originalName });
}

export async function completeStartupSplash(): Promise<void> {
  if (!isTauriRuntime()) return;
  await tauriInvoke("complete_startup_splash");
}

function apiPath(command: string) {
  return `/api/${command}`;
}

function bootstrapApiPath(args: Record<string, unknown>) {
  const channelId = typeof args.channelId === "string" ? args.channelId.trim() : "";
  const params = new URLSearchParams();
  if (args.currentChannelOnly === true) params.set("currentChannelOnly", "true");
  if (channelId) params.set("channelId", channelId);
  if (params.size === 0) return apiPath("bootstrap");
  return `${apiPath("bootstrap")}?${params.toString()}`;
}

export async function apiInvoke<T>(command: string, args: Record<string, unknown> = {}): Promise<T> {
  if (isTauriRuntime()) {
    return tauriInvoke<T>(command, args);
  }

  const response = command === "bootstrap"
    ? await fetch(bootstrapApiPath(args))
    : await fetch(apiPath(command), {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(args),
    });

  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json")
    ? await response.json()
    : await response.text();
  if (!response.ok) {
    const message = typeof payload === "object" && payload && "message" in payload
      ? String((payload as { message: unknown }).message)
      : String(payload || `${command} failed`);
    throw new Error(message);
  }
  return payload as T;
}

export type ApiInvokeMeasurement = {
  roundtripMs: number;
  payloadBytes?: number;
  parseMs?: number;
};

export async function apiInvokeMeasured<T>(
  command: string,
  args: Record<string, unknown> = {},
): Promise<{ payload: T; measurement: ApiInvokeMeasurement }> {
  const startedAt = performance.now();
  if (isTauriRuntime()) {
    const payload = await tauriInvoke<T>(command, args);
    return {
      payload,
      measurement: {
        roundtripMs: performance.now() - startedAt,
      },
    };
  }

  const response = command === "bootstrap"
    ? await fetch(bootstrapApiPath(args))
    : await fetch(apiPath(command), {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(args),
    });

  const contentType = response.headers.get("content-type") || "";
  const rawPayload = await response.text();
  const roundtripMs = performance.now() - startedAt;
  const payloadBytes = new TextEncoder().encode(rawPayload).length;
  const parseStartedAt = performance.now();
  const payload = contentType.includes("application/json")
    ? JSON.parse(rawPayload)
    : rawPayload;
  const parseMs = performance.now() - parseStartedAt;
  if (!response.ok) {
    const message = typeof payload === "object" && payload && "message" in payload
      ? String((payload as { message: unknown }).message)
      : String(payload || `${command} failed`);
    throw new Error(message);
  }
  return {
    payload: payload as T,
    measurement: {
      roundtripMs,
      payloadBytes,
      parseMs,
    },
  };
}

type BackendEventSubscriptionOptions = {
  cursor?: number | null;
  onCursor?: (cursor: number) => void;
  onReconnect?: () => void;
};

export async function subscribeBackendEvents(
  handler: (payload: string) => void,
  options: BackendEventSubscriptionOptions = {},
): Promise<UnlistenFn> {
  if (isTauriRuntime()) {
    return tauriListen<string>(UI_REFRESH_EVENT, (event) => handler(event.payload));
  }

  const cursor = Number.isSafeInteger(options.cursor) && (options.cursor ?? -1) >= 0
    ? options.cursor
    : null;
  const source = new EventSource(cursor === null ? "/api/events" : `/api/events?cursor=${cursor}`);
  let disconnected = false;
  source.addEventListener("lantor", (event) => {
    const message = event as MessageEvent<string>;
    const eventCursor = Number(message.lastEventId);
    if (message.lastEventId && Number.isSafeInteger(eventCursor) && eventCursor >= 0) {
      options.onCursor?.(eventCursor);
    }
    handler(message.data);
  });
  source.onopen = () => {
    if (!disconnected) return;
    disconnected = false;
    options.onReconnect?.();
  };
  source.onerror = () => {
    disconnected = true;
    console.error("Lantor web event stream disconnected");
  };
  return () => source.close();
}

export function attachmentAssetUrl(storagePath: string, attachmentId: string) {
  if (isTauriRuntime()) {
    return convertFileSrc(storagePath);
  }
  return `/api/attachments/${attachmentId}`;
}
