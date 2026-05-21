import { Activity, Monitor, Moon, RotateCcw, Sun, Type } from "lucide-react";
import { Modal } from "./Modal";

export type ThemePreference = "auto" | "light" | "dark";
export type ChatTextSize = "compact" | "default" | "large" | "xlarge";

export type RefreshMetricsSummary = {
  started_at: string;
  total: number;
  retained: number;
  retention_minutes: number;
  max_events: number;
  last_minute: number;
  rate_per_minute_1m: number;
  by_kind: Record<string, number>;
  by_reason: Record<string, number>;
};

export type RefreshMetricEvent = {
  id: number;
  at: string;
  kind: string;
  reason: string;
  status: string;
  detail?: string;
  duration_ms?: number;
  batch_size?: number;
  rate_per_minute_1m: number;
};

type SettingsModalProps = {
  open: boolean;
  themePreference: ThemePreference;
  chatTextSize: ChatTextSize;
  refreshMetricsSummary: RefreshMetricsSummary | null;
  refreshMetricEvents: RefreshMetricEvent[];
  onThemePreferenceChange: (value: ThemePreference) => void;
  onChatTextSizeChange: (value: ChatTextSize) => void;
  onRefreshMetricsReset: () => void;
  onClose: () => void;
};

const THEME_OPTIONS: Array<{
  value: ThemePreference;
  label: string;
  detail: string;
  icon: typeof Monitor;
}> = [
  { value: "auto", label: "Auto", detail: "Follow system", icon: Monitor },
  { value: "light", label: "Light", detail: "Bright surfaces", icon: Sun },
  { value: "dark", label: "Dark", detail: "Dim surfaces", icon: Moon },
];

const CHAT_TEXT_SIZE_OPTIONS: Array<{
  value: ChatTextSize;
  label: string;
  detail: string;
}> = [
  { value: "compact", label: "Small", detail: "Compact UI" },
  { value: "default", label: "Default", detail: "Current scale" },
  { value: "large", label: "Large", detail: "More readable" },
  { value: "xlarge", label: "Extra", detail: "Largest" },
];

export function SettingsModal({
  open,
  themePreference,
  chatTextSize,
  refreshMetricsSummary,
  refreshMetricEvents,
  onThemePreferenceChange,
  onChatTextSizeChange,
  onRefreshMetricsReset,
  onClose,
}: SettingsModalProps) {
  const topReasons = refreshMetricsSummary
    ? Object.entries(refreshMetricsSummary.by_reason)
      .sort((left, right) => right[1] - left[1])
      .slice(0, 6)
    : [];

  return (
    <Modal open={open} title="Settings" onClose={onClose} width={560}>
      <section className="settings-panel">
        <div className="settings-section-head">
          <h4>Appearance</h4>
          <p>Device-local preferences for this Lantor app.</p>
        </div>
        <fieldset className="settings-fieldset">
          <legend>Theme</legend>
          <div className="theme-choice-grid">
            {THEME_OPTIONS.map((option) => {
              const Icon = option.icon;
              return (
                <button
                  type="button"
                  key={option.value}
                  className={themePreference === option.value ? "selected" : ""}
                  aria-pressed={themePreference === option.value}
                  onClick={() => onThemePreferenceChange(option.value)}
                >
                  <Icon size={18} />
                  <span>
                    <strong>{option.label}</strong>
                    <small>{option.detail}</small>
                  </span>
                </button>
              );
            })}
          </div>
        </fieldset>
        <fieldset className="settings-fieldset">
          <legend>Text size</legend>
          <div className="chat-text-size-grid">
            {CHAT_TEXT_SIZE_OPTIONS.map((option) => (
              <button
                type="button"
                key={option.value}
                className={chatTextSize === option.value ? "selected" : ""}
                aria-pressed={chatTextSize === option.value}
                onClick={() => onChatTextSizeChange(option.value)}
              >
                <Type size={17} />
                <span>
                  <strong>{option.label}</strong>
                  <small>{option.detail}</small>
                </span>
              </button>
            ))}
          </div>
          <p className="settings-hint">Applies across messages, inputs, panels, and modals. Use Command +/- or Ctrl +/- to adjust without opening Settings. Command/Ctrl+0 resets.</p>
        </fieldset>
        <fieldset className="settings-fieldset">
          <legend>Diagnostics</legend>
          <section className="refresh-metrics-panel">
            <div className="refresh-metrics-head">
              <span className="refresh-metrics-icon" aria-hidden="true"><Activity size={17} /></span>
              <span>
                <strong>UI refresh metrics</strong>
                <small>
                  Counts and reasons for app-level refreshes in this session.
                  Keeps {refreshMetricsSummary?.retention_minutes ?? 60} min, max {refreshMetricsSummary?.max_events ?? 500} events.
                </small>
              </span>
              <button type="button" onClick={onRefreshMetricsReset} title="Reset refresh metrics" aria-label="Reset refresh metrics">
                <RotateCcw size={15} />
              </button>
            </div>
            <div className="refresh-metrics-stats">
              <span>
                <strong>{refreshMetricsSummary?.retained ?? 0}</strong>
                <small>Retained</small>
              </span>
              <span>
                <strong>{refreshMetricsSummary?.last_minute ?? 0}</strong>
                <small>Last minute</small>
              </span>
              <span>
                <strong>{refreshMetricsSummary?.rate_per_minute_1m ?? 0}/min</strong>
                <small>Rate</small>
              </span>
            </div>
            {topReasons.length > 0 && (
              <div className="refresh-metrics-reasons">
                {topReasons.map(([reason, count]) => (
                  <span key={reason}>
                    <b>{reason}</b>
                    <small>{count}</small>
                  </span>
                ))}
              </div>
            )}
            <ol className="refresh-metrics-events">
              {refreshMetricEvents.length === 0 ? (
                <li className="empty">No refresh events recorded yet.</li>
              ) : refreshMetricEvents.map((event) => (
                <li key={event.id}>
                  <time>{new Date(event.at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}</time>
                  <span>
                    <strong>{event.reason}</strong>
                    <small>
                      {event.kind} · {event.status}
                      {typeof event.duration_ms === "number" ? ` · ${event.duration_ms}ms` : ""}
                      {typeof event.batch_size === "number" ? ` · ${event.batch_size} item batch` : ""}
                    </small>
                  </span>
                </li>
              ))}
            </ol>
          </section>
        </fieldset>
      </section>
    </Modal>
  );
}
