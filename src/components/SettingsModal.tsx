import { Monitor, Moon, Sun } from "lucide-react";
import { Modal } from "./Modal";

export type ThemePreference = "auto" | "light" | "dark";

type SettingsModalProps = {
  open: boolean;
  themePreference: ThemePreference;
  onThemePreferenceChange: (value: ThemePreference) => void;
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

export function SettingsModal({
  open,
  themePreference,
  onThemePreferenceChange,
  onClose,
}: SettingsModalProps) {
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
      </section>
    </Modal>
  );
}
