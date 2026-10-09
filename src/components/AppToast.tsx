import { useSyncExternalStore } from "react";
import { createPortal } from "react-dom";
import { activeDialogPanel, subscribeDialogLayers } from "../dialog-layers";

export type AppToastAction = { label: string; onClick: () => void };

export function AppToast({ message, kind = "error", onDismiss, className = "", action }: {
  message: string; kind?: "error" | "success"; onDismiss: () => void; className?: string; action?: AppToastAction;
}) {
  const dialog = useSyncExternalStore(subscribeDialogLayers, activeDialogPanel, () => null);
  return createPortal(<div className={`app-toast ${kind} ${className}`} role={kind === "error" ? "alert" : "status"}>
    <span>{message}</span>
    {action && <button type="button" className="app-toast-action" onClick={action.onClick}>{action.label}</button>}
    <button type="button" onClick={onDismiss} aria-label="Dismiss notification">Dismiss</button>
  </div>, dialog ?? document.body);
}
