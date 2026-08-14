export type EscapeDismissEvent = {
  key: string;
  defaultPrevented: boolean;
  isComposing: boolean;
  keyCode: number;
  repeat: boolean;
};

export function shouldDismissOnEscape(event: EscapeDismissEvent) {
  return event.key === "Escape"
    && !event.defaultPrevented
    && !event.isComposing
    && event.keyCode !== 229
    && !event.repeat;
}
