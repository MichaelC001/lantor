import { useEffect, useRef, type PointerEvent as ReactPointerEvent, type ReactNode } from "react";
import { X } from "lucide-react";
import { shouldDismissOnEscape } from "../escape-dismiss";

type ModalProps = {
  open: boolean;
  title: string;
  onClose: () => void;
  children: ReactNode;
  width?: number;
  closeOnBackdrop?: boolean;
  closeOnEscape?: boolean;
};

export function Modal({
  open,
  title,
  onClose,
  children,
  width = 480,
  closeOnBackdrop = true,
  closeOnEscape = true,
}: ModalProps) {
  const backdropDismissArmedRef = useRef(false);

  useEffect(() => {
    if (!open) return;
    function onKey(event: KeyboardEvent) {
      if (!closeOnEscape || !shouldDismissOnEscape(event)) return;
      event.preventDefault();
      onClose();
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [closeOnEscape, open, onClose]);

  if (!open) return null;

  function handleBackdropPointerDown(event: ReactPointerEvent<HTMLDivElement>) {
    if (!closeOnBackdrop) {
      backdropDismissArmedRef.current = false;
      return;
    }
    backdropDismissArmedRef.current = event.target === event.currentTarget;
  }

  function handleBackdropPointerUp(event: ReactPointerEvent<HTMLDivElement>) {
    const shouldClose = closeOnBackdrop
      && backdropDismissArmedRef.current
      && event.target === event.currentTarget;
    backdropDismissArmedRef.current = false;
    if (shouldClose) onClose();
  }

  return (
    <div
      className="modal-backdrop"
      onPointerDown={handleBackdropPointerDown}
      onPointerUp={handleBackdropPointerUp}
      onPointerCancel={() => {
        backdropDismissArmedRef.current = false;
      }}
    >
      <div
        className="modal-card"
        style={{ width }}
      >
        <header className="modal-head">
          <h3>{title}</h3>
          <button type="button" className="modal-close" onClick={onClose} aria-label="Close">
            <X size={18} />
          </button>
        </header>
        <div className="modal-body">{children}</div>
      </div>
    </div>
  );
}
