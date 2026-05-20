import { useCallback, useLayoutEffect, useRef, type RefObject } from "react";

function cssPixelValue(value: string) {
  const parsed = Number.parseFloat(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : Number.POSITIVE_INFINITY;
}

export function useAutoGrowTextarea(textareaRef: RefObject<HTMLTextAreaElement | null>, value: string) {
  const animationFrameRef = useRef<number | null>(null);
  const lastObservedWidthRef = useRef<number | null>(null);

  const resizeTextarea = useCallback(() => {
    const textareaElement = textareaRef.current;
    if (!textareaElement) return;
    const computedStyle = window.getComputedStyle(textareaElement);
    const maxHeight = cssPixelValue(computedStyle.maxHeight);
    textareaElement.style.height = "auto";
    const nextHeight = Math.min(textareaElement.scrollHeight, maxHeight);
    textareaElement.style.height = `${nextHeight}px`;
    textareaElement.style.overflowY = textareaElement.scrollHeight > maxHeight ? "auto" : "hidden";
  }, [textareaRef]);

  const scheduleResize = useCallback(() => {
    if (animationFrameRef.current !== null) window.cancelAnimationFrame(animationFrameRef.current);
    animationFrameRef.current = window.requestAnimationFrame(() => {
      animationFrameRef.current = null;
      resizeTextarea();
    });
  }, [resizeTextarea]);

  useLayoutEffect(() => {
    resizeTextarea();
  }, [resizeTextarea, value]);

  useLayoutEffect(() => {
    const textareaElement = textareaRef.current;
    if (!textareaElement) return;
    const textarea: HTMLTextAreaElement = textareaElement;

    const observer = typeof ResizeObserver === "undefined"
      ? null
      : new ResizeObserver((entries) => {
        const observedWidth = entries[0]?.contentRect.width ?? textarea.clientWidth;
        if (lastObservedWidthRef.current !== null && Math.abs(observedWidth - lastObservedWidthRef.current) < 0.5) {
          return;
        }
        lastObservedWidthRef.current = observedWidth;
        scheduleResize();
      });
    observer?.observe(textarea);
    window.addEventListener("resize", scheduleResize);

    return () => {
      if (animationFrameRef.current !== null) window.cancelAnimationFrame(animationFrameRef.current);
      animationFrameRef.current = null;
      observer?.disconnect();
      window.removeEventListener("resize", scheduleResize);
    };
  }, [scheduleResize, textareaRef]);
}
