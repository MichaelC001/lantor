import { useState, type MouseEvent, type PointerEvent } from "react";
import { createMessagePreview } from "../message-preview";
import { MessageMarkdown } from "./MessageMarkdown";

type ExpandableMessageMarkdownProps = {
  body: string;
  previewLines?: number;
};

function isolateExpandEvent(event: MouseEvent<HTMLButtonElement> | PointerEvent<HTMLButtonElement>) {
  event.stopPropagation();
}

export function ExpandableMessageMarkdown({ body, previewLines = 8 }: ExpandableMessageMarkdownProps) {
  const [expanded, setExpanded] = useState(false);
  const preview = createMessagePreview(body, previewLines);

  if (expanded || !preview.truncated) {
    return <MessageMarkdown body={body} />;
  }

  return (
    <>
      <MessageMarkdown body={preview.body} />
      <button
        type="button"
        className="message-expand-toggle"
        aria-label="Show full message"
        title="Show full message"
        onPointerDown={isolateExpandEvent}
        onClick={(event) => {
          isolateExpandEvent(event);
          setExpanded(true);
        }}
      >
        …
      </button>
    </>
  );
}
