export type MessagePreview = {
  body: string;
  truncated: boolean;
};

export function createMessagePreview(text: string, lines = 8): MessagePreview {
  const split = text.trim().split("\n");
  return {
    body: split.slice(0, lines).join("\n"),
    truncated: split.length > lines,
  };
}
