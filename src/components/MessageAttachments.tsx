import { type MouseEvent, useEffect, useState } from "react";
import { FileText, X } from "lucide-react";
import { attachmentAssetUrl, isTauriRuntime, openExternalUrl } from "../apiClient";
import { MessageAttachment } from "../types";

type MessageAttachmentsProps = {
  attachments: MessageAttachment[];
};

type ImagePreview = {
  src: string;
  alt: string;
};

function formatBytes(value: number) {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / 1024 / 1024).toFixed(1)} MB`;
}

async function openStoredAttachment(event: MouseEvent<HTMLAnchorElement>, attachment: MessageAttachment) {
  if (attachment.local_url || !isTauriRuntime()) return;

  event.preventDefault();
  try {
    await openExternalUrl(attachment.storage_path);
  } catch (error) {
    console.error("Failed to open attachment", error);
  }
}

export function MessageAttachments({ attachments }: MessageAttachmentsProps) {
  const [imagePreview, setImagePreview] = useState<ImagePreview | null>(null);

  useEffect(() => {
    if (!imagePreview) return;
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") setImagePreview(null);
    }
    function handleHistoryNavigation() {
      setImagePreview(null);
    }
    window.addEventListener("keydown", handleKeyDown);
    window.addEventListener("popstate", handleHistoryNavigation);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
      window.removeEventListener("popstate", handleHistoryNavigation);
    };
  }, [imagePreview]);

  if (attachments.length === 0) return null;

  return (
    <>
      <div className="message-attachments">
        {attachments.map((attachment) => {
          const src = attachment.local_url ?? attachmentAssetUrl(attachment.storage_path, attachment.id);
          const isImage = attachment.mime_type.startsWith("image/");
          if (isImage) {
            return (
              <button
                key={attachment.id}
                type="button"
                className={`message-attachment image ${attachment.local_url ? "pending" : ""}`}
                aria-label={`Preview ${attachment.original_name}`}
                onPointerDown={(event) => event.stopPropagation()}
                onClick={(event) => {
                  event.stopPropagation();
                  setImagePreview({ src, alt: attachment.original_name });
                }}
              >
                <img src={src} alt="" loading="lazy" />
              </button>
            );
          }
          return (
            <a
              key={attachment.id}
              className={`message-attachment ${attachment.local_url ? "pending" : ""}`}
              href={src}
              target="_blank"
              rel="noreferrer"
              title={attachment.original_name}
              onPointerDown={(event) => event.stopPropagation()}
              onClick={(event) => {
                event.stopPropagation();
                void openStoredAttachment(event, attachment);
              }}
            >
              <span className="attachment-icon"><FileText size={18} /></span>
              <span className="attachment-meta">
                <span><FileText size={13} /> {attachment.original_name}</span>
                <small>{attachment.mime_type || "file"} · {formatBytes(attachment.size_bytes)}</small>
              </span>
            </a>
          );
        })}
      </div>
      {imagePreview && (
        <div
          className="attachment-lightbox"
          role="dialog"
          aria-modal="true"
          aria-label="Image preview"
          onPointerDown={(event) => event.stopPropagation()}
          onClick={(event) => event.stopPropagation()}
        >
          <button
            type="button"
            className="attachment-lightbox-backdrop"
            aria-label="Close image preview"
            onClick={() => setImagePreview(null)}
          />
          <button
            type="button"
            className="attachment-lightbox-close"
            aria-label="Close image preview"
            onClick={() => setImagePreview(null)}
          >
            <X size={18} />
          </button>
          <div className="attachment-lightbox-content">
            <img src={imagePreview.src} alt={imagePreview.alt} />
          </div>
        </div>
      )}
    </>
  );
}
