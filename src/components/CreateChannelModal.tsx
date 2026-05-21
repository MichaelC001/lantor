import { Check, UserPlus } from "lucide-react";
import { Agent } from "../types";
import { AgentAvatar } from "./AgentAvatar";
import { Modal } from "./Modal";

type CreateChannelModalProps = {
  open: boolean;
  channelName: string;
  nameError?: string | null;
  agents: Agent[];
  selectedAgentIds: Set<string>;
  onChange: (value: string) => void;
  onToggleAgent: (agentId: string, member: boolean) => void;
  onCreateAgent: () => void;
  onCancel: () => void;
  onSubmit: () => void;
};

export function CreateChannelModal({
  open,
  channelName,
  nameError,
  agents,
  selectedAgentIds,
  onChange,
  onToggleAgent,
  onCreateAgent,
  onCancel,
  onSubmit,
}: CreateChannelModalProps) {
  const selectedCount = agents.filter((agent) => selectedAgentIds.has(agent.id)).length;

  return (
    <Modal
      open={open}
      title="Create Channel"
      onClose={onCancel}
      width={560}
      closeOnBackdrop={false}
      closeOnEscape={false}
    >
      <div className="modal-form">
        <label>
          <span>Channel name</span>
          <input
            autoFocus
            autoCapitalize="none"
            autoComplete="new-password"
            autoCorrect="off"
            name="lantor-create-channel-slug"
            spellCheck={false}
            value={channelName}
            aria-invalid={nameError ? true : undefined}
            aria-describedby={nameError ? "create-channel-name-error" : undefined}
            onChange={(event) => onChange(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Enter") onSubmit();
            }}
            placeholder="lantor"
          />
          {nameError && (
            <p id="create-channel-name-error" className="modal-field-error">
              {nameError}
            </p>
          )}
        </label>
        <div className="modal-field">
          <span className="modal-field-label">Add agents</span>
          <div className="channel-agent-modal-intro">
            <span className="channel-agent-modal-icon" aria-hidden="true">
              <UserPlus size={18} />
            </span>
            <div>
              <strong>{selectedCount > 0 ? `${selectedCount} selected` : "Bring agents into this channel"}</strong>
              <p>{agents.length > 0 ? "Click Add next to an agent below to include them when this channel is created." : "Create an agent first, then add it to this channel."}</p>
            </div>
          </div>
          {agents.length === 0 ? (
            <div className="channel-agent-empty-cta">
              <p>No agents yet.</p>
              <button type="button" onClick={onCreateAgent}>
                <UserPlus size={16} />
                <span>Create first agent</span>
              </button>
            </div>
          ) : (
            <div className="member-editor modal-member-editor channel-agent-picker">
              {agents.map((agent) => {
                const isMember = selectedAgentIds.has(agent.id);
                return (
                  <label key={agent.id} className={`channel-agent-option ${isMember ? "selected" : ""}`}>
                    <input
                      className="channel-agent-checkbox"
                      type="checkbox"
                      checked={isMember}
                      onChange={(event) => onToggleAgent(agent.id, event.target.checked)}
                      aria-label={`${isMember ? "Remove" : "Add"} @${agent.handle}`}
                    />
                    <div className="channel-agent-profile">
                      <AgentAvatar agent={agent} size="sm" title={`@${agent.handle}`} />
                      <div className="agent-pick-row">
                        <strong>{agent.display_name}</strong>
                        <small>@{agent.handle}</small>
                      </div>
                    </div>
                    <span className={`channel-agent-option-state ${isMember ? "selected" : ""}`}>
                      {isMember ? <Check size={14} /> : <UserPlus size={14} />}
                      {isMember ? "Added" : "Add"}
                    </span>
                  </label>
                );
              })}
            </div>
          )}
        </div>
        <div className="modal-actions">
          <button onClick={onCancel}>Cancel</button>
          <button className="primary" disabled={!channelName.trim() || Boolean(nameError)} onClick={onSubmit}>Create</button>
        </div>
      </div>
    </Modal>
  );
}
