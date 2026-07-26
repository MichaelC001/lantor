use serde::Deserialize;
use uuid::Uuid;

pub(crate) mod agents;
pub(crate) mod artifacts;
pub(crate) mod bootstrap;
pub(crate) mod channels;
pub(crate) mod inbox;
pub(crate) mod messages;
pub(crate) mod tasks;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AgentIdRequest {
    pub(crate) agent_id: Uuid,
}
