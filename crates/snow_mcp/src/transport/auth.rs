use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolvedIdentity {
    pub actor: String,
    pub requester: String,
    pub client_id: Option<String>,
    pub scopes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActorClaimMapping {
    pub actor_claim: String,
    pub requester_claim: Option<String>,
}

impl Default for ActorClaimMapping {
    fn default() -> Self {
        Self {
            actor_claim: "preferred_username".to_string(),
            requester_claim: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct BearerAuthenticator {
    pub allowed_client_ids: Vec<String>,
    pub claim_mapping: ActorClaimMapping,
}
