use chrono::{Duration, Utc};
use sha2::{Digest, Sha256};

use crate::domain::knowledge::EvidenceVerdict;
use crate::domain::primitives::Citation;

pub struct KbGroundedPlanner {
    pub freshness_days: u32,
}

impl Default for KbGroundedPlanner {
    fn default() -> Self {
        Self {
            freshness_days: 365,
        }
    }
}

impl KbGroundedPlanner {
    pub fn evaluate(
        &self,
        citations: Vec<Citation>,
        rendered_bodies: &[String],
    ) -> EvidenceVerdict {
        if let Some((article_number, pattern)) = InjectionScreen::find(rendered_bodies) {
            return EvidenceVerdict::InjectionDetected {
                article_numbers: vec![article_number],
                pattern,
            };
        }
        if citations.is_empty() {
            return EvidenceVerdict::Insufficient {
                reason: "no cited KB article".to_string(),
            };
        }
        let cutoff = Utc::now() - Duration::days(self.freshness_days as i64);
        let stale = citations
            .iter()
            .filter(|citation| citation.updated < cutoff)
            .map(|citation| citation.article_number.clone())
            .collect::<Vec<_>>();
        if !stale.is_empty() {
            return EvidenceVerdict::Stale {
                article_numbers: stale,
                age_days_max: self.freshness_days,
            };
        }
        EvidenceVerdict::Sufficient { citations }
    }
}

pub struct InjectionScreen;

impl InjectionScreen {
    pub fn find(bodies: &[String]) -> Option<(String, String)> {
        let patterns = [
            "ignore previous instructions",
            "developer message",
            "system prompt",
            "exfiltrate",
            "bypass policy",
        ];
        for (index, body) in bodies.iter().enumerate() {
            let normalized = body.to_ascii_lowercase();
            if let Some(pattern) = patterns
                .iter()
                .find(|pattern| normalized.contains(**pattern))
            {
                return Some((format!("body-{index}"), (*pattern).to_string()));
            }
        }
        None
    }
}

pub fn content_hash(body: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(body.as_bytes());
    hex::encode(hasher.finalize())
}
