use crate::redact::{PhiRedactor, RedactionVerdict};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedactionRule {
    pub name: String,
    pub marker: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedactionRuleSet {
    pub version: String,
    pub rules: Vec<RedactionRule>,
}

impl Default for RedactionRuleSet {
    fn default() -> Self {
        Self {
            version: "builtin-v1".to_string(),
            rules: vec![
                RedactionRule {
                    name: "ssn".to_string(),
                    marker: "ssn".to_string(),
                },
                RedactionRule {
                    name: "mrn".to_string(),
                    marker: "mrn".to_string(),
                },
                RedactionRule {
                    name: "dob".to_string(),
                    marker: "dob".to_string(),
                },
            ],
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuleBasedRedactor {
    rules: RedactionRuleSet,
}

impl Default for RuleBasedRedactor {
    fn default() -> Self {
        Self {
            rules: RedactionRuleSet::default(),
        }
    }
}

impl PhiRedactor for RuleBasedRedactor {
    fn redact(&self, input: &str) -> RedactionVerdict {
        let mut redacted = input.to_string();
        let mut matched = Vec::new();
        for rule in &self.rules.rules {
            if input.to_ascii_lowercase().contains(&rule.marker) {
                matched.push(rule.name.clone());
                redacted = redacted.replace(&rule.marker, "[REDACTED]");
                redacted = redacted.replace(&rule.marker.to_ascii_uppercase(), "[REDACTED]");
            }
        }
        RedactionVerdict {
            redacted,
            matched_rules: matched,
        }
    }

    fn rules_version(&self) -> &str {
        &self.rules.version
    }
}
