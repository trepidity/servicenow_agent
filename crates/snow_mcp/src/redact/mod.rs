pub mod rules;

pub use rules::{RedactionRule, RedactionRuleSet, RuleBasedRedactor};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedactionVerdict {
    pub redacted: String,
    pub matched_rules: Vec<String>,
}

pub trait PhiRedactor {
    fn redact(&self, input: &str) -> RedactionVerdict;
    fn rules_version(&self) -> &str;
}
