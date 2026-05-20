use anyhow::{Context, Result, anyhow, ensure};
#[cfg(test)]
use blake3::Hasher;
use reqwest::Client as HttpClient;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

use crate::{KnowledgeArticle, KnowledgeEmbeddingCoverage, config::KbSemanticSearchConfig};

pub(crate) const HYBRID_RRF_K: usize = 60;

type EmbeddingFuture<'a> = Pin<Box<dyn Future<Output = Result<Vec<Vec<f32>>>> + 'a>>;

pub(crate) trait EmbeddingProvider {
    fn model(&self) -> &str;
    fn provider(&self) -> &str;
    fn embed<'a>(&'a self, inputs: &'a [String]) -> EmbeddingFuture<'a>;
}

#[derive(Debug)]
pub(crate) struct OllamaEmbeddingProvider {
    endpoint: String,
    model: String,
    timeout_seconds: u64,
    dimensions: Mutex<usize>,
}

impl OllamaEmbeddingProvider {
    pub(crate) fn new(config: &KbSemanticSearchConfig) -> Self {
        Self {
            endpoint: config.endpoint.trim_end_matches('/').to_string(),
            model: config.model.clone(),
            timeout_seconds: config.timeout_seconds.max(1),
            dimensions: Mutex::new(0),
        }
    }
}

impl EmbeddingProvider for OllamaEmbeddingProvider {
    fn model(&self) -> &str {
        &self.model
    }

    fn provider(&self) -> &str {
        "ollama"
    }

    fn embed<'a>(&'a self, inputs: &'a [String]) -> EmbeddingFuture<'a> {
        Box::pin(async move {
            if inputs.is_empty() {
                return Ok(Vec::new());
            }

            let endpoint = format!("{}/api/embed", self.endpoint);
            let client = HttpClient::builder()
                .timeout(Duration::from_secs(self.timeout_seconds))
                .build()
                .context("failed to build semantic embedding client")?;
            let response = client
                .post(endpoint)
                .json(&serde_json::json!({
                    "model": self.model,
                    "input": inputs,
                }))
                .send()
                .await
                .context("failed to request semantic embeddings")?
                .error_for_status()
                .context("semantic embedding provider returned an error")?;
            let body = response
                .json::<Value>()
                .await
                .context("failed to decode semantic embedding response")?;
            let Some(embeddings) = body.get("embeddings").and_then(Value::as_array) else {
                return Err(anyhow!(
                    "semantic embedding response missing embeddings array"
                ));
            };

            let mut out = Vec::with_capacity(embeddings.len());
            for embedding in embeddings {
                let Some(items) = embedding.as_array() else {
                    return Err(anyhow!("semantic embedding entry was not an array"));
                };
                let mut vector = Vec::with_capacity(items.len());
                for value in items {
                    let value = value
                        .as_f64()
                        .ok_or_else(|| anyhow!("semantic embedding value was not numeric"))?;
                    vector.push(value as f32);
                }
                let vector = normalize_unit_vector(&vector)?;
                let mut dimensions = self.dimensions.lock().expect("dimensions mutex");
                if *dimensions == 0 {
                    *dimensions = vector.len();
                } else {
                    ensure!(
                        *dimensions == vector.len(),
                        "semantic embedding dimensions changed from {} to {}",
                        *dimensions,
                        vector.len()
                    );
                }
                out.push(vector);
            }

            Ok(out)
        })
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct StubEmbeddingProvider {
    model: String,
    dimensions: usize,
}

#[cfg(test)]
impl StubEmbeddingProvider {
    pub(crate) fn new(model: impl Into<String>, dimensions: usize) -> Self {
        Self {
            model: model.into(),
            dimensions: dimensions.max(1),
        }
    }
}

#[cfg(test)]
impl EmbeddingProvider for StubEmbeddingProvider {
    fn model(&self) -> &str {
        &self.model
    }

    fn provider(&self) -> &str {
        "stub"
    }

    fn embed<'a>(
        &'a self,
        inputs: &'a [String],
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Vec<f32>>>> + 'a>> {
        Box::pin(async move {
            let mut out = Vec::with_capacity(inputs.len());
            for input in inputs {
                let mut vector = Vec::with_capacity(self.dimensions);
                for idx in 0..self.dimensions {
                    let mut hasher = Hasher::new();
                    hasher.update(self.model.as_bytes());
                    hasher.update(input.as_bytes());
                    hasher.update(&(idx as u64).to_le_bytes());
                    let hash = hasher.finalize();
                    let bytes = hash.as_bytes();
                    let raw = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                    vector.push((raw as f32 / u32::MAX as f32) * 2.0 - 1.0);
                }
                out.push(normalize_unit_vector(&vector)?);
            }
            Ok(out)
        })
    }
}

pub(crate) fn sanitize_semantic_text(value: &str, max_chars: usize) -> String {
    let trimmed = value
        .trim()
        .trim_matches(|ch| matches!(ch, '"' | '\'' | '`'));
    let stripped_html = strip_html_tags(trimmed);
    let mut out = String::with_capacity(stripped_html.len());
    let mut previous_space = false;

    for ch in stripped_html.chars() {
        if ch.is_whitespace() {
            if !previous_space {
                out.push(' ');
                previous_space = true;
            }
            continue;
        }
        out.push(ch);
        previous_space = false;
        if out.len() >= max_chars {
            break;
        }
    }

    out.trim().to_string()
}

pub(crate) fn render_embedding_input(
    article: &KnowledgeArticle,
    include_tags: bool,
) -> (String, KnowledgeEmbeddingCoverage) {
    let title = sanitize_semantic_text(&article.record.short_description, usize::MAX);
    let summary = sanitize_semantic_text(&article.record.description, usize::MAX);
    let body = sanitize_semantic_text(&article.content, usize::MAX);
    let coverage = if article.body_cached && !body.is_empty() {
        KnowledgeEmbeddingCoverage::FullText
    } else {
        KnowledgeEmbeddingCoverage::Metadata
    };
    let tags = if include_tags {
        article
            .sn_tags
            .iter()
            .chain(article.auto_tags.iter())
            .chain(article.user_tags.iter())
            .map(|tag| sanitize_semantic_text(tag, usize::MAX))
            .filter(|tag| !tag.is_empty())
            .collect::<Vec<_>>()
            .join(", ")
    } else {
        String::new()
    };

    let mut sections = vec![format!("title:\n{title}"), format!("summary:\n{summary}")];
    if include_tags && !tags.is_empty() {
        sections.push(format!("tags:\n{tags}"));
    }
    if matches!(coverage, KnowledgeEmbeddingCoverage::FullText) && !body.is_empty() {
        sections.push(format!("body:\n{body}"));
    }
    (sections.join("\n\n"), coverage)
}

pub(crate) fn content_hash(value: &str) -> String {
    blake3::hash(value.as_bytes()).to_hex().to_string()
}

pub(crate) fn normalize_title_match(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut prev_space = false;

    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            prev_space = false;
        } else if !prev_space {
            out.push(' ');
            prev_space = true;
        }
    }

    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub(crate) fn maybe_exact_kb_identifier(query: &str) -> Option<String> {
    let trimmed = query
        .trim()
        .trim_matches(|ch| matches!(ch, '"' | '\'' | '`'))
        .to_ascii_uppercase();
    if trimmed.len() < 3 || !trimmed.starts_with("KB") {
        return None;
    }
    if trimmed[2..].chars().all(|ch| ch.is_ascii_digit()) {
        Some(trimmed)
    } else {
        None
    }
}

pub(crate) fn normalize_unit_vector(values: &[f32]) -> Result<Vec<f32>> {
    ensure!(!values.is_empty(), "embedding vector must not be empty");
    let norm = values
        .iter()
        .map(|value| (*value as f64) * (*value as f64))
        .sum::<f64>()
        .sqrt();
    ensure!(
        norm.is_finite() && norm > 0.0,
        "embedding vector had zero norm"
    );
    Ok(values
        .iter()
        .map(|value| (*value as f64 / norm) as f32)
        .collect())
}

pub(crate) fn is_unit_length(values: &[f32]) -> bool {
    if values.is_empty() {
        return false;
    }
    let norm = values
        .iter()
        .map(|value| (*value as f64) * (*value as f64))
        .sum::<f64>()
        .sqrt();
    (norm - 1.0).abs() <= 1e-3
}

pub(crate) fn cosine_similarity(left: &[f32], right: &[f32]) -> Result<f32> {
    ensure!(left.len() == right.len(), "embedding dimension mismatch");
    ensure!(is_unit_length(left), "left vector was not unit-length");
    ensure!(is_unit_length(right), "right vector was not unit-length");
    Ok(left.iter().zip(right.iter()).map(|(a, b)| a * b).sum())
}

pub(crate) fn reciprocal_rank_fusion_score(rank: usize) -> f32 {
    1.0 / (HYBRID_RRF_K as f32 + rank as f32)
}

fn strip_html_tags(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut inside_tag = false;
    for ch in value.chars() {
        match ch {
            '<' => inside_tag = true,
            '>' => inside_tag = false,
            _ if !inside_tag => out.push(ch),
            _ => {}
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes_semantic_text() {
        assert_eq!(
            sanitize_semantic_text("  \"alpha   <b>beta</b>\"  ", 100),
            "alpha beta"
        );
    }

    #[test]
    fn detects_exact_kb_identifier() {
        assert_eq!(
            maybe_exact_kb_identifier("  kb0105015 "),
            Some("KB0105015".to_string())
        );
        assert_eq!(maybe_exact_kb_identifier("KB 0105015"), None);
    }

    #[test]
    fn normalizes_unit_vectors() {
        let vector = normalize_unit_vector(&[3.0, 4.0]).expect("unit vector");
        assert!(is_unit_length(&vector));
        assert!((cosine_similarity(&vector, &vector).expect("cosine") - 1.0).abs() < 1e-4);
    }
}
