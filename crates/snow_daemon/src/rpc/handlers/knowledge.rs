use super::*;

pub(in crate::rpc) fn extract_knowledge_search_filters(
    params: &Value,
) -> Result<(String, snow_core::KnowledgeSearchFilters)> {
    let Value::Object(map) = params else {
        return Err(anyhow!("expected object params"));
    };

    let query = map
        .get("query")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("missing required field `query`"))?;
    let limit = map
        .get("limit")
        .and_then(Value::as_u64)
        .map(|value| value as usize);

    Ok((
        query,
        snow_core::KnowledgeSearchFilters {
            knowledge_base: map
                .get("knowledge_base")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            category: map
                .get("category")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            limit,
        },
    ))
}

pub(crate) fn extract_kb_semantic_search_filters(
    params: &Value,
) -> Result<(String, KnowledgeSemanticSearchFilters)> {
    let filters: KbSemanticSearchParams = serde_json::from_value(params.clone())?;
    if filters.query.trim().is_empty() {
        return Err(anyhow!("missing required field `query`"));
    }

    Ok((
        filters.query,
        KnowledgeSemanticSearchFilters {
            knowledge_base: filters.knowledge_base,
            category: filters.category,
            limit: filters.limit,
            mode: filters.mode,
            min_score_millis: filters.min_score_millis,
        },
    ))
}

#[derive(Debug, Clone, Deserialize, Default)]
pub(in crate::rpc) struct KbSemanticSearchParams {
    pub(in crate::rpc) query: String,
    #[serde(default)]
    pub(in crate::rpc) knowledge_base: Option<String>,
    #[serde(default)]
    pub(in crate::rpc) category: Option<String>,
    #[serde(default)]
    pub(in crate::rpc) limit: Option<usize>,
    #[serde(default)]
    pub(in crate::rpc) mode: snow_core::KnowledgeSearchMode,
    #[serde(default)]
    pub(in crate::rpc) min_score_millis: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub(in crate::rpc) struct ListKnowledgeArticlesParams {
    #[serde(default)]
    pub(in crate::rpc) knowledge_base_sys_id: Option<String>,
    #[serde(default)]
    pub(in crate::rpc) category_sys_id: Option<String>,
    #[serde(default)]
    pub(in crate::rpc) limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub(crate) struct KbSyncParams {
    #[serde(default)]
    pub(crate) full: bool,
    #[serde(default)]
    pub(crate) with_bodies: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub(crate) struct KbListTagsParams {
    #[serde(default)]
    pub(crate) layer: Option<String>,
    #[serde(default = "default_min_count")]
    pub(crate) min_count: usize,
}

pub(in crate::rpc) fn extract_list_knowledge_articles_params(
    params: &Value,
) -> Result<ListKnowledgeArticlesParams> {
    Ok(serde_json::from_value(params.clone())?)
}

pub(crate) fn extract_kb_sync_params(params: &Value) -> Result<KbSyncParams> {
    Ok(serde_json::from_value(params.clone())?)
}

pub(crate) fn extract_kb_list_tags_params(params: &Value) -> Result<KbListTagsParams> {
    let params: KbListTagsParams = serde_json::from_value(params.clone())?;
    if params.min_count == 0 {
        return Err(anyhow!("`min_count` must be at least 1"));
    }
    if let Some(layer) = params.layer.as_deref() {
        validate_kb_tag_filter(layer)?;
    }
    Ok(params)
}

pub(in crate::rpc) fn parse_search_scope(scope: Option<&str>) -> SearchScope {
    match scope.unwrap_or("all").trim().to_ascii_lowercase().as_str() {
        "knowledge" => SearchScope::Knowledge,
        "work_notes" => SearchScope::WorkNotes,
        _ => SearchScope::All,
    }
}

pub(in crate::rpc) fn default_min_count() -> usize {
    1
}

pub(in crate::rpc) fn validate_kb_tag_filter(layer: &str) -> Result<()> {
    match layer.trim().to_ascii_lowercase().as_str() {
        "all" | "sn" | "auto" | "user" => Ok(()),
        _ => Err(anyhow!("unsupported tag layer `{layer}`")),
    }
}

pub(crate) fn core_kb_tag_layer(layer: &str) -> Result<snow_core::KnowledgeTagLayer> {
    match layer.trim().to_ascii_lowercase().as_str() {
        "sn" => Ok(snow_core::KnowledgeTagLayer::Sn),
        "auto" => Ok(snow_core::KnowledgeTagLayer::Auto),
        "user" => Ok(snow_core::KnowledgeTagLayer::User),
        _ => Err(anyhow!("unsupported tag layer `{layer}`")),
    }
}

pub(in crate::rpc) async fn dispatch_knowledge(
    method: RpcMethod,
    id: Option<Value>,
    request: &JsonRpcRequest,
    state: &Arc<DaemonState>,
    transport: &DaemonTransport<'_>,
) -> JsonRpcResponse {
    match method {
        RpcMethod::GetKnowledgeArticle | RpcMethod::GetArticle => {
            match extract_number(&request.params) {
                Ok(number) => match state
                    .core
                    .get_knowledge_article_cached_or_fresh(&number)
                    .await
                {
                    Ok(Some(article)) => match transport.knowledge_article(&article) {
                        Ok(article_dto) => JsonRpcResponse::ok(
                            id,
                            json!({
                                "article": article_dto,
                                "markdown": render_knowledge_article(&article),
                            }),
                        ),
                        Err(err) => internal_error(id, err),
                    },
                    Ok(None) => {
                        JsonRpcResponse::error(id, -32004, "knowledge article not found", None)
                    }
                    Err(err) => internal_error(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::GetKnowledgeArticleFresh | RpcMethod::GetArticleFresh => {
            match extract_number(&request.params) {
                Ok(number) => match state.core.get_knowledge_article_fresh(&number).await {
                    Ok(Some(article)) => match transport.knowledge_article(&article) {
                        Ok(article_dto) => JsonRpcResponse::ok(
                            id,
                            json!({
                                "article": article_dto,
                                "markdown": render_knowledge_article(&article),
                            }),
                        ),
                        Err(err) => internal_error(id, err),
                    },
                    Ok(None) => {
                        JsonRpcResponse::error(id, -32004, "knowledge article not found", None)
                    }
                    Err(err) => internal_error(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::SearchKnowledge => match extract_knowledge_search_filters(&request.params) {
            Ok((query, filters)) => match state.core.search_knowledge(&query, filters).await {
                Ok(articles) => {
                    let mut article_dtos = Vec::with_capacity(articles.len());
                    for article in articles {
                        match transport.knowledge_article(&article) {
                            Ok(article) => article_dtos.push(article),
                            Err(err) => return internal_error(id, err),
                        }
                    }
                    JsonRpcResponse::ok(id, json!({ "articles": article_dtos }))
                }
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::KbSemanticSearch => match extract_kb_semantic_search_filters(&request.params) {
            Ok((query, filters)) => {
                match state.core.search_knowledge_semantic(&query, filters).await {
                    Ok(hits) => {
                        let mut hit_dtos = Vec::with_capacity(hits.len());
                        for hit in hits {
                            match transport.knowledge_search_hit(&hit) {
                                Ok(hit) => hit_dtos.push(hit),
                                Err(err) => return internal_error(id, err),
                            }
                        }
                        JsonRpcResponse::ok(id, json!({ "hits": hit_dtos }))
                    }
                    Err(err) => internal_error(id, err),
                }
            }
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::ListKnowledgeBases => match state.core.list_knowledge_bases() {
            Ok(bases) => JsonRpcResponse::ok(
                id,
                json!({
                    "bases": bases
                        .into_iter()
                        .map(|base| transport.knowledge_base_summary(base))
                        .collect::<Vec<_>>()
                }),
            ),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::ListCategories => match extract_string(&request.params, "knowledge_base_sys_id")
        {
            Ok(knowledge_base_sys_id) => match state.core.list_categories(&knowledge_base_sys_id) {
                Ok(categories) => JsonRpcResponse::ok(
                    id,
                    json!({
                        "categories": categories
                            .into_iter()
                            .map(|category| transport.knowledge_category_summary(category))
                            .collect::<Vec<_>>()
                    }),
                ),
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::ListKnowledgeArticles => {
            match extract_list_knowledge_articles_params(&request.params) {
                Ok(params) => match state
                    .core
                    .list_knowledge_articles(
                        params.knowledge_base_sys_id.as_deref(),
                        params.category_sys_id.as_deref(),
                        params.limit,
                    )
                    .await
                {
                    Ok(articles) => {
                        let mut article_dtos = Vec::with_capacity(articles.len());
                        for article in articles {
                            match transport.knowledge_article(&article) {
                                Ok(article) => article_dtos.push(article),
                                Err(err) => return internal_error(id, err),
                            }
                        }
                        JsonRpcResponse::ok(id, json!({ "articles": article_dtos }))
                    }
                    Err(err) => internal_error(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::KbSync => match extract_kb_sync_params(&request.params) {
            Ok(params) => match state
                .core
                .sync_knowledge(params.full, params.with_bodies)
                .await
            {
                Ok(sync) => JsonRpcResponse::ok(
                    id,
                    json!({ "sync": DaemonKnowledgeSyncOutcome::from(sync) }),
                ),
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::KbListTags => match extract_kb_list_tags_params(&request.params) {
            Ok(params) => {
                let layer = match params.layer.as_deref() {
                    Some("all") | None => None,
                    Some(layer) => match core_kb_tag_layer(layer) {
                        Ok(layer) => Some(layer),
                        Err(err) => return invalid_params(id, err),
                    },
                };
                match state.core.list_knowledge_tags(layer, params.min_count) {
                    Ok(tags) => JsonRpcResponse::ok(
                        id,
                        json!({
                            "tags": tags
                                .into_iter()
                                .map(DaemonKnowledgeTagSummary::from)
                                .collect::<Vec<_>>()
                        }),
                    ),
                    Err(err) => internal_error(id, err),
                }
            }
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::KbStatus => match state.core.knowledge_status() {
            Ok(status) => {
                JsonRpcResponse::ok(id, json!({ "status": DaemonKnowledgeStatus::from(status) }))
            }
            Err(err) => internal_error(id, err),
        },
        RpcMethod::KbSemanticStatus => match state.core.knowledge_semantic_status().await {
            Ok(status) => JsonRpcResponse::ok(
                id,
                json!({ "status": DaemonKnowledgeSemanticStatus::from(status) }),
            ),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::KbSemanticRebuild => {
            let full = request
                .params
                .get("full")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            match state.core.rebuild_knowledge_semantic_index(full).await {
                Ok(summary) => JsonRpcResponse::ok(
                    id,
                    json!({ "summary": DaemonSemanticIndexSummary::from(summary) }),
                ),
                Err(err) => internal_error(id, err),
            }
        }
        _ => unreachable!("method routed to the wrong RPC feature handler"),
    }
}
