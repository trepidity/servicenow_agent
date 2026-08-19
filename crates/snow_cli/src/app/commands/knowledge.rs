use super::super::*;

pub(crate) async fn cmd_knowledge(
    core: &SnowCore,
    number: Option<String>,
    fresh: bool,
    action: Option<KnowledgeCommand>,
) -> Result<(), SnowError> {
    if fresh && action.is_some() {
        return Err(SnowError::Api(
            "--fresh is only valid when showing a single knowledge article".to_string(),
        ));
    }

    match (number, action) {
        (Some(number), None) => cmd_show_knowledge_runtime(core, &number, fresh).await,
        (
            None,
            Some(KnowledgeCommand::Search {
                query,
                mode,
                knowledge_base,
                category,
                limit,
                min_score_millis,
            }),
        ) => match mode {
            KnowledgeSearchModeArg::Lexical => {
                if min_score_millis.is_some() {
                    return Err(SnowError::Api(
                        "--min-score-millis is only valid with --mode semantic or --mode hybrid"
                            .to_string(),
                    ));
                }
                let articles = core
                    .search_knowledge(
                        &query,
                        KnowledgeSearchFilters {
                            knowledge_base,
                            category,
                            limit,
                        },
                    )
                    .await?;
                if articles.is_empty() {
                    println!("No knowledge articles found.");
                    return Ok(());
                }

                for article in articles {
                    print_knowledge_article_summary(&article);
                    println!();
                }
                Ok(())
            }
            mode => {
                let hits = core
                    .search_knowledge_semantic(
                        &query,
                        KnowledgeSemanticSearchFilters {
                            knowledge_base,
                            category,
                            limit,
                            mode: mode.into(),
                            min_score_millis,
                        },
                    )
                    .await?;
                if hits.is_empty() {
                    println!("No knowledge articles found.");
                    return Ok(());
                }

                for hit in hits {
                    print_knowledge_search_hit(&hit);
                    println!();
                }
                Ok(())
            }
        },
        (None, Some(KnowledgeCommand::Bases)) => {
            let bases = core.list_knowledge_bases()?;
            if bases.is_empty() {
                println!("No knowledge bases found.");
                return Ok(());
            }

            for base in bases {
                print_knowledge_base_summary(&base);
            }
            Ok(())
        }
        (None, Some(KnowledgeCommand::Categories { knowledge_base })) => {
            let categories = core.list_categories(&knowledge_base)?;
            if categories.is_empty() {
                println!("No knowledge categories found.");
                return Ok(());
            }

            for category in categories {
                print_knowledge_category_summary(&category);
            }
            Ok(())
        }
        (None, Some(KnowledgeCommand::Sync { full, with_bodies })) => {
            let outcome = core.sync_knowledge(full, with_bodies).await?;
            if !outcome.accepted {
                return Err(SnowError::Api(
                    outcome
                        .details
                        .unwrap_or_else(|| "KB sync request was not accepted".to_string()),
                ));
            }
            print_knowledge_sync_outcome(&outcome);
            Ok(())
        }
        (None, Some(KnowledgeCommand::Tags { .. })) | (None, Some(KnowledgeCommand::Status)) => {
            unreachable!("handled before auth setup")
        }
        (
            None,
            Some(KnowledgeCommand::Semantic {
                action: KnowledgeSemanticCommand::Status,
            }),
        ) => {
            let status = core.knowledge_semantic_status().await?;
            print_knowledge_semantic_status(&status);
            Ok(())
        }
        (
            None,
            Some(KnowledgeCommand::Semantic {
                action: KnowledgeSemanticCommand::Rebuild { full },
            }),
        ) => {
            let summary = core.rebuild_knowledge_semantic_index(full).await?;
            print_knowledge_semantic_rebuild_summary(&summary);
            Ok(())
        }
        (Some(_), Some(_)) => Err(SnowError::Api(
            "knowledge article numbers and subcommands are mutually exclusive".to_string(),
        )),
        (None, None) => Err(SnowError::Api(
            "knowledge requires a number or a subcommand".to_string(),
        )),
    }
}

pub(crate) async fn cmd_show_knowledge_runtime(
    core: &SnowCore,
    number: &str,
    fresh: bool,
) -> Result<(), SnowError> {
    let article = if fresh {
        core.get_knowledge_article_fresh(number).await?
    } else {
        core.get_knowledge_article(number).await?
    };
    match article {
        Some(article) => print_knowledge_article(&article),
        None => println!("Knowledge article not found: {number}"),
    }
    Ok(())
}
