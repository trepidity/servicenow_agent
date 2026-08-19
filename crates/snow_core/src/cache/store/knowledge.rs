use super::*;

impl Store {
    pub fn replace_tags(&self, record_sys_id: &str, tags: &[TagRow]) -> Result<()> {
        self.conn.execute(
            "DELETE FROM record_tags WHERE record_sys_id = ?1",
            params![record_sys_id],
        )?;
        for tag in tags {
            self.conn.execute(
                r#"
                INSERT INTO record_tags (record_sys_id, tag, source, weight)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(record_sys_id, tag) DO UPDATE SET
                    source = excluded.source,
                    weight = excluded.weight
                "#,
                params![&tag.record_sys_id, &tag.tag, &tag.source, tag.weight],
            )?;
        }
        Ok(())
    }

    pub fn list_tags(&self, record_sys_id: &str) -> Result<Vec<TagRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT record_sys_id, tag, source, weight
            FROM record_tags
            WHERE record_sys_id = ?1
            ORDER BY tag
            "#,
        )?;
        let rows = stmt.query_map(params![record_sys_id], |row| {
            Ok(TagRow {
                record_sys_id: row.get(0)?,
                tag: row.get(1)?,
                source: row.get(2)?,
                weight: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn replace_keywords(&self, record_sys_id: &str, keywords: &[KeywordRow]) -> Result<()> {
        self.conn.execute(
            "DELETE FROM record_keywords WHERE record_sys_id = ?1",
            params![record_sys_id],
        )?;
        for keyword in keywords {
            self.conn.execute(
                r#"
                INSERT INTO record_keywords (record_sys_id, keyword, source, weight)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(record_sys_id, keyword) DO UPDATE SET
                    source = excluded.source,
                    weight = excluded.weight
                "#,
                params![
                    &keyword.record_sys_id,
                    &keyword.keyword,
                    &keyword.source,
                    keyword.weight
                ],
            )?;
        }
        Ok(())
    }

    pub fn list_keywords(&self, record_sys_id: &str) -> Result<Vec<KeywordRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT record_sys_id, keyword, source, weight
            FROM record_keywords
            WHERE record_sys_id = ?1
            ORDER BY keyword
            "#,
        )?;
        let rows = stmt.query_map(params![record_sys_id], |row| {
            Ok(KeywordRow {
                record_sys_id: row.get(0)?,
                keyword: row.get(1)?,
                source: row.get(2)?,
                weight: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn replace_aliases(&self, record_sys_id: &str, aliases: &[AliasRow]) -> Result<()> {
        self.conn.execute(
            "DELETE FROM record_aliases WHERE record_sys_id = ?1",
            params![record_sys_id],
        )?;
        for alias in aliases {
            self.conn.execute(
                r#"
                INSERT INTO record_aliases (record_sys_id, alias, kind, source)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(record_sys_id, alias) DO UPDATE SET
                    kind = excluded.kind,
                    source = excluded.source
                "#,
                params![
                    &alias.record_sys_id,
                    &alias.alias,
                    &alias.kind,
                    &alias.source
                ],
            )?;
        }
        Ok(())
    }

    pub fn list_aliases(&self, record_sys_id: &str) -> Result<Vec<AliasRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT record_sys_id, alias, kind, source
            FROM record_aliases
            WHERE record_sys_id = ?1
            ORDER BY alias
            "#,
        )?;
        let rows = stmt.query_map(params![record_sys_id], |row| {
            Ok(AliasRow {
                record_sys_id: row.get(0)?,
                alias: row.get(1)?,
                kind: row.get(2)?,
                source: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn upsert_knowledge_article(&self, article: &KnowledgeArticleRow) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO knowledge_articles (
                record_sys_id, number, title, workflow_state, knowledge_base_sys_id,
                knowledge_base_name, category_sys_id, category_name, author_sys_id,
                author_name, published_at, valid_to, article_type, sys_updated_on,
                sn_tags, auto_tags, user_tags, body_cached
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5,
                ?6, ?7, ?8, ?9,
                ?10, ?11, ?12, ?13, ?14,
                ?15, ?16, ?17, ?18
            )
            ON CONFLICT(record_sys_id) DO UPDATE SET
                number = excluded.number,
                title = excluded.title,
                workflow_state = excluded.workflow_state,
                knowledge_base_sys_id = excluded.knowledge_base_sys_id,
                knowledge_base_name = excluded.knowledge_base_name,
                category_sys_id = excluded.category_sys_id,
                category_name = excluded.category_name,
                author_sys_id = excluded.author_sys_id,
                author_name = excluded.author_name,
                published_at = excluded.published_at,
                valid_to = excluded.valid_to,
                article_type = excluded.article_type,
                sys_updated_on = excluded.sys_updated_on,
                sn_tags = excluded.sn_tags,
                auto_tags = excluded.auto_tags,
                user_tags = excluded.user_tags,
                body_cached = excluded.body_cached
            "#,
            params![
                &article.record_sys_id,
                &article.number,
                &article.title,
                &article.workflow_state,
                &article.knowledge_base_sys_id,
                &article.knowledge_base_name,
                &article.category_sys_id,
                &article.category_name,
                &article.author_sys_id,
                &article.author_name,
                &article.published_at,
                &article.valid_to,
                &article.article_type,
                &article.sys_updated_on,
                serde_json::to_string(&article.sn_tags)?,
                serde_json::to_string(&article.auto_tags)?,
                serde_json::to_string(&article.user_tags)?,
                bool_to_i64(article.body_cached),
            ],
        )?;
        Ok(())
    }

    pub fn get_knowledge_article(
        &self,
        record_sys_id: &str,
    ) -> Result<Option<KnowledgeArticleRow>> {
        self.conn
            .query_row(
                r#"
                SELECT record_sys_id, number, title, workflow_state, knowledge_base_sys_id,
                       knowledge_base_name, category_sys_id, category_name, author_sys_id,
                       author_name, published_at, valid_to, article_type, sys_updated_on,
                       sn_tags, auto_tags, user_tags, body_cached
                FROM knowledge_articles
                WHERE record_sys_id = ?1
                "#,
                params![record_sys_id],
                |row| {
                    Ok(KnowledgeArticleRow {
                        record_sys_id: row.get(0)?,
                        number: row.get(1)?,
                        title: row.get(2)?,
                        workflow_state: row.get(3)?,
                        knowledge_base_sys_id: row.get(4)?,
                        knowledge_base_name: row.get(5)?,
                        category_sys_id: row.get(6)?,
                        category_name: row.get(7)?,
                        author_sys_id: row.get(8)?,
                        author_name: row.get(9)?,
                        published_at: row.get(10)?,
                        valid_to: row.get(11)?,
                        article_type: row.get(12)?,
                        sys_updated_on: row.get(13)?,
                        sn_tags: parse_string_vec_column(row, 14)?,
                        auto_tags: parse_string_vec_column(row, 15)?,
                        user_tags: parse_string_vec_column(row, 16)?,
                        body_cached: i64_to_bool(row.get(17)?),
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_knowledge_bases(&self) -> Result<Vec<KnowledgeBaseSummaryRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT ka.knowledge_base_sys_id, ka.knowledge_base_name, COUNT(*)
            FROM knowledge_articles ka
            INNER JOIN records r ON r.sys_id = ka.record_sys_id
            WHERE r.in_scope = 1
            GROUP BY knowledge_base_sys_id, knowledge_base_name
            ORDER BY knowledge_base_name, knowledge_base_sys_id
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(KnowledgeBaseSummaryRow {
                knowledge_base_sys_id: row.get(0)?,
                knowledge_base_name: row.get(1)?,
                article_count: row.get::<_, i64>(2)? as usize,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn list_knowledge_categories(
        &self,
        knowledge_base_sys_id: &str,
    ) -> Result<Vec<KnowledgeCategorySummaryRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT ka.category_sys_id, ka.category_name, ka.knowledge_base_sys_id, COUNT(*)
            FROM knowledge_articles ka
            INNER JOIN records r ON r.sys_id = ka.record_sys_id
            WHERE ka.knowledge_base_sys_id = ?1
              AND r.in_scope = 1
            GROUP BY category_sys_id, category_name, knowledge_base_sys_id
            ORDER BY category_name, category_sys_id
            "#,
        )?;
        let rows = stmt.query_map(params![knowledge_base_sys_id], |row| {
            Ok(KnowledgeCategorySummaryRow {
                category_sys_id: row.get(0)?,
                category_name: row.get(1)?,
                knowledge_base_sys_id: row.get(2)?,
                article_count: row.get::<_, i64>(3)? as usize,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn count_knowledge_articles(&self) -> Result<usize> {
        self.conn
            .query_row(
                r#"
                SELECT COUNT(*)
                FROM knowledge_articles ka
                INNER JOIN records r ON r.sys_id = ka.record_sys_id
                WHERE r.in_scope = 1
                "#,
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count as usize)
            .map_err(Into::into)
    }

    pub fn count_knowledge_articles_with_cached_body(&self) -> Result<usize> {
        self.conn
            .query_row(
                r#"
                SELECT COUNT(*)
                FROM knowledge_articles ka
                INNER JOIN records r ON r.sys_id = ka.record_sys_id
                WHERE r.in_scope = 1
                  AND ka.body_cached = 1
                "#,
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count as usize)
            .map_err(Into::into)
    }

    pub fn upsert_knowledge_embedding(&self, row: &KnowledgeEmbeddingRow) -> Result<()> {
        let vector_blob = encode_embedding_vector(&row.vector)?;
        self.conn.execute(
            r#"
            INSERT INTO knowledge_article_embeddings (
                record_sys_id, model, provider, dimensions, coverage, content_hash,
                vector_blob, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(record_sys_id) DO UPDATE SET
                model = excluded.model,
                provider = excluded.provider,
                dimensions = excluded.dimensions,
                coverage = excluded.coverage,
                content_hash = excluded.content_hash,
                vector_blob = excluded.vector_blob,
                updated_at = excluded.updated_at
            "#,
            params![
                &row.record_sys_id,
                &row.model,
                &row.provider,
                row.dimensions as i64,
                coverage_to_str(row.coverage),
                &row.content_hash,
                vector_blob,
                to_ts(row.updated_at),
            ],
        )?;
        Ok(())
    }

    pub fn get_knowledge_embedding(
        &self,
        record_sys_id: &str,
    ) -> Result<Option<KnowledgeEmbeddingRow>> {
        self.conn
            .query_row(
                r#"
                SELECT record_sys_id, model, provider, dimensions, coverage, content_hash,
                       vector_blob, updated_at
                FROM knowledge_article_embeddings
                WHERE record_sys_id = ?1
                "#,
                params![record_sys_id],
                |row| {
                    let dimensions = row.get::<_, i64>(3)? as usize;
                    let blob: Vec<u8> = row.get(6)?;
                    Ok(KnowledgeEmbeddingRow {
                        record_sys_id: row.get(0)?,
                        model: row.get(1)?,
                        provider: row.get(2)?,
                        dimensions,
                        coverage: coverage_from_str(&row.get::<_, String>(4)?)
                            .map_err(to_sqlite_err)?,
                        content_hash: row.get(5)?,
                        vector: decode_embedding_vector(&blob, dimensions)
                            .map_err(to_sqlite_err)?,
                        updated_at: from_ts(row.get(7)?).map_err(to_sqlite_err)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_knowledge_embeddings(&self) -> Result<Vec<KnowledgeEmbeddingRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT record_sys_id, model, provider, dimensions, coverage, content_hash,
                   vector_blob, updated_at
            FROM knowledge_article_embeddings
            ORDER BY record_sys_id
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            let dimensions = row.get::<_, i64>(3)? as usize;
            let blob: Vec<u8> = row.get(6)?;
            Ok(KnowledgeEmbeddingRow {
                record_sys_id: row.get(0)?,
                model: row.get(1)?,
                provider: row.get(2)?,
                dimensions,
                coverage: coverage_from_str(&row.get::<_, String>(4)?).map_err(to_sqlite_err)?,
                content_hash: row.get(5)?,
                vector: decode_embedding_vector(&blob, dimensions).map_err(to_sqlite_err)?,
                updated_at: from_ts(row.get(7)?).map_err(to_sqlite_err)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn delete_knowledge_embedding(&self, record_sys_id: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM knowledge_article_embeddings WHERE record_sys_id = ?1",
            params![record_sys_id],
        )?;
        Ok(())
    }

    pub fn prune_orphan_knowledge_embeddings(&self) -> Result<usize> {
        let removed = self.conn.execute(
            r#"
            DELETE FROM knowledge_article_embeddings
            WHERE NOT EXISTS (
                SELECT 1
                FROM knowledge_articles ka
                WHERE ka.record_sys_id = knowledge_article_embeddings.record_sys_id
            )
            "#,
            [],
        )?;
        Ok(removed)
    }

    pub fn count_knowledge_embeddings_by_coverage(
        &self,
        model: &str,
        coverage: KnowledgeEmbeddingCoverage,
    ) -> Result<usize> {
        self.conn
            .query_row(
                r#"
                SELECT COUNT(*)
                FROM knowledge_article_embeddings
                WHERE model = ?1
                  AND coverage = ?2
                "#,
                params![model, coverage_to_str(coverage)],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count as usize)
            .map_err(Into::into)
    }

    pub fn count_orphan_knowledge_embeddings(&self) -> Result<usize> {
        self.conn
            .query_row(
                r#"
                SELECT COUNT(*)
                FROM knowledge_article_embeddings kae
                LEFT JOIN knowledge_articles ka
                  ON ka.record_sys_id = kae.record_sys_id
                WHERE ka.record_sys_id IS NULL
                "#,
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count as usize)
            .map_err(Into::into)
    }

    pub fn knowledge_semantic_meta(&self) -> Result<KnowledgeSemanticMeta> {
        let last_rebuild_at = self
            .get_meta_value("kb_semantic_last_rebuild_at")?
            .map(|value| {
                value
                    .parse::<i64>()
                    .map_err(|_| StoreError::InvalidSchemaVersion(value.clone()))
                    .and_then(from_ts)
            })
            .transpose()?;
        let last_error = self.get_meta_value("kb_semantic_last_error")?;
        Ok(KnowledgeSemanticMeta {
            last_rebuild_at,
            last_error,
        })
    }

    pub fn set_knowledge_semantic_meta(
        &self,
        last_rebuild_at: Option<DateTime<Utc>>,
        last_error: Option<&str>,
    ) -> Result<()> {
        self.set_meta_value(
            "kb_semantic_last_rebuild_at",
            last_rebuild_at
                .map(|value| value.timestamp().to_string())
                .as_deref(),
        )?;
        self.set_meta_value("kb_semantic_last_error", last_error)?;
        Ok(())
    }

    pub fn list_active_knowledge_index_rows(&self) -> Result<Vec<KnowledgeIndexRow>> {
        self.list_active_knowledge_index_rows_filtered(None)
    }

    pub fn list_active_knowledge_index_rows_for_base(
        &self,
        knowledge_base_sys_id: &str,
    ) -> Result<Vec<KnowledgeIndexRow>> {
        self.list_active_knowledge_index_rows_filtered(Some(knowledge_base_sys_id))
    }

    fn list_active_knowledge_index_rows_filtered(
        &self,
        knowledge_base_sys_id: Option<&str>,
    ) -> Result<Vec<KnowledgeIndexRow>> {
        let (sql, params_vec) = if let Some(knowledge_base_sys_id) = knowledge_base_sys_id {
            (
                r#"
                SELECT ka.record_sys_id, ka.number, ka.title, ka.knowledge_base_sys_id,
                       ka.knowledge_base_name, ka.category_sys_id, ka.category_name, r.file_path,
                       ka.sn_tags, ka.auto_tags, ka.user_tags
                FROM knowledge_articles ka
                INNER JOIN records r ON r.sys_id = ka.record_sys_id
                WHERE r.in_scope = 1
                  AND r.file_path IS NOT NULL
                  AND ka.knowledge_base_sys_id = ?1
                ORDER BY ka.knowledge_base_name, ka.category_name, ka.title, ka.number
                "#,
                vec![knowledge_base_sys_id.to_string()],
            )
        } else {
            (
                r#"
                SELECT ka.record_sys_id, ka.number, ka.title, ka.knowledge_base_sys_id,
                       ka.knowledge_base_name, ka.category_sys_id, ka.category_name, r.file_path,
                       ka.sn_tags, ka.auto_tags, ka.user_tags
                FROM knowledge_articles ka
                INNER JOIN records r ON r.sys_id = ka.record_sys_id
                WHERE r.in_scope = 1
                  AND r.file_path IS NOT NULL
                ORDER BY ka.knowledge_base_name, ka.category_name, ka.title, ka.number
                "#,
                Vec::new(),
            )
        };

        let mut stmt = self.conn.prepare(sql)?;
        if let Some(value) = params_vec.first() {
            let rows = stmt.query_map(params![value], |row| {
                Ok(KnowledgeIndexRow {
                    record_sys_id: row.get(0)?,
                    number: row.get(1)?,
                    title: row.get(2)?,
                    knowledge_base_sys_id: row.get(3)?,
                    knowledge_base_name: row.get(4)?,
                    category_sys_id: row.get(5)?,
                    category_name: row.get(6)?,
                    file_path: row.get(7)?,
                    sn_tags: parse_string_vec_column(row, 8)?,
                    auto_tags: parse_string_vec_column(row, 9)?,
                    user_tags: parse_string_vec_column(row, 10)?,
                })
            })?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
        } else {
            let rows = stmt.query_map([], |row| {
                Ok(KnowledgeIndexRow {
                    record_sys_id: row.get(0)?,
                    number: row.get(1)?,
                    title: row.get(2)?,
                    knowledge_base_sys_id: row.get(3)?,
                    knowledge_base_name: row.get(4)?,
                    category_sys_id: row.get(5)?,
                    category_name: row.get(6)?,
                    file_path: row.get(7)?,
                    sn_tags: parse_string_vec_column(row, 8)?,
                    auto_tags: parse_string_vec_column(row, 9)?,
                    user_tags: parse_string_vec_column(row, 10)?,
                })
            })?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
        }
    }

    pub fn list_active_knowledge_local_scan_rows(&self) -> Result<Vec<KnowledgeLocalScanRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT r.sys_id, r.number, r.file_path, l.modified_at_ms
            FROM records r
            LEFT JOIN kb_local_file_state l ON l.record_sys_id = r.sys_id
            WHERE r.in_scope = 1
              AND r.resource_type = 'kb_knowledge'
              AND r.file_path IS NOT NULL
            ORDER BY r.number
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(KnowledgeLocalScanRow {
                record_sys_id: row.get(0)?,
                number: row.get(1)?,
                file_path: row.get(2)?,
                modified_at_ms: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn update_knowledge_local_state(
        &self,
        record_sys_id: &str,
        user_tags: &[String],
        body_cached: bool,
    ) -> Result<()> {
        self.conn.execute(
            r#"
            UPDATE knowledge_articles
            SET user_tags = ?2,
                body_cached = ?3
            WHERE record_sys_id = ?1
            "#,
            params![
                record_sys_id,
                serde_json::to_string(user_tags)?,
                bool_to_i64(body_cached),
            ],
        )?;
        Ok(())
    }

    pub fn load_kb_term_stats(&self) -> Result<HashMap<String, usize>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT term, doc_freq
            FROM kb_term_stats
            ORDER BY term
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as usize))
        })?;
        let mut stats = HashMap::new();
        for row in rows {
            let (term, doc_freq) = row?;
            stats.insert(term, doc_freq);
        }
        Ok(stats)
    }

    pub fn replace_kb_term_stats(&self, stats: &HashMap<String, usize>) -> Result<()> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            self.conn.execute("DELETE FROM kb_term_stats", [])?;
            for (term, doc_freq) in stats {
                self.conn.execute(
                    r#"
                    INSERT INTO kb_term_stats (term, doc_freq)
                    VALUES (?1, ?2)
                    "#,
                    params![term, *doc_freq as i64],
                )?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn get_kb_article_terms(&self, record_sys_id: &str) -> Result<Vec<String>> {
        self.conn
            .query_row(
                r#"
                SELECT terms_json
                FROM kb_article_terms
                WHERE record_sys_id = ?1
                "#,
                params![record_sys_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|raw| serde_json::from_str::<Vec<String>>(&raw).map_err(Into::into))
            .transpose()
            .map(|terms| terms.unwrap_or_default())
    }

    pub fn replace_all_kb_article_terms(&self, entries: &[(String, Vec<String>)]) -> Result<()> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            self.conn.execute("DELETE FROM kb_article_terms", [])?;
            for (record_sys_id, terms) in dedupe_kb_term_entries(entries) {
                self.conn.execute(
                    r#"
                    INSERT INTO kb_article_terms (record_sys_id, terms_json)
                    VALUES (?1, ?2)
                    "#,
                    params![record_sys_id, serde_json::to_string(&terms)?],
                )?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn replace_kb_article_terms_entries(
        &self,
        entries: &[(String, Vec<String>)],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            for (record_sys_id, terms) in dedupe_kb_term_entries(entries) {
                self.conn.execute(
                    r#"
                    INSERT INTO kb_article_terms (record_sys_id, terms_json)
                    VALUES (?1, ?2)
                    ON CONFLICT(record_sys_id) DO UPDATE SET
                        terms_json = excluded.terms_json
                    "#,
                    params![record_sys_id, serde_json::to_string(&terms)?],
                )?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn delete_kb_article_terms(&self, record_sys_ids: &[String]) -> Result<()> {
        if record_sys_ids.is_empty() {
            return Ok(());
        }
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            for record_sys_id in record_sys_ids {
                self.conn.execute(
                    "DELETE FROM kb_article_terms WHERE record_sys_id = ?1",
                    params![record_sys_id],
                )?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn upsert_kb_local_file_states(&self, entries: &[(String, String, i64)]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            for (record_sys_id, file_path, modified_at_ms) in entries {
                self.conn.execute(
                    r#"
                    INSERT INTO kb_local_file_state (record_sys_id, file_path, modified_at_ms)
                    VALUES (?1, ?2, ?3)
                    ON CONFLICT(record_sys_id) DO UPDATE SET
                        file_path = excluded.file_path,
                        modified_at_ms = excluded.modified_at_ms
                    "#,
                    params![record_sys_id, file_path, modified_at_ms],
                )?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn get_kb_sync_state(&self) -> Result<KbSyncStateRow> {
        self.conn
            .query_row(
                r#"
                SELECT last_full_at, last_incr_at, watermark_updated_at, watermark_sys_id, kb_sync_lock
                FROM kb_sync_state
                WHERE id = 1
                "#,
                [],
                |row| {
                    Ok(KbSyncStateRow {
                        last_full_at: row
                            .get::<_, Option<i64>>(0)?
                            .map(from_ts)
                            .transpose()
                            .map_err(to_sqlite_err)?,
                        last_incr_at: row
                            .get::<_, Option<i64>>(1)?
                            .map(from_ts)
                            .transpose()
                            .map_err(to_sqlite_err)?,
                        watermark_updated_at: row.get(2)?,
                        watermark_sys_id: row.get(3)?,
                        kb_sync_lock: row.get(4)?,
                    })
                },
            )
            .map_err(Into::into)
    }

    pub fn set_kb_sync_state(&self, row: &KbSyncStateRow) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO kb_sync_state (
                id, last_full_at, last_incr_at, watermark_updated_at, watermark_sys_id, kb_sync_lock
            ) VALUES (1, ?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(id) DO UPDATE SET
                last_full_at = excluded.last_full_at,
                last_incr_at = excluded.last_incr_at,
                watermark_updated_at = excluded.watermark_updated_at,
                watermark_sys_id = excluded.watermark_sys_id,
                kb_sync_lock = excluded.kb_sync_lock
            "#,
            params![
                opt_ts(row.last_full_at),
                opt_ts(row.last_incr_at),
                &row.watermark_updated_at,
                &row.watermark_sys_id,
                &row.kb_sync_lock,
            ],
        )?;
        Ok(())
    }

    pub fn acquire_kb_sync_lock(&self, now_ms: i64, stale_after_ms: i64) -> Result<bool> {
        self.conn
            .execute("INSERT OR IGNORE INTO kb_sync_state (id) VALUES (1)", [])?;
        let changed = self.conn.execute(
            r#"
            UPDATE kb_sync_state
            SET kb_sync_lock = ?1
            WHERE id = 1
              AND (kb_sync_lock IS NULL OR kb_sync_lock < ?2)
            "#,
            params![now_ms, now_ms - stale_after_ms],
        )?;
        Ok(changed > 0)
    }

    pub fn release_kb_sync_lock(&self) -> Result<()> {
        self.conn.execute(
            "UPDATE kb_sync_state SET kb_sync_lock = NULL WHERE id = 1",
            [],
        )?;
        Ok(())
    }

    pub fn list_knowledge_tags(
        &self,
        layer: &str,
        min_count: usize,
    ) -> Result<Vec<KnowledgeTagCountRow>> {
        let (sql, params_vec) = match layer {
            "sn" => (
                r#"
                SELECT tag, 'sn' AS layer, COUNT(*) AS article_count
                FROM (
                    SELECT ka.record_sys_id, LOWER(TRIM(json_each.value)) AS tag
                    FROM knowledge_articles ka
                    INNER JOIN records r ON r.sys_id = ka.record_sys_id
                    INNER JOIN json_each(ka.sn_tags)
                    WHERE r.in_scope = 1
                )
                WHERE tag <> ''
                GROUP BY tag
                HAVING COUNT(*) >= ?1
                ORDER BY article_count DESC, tag ASC
                "#,
                vec![min_count as i64],
            ),
            "auto" => (
                r#"
                SELECT tag, 'auto' AS layer, COUNT(*) AS article_count
                FROM (
                    SELECT ka.record_sys_id, LOWER(TRIM(json_each.value)) AS tag
                    FROM knowledge_articles ka
                    INNER JOIN records r ON r.sys_id = ka.record_sys_id
                    INNER JOIN json_each(ka.auto_tags)
                    WHERE r.in_scope = 1
                )
                WHERE tag <> ''
                GROUP BY tag
                HAVING COUNT(*) >= ?1
                ORDER BY article_count DESC, tag ASC
                "#,
                vec![min_count as i64],
            ),
            "user" => (
                r#"
                SELECT tag, 'user' AS layer, COUNT(*) AS article_count
                FROM (
                    SELECT ka.record_sys_id, LOWER(TRIM(json_each.value)) AS tag
                    FROM knowledge_articles ka
                    INNER JOIN records r ON r.sys_id = ka.record_sys_id
                    INNER JOIN json_each(ka.user_tags)
                    WHERE r.in_scope = 1
                )
                WHERE tag <> ''
                GROUP BY tag
                HAVING COUNT(*) >= ?1
                ORDER BY article_count DESC, tag ASC
                "#,
                vec![min_count as i64],
            ),
            _ => (
                r#"
                SELECT tag, 'all' AS layer, COUNT(*) AS article_count
                FROM (
                    SELECT ka.record_sys_id, LOWER(TRIM(json_each.value)) AS tag
                    FROM knowledge_articles ka
                    INNER JOIN records r ON r.sys_id = ka.record_sys_id
                    INNER JOIN json_each(ka.sn_tags)
                    WHERE r.in_scope = 1
                    UNION
                    SELECT ka.record_sys_id, LOWER(TRIM(json_each.value)) AS tag
                    FROM knowledge_articles ka
                    INNER JOIN records r ON r.sys_id = ka.record_sys_id
                    INNER JOIN json_each(ka.auto_tags)
                    WHERE r.in_scope = 1
                    UNION
                    SELECT ka.record_sys_id, LOWER(TRIM(json_each.value)) AS tag
                    FROM knowledge_articles ka
                    INNER JOIN records r ON r.sys_id = ka.record_sys_id
                    INNER JOIN json_each(ka.user_tags)
                    WHERE r.in_scope = 1
                )
                WHERE tag <> ''
                GROUP BY tag
                HAVING COUNT(*) >= ?1
                ORDER BY article_count DESC, tag ASC
                "#,
                vec![min_count as i64],
            ),
        };

        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt.query_map(params![params_vec[0]], |row| {
            Ok(KnowledgeTagCountRow {
                tag: row.get(0)?,
                layer: row.get(1)?,
                article_count: row.get::<_, i64>(2)? as usize,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn find_record_sys_ids_by_tag(&self, tag: &str, limit: usize) -> Result<Vec<String>> {
        self.find_record_sys_ids_by_enrichment("record_tags", "tag", tag, limit)
    }

    pub fn find_record_sys_ids_by_keyword(
        &self,
        keyword: &str,
        limit: usize,
    ) -> Result<Vec<String>> {
        self.find_record_sys_ids_by_enrichment("record_keywords", "keyword", keyword, limit)
    }

    pub fn find_record_sys_ids_by_alias(&self, alias: &str, limit: usize) -> Result<Vec<String>> {
        self.find_record_sys_ids_by_enrichment("record_aliases", "alias", alias, limit)
    }

    fn find_record_sys_ids_by_enrichment(
        &self,
        table: &str,
        column: &str,
        value: &str,
        limit: usize,
    ) -> Result<Vec<String>> {
        let sql = format!(
            r#"
            SELECT DISTINCT records.sys_id
            FROM {table}
            INNER JOIN records ON records.sys_id = {table}.record_sys_id
            WHERE {table}.{column} = ?1
              AND records.in_scope = 1
            ORDER BY records.number, records.sys_id
            LIMIT ?2
            "#
        );

        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params![value, limit as i64], |row| row.get::<_, String>(0))?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }
}
