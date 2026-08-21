use super::*;

impl Store {
    pub(super) fn create_schema_objects(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS records (
                sys_id TEXT PRIMARY KEY,
                number TEXT NOT NULL,
                table_name TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                state TEXT,
                short_desc TEXT,
                description TEXT,
                assigned_to TEXT,
                parent_id TEXT,
                file_path TEXT,
                synced_at INTEGER NOT NULL,
                sys_updated_on INTEGER NOT NULL,
                etag TEXT,
                in_scope INTEGER NOT NULL DEFAULT 1 CHECK (in_scope IN (0, 1)),
                last_seen_at INTEGER NOT NULL,
                tombstoned_at INTEGER,
                pruned_at INTEGER,
                raw_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS "references" (
                sys_id TEXT PRIMARY KEY,
                table_name TEXT NOT NULL,
                display_name TEXT NOT NULL,
                extra_json TEXT NOT NULL DEFAULT '{}',
                synced_at INTEGER NOT NULL,
                expires_at INTEGER
            );

            CREATE TABLE IF NOT EXISTS relationships (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id TEXT NOT NULL,
                target_id TEXT NOT NULL,
                rel_type TEXT NOT NULL,
                field_name TEXT NOT NULL,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                UNIQUE(source_id, target_id, rel_type, field_name)
            );

            CREATE TABLE IF NOT EXISTS sync_state (
                resource_type TEXT PRIMARY KEY,
                last_full INTEGER,
                last_incr INTEGER,
                high_watermark INTEGER,
                cursor TEXT,
                filter_hash TEXT,
                updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
            );

            CREATE TABLE IF NOT EXISTS record_tags (
                record_sys_id TEXT NOT NULL,
                tag TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'derived',
                weight REAL NOT NULL DEFAULT 1.0,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                UNIQUE(record_sys_id, tag)
            );

            CREATE TABLE IF NOT EXISTS record_keywords (
                record_sys_id TEXT NOT NULL,
                keyword TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'derived',
                weight REAL NOT NULL DEFAULT 1.0,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                UNIQUE(record_sys_id, keyword)
            );

            CREATE TABLE IF NOT EXISTS record_aliases (
                record_sys_id TEXT NOT NULL,
                alias TEXT NOT NULL,
                kind TEXT NOT NULL DEFAULT 'derived',
                source TEXT NOT NULL DEFAULT 'derived',
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                UNIQUE(record_sys_id, alias)
            );

            CREATE TABLE IF NOT EXISTS knowledge_articles (
                record_sys_id TEXT PRIMARY KEY,
                number TEXT NOT NULL,
                title TEXT NOT NULL,
                workflow_state TEXT NOT NULL,
                knowledge_base_sys_id TEXT NOT NULL,
                knowledge_base_name TEXT NOT NULL,
                category_sys_id TEXT NOT NULL,
                category_name TEXT NOT NULL,
                author_sys_id TEXT,
                author_name TEXT,
                published_at TEXT,
                valid_to TEXT,
                article_type TEXT NOT NULL,
                sys_updated_on TEXT,
                sn_tags TEXT NOT NULL DEFAULT '[]',
                auto_tags TEXT NOT NULL DEFAULT '[]',
                user_tags TEXT NOT NULL DEFAULT '[]',
                body_cached INTEGER NOT NULL DEFAULT 0 CHECK (body_cached IN (0, 1))
            );

            CREATE VIRTUAL TABLE IF NOT EXISTS fts_records USING fts5(
                number,
                short_desc,
                description,
                work_notes,
                content,
                tag_tokens
            );

            CREATE TABLE IF NOT EXISTS kb_sync_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                last_full_at INTEGER,
                last_incr_at INTEGER,
                watermark_updated_at TEXT,
                watermark_sys_id TEXT,
                kb_sync_lock INTEGER
            );

            CREATE TABLE IF NOT EXISTS kb_term_stats (
                term TEXT PRIMARY KEY,
                doc_freq INTEGER NOT NULL CHECK (doc_freq >= 0)
            );

            CREATE TABLE IF NOT EXISTS kb_article_terms (
                record_sys_id TEXT PRIMARY KEY,
                terms_json TEXT NOT NULL DEFAULT '[]'
            );

            CREATE TABLE IF NOT EXISTS kb_local_file_state (
                record_sys_id TEXT PRIMARY KEY,
                file_path TEXT NOT NULL,
                modified_at_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS knowledge_article_embeddings (
                record_sys_id TEXT PRIMARY KEY
                    REFERENCES knowledge_articles(record_sys_id)
                    ON DELETE CASCADE,
                model TEXT NOT NULL,
                provider TEXT NOT NULL,
                dimensions INTEGER NOT NULL,
                coverage TEXT NOT NULL
                    CHECK (coverage IN ('metadata', 'full_text')),
                content_hash TEXT NOT NULL,
                vector_blob BLOB NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS business_applications (
                record_sys_id TEXT PRIMARY KEY
                    REFERENCES records(sys_id)
                    ON DELETE CASCADE,
                name TEXT NOT NULL,
                number TEXT,
                business_owner_sys_id TEXT,
                business_owner_name TEXT,
                is_owner_sys_id TEXT,
                is_owner_name TEXT,
                ci_owner_group_sys_id TEXT,
                ci_owner_group_name TEXT,
                primary_support_group_sys_id TEXT,
                primary_support_group_name TEXT,
                operational_status_value TEXT,
                operational_status_display TEXT,
                primary_portfolio_sys_id TEXT,
                primary_portfolio_name TEXT,
                primary_portfolio_table TEXT,
                attested_date TEXT,
                sys_updated_on TEXT,
                field_count INTEGER NOT NULL DEFAULT 0,
                reference_count INTEGER NOT NULL DEFAULT 0,
                unresolved_reference_count INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS business_application_fields (
                record_sys_id TEXT NOT NULL
                    REFERENCES records(sys_id)
                    ON DELETE CASCADE,
                field_name TEXT NOT NULL,
                field_label TEXT,
                field_type TEXT,
                value_text TEXT,
                display_value TEXT,
                value_number REAL,
                value_date TEXT,
                value_bool INTEGER CHECK (value_bool IN (0, 1)),
                reference_sys_id TEXT,
                reference_table TEXT,
                raw_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY(record_sys_id, field_name)
            );

            CREATE TABLE IF NOT EXISTS business_application_field_dictionary (
                table_name TEXT NOT NULL,
                field_name TEXT NOT NULL,
                field_label TEXT,
                field_type TEXT,
                reference_table TEXT,
                choice INTEGER NOT NULL DEFAULT 0 CHECK (choice IN (0, 1)),
                mandatory INTEGER NOT NULL DEFAULT 0 CHECK (mandatory IN (0, 1)),
                read_only INTEGER NOT NULL DEFAULT 0 CHECK (read_only IN (0, 1)),
                max_length INTEGER,
                active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
                synced_at INTEGER NOT NULL,
                raw_json TEXT NOT NULL DEFAULT '{}',
                PRIMARY KEY(table_name, field_name)
            );

            CREATE TABLE IF NOT EXISTS business_application_servers (
                ba_sys_id TEXT NOT NULL
                    REFERENCES records(sys_id)
                    ON DELETE CASCADE,
                server_sys_id TEXT NOT NULL
                    REFERENCES records(sys_id)
                    ON DELETE CASCADE,
                server_table TEXT NOT NULL,
                provenance TEXT NOT NULL,
                min_depth INTEGER NOT NULL CHECK (min_depth >= 0),
                paths_json TEXT NOT NULL DEFAULT '[]',
                discovered_at INTEGER NOT NULL,
                last_seen_at INTEGER NOT NULL,
                tombstoned_at INTEGER,
                PRIMARY KEY(ba_sys_id, server_sys_id, provenance)
            );

            CREATE TABLE IF NOT EXISTS business_application_server_inventory_health (
                ba_sys_id TEXT PRIMARY KEY
                    REFERENCES records(sys_id)
                    ON DELETE CASCADE,
                run_started_at INTEGER NOT NULL,
                run_completed_at INTEGER NOT NULL,
                service_membership_status TEXT NOT NULL
                    CHECK (service_membership_status IN (
                        'ok',
                        'not_attempted',
                        'acl_restricted',
                        'association_budget_exhausted',
                        'page_budget_exhausted'
                    )),
                relationship_status TEXT NOT NULL
                    CHECK (relationship_status IN (
                        'ok',
                        'depth_limited',
                        'edge_budget_exhausted',
                        'ci_budget_exhausted',
                        'acl_restricted',
                        'truncated'
                    )),
                inventory_status TEXT NOT NULL
                    CHECK (inventory_status IN (
                        'complete',
                        'service_membership_degraded',
                        'relationship_degraded',
                        'truncated',
                        'failed'
                    )),
                summary_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS primitive_objects (
                sys_id TEXT PRIMARY KEY,
                table_name TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                display_name TEXT NOT NULL,
                number TEXT,
                file_path TEXT,
                raw_json TEXT NOT NULL DEFAULT '{}',
                synced_at INTEGER NOT NULL,
                sys_updated_on TEXT,
                resolution_status TEXT NOT NULL
                    CHECK (resolution_status IN ('resolved','unresolved','unknown_table','not_found','acl_restricted','error')),
                last_error TEXT
            );

            CREATE TABLE IF NOT EXISTS primitive_object_fields (
                primitive_sys_id TEXT NOT NULL
                    REFERENCES primitive_objects(sys_id)
                    ON DELETE CASCADE,
                field_name TEXT NOT NULL,
                field_label TEXT,
                field_type TEXT,
                value_text TEXT,
                display_value TEXT,
                value_number REAL,
                value_date TEXT,
                value_bool INTEGER CHECK (value_bool IN (0, 1)),
                reference_sys_id TEXT,
                reference_table TEXT,
                raw_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY(primitive_sys_id, field_name)
            );

            CREATE TABLE IF NOT EXISTS cached_users (
                sys_id TEXT PRIMARY KEY,
                user_name TEXT,
                name TEXT,
                first_name TEXT,
                last_name TEXT,
                email TEXT,
                employee_number TEXT,
                active INTEGER CHECK (active IN (0, 1)),
                department TEXT,
                location TEXT,
                title TEXT,
                raw_json TEXT NOT NULL DEFAULT '{}',
                synced_at INTEGER NOT NULL,
                sys_updated_on TEXT
            );

            CREATE TABLE IF NOT EXISTS cached_user_queries (
                query_key TEXT PRIMARY KEY,
                result_sys_ids_json TEXT NOT NULL DEFAULT '[]',
                synced_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS catalog_products_complete (
                sys_id TEXT PRIMARY KEY,
                item_json TEXT NOT NULL,
                last_refreshed_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS catalog_products_narrowed (
                sys_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                short_description TEXT NOT NULL,
                item_json TEXT NOT NULL,
                last_refreshed_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_records_number ON records(number);
            CREATE INDEX IF NOT EXISTS idx_records_table_scope ON records(table_name, in_scope, number);
            CREATE INDEX IF NOT EXISTS idx_records_parent ON records(parent_id);
            CREATE INDEX IF NOT EXISTS idx_records_resource_type ON records(resource_type, in_scope);
            CREATE INDEX IF NOT EXISTS idx_records_updated ON records(sys_updated_on);
            CREATE INDEX IF NOT EXISTS idx_references_table ON "references"(table_name);
            CREATE INDEX IF NOT EXISTS idx_relationships_source ON relationships(source_id);
            CREATE INDEX IF NOT EXISTS idx_relationships_target ON relationships(target_id);
            CREATE INDEX IF NOT EXISTS idx_sync_state_updated ON sync_state(updated_at);
            CREATE INDEX IF NOT EXISTS idx_record_tags_tag ON record_tags(tag);
            CREATE INDEX IF NOT EXISTS idx_record_tags_record ON record_tags(record_sys_id);
            CREATE INDEX IF NOT EXISTS idx_record_keywords_keyword ON record_keywords(keyword);
            CREATE INDEX IF NOT EXISTS idx_record_keywords_record ON record_keywords(record_sys_id);
            CREATE INDEX IF NOT EXISTS idx_record_aliases_alias ON record_aliases(alias);
            CREATE INDEX IF NOT EXISTS idx_record_aliases_record ON record_aliases(record_sys_id);
            CREATE INDEX IF NOT EXISTS idx_knowledge_articles_base ON knowledge_articles(knowledge_base_sys_id, knowledge_base_name);
            CREATE INDEX IF NOT EXISTS idx_knowledge_articles_category ON knowledge_articles(knowledge_base_sys_id, category_sys_id, category_name);
            CREATE INDEX IF NOT EXISTS idx_knowledge_articles_number ON knowledge_articles(number);
            CREATE INDEX IF NOT EXISTS idx_kb_local_file_state_path ON kb_local_file_state(file_path);
            CREATE INDEX IF NOT EXISTS idx_kb_embeddings_model
                ON knowledge_article_embeddings(model, coverage);
            CREATE INDEX IF NOT EXISTS idx_ba_name ON business_applications(name);
            CREATE INDEX IF NOT EXISTS idx_ba_business_owner
                ON business_applications(business_owner_sys_id, business_owner_name);
            CREATE INDEX IF NOT EXISTS idx_ba_is_owner
                ON business_applications(is_owner_sys_id, is_owner_name);
            CREATE INDEX IF NOT EXISTS idx_ba_ci_owner_group
                ON business_applications(ci_owner_group_sys_id, ci_owner_group_name);
            CREATE INDEX IF NOT EXISTS idx_ba_support_group
                ON business_applications(primary_support_group_sys_id, primary_support_group_name);
            CREATE INDEX IF NOT EXISTS idx_ba_operational_status
                ON business_applications(operational_status_value, operational_status_display);
            CREATE INDEX IF NOT EXISTS idx_ba_portfolio
                ON business_applications(primary_portfolio_table, primary_portfolio_sys_id, primary_portfolio_name);
            CREATE INDEX IF NOT EXISTS idx_ba_attested_date ON business_applications(attested_date);
            CREATE INDEX IF NOT EXISTS idx_ba_fields_name_text
                ON business_application_fields(field_name, value_text);
            CREATE INDEX IF NOT EXISTS idx_ba_fields_name_display
                ON business_application_fields(field_name, display_value);
            CREATE INDEX IF NOT EXISTS idx_ba_fields_name_date
                ON business_application_fields(field_name, value_date);
            CREATE INDEX IF NOT EXISTS idx_ba_fields_name_number
                ON business_application_fields(field_name, value_number);
            CREATE INDEX IF NOT EXISTS idx_ba_fields_ref
                ON business_application_fields(field_name, reference_table, reference_sys_id);
            CREATE INDEX IF NOT EXISTS idx_ba_servers_ba
                ON business_application_servers(ba_sys_id, tombstoned_at, min_depth, server_sys_id);
            CREATE INDEX IF NOT EXISTS idx_ba_servers_server
                ON business_application_servers(server_sys_id, tombstoned_at, min_depth, ba_sys_id);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_ba_servers_one_live_pair
                ON business_application_servers(ba_sys_id, server_sys_id)
                WHERE tombstoned_at IS NULL;
            CREATE INDEX IF NOT EXISTS idx_kb_articles_updated
                ON knowledge_articles(sys_updated_on, record_sys_id);
            CREATE INDEX IF NOT EXISTS idx_primitive_objects_table
                ON primitive_objects(table_name, display_name);
            CREATE INDEX IF NOT EXISTS idx_primitive_fields_name_text
                ON primitive_object_fields(field_name, value_text);
            CREATE INDEX IF NOT EXISTS idx_primitive_fields_name_display
                ON primitive_object_fields(field_name, display_value);
            CREATE INDEX IF NOT EXISTS idx_primitive_fields_name_date
                ON primitive_object_fields(field_name, value_date);
            CREATE INDEX IF NOT EXISTS idx_primitive_fields_name_number
                ON primitive_object_fields(field_name, value_number);
            CREATE INDEX IF NOT EXISTS idx_primitive_fields_ref
                ON primitive_object_fields(field_name, reference_table, reference_sys_id);
            CREATE INDEX IF NOT EXISTS idx_cached_users_user_name
                ON cached_users(user_name);
            CREATE INDEX IF NOT EXISTS idx_cached_users_email
                ON cached_users(email);
            CREATE INDEX IF NOT EXISTS idx_cached_users_employee_number
                ON cached_users(employee_number);
            CREATE INDEX IF NOT EXISTS idx_cached_users_name
                ON cached_users(name);
            CREATE INDEX IF NOT EXISTS idx_cached_users_first_last
                ON cached_users(first_name, last_name);
            CREATE INDEX IF NOT EXISTS idx_cached_user_queries_expires
                ON cached_user_queries(expires_at);
            CREATE INDEX IF NOT EXISTS idx_catalog_products_narrowed_name
                ON catalog_products_narrowed(name, sys_id);
            "#,
        )?;
        self.conn
            .execute("INSERT OR IGNORE INTO kb_sync_state (id) VALUES (1)", [])?;
        Ok(())
    }
}
