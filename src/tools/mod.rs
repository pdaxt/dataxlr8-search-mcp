use dataxlr8_mcp_core::Database;
use rmcp::model::*;
use rmcp::service::{RequestContext, RoleServer};
use rmcp::ServerHandler;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info, warn};

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_LIMIT: i64 = 50;
const DEFAULT_OFFSET: i64 = 0;
const MAX_LIMIT: i64 = 200;
const MAX_QUERY_LEN: usize = 500;

// ============================================================================
// Data types
// ============================================================================

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct SavedSearch {
    pub id: String,
    pub name: String,
    pub query_type: String,
    pub query_params: serde_json::Value,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct SearchLogEntry {
    pub id: String,
    pub query: String,
    pub result_count: i32,
    pub searched_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize)]
pub struct SearchResult {
    pub source: String,
    pub id: String,
    pub title: String,
    pub snippet: String,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize)]
pub struct SearchResponse {
    pub query: String,
    pub total: usize,
    pub limit: i64,
    pub offset: i64,
    pub results: Vec<SearchResult>,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct SearchStat {
    pub query: String,
    pub search_count: i64,
    pub last_searched: chrono::DateTime<chrono::Utc>,
}

// ============================================================================
// Tool schema helpers
// ============================================================================

fn make_schema(
    properties: serde_json::Value,
    required: Vec<&str>,
) -> Arc<serde_json::Map<String, serde_json::Value>> {
    let mut m = serde_json::Map::new();
    m.insert(
        "type".to_string(),
        serde_json::Value::String("object".to_string()),
    );
    m.insert("properties".to_string(), properties);
    if !required.is_empty() {
        m.insert(
            "required".to_string(),
            serde_json::Value::Array(
                required
                    .into_iter()
                    .map(|s| serde_json::Value::String(s.to_string()))
                    .collect(),
            ),
        );
    }
    Arc::new(m)
}

fn empty_schema() -> Arc<serde_json::Map<String, serde_json::Value>> {
    let mut m = serde_json::Map::new();
    m.insert(
        "type".to_string(),
        serde_json::Value::String("object".to_string()),
    );
    Arc::new(m)
}

fn pagination_props() -> serde_json::Value {
    serde_json::json!({
        "limit": { "type": "integer", "description": "Max results (default 50, max 200)" },
        "offset": { "type": "integer", "description": "Offset for pagination (default 0)" }
    })
}

/// Merge pagination properties into an existing properties object
fn with_pagination(mut props: serde_json::Value) -> serde_json::Value {
    if let Some(obj) = props.as_object_mut() {
        let pag = pagination_props();
        if let Some(pag_obj) = pag.as_object() {
            for (k, v) in pag_obj {
                obj.insert(k.clone(), v.clone());
            }
        }
    }
    props
}

fn build_tools() -> Vec<Tool> {
    vec![
        Tool {
            name: "search_all".into(),
            title: None,
            description: Some(
                "Search across contacts, deals, notes, emails by keyword. Returns ranked results with source."
                    .into(),
            ),
            input_schema: make_schema(
                with_pagination(serde_json::json!({
                    "query": { "type": "string", "description": "Search keyword (required, max 500 chars)" }
                })),
                vec!["query"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "search_contacts".into(),
            title: None,
            description: Some(
                "Dedicated contact search with advanced filters: company, tags, date range".into(),
            ),
            input_schema: make_schema(
                with_pagination(serde_json::json!({
                    "query": { "type": "string", "description": "Search term for name, email, company" },
                    "company": { "type": "string", "description": "Filter by company name" },
                    "tags": { "type": "array", "items": { "type": "string" }, "description": "Filter by tags (AND)" },
                    "date_from": { "type": "string", "description": "Created after (ISO 8601)" },
                    "date_to": { "type": "string", "description": "Created before (ISO 8601)" }
                })),
                vec![],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "search_deals".into(),
            title: None,
            description: Some(
                "Search deals by title, stage, or value range".into(),
            ),
            input_schema: make_schema(
                with_pagination(serde_json::json!({
                    "query": { "type": "string", "description": "Search term for deal title or company" },
                    "stage": { "type": "string", "description": "Filter by deal stage" },
                    "value_min": { "type": "number", "description": "Minimum deal value" },
                    "value_max": { "type": "number", "description": "Maximum deal value" }
                })),
                vec![],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "search_emails".into(),
            title: None,
            description: Some(
                "Search sent emails by recipient, subject, or status".into(),
            ),
            input_schema: make_schema(
                with_pagination(serde_json::json!({
                    "query": { "type": "string", "description": "Search term for recipient or subject" },
                    "status": { "type": "string", "description": "Filter by email status (sent, delivered, bounced)" },
                    "date_from": { "type": "string", "description": "Sent after (ISO 8601)" },
                    "date_to": { "type": "string", "description": "Sent before (ISO 8601)" }
                })),
                vec![],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "search_notes".into(),
            title: None,
            description: Some(
                "Full-text search on notes with optional tag filter".into(),
            ),
            input_schema: make_schema(
                with_pagination(serde_json::json!({
                    "query": { "type": "string", "description": "Full-text search term (required, max 500 chars)" },
                    "tags": { "type": "array", "items": { "type": "string" }, "description": "Filter by tags" }
                })),
                vec!["query"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "recent_activity".into(),
            title: None,
            description: Some(
                "Show most recent items across all schemas (contacts, deals, emails, notes)".into(),
            ),
            input_schema: make_schema(
                with_pagination(serde_json::json!({})),
                vec![],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "saved_search".into(),
            title: None,
            description: Some(
                "Save a search query with a name for reuse, or list/run saved searches".into(),
            ),
            input_schema: make_schema(
                serde_json::json!({
                    "action": { "type": "string", "enum": ["save", "list", "run", "delete"], "description": "Action to perform (required)" },
                    "name": { "type": "string", "description": "Name for the saved search" },
                    "query_type": { "type": "string", "enum": ["all", "contacts", "deals", "emails", "notes"], "description": "Type of search (required for save)" },
                    "query_params": { "type": "object", "description": "Search parameters to save (required for save)" },
                    "limit": { "type": "integer", "description": "Max results when running (default 50, max 200)" },
                    "offset": { "type": "integer", "description": "Offset for pagination (default 0)" }
                }),
                vec!["action"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "search_stats".into(),
            title: None,
            description: Some(
                "Most searched terms, result counts, and search frequency".into(),
            ),
            input_schema: make_schema(
                with_pagination(serde_json::json!({})),
                vec![],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
    ]
}

// ============================================================================
// MCP Server
// ============================================================================

#[derive(Clone)]
pub struct SearchMcpServer {
    db: Database,
}

impl SearchMcpServer {
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    fn json_result<T: Serialize>(data: &T) -> CallToolResult {
        match serde_json::to_string_pretty(data) {
            Ok(json) => CallToolResult::success(vec![Content::text(json)]),
            Err(e) => {
                error!(error = %e, "Failed to serialize result");
                CallToolResult::error(vec![Content::text(format!(
                    "Serialization error: {e}"
                ))])
            }
        }
    }

    fn error_result(msg: &str) -> CallToolResult {
        CallToolResult::error(vec![Content::text(msg.to_string())])
    }

    fn get_str(args: &serde_json::Value, key: &str) -> Option<String> {
        args.get(key)
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }

    fn get_i64(args: &serde_json::Value, key: &str) -> Option<i64> {
        args.get(key).and_then(|v| v.as_i64())
    }

    fn get_f64(args: &serde_json::Value, key: &str) -> Option<f64> {
        args.get(key).and_then(|v| v.as_f64())
    }

    fn get_str_array(args: &serde_json::Value, key: &str) -> Vec<String> {
        args.get(key)
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.trim().to_string()))
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Clamp limit to [1, MAX_LIMIT] with a given default
    fn clamp_limit(args: &serde_json::Value, default: i64) -> i64 {
        Self::get_i64(args, "limit")
            .unwrap_or(default)
            .clamp(1, MAX_LIMIT)
    }

    /// Get offset, ensuring it is non-negative
    fn clamp_offset(args: &serde_json::Value) -> i64 {
        Self::get_i64(args, "offset")
            .unwrap_or(DEFAULT_OFFSET)
            .max(0)
    }

    /// Validate a query string: non-empty after trim, within max length
    fn validate_query(query: &str) -> Result<(), String> {
        if query.is_empty() {
            return Err("Query parameter cannot be empty".to_string());
        }
        if query.len() > MAX_QUERY_LEN {
            return Err(format!(
                "Query too long ({} chars). Maximum is {} chars",
                query.len(),
                MAX_QUERY_LEN
            ));
        }
        Ok(())
    }

    /// Validate an ISO 8601 date string
    fn validate_date(date_str: &str, field_name: &str) -> Result<(), String> {
        if chrono::DateTime::parse_from_rfc3339(date_str).is_err()
            && chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d").is_err()
        {
            return Err(format!(
                "Invalid date format for '{field_name}': '{date_str}'. Expected ISO 8601 (e.g. 2025-01-15 or 2025-01-15T00:00:00Z)"
            ));
        }
        Ok(())
    }

    async fn log_search(&self, query: &str, result_count: i32) {
        let id = uuid::Uuid::new_v4().to_string();
        if let Err(e) = sqlx::query(
            "INSERT INTO search.search_log (id, query, result_count) VALUES ($1, $2, $3)",
        )
        .bind(&id)
        .bind(query)
        .bind(result_count)
        .execute(self.db.pool())
        .await
        {
            warn!(error = %e, query = query, "Failed to log search");
        }
    }

    /// Check if a schema and table exist before querying
    async fn table_exists(&self, schema: &str, table: &str) -> bool {
        match sqlx::query_as::<_, (bool,)>(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)",
        )
        .bind(schema)
        .bind(table)
        .fetch_optional(self.db.pool())
        .await
        {
            Ok(Some(row)) => row.0,
            Ok(None) => false,
            Err(e) => {
                error!(error = %e, schema = schema, table = table, "Failed to check table existence");
                false
            }
        }
    }

    // ---- Tool handlers ----

    async fn handle_search_all(&self, query: &str, limit: i64, offset: i64) -> CallToolResult {
        if let Err(msg) = Self::validate_query(query) {
            return Self::error_result(&msg);
        }

        // Each sub-source must fetch enough rows so the merged set can satisfy offset+limit
        let fetch_limit = limit + offset;
        let pattern = format!("%{query}%");
        let mut results: Vec<SearchResult> = Vec::new();

        // Search contacts
        if self.table_exists("contacts", "contacts").await {
            #[derive(sqlx::FromRow)]
            struct Row {
                id: String,
                first_name: String,
                last_name: String,
                company: Option<String>,
                email: Option<String>,
                updated_at: chrono::DateTime<chrono::Utc>,
            }

            match sqlx::query_as::<_, Row>(
                "SELECT id, first_name, last_name, company, email, updated_at \
                 FROM contacts.contacts \
                 WHERE first_name ILIKE $1 OR last_name ILIKE $1 OR company ILIKE $1 OR email ILIKE $1 \
                 ORDER BY updated_at DESC LIMIT $2",
            )
            .bind(&pattern)
            .bind(fetch_limit)
            .fetch_all(self.db.pool())
            .await
            {
                Ok(rows) => {
                    for r in rows {
                        results.push(SearchResult {
                            source: "contacts".into(),
                            id: r.id,
                            title: format!("{} {}", r.first_name, r.last_name),
                            snippet: r.company.or(r.email).unwrap_or_default(),
                            updated_at: r.updated_at,
                        });
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to search contacts");
                }
            }
        }

        // Search deals
        if self.table_exists("deals", "deals").await {
            #[derive(sqlx::FromRow)]
            struct Row {
                id: String,
                title: String,
                stage: Option<String>,
                value: Option<f64>,
                updated_at: chrono::DateTime<chrono::Utc>,
            }

            match sqlx::query_as::<_, Row>(
                "SELECT id, company AS title, stage, value::float8 as value, updated_at \
                 FROM deals.deals \
                 WHERE company ILIKE $1 \
                 ORDER BY updated_at DESC LIMIT $2",
            )
            .bind(&pattern)
            .bind(fetch_limit)
            .fetch_all(self.db.pool())
            .await
            {
                Ok(rows) => {
                    for r in rows {
                        let snippet = match (r.stage.as_deref(), r.value) {
                            (Some(s), Some(v)) => format!("{s} — ${v:.0}"),
                            (Some(s), None) => s.to_string(),
                            (None, Some(v)) => format!("${v:.0}"),
                            _ => String::new(),
                        };
                        results.push(SearchResult {
                            source: "deals".into(),
                            id: r.id,
                            title: r.title,
                            snippet,
                            updated_at: r.updated_at,
                        });
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to search deals");
                }
            }
        }

        // Search emails
        if self.table_exists("email", "emails").await {
            #[derive(sqlx::FromRow)]
            struct Row {
                id: String,
                recipient: String,
                subject: Option<String>,
                status: Option<String>,
                created_at: chrono::DateTime<chrono::Utc>,
            }

            match sqlx::query_as::<_, Row>(
                "SELECT id, recipient, subject, status, created_at \
                 FROM email.emails \
                 WHERE recipient ILIKE $1 OR subject ILIKE $1 \
                 ORDER BY created_at DESC LIMIT $2",
            )
            .bind(&pattern)
            .bind(fetch_limit)
            .fetch_all(self.db.pool())
            .await
            {
                Ok(rows) => {
                    for r in rows {
                        results.push(SearchResult {
                            source: "emails".into(),
                            id: r.id,
                            title: r.subject.unwrap_or_else(|| "(no subject)".into()),
                            snippet: format!("To: {} [{}]", r.recipient, r.status.unwrap_or_default()),
                            updated_at: r.created_at,
                        });
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to search emails");
                }
            }
        }

        // Search notes (contact interactions of type 'note')
        if self.table_exists("contacts", "contact_interactions").await {
            #[derive(sqlx::FromRow)]
            struct Row {
                id: String,
                subject: Option<String>,
                notes: Option<String>,
                created_at: chrono::DateTime<chrono::Utc>,
            }

            match sqlx::query_as::<_, Row>(
                "SELECT id, subject, notes, created_at \
                 FROM contacts.contact_interactions \
                 WHERE (subject ILIKE $1 OR notes ILIKE $1) \
                 ORDER BY created_at DESC LIMIT $2",
            )
            .bind(&pattern)
            .bind(fetch_limit)
            .fetch_all(self.db.pool())
            .await
            {
                Ok(rows) => {
                    for r in rows {
                        let snippet = r
                            .notes
                            .as_deref()
                            .unwrap_or("")
                            .chars()
                            .take(100)
                            .collect::<String>();
                        results.push(SearchResult {
                            source: "notes".into(),
                            id: r.id,
                            title: r.subject.unwrap_or_else(|| "(untitled note)".into()),
                            snippet,
                            updated_at: r.created_at,
                        });
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to search notes");
                }
            }
        }

        // Sort all results by updated_at descending
        results.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));

        let total = results.len();

        // Apply offset and limit to combined results
        let paginated: Vec<SearchResult> = results
            .into_iter()
            .skip(offset as usize)
            .take(limit as usize)
            .collect();

        self.log_search(query, total as i32).await;

        Self::json_result(&SearchResponse {
            query: query.to_string(),
            total,
            limit,
            offset,
            results: paginated,
        })
    }

    async fn handle_search_contacts(&self, args: &serde_json::Value) -> CallToolResult {
        if !self.table_exists("contacts", "contacts").await {
            return Self::error_result("contacts.contacts table does not exist");
        }

        let query = Self::get_str(args, "query");
        let company = Self::get_str(args, "company");
        let tags = Self::get_str_array(args, "tags");
        let date_from = Self::get_str(args, "date_from");
        let date_to = Self::get_str(args, "date_to");
        let limit = Self::clamp_limit(args, DEFAULT_LIMIT);
        let offset = Self::clamp_offset(args);

        // Validate query length if provided
        if let Some(ref q) = query {
            if let Err(msg) = Self::validate_query(q) {
                return Self::error_result(&msg);
            }
        }

        // Validate filter string lengths
        if let Some(ref c) = company {
            if c.len() > MAX_QUERY_LEN {
                return Self::error_result(&format!(
                    "Company filter too long ({} chars). Maximum is {MAX_QUERY_LEN} chars",
                    c.len()
                ));
            }
        }

        // Validate date formats
        if let Some(ref df) = date_from {
            if let Err(msg) = Self::validate_date(df, "date_from") {
                return Self::error_result(&msg);
            }
        }
        if let Some(ref dt) = date_to {
            if let Err(msg) = Self::validate_date(dt, "date_to") {
                return Self::error_result(&msg);
            }
        }

        let mut sql = String::from(
            "SELECT id, type, first_name, last_name, company, role, email, phone, \
             mobile, address, city, state, country, notes, source, status, metadata, \
             created_at, updated_at FROM contacts.contacts WHERE 1=1",
        );
        let mut param_idx = 1u32;
        let mut params: Vec<String> = Vec::new();

        if let Some(ref q) = query {
            let p = format!("%{q}%");
            sql.push_str(&format!(
                " AND (first_name ILIKE ${pi} OR last_name ILIKE ${pi} OR email ILIKE ${pi} OR company ILIKE ${pi})",
                pi = param_idx
            ));
            param_idx += 1;
            params.push(p);
        }
        if let Some(ref c) = company {
            let p = format!("%{c}%");
            sql.push_str(&format!(" AND company ILIKE ${param_idx}"));
            param_idx += 1;
            params.push(p);
        }
        if let Some(ref df) = date_from {
            sql.push_str(&format!(" AND created_at >= ${param_idx}::timestamptz"));
            param_idx += 1;
            params.push(df.clone());
        }
        if let Some(ref dt) = date_to {
            sql.push_str(&format!(" AND created_at <= ${param_idx}::timestamptz"));
            param_idx += 1;
            params.push(dt.clone());
        }

        // Tag filter: contacts that have ALL specified tags
        for tag in &tags {
            sql.push_str(&format!(
                " AND id IN (SELECT contact_id FROM contacts.contact_tags WHERE tag = ${param_idx})"
            ));
            param_idx += 1;
            params.push(tag.clone());
        }

        sql.push_str(&format!(
            " ORDER BY updated_at DESC LIMIT ${}::bigint OFFSET ${}::bigint",
            param_idx,
            param_idx + 1,
        ));

        #[derive(sqlx::FromRow, Serialize)]
        struct ContactRow {
            id: String,
            #[sqlx(rename = "type")]
            #[serde(rename = "type")]
            contact_type: String,
            first_name: String,
            last_name: String,
            company: Option<String>,
            role: Option<String>,
            email: Option<String>,
            phone: Option<String>,
            mobile: Option<String>,
            address: Option<String>,
            city: Option<String>,
            state: Option<String>,
            country: Option<String>,
            notes: Option<String>,
            source: Option<String>,
            status: String,
            metadata: serde_json::Value,
            created_at: chrono::DateTime<chrono::Utc>,
            updated_at: chrono::DateTime<chrono::Utc>,
        }

        let mut q = sqlx::query_as::<_, ContactRow>(&sql);
        for p in &params {
            q = q.bind(p);
        }
        q = q.bind(limit);
        q = q.bind(offset);

        match q.fetch_all(self.db.pool()).await {
            Ok(contacts) => {
                let search_term = query.as_deref().unwrap_or("*");
                self.log_search(
                    &format!("contacts:{search_term}"),
                    contacts.len() as i32,
                )
                .await;
                Self::json_result(&serde_json::json!({
                    "results": contacts,
                    "count": contacts.len(),
                    "limit": limit,
                    "offset": offset,
                }))
            }
            Err(e) => {
                error!(error = %e, "Failed to search contacts");
                Self::error_result(&format!("Database error: {e}"))
            }
        }
    }

    async fn handle_search_deals(&self, args: &serde_json::Value) -> CallToolResult {
        if !self.table_exists("deals", "deals").await {
            return Self::error_result("deals.deals table does not exist");
        }

        let query = Self::get_str(args, "query");
        let stage = Self::get_str(args, "stage");
        let value_min = Self::get_f64(args, "value_min");
        let value_max = Self::get_f64(args, "value_max");
        let limit = Self::clamp_limit(args, DEFAULT_LIMIT);
        let offset = Self::clamp_offset(args);

        // Validate query length if provided
        if let Some(ref q) = query {
            if let Err(msg) = Self::validate_query(q) {
                return Self::error_result(&msg);
            }
        }

        // Validate value range
        if let (Some(vmin), Some(vmax)) = (value_min, value_max) {
            if vmin > vmax {
                return Self::error_result(&format!(
                    "value_min ({vmin}) cannot be greater than value_max ({vmax})"
                ));
            }
        }
        if let Some(vmin) = value_min {
            if vmin < 0.0 {
                return Self::error_result("value_min cannot be negative");
            }
        }
        if let Some(ref s) = stage {
            if s.len() > MAX_QUERY_LEN {
                return Self::error_result(&format!(
                    "Stage filter too long ({} chars). Maximum is {MAX_QUERY_LEN} chars",
                    s.len()
                ));
            }
        }

        let mut sql = String::from(
            "SELECT id, company AS title, stage, value::float8 as value, \
             contact_name, contact_email, description, created_at, updated_at \
             FROM deals.deals WHERE 1=1",
        );
        let mut all_params: Vec<String> = Vec::new();
        let mut pidx = 1u32;

        if let Some(ref q) = query {
            let p = format!("%{q}%");
            sql.push_str(&format!(
                " AND company ILIKE ${pi}",
                pi = pidx
            ));
            pidx += 1;
            all_params.push(p);
        }
        if let Some(ref s) = stage {
            sql.push_str(&format!(" AND stage = ${pidx}"));
            pidx += 1;
            all_params.push(s.clone());
        }
        if let Some(vmin) = value_min {
            sql.push_str(&format!(" AND value >= ${pidx}::numeric"));
            pidx += 1;
            all_params.push(vmin.to_string());
        }
        if let Some(vmax) = value_max {
            sql.push_str(&format!(" AND value <= ${pidx}::numeric"));
            pidx += 1;
            all_params.push(vmax.to_string());
        }

        sql.push_str(&format!(
            " ORDER BY updated_at DESC LIMIT ${}::bigint OFFSET ${}::bigint",
            pidx,
            pidx + 1,
        ));
        all_params.push(limit.to_string());
        all_params.push(offset.to_string());

        #[derive(sqlx::FromRow, Serialize)]
        struct DealRow {
            id: String,
            title: String,
            stage: Option<String>,
            value: Option<f64>,
            contact_name: Option<String>,
            contact_email: Option<String>,
            description: Option<String>,
            created_at: chrono::DateTime<chrono::Utc>,
            updated_at: chrono::DateTime<chrono::Utc>,
        }

        let mut q2 = sqlx::query_as::<_, DealRow>(&sql);
        for p in &all_params {
            q2 = q2.bind(p);
        }

        match q2.fetch_all(self.db.pool()).await {
            Ok(deals) => {
                let search_term = query.as_deref().unwrap_or("*");
                self.log_search(&format!("deals:{search_term}"), deals.len() as i32)
                    .await;
                Self::json_result(&serde_json::json!({
                    "results": deals,
                    "count": deals.len(),
                    "limit": limit,
                    "offset": offset,
                }))
            }
            Err(e) => {
                error!(error = %e, "Failed to search deals");
                Self::error_result(&format!("Database error: {e}"))
            }
        }
    }

    async fn handle_search_emails(&self, args: &serde_json::Value) -> CallToolResult {
        if !self.table_exists("email", "emails").await {
            return Self::error_result("email.emails table does not exist");
        }

        let query = Self::get_str(args, "query");
        let status = Self::get_str(args, "status");
        let date_from = Self::get_str(args, "date_from");
        let date_to = Self::get_str(args, "date_to");
        let limit = Self::clamp_limit(args, DEFAULT_LIMIT);
        let offset = Self::clamp_offset(args);

        // Validate query length if provided
        if let Some(ref q) = query {
            if let Err(msg) = Self::validate_query(q) {
                return Self::error_result(&msg);
            }
        }

        // Validate status enum
        if let Some(ref s) = status {
            let valid_statuses = ["sent", "delivered", "bounced", "failed", "pending"];
            if !valid_statuses.contains(&s.as_str()) {
                return Self::error_result(&format!(
                    "Invalid status: '{s}'. Valid values: {}",
                    valid_statuses.join(", ")
                ));
            }
        }

        // Validate date formats
        if let Some(ref df) = date_from {
            if let Err(msg) = Self::validate_date(df, "date_from") {
                return Self::error_result(&msg);
            }
        }
        if let Some(ref dt) = date_to {
            if let Err(msg) = Self::validate_date(dt, "date_to") {
                return Self::error_result(&msg);
            }
        }

        let mut sql = String::from(
            "SELECT id, recipient, subject, status, template, created_at \
             FROM email.emails WHERE 1=1",
        );
        let mut param_idx = 1u32;
        let mut params: Vec<String> = Vec::new();

        if let Some(ref q) = query {
            let p = format!("%{q}%");
            sql.push_str(&format!(
                " AND (recipient ILIKE ${pi} OR subject ILIKE ${pi})",
                pi = param_idx
            ));
            param_idx += 1;
            params.push(p);
        }
        if let Some(ref s) = status {
            sql.push_str(&format!(" AND status = ${param_idx}"));
            param_idx += 1;
            params.push(s.clone());
        }
        if let Some(ref df) = date_from {
            sql.push_str(&format!(" AND created_at >= ${param_idx}::timestamptz"));
            param_idx += 1;
            params.push(df.clone());
        }
        if let Some(ref dt) = date_to {
            sql.push_str(&format!(" AND created_at <= ${param_idx}::timestamptz"));
            param_idx += 1;
            params.push(dt.clone());
        }

        sql.push_str(&format!(
            " ORDER BY created_at DESC LIMIT ${}::bigint OFFSET ${}::bigint",
            param_idx,
            param_idx + 1,
        ));

        #[derive(sqlx::FromRow, Serialize)]
        struct EmailRow {
            id: String,
            recipient: String,
            subject: Option<String>,
            status: Option<String>,
            template: Option<String>,
            created_at: chrono::DateTime<chrono::Utc>,
        }

        let mut q = sqlx::query_as::<_, EmailRow>(&sql);
        for p in &params {
            q = q.bind(p);
        }
        q = q.bind(limit);
        q = q.bind(offset);

        match q.fetch_all(self.db.pool()).await {
            Ok(emails) => {
                let search_term = query.as_deref().unwrap_or("*");
                self.log_search(&format!("emails:{search_term}"), emails.len() as i32)
                    .await;
                Self::json_result(&serde_json::json!({
                    "results": emails,
                    "count": emails.len(),
                    "limit": limit,
                    "offset": offset,
                }))
            }
            Err(e) => {
                error!(error = %e, "Failed to search emails");
                Self::error_result(&format!("Database error: {e}"))
            }
        }
    }

    async fn handle_search_notes(&self, query: &str, args: &serde_json::Value) -> CallToolResult {
        if !self.table_exists("contacts", "contact_interactions").await {
            return Self::error_result("contacts.contact_interactions table does not exist");
        }

        if let Err(msg) = Self::validate_query(query) {
            return Self::error_result(&msg);
        }

        let tags = Self::get_str_array(args, "tags");
        let limit = Self::clamp_limit(args, DEFAULT_LIMIT);
        let offset = Self::clamp_offset(args);
        let pattern = format!("%{query}%");

        let mut sql = String::from(
            "SELECT ci.id, ci.contact_id, ci.type, ci.subject, ci.notes, ci.date, ci.created_at \
             FROM contacts.contact_interactions ci WHERE (ci.subject ILIKE $1 OR ci.notes ILIKE $1)",
        );
        let mut param_idx = 2u32;
        let mut tag_params: Vec<String> = Vec::new();

        // Filter by tags on the parent contact
        for tag in &tags {
            sql.push_str(&format!(
                " AND ci.contact_id IN (SELECT contact_id FROM contacts.contact_tags WHERE tag = ${param_idx})"
            ));
            param_idx += 1;
            tag_params.push(tag.clone());
        }

        sql.push_str(&format!(
            " ORDER BY ci.created_at DESC LIMIT ${}::bigint OFFSET ${}::bigint",
            param_idx,
            param_idx + 1,
        ));

        #[derive(sqlx::FromRow, Serialize)]
        struct NoteRow {
            id: String,
            contact_id: String,
            #[sqlx(rename = "type")]
            #[serde(rename = "type")]
            note_type: String,
            subject: Option<String>,
            notes: Option<String>,
            date: chrono::DateTime<chrono::Utc>,
            created_at: chrono::DateTime<chrono::Utc>,
        }

        let mut q = sqlx::query_as::<_, NoteRow>(&sql);
        q = q.bind(&pattern);
        for p in &tag_params {
            q = q.bind(p);
        }
        q = q.bind(limit);
        q = q.bind(offset);

        match q.fetch_all(self.db.pool()).await {
            Ok(notes) => {
                self.log_search(&format!("notes:{query}"), notes.len() as i32)
                    .await;
                Self::json_result(&serde_json::json!({
                    "results": notes,
                    "count": notes.len(),
                    "limit": limit,
                    "offset": offset,
                }))
            }
            Err(e) => {
                error!(error = %e, query = query, "Failed to search notes");
                Self::error_result(&format!("Database error: {e}"))
            }
        }
    }

    async fn handle_recent_activity(&self, limit: i64, offset: i64) -> CallToolResult {
        // Each sub-source must fetch enough rows so the merged set can satisfy offset+limit
        let fetch_limit = limit + offset;
        let mut results: Vec<SearchResult> = Vec::new();

        // Recent contacts
        if self.table_exists("contacts", "contacts").await {
            #[derive(sqlx::FromRow)]
            struct Row {
                id: String,
                first_name: String,
                last_name: String,
                company: Option<String>,
                updated_at: chrono::DateTime<chrono::Utc>,
            }

            match sqlx::query_as::<_, Row>(
                "SELECT id, first_name, last_name, company, updated_at \
                 FROM contacts.contacts ORDER BY updated_at DESC LIMIT $1",
            )
            .bind(fetch_limit)
            .fetch_all(self.db.pool())
            .await
            {
                Ok(rows) => {
                    for r in rows {
                        results.push(SearchResult {
                            source: "contacts".into(),
                            id: r.id,
                            title: format!("{} {}", r.first_name, r.last_name),
                            snippet: r.company.unwrap_or_default(),
                            updated_at: r.updated_at,
                        });
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to fetch recent contacts");
                }
            }
        }

        // Recent deals
        if self.table_exists("deals", "deals").await {
            #[derive(sqlx::FromRow)]
            struct Row {
                id: String,
                title: String,
                stage: Option<String>,
                updated_at: chrono::DateTime<chrono::Utc>,
            }

            match sqlx::query_as::<_, Row>(
                "SELECT id, company AS title, stage, updated_at \
                 FROM deals.deals ORDER BY updated_at DESC LIMIT $1",
            )
            .bind(fetch_limit)
            .fetch_all(self.db.pool())
            .await
            {
                Ok(rows) => {
                    for r in rows {
                        results.push(SearchResult {
                            source: "deals".into(),
                            id: r.id,
                            title: r.title,
                            snippet: r.stage.unwrap_or_default(),
                            updated_at: r.updated_at,
                        });
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to fetch recent deals");
                }
            }
        }

        // Recent emails
        if self.table_exists("email", "emails").await {
            #[derive(sqlx::FromRow)]
            struct Row {
                id: String,
                recipient: String,
                subject: Option<String>,
                created_at: chrono::DateTime<chrono::Utc>,
            }

            match sqlx::query_as::<_, Row>(
                "SELECT id, recipient, subject, created_at \
                 FROM email.emails ORDER BY created_at DESC LIMIT $1",
            )
            .bind(fetch_limit)
            .fetch_all(self.db.pool())
            .await
            {
                Ok(rows) => {
                    for r in rows {
                        results.push(SearchResult {
                            source: "emails".into(),
                            id: r.id,
                            title: r.subject.unwrap_or_else(|| "(no subject)".into()),
                            snippet: r.recipient,
                            updated_at: r.created_at,
                        });
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to fetch recent emails");
                }
            }
        }

        // Recent notes
        if self.table_exists("contacts", "contact_interactions").await {
            #[derive(sqlx::FromRow)]
            struct Row {
                id: String,
                subject: Option<String>,
                notes: Option<String>,
                created_at: chrono::DateTime<chrono::Utc>,
            }

            match sqlx::query_as::<_, Row>(
                "SELECT id, subject, notes, created_at \
                 FROM contacts.contact_interactions ORDER BY created_at DESC LIMIT $1",
            )
            .bind(fetch_limit)
            .fetch_all(self.db.pool())
            .await
            {
                Ok(rows) => {
                    for r in rows {
                        let snippet = r
                            .notes
                            .as_deref()
                            .unwrap_or("")
                            .chars()
                            .take(80)
                            .collect::<String>();
                        results.push(SearchResult {
                            source: "notes".into(),
                            id: r.id,
                            title: r.subject.unwrap_or_else(|| "(untitled)".into()),
                            snippet,
                            updated_at: r.created_at,
                        });
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to fetch recent notes");
                }
            }
        }

        results.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));

        let total = results.len();

        // Apply offset and limit to combined results
        let paginated: Vec<SearchResult> = results
            .into_iter()
            .skip(offset as usize)
            .take(limit as usize)
            .collect();

        Self::json_result(&serde_json::json!({
            "total": total,
            "limit": limit,
            "offset": offset,
            "results": paginated
        }))
    }

    async fn handle_saved_search(&self, args: &serde_json::Value) -> CallToolResult {
        let action = match Self::get_str(args, "action") {
            Some(a) => a,
            None => return Self::error_result("Missing required parameter: action"),
        };

        // Validate action enum
        let valid_actions = ["save", "list", "run", "delete"];
        if !valid_actions.contains(&action.as_str()) {
            return Self::error_result(&format!(
                "Invalid action: '{action}'. Valid values: {}",
                valid_actions.join(", ")
            ));
        }

        match action.as_str() {
            "save" => {
                let name = match Self::get_str(args, "name") {
                    Some(n) => n,
                    None => return Self::error_result("Missing required parameter: name (required for 'save' action)"),
                };
                if name.len() > 100 {
                    return Self::error_result("Name too long. Maximum is 100 characters");
                }
                let query_type = match Self::get_str(args, "query_type") {
                    Some(t) => t,
                    None => return Self::error_result("Missing required parameter: query_type (required for 'save' action)"),
                };
                let valid_types = ["all", "contacts", "deals", "emails", "notes"];
                if !valid_types.contains(&query_type.as_str()) {
                    return Self::error_result(&format!(
                        "Invalid query_type: '{query_type}'. Valid values: {}",
                        valid_types.join(", ")
                    ));
                }
                let query_params = args
                    .get("query_params")
                    .cloned()
                    .unwrap_or(serde_json::json!({}));
                if !query_params.is_object() {
                    return Self::error_result("query_params must be a JSON object");
                }

                let id = uuid::Uuid::new_v4().to_string();
                match sqlx::query_as::<_, SavedSearch>(
                    "INSERT INTO search.saved_searches (id, name, query_type, query_params) \
                     VALUES ($1, $2, $3, $4) \
                     ON CONFLICT (name) DO UPDATE SET query_type = $3, query_params = $4 \
                     RETURNING *",
                )
                .bind(&id)
                .bind(&name)
                .bind(&query_type)
                .bind(&query_params)
                .fetch_one(self.db.pool())
                .await
                {
                    Ok(saved) => {
                        info!(name = name, query_type = query_type, "Saved search created/updated");
                        Self::json_result(&saved)
                    }
                    Err(e) => {
                        error!(error = %e, name = name, "Failed to save search");
                        Self::error_result(&format!("Failed to save search: {e}"))
                    }
                }
            }
            "list" => {
                let limit = Self::clamp_limit(args, DEFAULT_LIMIT);
                let offset = Self::clamp_offset(args);

                match sqlx::query_as::<_, SavedSearch>(
                    "SELECT * FROM search.saved_searches ORDER BY created_at DESC LIMIT $1 OFFSET $2",
                )
                .bind(limit)
                .bind(offset)
                .fetch_all(self.db.pool())
                .await
                {
                    Ok(searches) => Self::json_result(&serde_json::json!({
                        "results": searches,
                        "count": searches.len(),
                        "limit": limit,
                        "offset": offset,
                    })),
                    Err(e) => {
                        error!(error = %e, "Failed to list saved searches");
                        Self::error_result(&format!("Database error: {e}"))
                    }
                }
            }
            "run" => {
                let name = match Self::get_str(args, "name") {
                    Some(n) => n,
                    None => return Self::error_result("Missing required parameter: name (required for 'run' action)"),
                };
                let limit = Self::clamp_limit(args, DEFAULT_LIMIT);
                let offset = Self::clamp_offset(args);

                let saved: Option<SavedSearch> = match sqlx::query_as(
                    "SELECT * FROM search.saved_searches WHERE name = $1",
                )
                .bind(&name)
                .fetch_optional(self.db.pool())
                .await
                {
                    Ok(s) => s,
                    Err(e) => {
                        error!(error = %e, name = name, "Failed to fetch saved search");
                        return Self::error_result(&format!("Database error: {e}"));
                    }
                };

                let saved = match saved {
                    Some(s) => s,
                    None => {
                        return Self::error_result(&format!("Saved search '{name}' not found"))
                    }
                };

                // Merge the saved params with the limit/offset override
                let mut params = saved.query_params.clone();
                if let Some(obj) = params.as_object_mut() {
                    obj.insert("limit".to_string(), serde_json::json!(limit));
                    obj.insert("offset".to_string(), serde_json::json!(offset));
                }

                // Route to appropriate handler
                match saved.query_type.as_str() {
                    "all" => {
                        let q = params
                            .get("query")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        self.handle_search_all(q, limit, offset).await
                    }
                    "contacts" => self.handle_search_contacts(&params).await,
                    "deals" => self.handle_search_deals(&params).await,
                    "emails" => self.handle_search_emails(&params).await,
                    "notes" => {
                        let q = params
                            .get("query")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        self.handle_search_notes(q, &params).await
                    }
                    _ => Self::error_result(&format!(
                        "Unknown query_type: {}",
                        saved.query_type
                    )),
                }
            }
            "delete" => {
                let name = match Self::get_str(args, "name") {
                    Some(n) => n,
                    None => return Self::error_result("Missing required parameter: name (required for 'delete' action)"),
                };

                match sqlx::query("DELETE FROM search.saved_searches WHERE name = $1")
                    .bind(&name)
                    .execute(self.db.pool())
                    .await
                {
                    Ok(r) => {
                        if r.rows_affected() > 0 {
                            info!(name = name, "Saved search deleted");
                            Self::json_result(&serde_json::json!({ "deleted": true, "name": name }))
                        } else {
                            Self::error_result(&format!("Saved search '{name}' not found"))
                        }
                    }
                    Err(e) => {
                        error!(error = %e, name = name, "Failed to delete saved search");
                        Self::error_result(&format!("Failed to delete: {e}"))
                    }
                }
            }
            _ => Self::error_result(&format!("Unknown action: {action}. Use save, list, run, or delete")),
        }
    }

    async fn handle_search_stats(&self, limit: i64, offset: i64) -> CallToolResult {
        match sqlx::query_as::<_, SearchStat>(
            "SELECT query, COUNT(*) as search_count, MAX(searched_at) as last_searched \
             FROM search.search_log \
             GROUP BY query \
             ORDER BY search_count DESC \
             LIMIT $1 OFFSET $2",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(self.db.pool())
        .await
        {
            Ok(stats) => {
                let total_searches: (i64,) = match sqlx::query_as(
                    "SELECT COUNT(*) FROM search.search_log",
                )
                .fetch_one(self.db.pool())
                .await
                {
                    Ok(row) => row,
                    Err(e) => {
                        warn!(error = %e, "Failed to count total searches");
                        (0,)
                    }
                };

                Self::json_result(&serde_json::json!({
                    "total_searches": total_searches.0,
                    "top_queries": stats,
                    "count": stats.len(),
                    "limit": limit,
                    "offset": offset,
                }))
            }
            Err(e) => {
                error!(error = %e, "Failed to fetch search stats");
                Self::error_result(&format!("Database error: {e}"))
            }
        }
    }
}

// ============================================================================
// ServerHandler trait implementation
// ============================================================================

impl ServerHandler for SearchMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation::from_build_env(),
            instructions: Some(
                "DataXLR8 Search MCP — unified search across contacts, deals, emails, notes with saved searches and stats"
                    .into(),
            ),
        }
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, rmcp::ErrorData>> + Send + '_
    {
        async {
            Ok(ListToolsResult {
                tools: build_tools(),
                next_cursor: None,
                meta: None,
            })
        }
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, rmcp::ErrorData>> + Send + '_
    {
        async move {
            let args =
                serde_json::to_value(&request.arguments).unwrap_or(serde_json::Value::Null);
            let name_str: &str = request.name.as_ref();

            info!(tool = name_str, "Tool call received");

            let result = match name_str {
                "search_all" => {
                    let query = match Self::get_str(&args, "query") {
                        Some(q) => q,
                        None => {
                            return Ok(Self::error_result(
                                "Missing required parameter: query",
                            ))
                        }
                    };
                    let limit = Self::clamp_limit(&args, DEFAULT_LIMIT);
                    let offset = Self::clamp_offset(&args);
                    self.handle_search_all(&query, limit, offset).await
                }
                "search_contacts" => self.handle_search_contacts(&args).await,
                "search_deals" => self.handle_search_deals(&args).await,
                "search_emails" => self.handle_search_emails(&args).await,
                "search_notes" => {
                    let query = match Self::get_str(&args, "query") {
                        Some(q) => q,
                        None => {
                            return Ok(Self::error_result("Missing required parameter: query"))
                        }
                    };
                    self.handle_search_notes(&query, &args).await
                }
                "recent_activity" => {
                    let limit = Self::clamp_limit(&args, DEFAULT_LIMIT);
                    let offset = Self::clamp_offset(&args);
                    self.handle_recent_activity(limit, offset).await
                }
                "saved_search" => self.handle_saved_search(&args).await,
                "search_stats" => {
                    let limit = Self::clamp_limit(&args, DEFAULT_LIMIT);
                    let offset = Self::clamp_offset(&args);
                    self.handle_search_stats(limit, offset).await
                }
                _ => {
                    warn!(tool = name_str, "Unknown tool called");
                    Self::error_result(&format!("Unknown tool: {}", request.name))
                }
            };

            Ok(result)
        }
    }
}
