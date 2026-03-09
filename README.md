# :mag_right: dataxlr8-search-mcp

Unified full-text search for AI agents — query across contacts, deals, emails, and notes from a single MCP tool.

[![Rust](https://img.shields.io/badge/Rust-2024_edition-orange?logo=rust)](https://www.rust-lang.org/)
[![MCP](https://img.shields.io/badge/MCP-rmcp_0.17-blue)](https://modelcontextprotocol.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## What It Does

Provides a single search interface across the entire DataXLR8 data layer. Query contacts, deals, emails, and notes with one keyword search, or use dedicated tools for domain-specific filtering. Save frequent searches for reuse, and track search analytics to understand what gets looked up most — all backed by PostgreSQL full-text search.

## Architecture

```
                    ┌─────────────────────────┐
AI Agent ──stdio──▶ │  dataxlr8-search-mcp    │
                    │  (rmcp 0.17 server)      │
                    └──────────┬──────────────┘
                               │ sqlx 0.8
                               │ (cross-schema FTS)
                               ▼
                    ┌─────────────────────────┐
                    │  PostgreSQL              │
                    │  reads: crm, email,      │
                    │         notes, deals     │
                    │  owns:  search           │
                    │  ├── saved_searches      │
                    │  └── search_log          │
                    └─────────────────────────┘
```

## Tools

| Tool | Description |
|------|-------------|
| `search_all` | Search across contacts, deals, notes, and emails by keyword |
| `search_contacts` | Search contacts with filters: company, tags, date range |
| `search_deals` | Search deals by title, stage, or value range |
| `search_emails` | Search sent emails by recipient, subject, or status |
| `search_notes` | Full-text search of notes with date filtering |
| `saved_search` | Save a search query for reuse |
| `recent_activity` | Get recent activity across all entity types |
| `search_stats` | Popular searches and search volume by type |

## Quick Start

```bash
git clone https://github.com/pdaxt/dataxlr8-search-mcp
cd dataxlr8-search-mcp
cargo build --release

export DATABASE_URL=postgres://user:pass@localhost:5432/dataxlr8
./target/release/dataxlr8-search-mcp
```

The server auto-creates the `search` schema and all tables on first run.

## Configuration

| Variable | Required | Description |
|----------|----------|-------------|
| `DATABASE_URL` | Yes | PostgreSQL connection string |
| `LOG_LEVEL` | No | Tracing level (default: `info`) |

## Claude Desktop Integration

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "dataxlr8-search": {
      "command": "./target/release/dataxlr8-search-mcp",
      "env": {
        "DATABASE_URL": "postgres://user:pass@localhost:5432/dataxlr8"
      }
    }
  }
}
```

## Part of DataXLR8

One of 14 Rust MCP servers that form the [DataXLR8](https://github.com/pdaxt) platform — a modular, AI-native business operations suite. Each server owns a single domain, shares a PostgreSQL instance, and communicates over the Model Context Protocol.

## License

MIT
