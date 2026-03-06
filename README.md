# dataxlr8-search-mcp

Full-text search MCP for DataXLR8 — search across contacts, deals, emails, and notes with advanced filtering and saved search management.

## Tools

| Tool | Description |
|------|-------------|
| search_all | Search across contacts, deals, notes, emails by keyword. Returns ranked results with source. |
| search_contacts | Dedicated contact search with advanced filters: company, tags, date range |
| search_deals | Search deals by title, stage, or value range |
| search_emails | Search sent emails by recipient, subject, or status |
| search_notes | Full-text search of notes with date filtering |
| save_search | Save a search query for reuse |
| list_saved_searches | List all saved searches |
| delete_saved_search | Delete a saved search |
| search_stats | Show popular searches and search volume by type |

## Setup

```bash
DATABASE_URL=postgres://dataxlr8:dataxlr8@localhost:5432/dataxlr8 cargo run
```

## Schema

Creates `search.*` schema in PostgreSQL with tables for:
- `saved_searches` — saved search queries with parameters
- `search_log` — search history and analytics

## Part of

[DataXLR8](https://github.com/pdaxt) - AI-powered recruitment platform
