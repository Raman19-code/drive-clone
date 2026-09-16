# Schema Design

See [`db/schema.sql`](../db/schema.sql) for the canonical DDL.

## Core tables

- **users** — auth + per-user storage quota/usage
- **folders** — self-referencing tree (`parent_id`), indexed on `(parent_id, owner_id)`
- **files** — metadata only; `storage_key` points to the MinIO object; FULLTEXT
  index on `name` for filename search alongside semantic search
- **file_versions** — append-only version history per file
- **permissions** — per-resource RBAC (`viewer`/`editor`/`owner`)
- **share_links** — expiring, token-based public/semi-public access
- **audit_log** — every access/share/delete/permission-change event

## Sharding

`owner_id` (or a future `workspace_id`) is the intended shard key once a
single MySQL primary becomes the write bottleneck — every hot table already
carries it in its primary access-pattern index.
