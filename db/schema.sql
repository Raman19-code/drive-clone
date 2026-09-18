-- =============================================================================
-- DriveX Canonical Relational Database Schema
-- Target RDBMS: MySQL 8.0+
-- Storage Engine: InnoDB
-- Character Set: utf8mb4
-- Collation: utf8mb4_unicode_ci
-- =============================================================================

SET FOREIGN_KEY_CHECKS = 0;

DROP TABLE IF EXISTS event_outbox;
DROP TABLE IF EXISTS upload_sessions;
DROP TABLE IF EXISTS file_tags;
DROP TABLE IF EXISTS audit_log;
DROP TABLE IF EXISTS refresh_tokens;
DROP TABLE IF EXISTS share_links;
DROP TABLE IF EXISTS permissions;
DROP TABLE IF EXISTS file_versions;
DROP TABLE IF EXISTS files;
DROP TABLE IF EXISTS folders;
DROP TABLE IF EXISTS users;

SET FOREIGN_KEY_CHECKS = 1;

-- -----------------------------------------------------------------------------
-- 1. Table: users
-- Core authentication, identity, and user-level storage quota accounting.
-- -----------------------------------------------------------------------------
CREATE TABLE users (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    email VARCHAR(255) NOT NULL,
    password_hash VARCHAR(255) NOT NULL COMMENT 'Argon2id salted password hash ($argon2id$v=19$m=65536,t=3,p=4$...)',
    storage_quota_bytes BIGINT UNSIGNED NOT NULL DEFAULT 16106127360 COMMENT 'Allocated storage quota in bytes (default 15 GB)',
    storage_used_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Total active storage consumed in bytes',
    status ENUM('active', 'suspended', 'deleted') NOT NULL DEFAULT 'active' COMMENT 'User account lifecycle status',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_users_email (email),
    INDEX idx_users_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- 2. Table: folders
-- Self-referential hierarchical folder tree structure with owner isolation.
-- -----------------------------------------------------------------------------
CREATE TABLE folders (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    parent_id BIGINT UNSIGNED NULL DEFAULT NULL COMMENT 'Parent folder ID; NULL indicates root directory folder',
    owner_id BIGINT UNSIGNED NOT NULL COMMENT 'User ID of the folder owner',
    is_trashed BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'Soft-deletion status flag',
    trashed_at TIMESTAMP NULL DEFAULT NULL COMMENT 'Timestamp when folder was moved to trash',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT fk_folders_parent FOREIGN KEY (parent_id) 
        REFERENCES folders (id) ON DELETE CASCADE,
    CONSTRAINT fk_folders_owner FOREIGN KEY (owner_id) 
        REFERENCES users (id) ON DELETE RESTRICT,
    INDEX idx_folders_hierarchy (owner_id, parent_id, is_trashed, name),
    INDEX idx_folders_owner_trashed (owner_id, is_trashed),
    INDEX idx_folders_parent (parent_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- 3. Table: files
-- Logical file metadata entity. Binary payloads are stored in MinIO object
-- storage and referenced via storage_key.
-- -----------------------------------------------------------------------------
CREATE TABLE files (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    folder_id BIGINT UNSIGNED NULL DEFAULT NULL COMMENT 'Parent folder ID; NULL indicates root directory file',
    owner_id BIGINT UNSIGNED NOT NULL COMMENT 'User ID of the file owner',
    mime_type VARCHAR(127) NOT NULL DEFAULT 'application/octet-stream' COMMENT 'MIME content type',
    size_bytes BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Byte size of the active file version',
    storage_key VARCHAR(512) NOT NULL COMMENT 'MinIO S3 object key (e.g., blobs/105/2026-09/uuid.pdf)',
    content_hash CHAR(64) NOT NULL COMMENT 'SHA-256 hexadecimal digest of file content for deduplication',
    phash VARCHAR(64) NULL DEFAULT NULL COMMENT 'Perceptual hash string (pHash/dHash) for visual duplicate detection',
    processing_status ENUM('PENDING', 'INDEXED', 'FAILED') NOT NULL DEFAULT 'PENDING' COMMENT 'Asynchronous AI/ML extraction and vector embedding status',
    current_version_id BIGINT UNSIGNED NULL DEFAULT NULL COMMENT 'Pointer to current active file_version record',
    is_trashed BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'Soft-deletion status flag',
    trashed_at TIMESTAMP NULL DEFAULT NULL COMMENT 'Timestamp when file was moved to trash',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT fk_files_folder FOREIGN KEY (folder_id) 
        REFERENCES folders (id) ON DELETE CASCADE,
    CONSTRAINT fk_files_owner FOREIGN KEY (owner_id) 
        REFERENCES users (id) ON DELETE RESTRICT,
    INDEX idx_files_listing (owner_id, folder_id, is_trashed, name),
    INDEX idx_files_owner_trashed (owner_id, is_trashed),
    INDEX idx_files_content_hash (content_hash),
    INDEX idx_files_phash (phash),
    INDEX idx_files_processing_status (processing_status),
    INDEX idx_files_storage_key (storage_key(191)),
    FULLTEXT INDEX ft_files_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- 4. Table: file_versions
-- Append-only revision history tracking immutable file snapshots over time.
-- -----------------------------------------------------------------------------
CREATE TABLE file_versions (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    file_id BIGINT UNSIGNED NOT NULL COMMENT 'Parent file entity reference',
    version_num INT UNSIGNED NOT NULL DEFAULT 1 COMMENT 'Monotonically increasing version number (1, 2, ...)',
    storage_key VARCHAR(512) NOT NULL COMMENT 'MinIO S3 object key for this specific version payload',
    size_bytes BIGINT UNSIGNED NOT NULL COMMENT 'Byte size of this version payload',
    content_hash CHAR(64) NOT NULL COMMENT 'SHA-256 hexadecimal checksum for this version payload',
    phash VARCHAR(64) NULL DEFAULT NULL COMMENT 'Perceptual hash for image versions',
    processing_status ENUM('PENDING', 'INDEXED', 'FAILED') NOT NULL DEFAULT 'PENDING' COMMENT 'AI/ML vector indexing status for this version',
    created_by BIGINT UNSIGNED NOT NULL COMMENT 'User ID who uploaded this revision',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT fk_file_versions_file FOREIGN KEY (file_id) 
        REFERENCES files (id) ON DELETE CASCADE,
    CONSTRAINT fk_file_versions_creator FOREIGN KEY (created_by) 
        REFERENCES users (id) ON DELETE RESTRICT,
    UNIQUE KEY uq_file_versions_file_num (file_id, version_num),
    INDEX idx_file_versions_num_desc (file_id, version_num DESC),
    INDEX idx_file_versions_created_at (file_id, created_at DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Deferred foreign key constraint linking files to their active file_versions record
ALTER TABLE files
    ADD CONSTRAINT fk_files_current_version FOREIGN KEY (current_version_id)
    REFERENCES file_versions (id) ON DELETE SET NULL;

-- -----------------------------------------------------------------------------
-- 5. Table: permissions
-- Granular Role-Based Access Control (RBAC) grants for files and folders.
-- -----------------------------------------------------------------------------
CREATE TABLE permissions (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    resource_type ENUM('file', 'folder') NOT NULL COMMENT 'Target entity type',
    resource_id BIGINT UNSIGNED NOT NULL COMMENT 'Target entity primary key',
    user_id BIGINT UNSIGNED NOT NULL COMMENT 'Subject grantee user ID',
    role ENUM('viewer', 'editor', 'owner') NOT NULL DEFAULT 'viewer' COMMENT 'Granted authorization level',
    granted_by BIGINT UNSIGNED NULL DEFAULT NULL COMMENT 'User ID who granted this permission',
    granted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT fk_permissions_user FOREIGN KEY (user_id) 
        REFERENCES users (id) ON DELETE CASCADE,
    CONSTRAINT fk_permissions_granter FOREIGN KEY (granted_by) 
        REFERENCES users (id) ON DELETE SET NULL,
    UNIQUE KEY uq_permissions_resource_user (resource_type, resource_id, user_id),
    INDEX idx_permissions_user_role (user_id, role),
    INDEX idx_permissions_resource (resource_type, resource_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- 6. Table: share_links
-- Tokenized, expiring public or semi-public links with optional passcode gating.
-- -----------------------------------------------------------------------------
CREATE TABLE share_links (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    resource_type ENUM('file', 'folder') NOT NULL COMMENT 'Shared entity type',
    resource_id BIGINT UNSIGNED NOT NULL COMMENT 'Shared entity primary key',
    token VARCHAR(64) NOT NULL COMMENT 'Cryptographically secure URL token',
    permission_role ENUM('viewer', 'editor') NOT NULL DEFAULT 'viewer' COMMENT 'Access role granted via link',
    password_hash VARCHAR(255) NULL DEFAULT NULL COMMENT 'Argon2id password hash for password-gated links',
    max_downloads INT UNSIGNED NULL DEFAULT NULL COMMENT 'Maximum download count allowed; NULL for unlimited',
    download_count INT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Cumulative download/access counter',
    created_by BIGINT UNSIGNED NOT NULL COMMENT 'User ID of link creator',
    expires_at TIMESTAMP NULL DEFAULT NULL COMMENT 'Expiration timestamp; NULL indicates permanent link',
    is_active BOOLEAN NOT NULL DEFAULT TRUE COMMENT 'Administrative or manual revocation flag',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_share_links_token (token),
    CONSTRAINT fk_share_links_creator FOREIGN KEY (created_by) 
        REFERENCES users (id) ON DELETE CASCADE,
    INDEX idx_share_links_resource (resource_type, resource_id),
    INDEX idx_share_links_creator (created_by),
    INDEX idx_share_links_expiry (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- 7. Table: audit_log
-- Immutable security, data-mutation, and access telemetry log.
-- -----------------------------------------------------------------------------
CREATE TABLE audit_log (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    user_id BIGINT UNSIGNED NULL DEFAULT NULL COMMENT 'User ID; NULL for unauthenticated or public guest actions',
    action VARCHAR(64) NOT NULL COMMENT 'Action code (e.g., FILE_UPLOAD, FILE_DOWNLOAD, PERMISSION_GRANT)',
    resource_type ENUM('file', 'folder', 'auth', 'user', 'share_link') NOT NULL COMMENT 'Affected entity category',
    resource_id BIGINT UNSIGNED NOT NULL COMMENT 'Affected entity primary key',
    ip_address VARCHAR(45) NULL DEFAULT NULL COMMENT 'Client IPv4 (up to 15 chars) or IPv6 (up to 45 chars)',
    user_agent VARCHAR(512) NULL DEFAULT NULL COMMENT 'Client HTTP user agent string',
    metadata JSON NULL DEFAULT NULL COMMENT 'Structured event telemetry payload',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT fk_audit_log_user FOREIGN KEY (user_id) 
        REFERENCES users (id) ON DELETE SET NULL,
    INDEX idx_audit_log_user_created (user_id, created_at),
    INDEX idx_audit_log_resource (resource_type, resource_id, created_at),
    INDEX idx_audit_log_action (action, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- 8. Table: refresh_tokens
-- Persistent cryptographic session registry supporting single-use token rotation.
-- -----------------------------------------------------------------------------
CREATE TABLE refresh_tokens (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    user_id BIGINT UNSIGNED NOT NULL COMMENT 'Account owner user ID',
    token_hash CHAR(64) NOT NULL COMMENT 'SHA-256 digest of the raw refresh token secret',
    device_info VARCHAR(255) NULL DEFAULT NULL COMMENT 'Device descriptor or browser user agent summary',
    ip_address VARCHAR(45) NULL DEFAULT NULL COMMENT 'Client IP address from session creation',
    revoked BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'Revocation flag for token rotation reuse detection',
    expires_at TIMESTAMP NOT NULL COMMENT 'Session expiration timestamp',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_refresh_tokens_hash (token_hash),
    CONSTRAINT fk_refresh_tokens_user FOREIGN KEY (user_id) 
        REFERENCES users (id) ON DELETE CASCADE,
    INDEX idx_refresh_tokens_user (user_id, revoked, expires_at),
    INDEX idx_refresh_tokens_expiry (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- 9. Table: file_tags
-- Multi-label classification tags generated by AI models or assigned by users.
-- -----------------------------------------------------------------------------
CREATE TABLE file_tags (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    file_id BIGINT UNSIGNED NOT NULL COMMENT 'Parent file reference',
    tag VARCHAR(64) NOT NULL COMMENT 'Categorical tag keyword',
    confidence FLOAT NOT NULL DEFAULT 1.0 COMMENT 'Classification confidence score between 0.0 and 1.0',
    source ENUM('AI', 'USER') NOT NULL DEFAULT 'AI' COMMENT 'Source origin of the assigned tag',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    CONSTRAINT fk_file_tags_file FOREIGN KEY (file_id) 
        REFERENCES files (id) ON DELETE CASCADE,
    UNIQUE KEY uq_file_tags_file_tag (file_id, tag),
    INDEX idx_file_tags_tag (tag),
    INDEX idx_file_tags_file (file_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- 10. Table: upload_sessions
-- Ephemeral state tracker for pre-signed direct MinIO upload transactions.
-- -----------------------------------------------------------------------------
CREATE TABLE upload_sessions (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    session_id VARCHAR(64) NOT NULL COMMENT 'Public upload session UUIDv4 identifier',
    owner_id BIGINT UNSIGNED NOT NULL COMMENT 'Initiating user ID',
    folder_id BIGINT UNSIGNED NULL DEFAULT NULL COMMENT 'Target destination folder ID; NULL for root',
    name VARCHAR(255) NOT NULL COMMENT 'Destination file name',
    size_bytes BIGINT UNSIGNED NOT NULL COMMENT 'Pre-declared file size in bytes',
    mime_type VARCHAR(127) NOT NULL DEFAULT 'application/octet-stream' COMMENT 'Declared MIME type',
    storage_key VARCHAR(512) NOT NULL COMMENT 'Pre-allocated MinIO S3 object key',
    content_hash CHAR(64) NOT NULL COMMENT 'Pre-declared SHA-256 checksum',
    status ENUM('PENDING', 'COMPLETED', 'ABORTED', 'EXPIRED') NOT NULL DEFAULT 'PENDING' COMMENT 'Upload lifecycle state',
    expires_at TIMESTAMP NOT NULL COMMENT 'Pre-signed URL validity expiration timestamp',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_upload_sessions_session_id (session_id),
    CONSTRAINT fk_upload_sessions_owner FOREIGN KEY (owner_id) 
        REFERENCES users (id) ON DELETE CASCADE,
    CONSTRAINT fk_upload_sessions_folder FOREIGN KEY (folder_id) 
        REFERENCES folders (id) ON DELETE SET NULL,
    INDEX idx_upload_sessions_status_expiry (status, expires_at),
    INDEX idx_upload_sessions_owner (owner_id, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- -----------------------------------------------------------------------------
-- 11. Table: event_outbox
-- Transactional Outbox table supporting asynchronous event publication and
-- graceful degradation during message broker (RabbitMQ) outages.
-- -----------------------------------------------------------------------------
CREATE TABLE event_outbox (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    event_type VARCHAR(64) NOT NULL COMMENT 'Event classification (e.g., file.uploaded, file.deleted)',
    routing_key VARCHAR(128) NOT NULL COMMENT 'AMQP topic routing key (e.g., file.uploaded.application.pdf)',
    payload JSON NOT NULL COMMENT 'Serialized event payload conforming to Draft-07 event schema',
    status ENUM('PENDING', 'PROCESSED', 'FAILED') NOT NULL DEFAULT 'PENDING' COMMENT 'Outbox relay publishing status',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP NULL DEFAULT NULL COMMENT 'Timestamp when event was successfully dispatched to broker',
    PRIMARY KEY (id),
    INDEX idx_event_outbox_status_created (status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
