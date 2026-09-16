CREATE TABLE users (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  email VARCHAR(255) UNIQUE NOT NULL,
  password_hash VARCHAR(255) NOT NULL,
  storage_quota_bytes BIGINT NOT NULL DEFAULT 16106127360,
  storage_used_bytes BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE folders (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,
  parent_id BIGINT NULL REFERENCES folders(id),
  owner_id BIGINT NOT NULL REFERENCES users(id),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_parent_owner (parent_id, owner_id)
);

CREATE TABLE files (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(255) NOT NULL,
  folder_id BIGINT NULL REFERENCES folders(id),
  owner_id BIGINT NOT NULL REFERENCES users(id),
  mime_type VARCHAR(127),
  size_bytes BIGINT NOT NULL,
  storage_key VARCHAR(512) NOT NULL,
  current_version_id BIGINT NULL,
  is_trashed BOOLEAN DEFAULT FALSE,
  trashed_at TIMESTAMP NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_folder_owner (folder_id, owner_id),
  FULLTEXT INDEX idx_filename (name)
);

CREATE TABLE file_versions (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  file_id BIGINT NOT NULL REFERENCES files(id),
  storage_key VARCHAR(512) NOT NULL,
  size_bytes BIGINT NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE permissions (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  resource_type ENUM('file','folder') NOT NULL,
  resource_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL REFERENCES users(id),
  role ENUM('viewer','editor','owner') NOT NULL,
  granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_resource_user (resource_type, resource_id, user_id)
);

CREATE TABLE share_links (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  resource_type ENUM('file','folder') NOT NULL,
  resource_id BIGINT NOT NULL,
  token VARCHAR(64) UNIQUE NOT NULL,
  permission_role ENUM('viewer','editor') NOT NULL DEFAULT 'viewer',
  expires_at TIMESTAMP NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE audit_log (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  user_id BIGINT NOT NULL,
  action VARCHAR(64) NOT NULL,
  resource_type ENUM('file','folder') NOT NULL,
  resource_id BIGINT NOT NULL,
  metadata JSON NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_user_time (user_id, created_at)
);
