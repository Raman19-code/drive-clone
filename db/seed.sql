-- Optional dev seed data
INSERT INTO users (email, password_hash) VALUES
  ('demo@drivex.dev', '$argon2id$replace-with-real-hash');

INSERT INTO folders (name, parent_id, owner_id) VALUES
  ('Root', NULL, 1);
