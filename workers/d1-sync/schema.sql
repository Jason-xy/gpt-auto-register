CREATE TABLE IF NOT EXISTS accounts (
  email TEXT PRIMARY KEY,
  password TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (unixepoch()),
  updated_at INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE INDEX IF NOT EXISTS idx_accounts_updated_at ON accounts(updated_at DESC);

PRAGMA optimize;
