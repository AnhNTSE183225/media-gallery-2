-- Phase 1 schema for media-gallery-2
-- PostgreSQL only. Raw SQL. Monolith-first.

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE IF NOT EXISTS profiles (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  root_directory TEXT NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_profiles_single_active
  ON profiles ((is_active))
  WHERE is_active = TRUE;

CREATE TABLE IF NOT EXISTS profile_allowed_tags (
  profile_id BIGINT NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  tag TEXT NOT NULL,
  PRIMARY KEY (profile_id, tag)
);

CREATE TABLE IF NOT EXISTS profile_allowed_extensions (
  profile_id BIGINT NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  extension TEXT NOT NULL,
  PRIMARY KEY (profile_id, extension)
);

-- kind: 1=image, 2=story
CREATE TABLE IF NOT EXISTS assets (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  profile_id BIGINT NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  kind SMALLINT NOT NULL CHECK (kind IN (1, 2)),
  artist TEXT NOT NULL,
  name TEXT NOT NULL,
  source_path TEXT NOT NULL,
  story_pages TEXT[] NULL,
  mime TEXT NULL,
  byte_size BIGINT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (profile_id, source_path)
);

CREATE INDEX IF NOT EXISTS idx_assets_profile_artist_name
  ON assets (profile_id, artist, name);

CREATE INDEX IF NOT EXISTS idx_assets_profile_kind
  ON assets (profile_id, kind);

CREATE INDEX IF NOT EXISTS idx_assets_text_trgm
  ON assets USING GIN ((artist || ' ' || name) gin_trgm_ops);

CREATE TABLE IF NOT EXISTS asset_tags (
  asset_id BIGINT NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
  tag TEXT NOT NULL,
  PRIMARY KEY (asset_id, tag)
);

CREATE INDEX IF NOT EXISTS idx_asset_tags_tag
  ON asset_tags (tag);

CREATE INDEX IF NOT EXISTS idx_asset_tags_tag_lower
  ON asset_tags ((LOWER(tag)));

CREATE TABLE IF NOT EXISTS scan_runs (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  profile_id BIGINT NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  trigger TEXT NOT NULL CHECK (trigger IN ('startup', 'manual', 'profile-switch')),
  status TEXT NOT NULL CHECK (status IN ('running', 'complete', 'error')),
  processed_artists INTEGER NOT NULL DEFAULT 0,
  total_artists INTEGER NOT NULL DEFAULT 0,
  current_artist TEXT NOT NULL DEFAULT '',
  error_message TEXT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_scan_runs_profile_started
  ON scan_runs (profile_id, started_at DESC);

CREATE TABLE IF NOT EXISTS scan_checkpoints (
  profile_id BIGINT PRIMARY KEY REFERENCES profiles(id) ON DELETE CASCADE,
  root_mtime_epoch_ms BIGINT NOT NULL DEFAULT 0,
  indexed_file_count INTEGER NOT NULL DEFAULT 0,
  sampled_hash TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS thumbnail_cache (
  asset_id BIGINT NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
  page_index INTEGER NOT NULL DEFAULT 0,
  variant TEXT NOT NULL,
  cache_key TEXT NOT NULL UNIQUE,
  mime TEXT NOT NULL,
  width INTEGER NOT NULL,
  height INTEGER NOT NULL,
  byte_size BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (asset_id, page_index, variant)
);
