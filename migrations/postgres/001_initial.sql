CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    google_sub TEXT UNIQUE,
    account_type TEXT NOT NULL CHECK(account_type IN ('HUMAN', 'AI')),
    current_chips INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    last_daily_credit_date TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    token TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    FOREIGN KEY(user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS oauth_states (
    state TEXT PRIMARY KEY,
    redirect_to TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS phone_verification_challenges (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    phone_number TEXT NOT NULL,
    provider TEXT NOT NULL,
    provider_sid TEXT,
    otp_code TEXT,
    status TEXT NOT NULL CHECK(status IN ('PENDING', 'VERIFIED', 'EXPIRED')),
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    verified_at TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY(user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS user_phones (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    phone_number TEXT NOT NULL UNIQUE,
    verified_at TEXT NOT NULL,
    FOREIGN KEY(user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS chip_ledger (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    cycle_id TEXT,
    event_type TEXT NOT NULL,
    chips_delta INTEGER NOT NULL,
    metadata TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY(user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS cycles (
    id TEXT PRIMARY KEY,
    cycle_date TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('OPEN', 'SETTLED')),
    opened_at TEXT NOT NULL,
    closed_at TEXT
);

CREATE TABLE IF NOT EXISTS candidate_links (
    id TEXT PRIMARY KEY,
    cycle_id TEXT NOT NULL,
    submitted_by_user_id TEXT NOT NULL,
    original_url TEXT NOT NULL,
    canonical_url TEXT NOT NULL,
    domain TEXT NOT NULL,
    title TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(cycle_id, canonical_url),
    FOREIGN KEY(cycle_id) REFERENCES cycles(id),
    FOREIGN KEY(submitted_by_user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS picks (
    id TEXT PRIMARY KEY,
    cycle_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    rank INTEGER NOT NULL,
    picked_at TEXT NOT NULL,
    UNIQUE(cycle_id, user_id, rank),
    UNIQUE(cycle_id, user_id, candidate_id),
    FOREIGN KEY(cycle_id) REFERENCES cycles(id),
    FOREIGN KEY(user_id) REFERENCES users(id),
    FOREIGN KEY(candidate_id) REFERENCES candidate_links(id)
);

CREATE TABLE IF NOT EXISTS cycle_results (
    cycle_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    is_winner INTEGER NOT NULL,
    PRIMARY KEY(cycle_id, candidate_id),
    FOREIGN KEY(cycle_id) REFERENCES cycles(id),
    FOREIGN KEY(candidate_id) REFERENCES candidate_links(id)
);

CREATE TABLE IF NOT EXISTS click_events (
    id TEXT PRIMARY KEY,
    cycle_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    clicked_by_user_id TEXT,
    fingerprint_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(candidate_id, fingerprint_hash),
    FOREIGN KEY(cycle_id) REFERENCES cycles(id),
    FOREIGN KEY(candidate_id) REFERENCES candidate_links(id),
    FOREIGN KEY(clicked_by_user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS curation_rewards (
    cycle_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    rank INTEGER NOT NULL,
    unique_clicks INTEGER NOT NULL,
    reward_chips INTEGER NOT NULL,
    awarded_at TEXT NOT NULL,
    PRIMARY KEY(cycle_id, user_id),
    FOREIGN KEY(cycle_id) REFERENCES cycles(id),
    FOREIGN KEY(user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS model_predictions (
    cycle_id TEXT NOT NULL,
    model_user_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    probability REAL NOT NULL,
    explanation TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(cycle_id, model_user_id, candidate_id),
    FOREIGN KEY(cycle_id) REFERENCES cycles(id),
    FOREIGN KEY(model_user_id) REFERENCES users(id),
    FOREIGN KEY(candidate_id) REFERENCES candidate_links(id)
);

CREATE TABLE IF NOT EXISTS source_posts (
    id TEXT PRIMARY KEY,
    source_post_url TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    published_at TEXT NOT NULL,
    extracted_links_json TEXT NOT NULL,
    processed_at TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS archive_links (
    id TEXT PRIMARY KEY,
    post_date TEXT NOT NULL,
    url TEXT NOT NULL,
    canonical_url TEXT NOT NULL,
    domain TEXT NOT NULL,
    title TEXT NOT NULL,
    source_post_url TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(post_date, canonical_url)
);

CREATE TABLE IF NOT EXISTS job_runs (
    id TEXT PRIMARY KEY,
    job_name TEXT NOT NULL,
    run_key TEXT NOT NULL,
    status TEXT NOT NULL,
    details_json TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(job_name, run_key)
);

CREATE INDEX IF NOT EXISTS idx_cycles_status ON cycles(status);
CREATE INDEX IF NOT EXISTS idx_candidate_cycle ON candidate_links(cycle_id);
CREATE INDEX IF NOT EXISTS idx_picks_cycle ON picks(cycle_id);
CREATE INDEX IF NOT EXISTS idx_ledger_user ON chip_ledger(user_id);
CREATE INDEX IF NOT EXISTS idx_archive_domain ON archive_links(domain);
CREATE INDEX IF NOT EXISTS idx_user_phones_phone ON user_phones(phone_number);
CREATE INDEX IF NOT EXISTS idx_clicks_cycle_candidate ON click_events(cycle_id, candidate_id);
CREATE INDEX IF NOT EXISTS idx_source_posts_published ON source_posts(published_at);
CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at);
