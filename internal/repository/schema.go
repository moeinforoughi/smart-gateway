package repository

// schemaV1 is the initial schema (FR-300 series).
const schemaV1 = `
CREATE TABLE IF NOT EXISTS subscriptions (
	id                TEXT PRIMARY KEY,
	url               TEXT NOT NULL UNIQUE,
	name              TEXT NOT NULL DEFAULT '',
	provider          TEXT NOT NULL DEFAULT '',
	priority          INTEGER NOT NULL DEFAULT 0,
	tags              TEXT NOT NULL DEFAULT '[]',
	enabled           INTEGER NOT NULL DEFAULT 1,
	headers           TEXT NOT NULL DEFAULT '{}',
	update_every      TEXT NOT NULL DEFAULT '',
	timeout           TEXT NOT NULL DEFAULT '',
	etag              TEXT NOT NULL DEFAULT '',
	last_modified     TEXT NOT NULL DEFAULT '',
	state             TEXT NOT NULL DEFAULT 'active',
	consecutive_failures INTEGER NOT NULL DEFAULT 0,
	last_fetch_at     TEXT,
	next_fetch_at     TEXT,
	node_count        INTEGER NOT NULL DEFAULT 0,
	last_error        TEXT NOT NULL DEFAULT '',
	created_at        TEXT NOT NULL,
	updated_at        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes (
	id               TEXT PRIMARY KEY,
	dedupe_hash      TEXT NOT NULL UNIQUE,
	subscription_id  TEXT NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
	protocol         TEXT NOT NULL,
	server           TEXT NOT NULL,
	port             INTEGER NOT NULL,
	name             TEXT NOT NULL DEFAULT '',
	transport        TEXT NOT NULL DEFAULT '',
	tls              INTEGER NOT NULL DEFAULT 0,
	sni              TEXT NOT NULL DEFAULT '',
	fingerprint      TEXT NOT NULL DEFAULT '',
	host             TEXT NOT NULL DEFAULT '',
	path             TEXT NOT NULL DEFAULT '',
	service_name     TEXT NOT NULL DEFAULT '',
	flow             TEXT NOT NULL DEFAULT '',
	params           TEXT NOT NULL DEFAULT '{}',
	raw              TEXT NOT NULL DEFAULT '',
	tags             TEXT NOT NULL DEFAULT '[]',
	state            TEXT NOT NULL DEFAULT 'unknown',
	state_changed_at TEXT,
	created_at       TEXT NOT NULL,
	updated_at       TEXT NOT NULL,
	deleted_at       TEXT
);
CREATE INDEX IF NOT EXISTS idx_nodes_dedupe ON nodes(dedupe_hash);
CREATE INDEX IF NOT EXISTS idx_nodes_sub ON nodes(subscription_id);
CREATE INDEX IF NOT EXISTS idx_nodes_server ON nodes(server, port);

CREATE TABLE IF NOT EXISTS node_measurements (
	node_id    TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
	probed_at  TEXT NOT NULL,
	latency_ms INTEGER NOT NULL DEFAULT 0,
	success    INTEGER NOT NULL DEFAULT 1,
	PRIMARY KEY (node_id, probed_at)
);
CREATE INDEX IF NOT EXISTS idx_measurements_node ON node_measurements(node_id);

CREATE TABLE IF NOT EXISTS switch_log (
	id         TEXT PRIMARY KEY,
	at         TEXT NOT NULL,
	from_id    TEXT NOT NULL DEFAULT '',
	to_id      TEXT NOT NULL,
	reason     TEXT NOT NULL DEFAULT '',
	prev_score REAL NOT NULL DEFAULT 0,
	new_score  REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_switch_log_at ON switch_log(at DESC);
`

// schemaV2 adds passive-throughput records (M3). Folded into GetNodeHealth so
// the scorer sees measured bandwidth even between speed tests.
const schemaV2 = `
CREATE TABLE IF NOT EXISTS node_throughput (
	node_id     TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
	measured_at TEXT NOT NULL,
	bytes_per_sec REAL NOT NULL DEFAULT 0,
	PRIMARY KEY (node_id, measured_at)
);
CREATE INDEX IF NOT EXISTS idx_throughput_node ON node_throughput(node_id);
`

// schemaV3 adds the materialized per-node scores and their history (M4,
// FR-607/608). node_scores is keyed by node so TopEligible runs as an indexed
// query over 10k nodes (< 1s, NFR-044); score_history keeps every evaluation
// for auditability (FR-608).
const schemaV3 = `
CREATE TABLE IF NOT EXISTS node_scores (
	node_id      TEXT PRIMARY KEY REFERENCES nodes(id) ON DELETE CASCADE,
	total        REAL NOT NULL DEFAULT 0,
	latency      REAL NOT NULL DEFAULT 0,
	stability    REAL NOT NULL DEFAULT 0,
	throughput   REAL NOT NULL DEFAULT 0,
	loss         REAL NOT NULL DEFAULT 0,
	confidence   INTEGER NOT NULL DEFAULT 0,
	eligible     INTEGER NOT NULL DEFAULT 0,
	state        TEXT NOT NULL DEFAULT 'calculated',
	evaluated_at TEXT,
	reason       TEXT NOT NULL DEFAULT '',
	updated_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_scores_total ON node_scores(eligible, total DESC, node_id ASC);

CREATE TABLE IF NOT EXISTS score_history (
	node_id      TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
	evaluated_at TEXT NOT NULL,
	total        REAL NOT NULL DEFAULT 0,
	latency      REAL NOT NULL DEFAULT 0,
	stability    REAL NOT NULL DEFAULT 0,
	throughput   REAL NOT NULL DEFAULT 0,
	loss         REAL NOT NULL DEFAULT 0,
	confidence   INTEGER NOT NULL DEFAULT 0,
	eligible     INTEGER NOT NULL DEFAULT 0,
	state        TEXT NOT NULL DEFAULT 'calculated',
	reason       TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (node_id, evaluated_at)
);
CREATE INDEX IF NOT EXISTS idx_score_history_node ON score_history(node_id, evaluated_at DESC);
`
