/**
 * Vessel review persistence — stored in a local DuckDB table (OPFS-backed).
 *
 * Schema is created on first use via initReviewSchema().  All state lives
 * in-browser; no backend API required.
 *
 * Tables
 * ------
 *   vessel_reviews        — one row per vessel, current state
 *   vessel_review_evidence — evidence references (many per vessel)
 *   vessel_reviews_audit  — append-only state-change log
 */

import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";

// ── Types ─────────────────────────────────────────────────────────────────────

export type DecisionTier =
  | "Confirmed"
  | "Probable"
  | "Suspect"
  | "Cleared"
  | "Inconclusive";

export type HandoffState =
  | "queued_review"
  | "in_review"
  | "handoff_recommended"
  | "handoff_accepted"
  | "handoff_completed"
  | "closed";

export type OutcomeValue = "Confirmed" | "Cleared" | "Inconclusive";

export interface EvidenceRef {
  id: string;
  mmsi: string;
  source: string;
  url: string;
  publication_date: string;
  credibility: "high" | "medium" | "weak";
}

export interface VesselReview {
  mmsi: string;
  decision_tier: DecisionTier | null;
  handoff_state: HandoffState;
  reviewer_id: string;
  rationale: string;
  identifier_basis: string;
  outcome: OutcomeValue | null;
  outcome_notes: string | null;
  officer_id: string | null;
  created_at: string;
  updated_at: string;
}

// ── Schema init ───────────────────────────────────────────────────────────────

export async function initReviewSchema(
  conn: AsyncDuckDBConnection
): Promise<void> {
  await conn.query(`
    CREATE TABLE IF NOT EXISTS vessel_reviews (
      mmsi              TEXT PRIMARY KEY,
      decision_tier     TEXT,
      handoff_state     TEXT NOT NULL DEFAULT 'queued_review',
      reviewer_id       TEXT NOT NULL DEFAULT '',
      rationale         TEXT NOT NULL DEFAULT '',
      identifier_basis  TEXT NOT NULL DEFAULT 'MMSI',
      created_at        TIMESTAMP DEFAULT now(),
      updated_at        TIMESTAMP DEFAULT now()
    )
  `);

  await conn.query(`
    CREATE TABLE IF NOT EXISTS vessel_review_evidence (
      id               TEXT PRIMARY KEY,
      mmsi             TEXT NOT NULL,
      source           TEXT NOT NULL DEFAULT '',
      url              TEXT NOT NULL DEFAULT '',
      publication_date TEXT NOT NULL DEFAULT '',
      credibility      TEXT NOT NULL DEFAULT 'medium',
      created_at       TIMESTAMP DEFAULT now()
    )
  `);

  // Migrate: add outcome columns to existing tables (no-op if already present)
  for (const col of [
    "ALTER TABLE vessel_reviews ADD COLUMN IF NOT EXISTS outcome TEXT",
    "ALTER TABLE vessel_reviews ADD COLUMN IF NOT EXISTS outcome_notes TEXT",
    "ALTER TABLE vessel_reviews ADD COLUMN IF NOT EXISTS officer_id TEXT",
  ]) {
    try { await conn.query(col); } catch { /* column already exists */ }
  }

  await conn.query(`
    CREATE TABLE IF NOT EXISTS vessel_reviews_audit (
      mmsi         TEXT NOT NULL,
      changed_at   TIMESTAMP DEFAULT now(),
      reviewer_id  TEXT NOT NULL DEFAULT '',
      from_state   TEXT,
      to_state     TEXT NOT NULL,
      rationale    TEXT NOT NULL DEFAULT ''
    )
  `);
}

// ── Read helpers ──────────────────────────────────────────────────────────────

export async function getReview(
  conn: AsyncDuckDBConnection,
  mmsi: string
): Promise<VesselReview | null> {
  const result = await conn.query(
    `SELECT * FROM vessel_reviews WHERE mmsi = '${esc(mmsi)}' LIMIT 1`
  );
  const rows = result.toArray();
  if (rows.length === 0) return null;
  return rows[0].toJSON() as VesselReview;
}

export async function getEvidence(
  conn: AsyncDuckDBConnection,
  mmsi: string
): Promise<EvidenceRef[]> {
  const result = await conn.query(
    `SELECT * FROM vessel_review_evidence WHERE mmsi = '${esc(mmsi)}' ORDER BY created_at`
  );
  return result.toArray().map((r) => r.toJSON() as EvidenceRef);
}

export async function getAuditLog(
  conn: AsyncDuckDBConnection,
  mmsi: string
): Promise<Array<{ changed_at: string; reviewer_id: string; from_state: string | null; to_state: string; rationale: string }>> {
  const result = await conn.query(
    `SELECT * FROM vessel_reviews_audit WHERE mmsi = '${esc(mmsi)}' ORDER BY changed_at DESC`
  );
  return result.toArray().map((r) => r.toJSON() as ReturnType<typeof getAuditLog> extends Promise<Array<infer T>> ? T : never);
}

/** Load review state for a list of MMSIs — used by WatchlistTable for badges. */
export async function getBulkReviewStates(
  conn: AsyncDuckDBConnection,
  mmsis: string[]
): Promise<Map<string, { decision_tier: DecisionTier | null; handoff_state: HandoffState }>> {
  if (mmsis.length === 0) return new Map();
  const list = mmsis.map((m) => `'${esc(m)}'`).join(",");
  const result = await conn.query(
    `SELECT mmsi, decision_tier, handoff_state FROM vessel_reviews WHERE mmsi IN (${list})`
  );
  const map = new Map<string, { decision_tier: DecisionTier | null; handoff_state: HandoffState }>();
  for (const row of result.toArray()) {
    const r = row.toJSON() as { mmsi: string; decision_tier: string | null; handoff_state: string };
    map.set(r.mmsi, {
      decision_tier: (r.decision_tier as DecisionTier) ?? null,
      handoff_state: (r.handoff_state as HandoffState) ?? "queued_review",
    });
  }
  return map;
}

// ── Write helpers ─────────────────────────────────────────────────────────────

export interface SaveReviewInput {
  mmsi: string;
  decision_tier: DecisionTier | null;
  handoff_state: HandoffState;
  reviewer_id: string;
  rationale: string;
  identifier_basis: string;
  evidence: Array<Omit<EvidenceRef, "id" | "mmsi" | "created_at"> & { id?: string }>;
}

export async function saveReview(
  conn: AsyncDuckDBConnection,
  input: SaveReviewInput
): Promise<void> {
  const existing = await getReview(conn, input.mmsi);
  const tier = input.decision_tier ? `'${esc(input.decision_tier)}'` : "NULL";

  if (existing) {
    // Append audit row for the state transition
    await conn.query(`
      INSERT INTO vessel_reviews_audit (mmsi, reviewer_id, from_state, to_state, rationale)
      VALUES (
        '${esc(input.mmsi)}',
        '${esc(input.reviewer_id)}',
        ${existing.handoff_state ? `'${esc(existing.handoff_state)}'` : "NULL"},
        '${esc(input.handoff_state)}',
        '${esc(input.rationale)}'
      )
    `);
    await conn.query(`
      UPDATE vessel_reviews SET
        decision_tier    = ${tier},
        handoff_state    = '${esc(input.handoff_state)}',
        reviewer_id      = '${esc(input.reviewer_id)}',
        rationale        = '${esc(input.rationale)}',
        identifier_basis = '${esc(input.identifier_basis)}',
        updated_at       = now()
      WHERE mmsi = '${esc(input.mmsi)}'
    `);
  } else {
    await conn.query(`
      INSERT INTO vessel_reviews (mmsi, decision_tier, handoff_state, reviewer_id, rationale, identifier_basis)
      VALUES (
        '${esc(input.mmsi)}',
        ${tier},
        '${esc(input.handoff_state)}',
        '${esc(input.reviewer_id)}',
        '${esc(input.rationale)}',
        '${esc(input.identifier_basis)}'
      )
    `);
    await conn.query(`
      INSERT INTO vessel_reviews_audit (mmsi, reviewer_id, from_state, to_state, rationale)
      VALUES ('${esc(input.mmsi)}', '${esc(input.reviewer_id)}', NULL, '${esc(input.handoff_state)}', '${esc(input.rationale)}')
    `);
  }

  // Replace evidence refs: delete existing, insert new
  await conn.query(`DELETE FROM vessel_review_evidence WHERE mmsi = '${esc(input.mmsi)}'`);
  for (const ev of input.evidence) {
    const id = ev.id ?? crypto.randomUUID();
    await conn.query(`
      INSERT INTO vessel_review_evidence (id, mmsi, source, url, publication_date, credibility)
      VALUES (
        '${esc(id)}',
        '${esc(input.mmsi)}',
        '${esc(ev.source)}',
        '${esc(ev.url)}',
        '${esc(ev.publication_date)}',
        '${esc(ev.credibility)}'
      )
    `);
  }
}

export interface SaveOutcomeInput {
  mmsi: string;
  outcome: OutcomeValue;
  outcome_notes: string;
  officer_id: string;
}

/** Log patrol outcome for a handoff_completed vessel and transition to closed. */
export async function saveOutcome(
  conn: AsyncDuckDBConnection,
  input: SaveOutcomeInput
): Promise<void> {
  const existing = await getReview(conn, input.mmsi);
  const fromState = existing?.handoff_state ?? null;

  await conn.query(`
    INSERT INTO vessel_reviews_audit (mmsi, reviewer_id, from_state, to_state, rationale)
    VALUES (
      '${esc(input.mmsi)}',
      '${esc(input.officer_id)}',
      ${fromState ? `'${esc(fromState)}'` : "NULL"},
      'closed',
      '${esc(`Outcome: ${input.outcome}. ${input.outcome_notes}`)}'
    )
  `);

  if (existing) {
    await conn.query(`
      UPDATE vessel_reviews SET
        outcome        = '${esc(input.outcome)}',
        outcome_notes  = '${esc(input.outcome_notes)}',
        officer_id     = '${esc(input.officer_id)}',
        handoff_state  = 'closed',
        updated_at     = now()
      WHERE mmsi = '${esc(input.mmsi)}'
    `);
  } else {
    await conn.query(`
      INSERT INTO vessel_reviews (mmsi, handoff_state, reviewer_id, rationale, identifier_basis, outcome, outcome_notes, officer_id)
      VALUES (
        '${esc(input.mmsi)}', 'closed', '${esc(input.officer_id)}', '', 'MMSI',
        '${esc(input.outcome)}', '${esc(input.outcome_notes)}', '${esc(input.officer_id)}'
      )
    `);
  }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/** Escape single quotes for SQL string literals. */
function esc(s: string): string {
  return s.replace(/'/g, "''");
}

// ── Tier / state metadata ─────────────────────────────────────────────────────

export const TIERS: DecisionTier[] = [
  "Confirmed",
  "Probable",
  "Suspect",
  "Cleared",
  "Inconclusive",
];

export const HANDOFF_STATES: HandoffState[] = [
  "queued_review",
  "in_review",
  "handoff_recommended",
  "handoff_accepted",
  "handoff_completed",
  "closed",
];

export function tierColor(tier: DecisionTier | null): string {
  switch (tier) {
    case "Confirmed":    return "#fc8181";
    case "Probable":     return "#f6ad55";
    case "Suspect":      return "#fbd38d";
    case "Cleared":      return "#68d391";
    case "Inconclusive": return "#718096";
    default:             return "#4a5568";
  }
}

export function handoffLabel(state: HandoffState): string {
  return state.replace(/_/g, " ");
}

/** Validate transition: returns an error string or null if valid. */
export function validateTransition(
  _from: HandoffState | undefined,
  to: HandoffState,
  tier: DecisionTier | null,
  rationale: string
): string | null {
  if (to === "handoff_recommended" && !rationale.trim()) {
    return "Rationale is required before recommending handoff.";
  }
  if (to === "handoff_recommended" && !tier) {
    return "A decision tier must be set before recommending handoff.";
  }
  return null;
}
