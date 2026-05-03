/**
 * Investigation session store — persisted in DuckDB (OPFS-backed).
 *
 * One session per vessel (keyed by MMSI). Each session progresses through
 * the structured OSINT investigation workflow:
 *   triage → osint_notes (analyst input) → synthesis → briefing → approved
 */

import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";

function esc(s: string): string {
  return s.replace(/'/g, "''");
}

export interface InvestigationSession {
  mmsi: string;
  triage: string | null;
  osint_notes: string | null;
  synthesis: string | null;
  briefing: string | null;
  approved: boolean;
  updated_at: string | null;
}

export async function initInvestigationStore(conn: AsyncDuckDBConnection): Promise<void> {
  await conn.query(`
    CREATE TABLE IF NOT EXISTS investigation_sessions (
      mmsi        TEXT PRIMARY KEY,
      triage      TEXT,
      osint_notes TEXT,
      synthesis   TEXT,
      briefing    TEXT,
      approved    BOOLEAN DEFAULT false,
      updated_at  TIMESTAMP DEFAULT now()
    )
  `);
}

export async function getInvestigationSession(
  conn: AsyncDuckDBConnection,
  mmsi: string
): Promise<InvestigationSession | null> {
  try {
    const result = await conn.query(
      `SELECT mmsi, triage, osint_notes, synthesis, briefing, approved,
              CAST(updated_at AS VARCHAR) AS updated_at
       FROM investigation_sessions WHERE mmsi = '${esc(mmsi)}' LIMIT 1`
    );
    const rows = result.toArray();
    if (rows.length === 0) return null;
    return rows[0].toJSON() as InvestigationSession;
  } catch {
    return null;
  }
}

export async function saveInvestigationField(
  conn: AsyncDuckDBConnection,
  mmsi: string,
  field: "triage" | "osint_notes" | "synthesis" | "briefing" | "approved",
  value: string | boolean
): Promise<void> {
  const escaped = typeof value === "boolean" ? (value ? "true" : "false") : `'${esc(String(value))}'`;
  await conn.query(`
    INSERT INTO investigation_sessions (mmsi, ${field}, updated_at)
    VALUES ('${esc(mmsi)}', ${escaped}, now())
    ON CONFLICT (mmsi) DO UPDATE SET ${field} = excluded.${field}, updated_at = now()
  `);
}

export async function resetInvestigationSession(
  conn: AsyncDuckDBConnection,
  mmsi: string
): Promise<void> {
  await conn.query(`DELETE FROM investigation_sessions WHERE mmsi = '${esc(mmsi)}'`);
}
