/**
 * push.ts — upload local analyst state to R2 and pull remote state from R2.
 *
 * Upload flow (per user, manual):
 *   1. Export vessel_reviews, vessel_reviews_audit, analyst_briefs to Parquet
 *      via DuckDB-WASM COPY TO (writes to the in-memory VFS).
 *   2. POST the Parquet buffers to /api/reviews/push (CF Pages Function).
 *   3. The Worker writes to reviews/<email>/*.parquet and updates
 *      reviews/index.json in R2.
 *
 * Pull flow (automatic on sync):
 *   1. Fetch reviews/index.json from the public R2 bucket.
 *   2. For each user listed, download their three Parquet files.
 *   3. Merge into the local DuckDB-WASM tables with the conflict strategies
 *      defined in issue #369:
 *        vessel_reviews      → last-write-wins on updated_at
 *        vessel_reviews_audit → append-only, dedup on (mmsi, changed_at)
 *        analyst_briefs      → last-write-wins on generated_at
 */

import type { AsyncDuckDB, AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
import { R2_BASE_URL } from "./opfs";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type PushStatus =
  | { phase: "idle" }
  | { phase: "exporting" }
  | { phase: "uploading" }
  | { phase: "done"; pushedAt: string }
  | { phase: "error"; message: string };

interface ReviewsIndex {
  users: { email: string; updatedAt: string }[];
}

// ---------------------------------------------------------------------------
// Push: local → R2
// ---------------------------------------------------------------------------

/**
 * Serialise the three analyst-state tables to Parquet and POST them to the
 * CF Pages Function at /api/reviews/push.  The function validates CF Access
 * auth and writes the files under reviews/<email>/ in R2.
 *
 * Throws on auth failure or network error so the caller can surface the
 * message via onStatus({ phase: "error", message }).
 */
export async function pushReviews(
  db: AsyncDuckDB,
  conn: AsyncDuckDBConnection,
  onStatus: (s: PushStatus) => void
): Promise<void> {
  onStatus({ phase: "exporting" });

  const exports = [
    { table: "vessel_reviews",       file: "_push_reviews.parquet", field: "reviews" },
    { table: "vessel_reviews_audit", file: "_push_audit.parquet",   field: "audit"   },
    { table: "analyst_briefs",       file: "_push_briefs.parquet",  field: "briefs"  },
  ] as const;

  const form = new FormData();
  for (const { table, file, field } of exports) {
    await conn.query(`COPY ${table} TO '${file}' (FORMAT PARQUET)`);
    const bytes = await db.copyFileToBuffer(file);
    await db.dropFile(file);
    form.append(field, new Blob([bytes.buffer as ArrayBuffer], { type: "application/octet-stream" }), `${field}.parquet`);
  }

  onStatus({ phase: "uploading" });

  const resp = await fetch("/api/reviews/push", {
    method: "POST",
    credentials: "include", // send CF_Authorization cookie
    body: form,
  });

  if (resp.status === 401) throw new Error("Sign in required to push changes");
  if (!resp.ok) {
    const text = await resp.text().catch(() => String(resp.status));
    throw new Error(`Push failed: ${text}`);
  }

  const { updatedAt } = (await resp.json()) as { updatedAt: string };
  onStatus({ phase: "done", pushedAt: updatedAt });
}

// ---------------------------------------------------------------------------
// Pull: R2 → local
// ---------------------------------------------------------------------------

/**
 * Fetch all users' review Parquet files from the public R2 bucket and merge
 * them into the local DuckDB-WASM tables.  Called automatically at the end
 * of each sync so every analyst sees the latest shared state.
 *
 * Returns the number of user prefixes successfully merged (0 = nothing to
 * pull or index not found yet).
 */
export async function pullRemoteReviews(
  db: AsyncDuckDB,
  conn: AsyncDuckDBConnection
): Promise<number> {
  let index: ReviewsIndex;
  try {
    const resp = await fetch(`${R2_BASE_URL}/reviews/index.json`, { cache: "no-store" });
    if (!resp.ok) return 0;
    index = (await resp.json()) as ReviewsIndex;
  } catch {
    return 0;
  }

  if (!index.users?.length) return 0;

  let merged = 0;
  for (const { email } of index.users) {
    const prefix = `${R2_BASE_URL}/reviews/${encodeURIComponent(email)}`;
    try {
      const [reviewsBuf, auditBuf, briefsBuf] = await Promise.all([
        fetchBuf(`${prefix}/reviews.parquet`),
        fetchBuf(`${prefix}/audit.parquet`),
        fetchBuf(`${prefix}/briefs.parquet`),
      ]);

      const tag = String(merged); // unique suffix per user to avoid VFS name collisions

      if (reviewsBuf) {
        const name = `_remote_reviews_${tag}.parquet`;
        await db.registerFileBuffer(name, new Uint8Array(reviewsBuf));
        await conn.query(`
          INSERT OR REPLACE INTO vessel_reviews (
            mmsi, decision_tier, handoff_state, reviewer_id, rationale,
            identifier_basis, outcome, outcome_notes, officer_id, created_at, updated_at
          )
          SELECT
            mmsi, decision_tier, handoff_state, reviewer_id, rationale,
            identifier_basis, outcome, outcome_notes, officer_id, created_at, updated_at
          FROM read_parquet('${name}') AS r
          WHERE r.updated_at >= COALESCE(
            (SELECT updated_at FROM vessel_reviews WHERE mmsi = r.mmsi),
            '1970-01-01'::TIMESTAMP
          )
        `);
        await db.dropFile(name);
      }

      if (auditBuf) {
        const name = `_remote_audit_${tag}.parquet`;
        await db.registerFileBuffer(name, new Uint8Array(auditBuf));
        await conn.query(`
          INSERT INTO vessel_reviews_audit (mmsi, changed_at, reviewer_id, from_state, to_state, rationale)
          SELECT mmsi, changed_at, reviewer_id, from_state, to_state, rationale
          FROM read_parquet('${name}') AS r
          WHERE NOT EXISTS (
            SELECT 1 FROM vessel_reviews_audit a
            WHERE a.mmsi = r.mmsi AND a.changed_at = r.changed_at
          )
        `);
        await db.dropFile(name);
      }

      if (briefsBuf) {
        const name = `_remote_briefs_${tag}.parquet`;
        await db.registerFileBuffer(name, new Uint8Array(briefsBuf));
        await conn.query(`
          INSERT OR REPLACE INTO analyst_briefs (mmsi, brief, generated_at)
          SELECT mmsi, brief, generated_at
          FROM read_parquet('${name}') AS r
          WHERE r.generated_at >= COALESCE(
            (SELECT generated_at FROM analyst_briefs WHERE mmsi = r.mmsi),
            '1970-01-01'::TIMESTAMP
          )
        `);
        await db.dropFile(name);
      }

      merged++;
    } catch (err) {
      console.warn(`[push] Failed to pull reviews for ${email}:`, err);
    }
  }

  return merged;
}

async function fetchBuf(url: string): Promise<ArrayBuffer | null> {
  try {
    const resp = await fetch(url, { credentials: "omit" });
    return resp.ok ? resp.arrayBuffer() : null;
  } catch {
    return null;
  }
}
