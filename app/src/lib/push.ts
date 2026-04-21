/**
 * push.ts — upload local analyst state to R2 and apply server-merged state on sync.
 *
 * Upload flow (per user, manual):
 *   1. Export vessel_reviews, vessel_reviews_audit, analyst_briefs to Parquet
 *      via DuckDB-WASM COPY TO.
 *   2. POST the buffers to /api/reviews/push (CF Pages Function).
 *   3. The Worker writes reviews/<email>/*.parquet to R2 and enqueues a merge
 *      job to the CF Queue.
 *   4. The queue consumer calls POST /api/reviews/merge on the Python pipeline
 *      server, which runs sync_r2.py merge-reviews and patches the manifest.
 *
 * Pull / apply flow (automatic on every sync):
 *   1. syncAndLoad() downloads reviews_merged.parquet, reviews_audit_merged.parquet,
 *      and reviews_briefs_merged.parquet when the manifest reports new size_bytes.
 *   2. mergeDownloadedReviews() applies them to the local DuckDB-WASM tables
 *      using the same conflict strategies (last-write-wins / append-only dedup).
 *
 * No client-side index.json fetching or per-user file downloads — all merging
 * happens server-side in the Python pipeline.
 */

import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
import type { AsyncDuckDB } from "@duckdb/duckdb-wasm";
import type { AppConfig } from "./config";
import { isParquetRegistered } from "./duckdb";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type PushStatus =
  | { phase: "idle" }
  | { phase: "exporting" }
  | { phase: "uploading" }
  | { phase: "done"; pushedAt: string }
  | { phase: "error"; message: string };

// ---------------------------------------------------------------------------
// Push: local → R2
// ---------------------------------------------------------------------------

/**
 * Serialise the three analyst-state tables to Parquet and POST them to
 * /api/reviews/push.  The CF Pages Function validates auth (CF Access JWT),
 * writes the files to R2, and enqueues a merge job.
 *
 * Throws on auth failure or network error so the caller can surface the
 * message via onStatus({ phase: "error", message }).
 */
export async function pushReviews(
  db: AsyncDuckDB,
  conn: AsyncDuckDBConnection,
  config: AppConfig,
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
    // DuckDB safe-writes via a tmp_ staging file before renaming to the final path.
    // Pre-register both so the WASM layer doesn't log "Buffering missing file".
    await db.registerFileBuffer(`tmp_${file}`, new Uint8Array(0));
    await db.registerFileBuffer(file, new Uint8Array(0));
    await conn.query(`COPY ${table} TO '${file}' (FORMAT PARQUET)`);
    const bytes = await db.copyFileToBuffer(file);
    await db.dropFile(file);
    await db.dropFile(`tmp_${file}`).catch(() => {});
    form.append(
      field,
      new Blob([bytes.buffer as ArrayBuffer], { type: "application/octet-stream" }),
      `${field}.parquet`
    );
  }

  onStatus({ phase: "uploading" });

  const resp = await fetch(pushEndpointUrl(config), {
    method: "POST",
    credentials: "include",
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
// Helpers
// ---------------------------------------------------------------------------

/**
 * For cloudflare-access deployments, push through the private Worker which
 * has CF Access enabled — the email header is reliably injected there.
 * The Pages Function route (/api/reviews/push) lacks CF Access protection
 * and always returns 401 because the header is never injected.
 */
function pushEndpointUrl(config: AppConfig): string {
  if (config.authProvider === "cloudflare-access" && config.privateManifestUrl) {
    const origin = new URL(config.privateManifestUrl).origin;
    return `${origin}/push-reviews`;
  }
  return "/api/reviews/push";
}

// ---------------------------------------------------------------------------
// Apply: merge server-merged Parquet files into local DuckDB
// ---------------------------------------------------------------------------

/**
 * Apply the server-merged review files that syncAndLoad() just downloaded into
 * the local DuckDB-WASM tables.  Called automatically after every sync.
 *
 * The files are registered by syncAndLoad() as:
 *   reviews_merged.parquet        → vessel_reviews
 *   reviews_audit_merged.parquet  → vessel_reviews_audit
 *   reviews_briefs_merged.parquet → analyst_briefs
 *
 * If the manifest does not yet contain these files (no reviews have ever been
 * pushed) the queries fail silently and 0 is returned.
 *
 * Returns the number of merge operations that succeeded (0–3).
 */
export async function mergeDownloadedReviews(
  conn: AsyncDuckDBConnection
): Promise<number> {
  let applied = 0;

  const ops: Array<{ file: string; sql: string }> = [
    {
      file: "reviews_merged.parquet",
      sql: `
        INSERT OR REPLACE INTO vessel_reviews (
          mmsi, decision_tier, handoff_state, reviewer_id, rationale,
          identifier_basis, outcome, outcome_notes, officer_id, created_at, updated_at
        )
        SELECT
          mmsi, decision_tier, handoff_state, reviewer_id, rationale,
          identifier_basis, outcome, outcome_notes, officer_id, created_at, updated_at
        FROM read_parquet('reviews_merged.parquet') AS r
        WHERE r.updated_at >= COALESCE(
          (SELECT updated_at FROM vessel_reviews WHERE mmsi = r.mmsi),
          '1970-01-01'::TIMESTAMP
        )
      `,
    },
    {
      file: "reviews_audit_merged.parquet",
      sql: `
        INSERT INTO vessel_reviews_audit (mmsi, changed_at, reviewer_id, from_state, to_state, rationale)
        SELECT mmsi, changed_at, reviewer_id, from_state, to_state, rationale
        FROM read_parquet('reviews_audit_merged.parquet') AS r
        WHERE NOT EXISTS (
          SELECT 1 FROM vessel_reviews_audit a
          WHERE a.mmsi = r.mmsi AND a.changed_at = r.changed_at
        )
      `,
    },
    {
      file: "reviews_briefs_merged.parquet",
      sql: `
        INSERT OR REPLACE INTO analyst_briefs (mmsi, brief, generated_at)
        SELECT mmsi, brief, generated_at
        FROM read_parquet('reviews_briefs_merged.parquet') AS r
        WHERE r.generated_at >= COALESCE(
          (SELECT generated_at FROM analyst_briefs WHERE mmsi = r.mmsi),
          '1970-01-01'::TIMESTAMP
        )
      `,
    },
  ];

  for (const { file, sql } of ops) {
    if (!isParquetRegistered(file)) continue;
    try {
      await conn.query(sql);
      applied++;
    } catch {
      // unexpected error — skip silently
    }
  }

  return applied;
}
