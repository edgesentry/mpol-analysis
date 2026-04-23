/**
 * push.ts — apply server-merged public reviews on sync.
 *
 * Pushing analyst reviews to R2 and merging them is a commercial feature.
 * OSS users receive the already-merged public reviews via syncAndLoad() and
 * apply them locally with mergeDownloadedReviews().
 */

import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
import { isParquetRegistered } from "./duckdb";

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
 * If the manifest does not yet contain these files (no reviews published yet)
 * the queries fail silently and 0 is returned.
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
