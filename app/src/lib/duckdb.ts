/**
 * DuckDB-WASM initialisation and query helpers.
 *
 * DuckDB-WASM runs entirely in-browser via WebAssembly.  Parquet files are
 * registered as in-memory buffers (sourced from OPFS or direct R2 fetch) and
 * queried with standard SQL.
 *
 * Threading notes
 * ---------------
 * SharedArrayBuffer is required for the multi-threaded (eh) bundle.  Both COOP
 * (`Cross-Origin-Opener-Policy: same-origin`) and COEP (`Cross-Origin-Embedder-Policy:
 * require-corp`) headers must be set — see `vite.config.ts` (dev) and
 * `public/_headers` (Cloudflare Pages prod).
 *
 * If SharedArrayBuffer is not available the single-threaded (mvp) bundle is
 * used automatically via `selectBundle`.
 */

import * as duckdb from "@duckdb/duckdb-wasm";

// Use DuckDB-WASM's built-in jsDelivr CDN bundle URLs to avoid bundling
// 30-40 MB WASM files into the Cloudflare Pages deployment (25 MB/file limit).
// Workers must be same-origin, so we wrap the CDN worker in a blob URL.
const BUNDLES = duckdb.getJsDelivrBundles();

let _db: duckdb.AsyncDuckDB | null = null;
let _conn: duckdb.AsyncDuckDBConnection | null = null;
const _registeredFiles = new Set<string>();

/** Initialise DuckDB-WASM once; subsequent calls return the cached instance. */
export async function initDuckDB(): Promise<{
  db: duckdb.AsyncDuckDB;
  conn: duckdb.AsyncDuckDBConnection;
}> {
  if (_db && _conn) return { db: _db, conn: _conn };

  const bundle = await duckdb.selectBundle(BUNDLES);
  // Workers must be same-origin. Wrap the CDN worker URL in a blob so the
  // browser treats it as same-origin, then importScripts fetches the real code.
  const workerBlob = new Blob(
    [`importScripts("${bundle.mainWorker!}");`],
    { type: "text/javascript" }
  );
  const workerUrl = URL.createObjectURL(workerBlob);
  const worker = new Worker(workerUrl);
  URL.revokeObjectURL(workerUrl);
  const logger = new duckdb.VoidLogger();
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

  _db = db;
  _conn = await db.connect();
  return { db, conn: _conn };
}

/** Register an ArrayBuffer as a named file in DuckDB's virtual filesystem. */
export async function registerParquet(
  db: duckdb.AsyncDuckDB,
  name: string,
  buffer: ArrayBuffer
): Promise<void> {
  await db.registerFileBuffer(name, new Uint8Array(buffer));
  _registeredFiles.add(name);
}

/** Returns true if the named Parquet file has been registered. */
export function isParquetRegistered(name: string): boolean {
  return _registeredFiles.has(name);
}

// ---------------------------------------------------------------------------
// Query helpers
// ---------------------------------------------------------------------------

export interface VesselRow {
  mmsi: string;
  imo: string | null;
  vessel_name: string;
  flag: string;
  vessel_type: string;
  confidence: number;
  last_lat: number | null;
  last_lon: number | null;
  last_seen: string | null;
  region: string;
  top_signals: string | null;
  ais_gap_count_30d: number | null;
  sts_candidate_count: number | null;
}

export interface MetricsRow {
  [key: string]: number | string | null;
}

/**
 * Query the watchlist from registered Parquet files.
 *
 * Expects `watchlist.parquet` to be registered in the DuckDB VFS beforehand
 * via `registerParquet`.
 */
/** Check whether a column exists in watchlist.parquet (cached per session). */
const _watchlistHasCol: Record<string, boolean | null> = {};
async function watchlistHasCol(conn: duckdb.AsyncDuckDBConnection, col: string): Promise<boolean> {
  if (_watchlistHasCol[col] != null) return _watchlistHasCol[col]!;
  try {
    await conn.query(`SELECT ${col} FROM read_parquet('watchlist.parquet') LIMIT 0`);
    _watchlistHasCol[col] = true;
  } catch {
    _watchlistHasCol[col] = false;
  }
  return _watchlistHasCol[col]!;
}

export async function queryWatchlist(
  conn: duckdb.AsyncDuckDBConnection,
  opts: { minConfidence?: number; regions?: string[] } = {}
): Promise<VesselRow[]> {
  const { minConfidence = 0, regions } = opts;
  const [hasImo, hasRegion, hasTopSignals, hasAisGap, hasSts] = await Promise.all([
    watchlistHasCol(conn, "imo"),
    watchlistHasCol(conn, "region"),
    watchlistHasCol(conn, "top_signals"),
    watchlistHasCol(conn, "ais_gap_count_30d"),
    watchlistHasCol(conn, "sts_candidate_count"),
  ]);

  let sql = `
    SELECT
      mmsi,
      ${hasImo ? "imo," : "NULL AS imo,"}
      vessel_name,
      flag,
      vessel_type,
      CAST(confidence AS DOUBLE) AS confidence,
      last_lat,
      last_lon,
      CAST(last_seen AS VARCHAR) AS last_seen,
      ${hasRegion ? "region" : "NULL AS region"},
      ${hasTopSignals ? "CAST(top_signals AS VARCHAR) AS top_signals" : "NULL AS top_signals"},
      ${hasAisGap ? "CAST(ais_gap_count_30d AS INTEGER) AS ais_gap_count_30d" : "NULL AS ais_gap_count_30d"},
      ${hasSts ? "CAST(sts_candidate_count AS INTEGER) AS sts_candidate_count" : "NULL AS sts_candidate_count"}
    FROM read_parquet('watchlist.parquet')
    WHERE confidence >= ${minConfidence}
  `;
  if (hasRegion && regions && regions.length > 0) {
    const list = regions.map((r) => `'${r.replace(/'/g, "''")}'`).join(", ");
    sql += ` AND region IN (${list})`;
  }
  sql += " ORDER BY confidence DESC LIMIT 500";

  const result = await conn.query(sql);
  return result.toArray().map((row) => row.toJSON() as VesselRow);
}

/**
 * Query validation metrics from `validation_metrics.parquet` (if registered).
 * Falls back to null values when the table is not available.
 */
export async function queryMetrics(
  conn: duckdb.AsyncDuckDBConnection
): Promise<MetricsRow | null> {
  try {
    const result = await conn.query(
      "SELECT * FROM read_parquet('validation_metrics.parquet') LIMIT 1"
    );
    const rows = result.toArray();
    if (rows.length === 0) return null;
    return rows[0].toJSON() as MetricsRow;
  } catch {
    return null;
  }
}

export interface CausalEffectRow {
  mmsi: string;
  regime: string;
  att_estimate: number;
  att_ci_lower: number;
  att_ci_upper: number;
  p_value: number;
  is_significant: boolean;
}

/** Query causal ATT for a single vessel. Returns null if not found or table absent. */
export async function queryCausalEffect(
  conn: duckdb.AsyncDuckDBConnection,
  mmsi: string
): Promise<CausalEffectRow | null> {
  if (!isParquetRegistered("causal_effects.parquet")) return null;
  try {
    const result = await conn.query(
      `SELECT mmsi, regime, att_estimate, att_ci_lower, att_ci_upper, p_value, is_significant
       FROM read_parquet('causal_effects.parquet')
       WHERE mmsi = '${mmsi.replace(/'/g, "''")}'
       LIMIT 1`
    );
    const rows = result.toArray();
    if (rows.length === 0) return null;
    return rows[0].toJSON() as CausalEffectRow;
  } catch {
    return null;
  }
}

/**
 * Load 30-day score history for all vessels in one query.
 * Returns a map of mmsi → array of 30 confidence values (oldest first).
 * Falls back to empty map if the table is not registered yet.
 */
export async function queryScoreHistoryBulk(
  conn: duckdb.AsyncDuckDBConnection
): Promise<Map<string, number[]>> {
  const map = new Map<string, number[]>();
  if (!isParquetRegistered("score_history.parquet")) return map;
  try {
    const result = await conn.query(
      `SELECT mmsi, confidence
       FROM read_parquet('score_history.parquet')
       ORDER BY mmsi, score_date ASC`
    );
    for (const row of result.toArray()) {
      const { mmsi, confidence } = row.toJSON() as { mmsi: string; confidence: number };
      if (!map.has(mmsi)) map.set(mmsi, []);
      map.get(mmsi)!.push(confidence);
    }
  } catch {
    // table not yet synced — return empty map
  }
  return map;
}

/** Derive available regions from the watchlist. */
export async function queryRegions(
  conn: duckdb.AsyncDuckDBConnection
): Promise<string[]> {
  try {
    const result = await conn.query(
      "SELECT DISTINCT region FROM read_parquet('watchlist.parquet') WHERE region IS NOT NULL ORDER BY region"
    );
    return result.toArray().map((r) => (r.toJSON() as { region: string }).region);
  } catch {
    return [];
  }
}
