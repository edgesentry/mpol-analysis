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
/** Check whether `imo` column exists in watchlist.parquet (cached per session). */
let _watchlistHasImo: boolean | null = null;
async function watchlistHasImo(conn: duckdb.AsyncDuckDBConnection): Promise<boolean> {
  if (_watchlistHasImo !== null) return _watchlistHasImo;
  try {
    await conn.query("SELECT imo FROM read_parquet('watchlist.parquet') LIMIT 0");
    _watchlistHasImo = true;
  } catch {
    _watchlistHasImo = false;
  }
  return _watchlistHasImo;
}

/** Check whether `top_signals` column exists in watchlist.parquet (cached per session). */
let _watchlistHasTopSignals: boolean | null = null;
async function watchlistHasTopSignals(conn: duckdb.AsyncDuckDBConnection): Promise<boolean> {
  if (_watchlistHasTopSignals !== null) return _watchlistHasTopSignals;
  try {
    await conn.query("SELECT top_signals FROM read_parquet('watchlist.parquet') LIMIT 0");
    _watchlistHasTopSignals = true;
  } catch {
    _watchlistHasTopSignals = false;
  }
  return _watchlistHasTopSignals;
}

export async function queryWatchlist(
  conn: duckdb.AsyncDuckDBConnection,
  opts: { minConfidence?: number; region?: string } = {}
): Promise<VesselRow[]> {
  const { minConfidence = 0, region } = opts;
  const [hasImo, hasTopSignals] = await Promise.all([
    watchlistHasImo(conn),
    watchlistHasTopSignals(conn),
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
      region,
      ${hasTopSignals ? "CAST(top_signals AS VARCHAR) AS top_signals" : "NULL AS top_signals"}
    FROM read_parquet('watchlist.parquet')
    WHERE confidence >= ${minConfidence}
  `;
  if (region) {
    sql += ` AND region = '${region.replace(/'/g, "''")}'`;
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
