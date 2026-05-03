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

// ITU-allocated MIDs (Maritime Identification Digits).
// Any vessel MMSI whose 3-digit MID prefix is absent from this set is
// broadcasting an unallocated identifier — a confirmed stateless MMSI.
// Source: ITU-R M.585 Table of Maritime Identification Digits.
const ALLOCATED_MIDS = new Set([
  // Europe
  201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,218,219,
  220,224,225,226,227,228,229,230,231,232,233,234,235,236,237,238,239,
  240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,
  257,258,259,261,262,263,264,265,266,267,268,269,270,271,272,273,274,
  275,276,277,278,279,
  // Americas (Caribbean, Central, North)
  301,303,304,305,306,307,308,309,310,311,312,313,314,315,316,317,318,319,
  320,321,323,324,325,327,328,329,330,331,332,333,334,335,336,338,339,
  341,343,345,347,348,349,350,351,352,353,354,355,356,357,358,359,
  361,362,364,365,366,367,368,369,370,371,372,373,374,375,376,377,378,379,
  // Asia-Pacific
  401,403,405,408,412,413,414,416,417,419,422,423,425,428,431,432,433,
  434,436,438,440,441,443,445,447,450,451,452,453,455,457,459,461,462,
  463,466,467,468,470,471,472,473,474,477,478,
  // Southeast Asia / Oceania
  501,503,506,508,509,510,511,512,514,515,516,518,519,520,523,525,526,529,
  531,533,536,538,540,542,543,544,546,548,553,555,557,559,561,563,564,565,
  566,567,570,572,574,576,577,578,580,582,584,
  // Africa
  601,603,605,607,608,609,610,611,612,613,615,616,617,618,619,620,621,
  622,624,625,626,627,628,629,630,631,632,633,634,635,636,637,638,642,
  644,645,647,649,650,654,655,656,657,659,660,661,662,663,664,665,666,
  667,668,669,670,671,672,673,674,675,676,677,678,679,
  680,681,682,683,684,685,686,687,688,689,690,691,692,693,694,695,696,697,698,699,
  // South America
  701,710,720,725,730,734,735,740,745,750,755,756,760,765,770,775,780,790,
]);

/**
 * Query vessels with ITU-unallocated MMSI prefixes (stateless MMSIs).
 * These are returned regardless of confidence rank or LIMIT 500 cutoff.
 */
export async function queryStatelessVessels(
  conn: duckdb.AsyncDuckDBConnection
): Promise<VesselRow[]> {
  const [hasImo, hasRegion, hasTopSignals, hasAisGap, hasSts] = await Promise.all([
    watchlistHasCol(conn, "imo"),
    watchlistHasCol(conn, "region"),
    watchlistHasCol(conn, "top_signals"),
    watchlistHasCol(conn, "ais_gap_count_30d"),
    watchlistHasCol(conn, "sts_candidate_count"),
  ]);

  // Use string comparison to avoid TRY_CAST compatibility issues with DuckDB-WASM.
  // Vessel MMSIs start with 2-7; 0xx/1xx are coast stations, 8xx/9xx are nav aids.
  const allocatedList = [...ALLOCATED_MIDS].map((n) => `'${String(n).padStart(3, "0")}'`).join(",");
  const sql = `
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
    WHERE LENGTH(mmsi) = 9
      AND LEFT(mmsi, 1) BETWEEN '2' AND '7'
      AND LEFT(mmsi, 3) NOT IN (${allocatedList})
    ORDER BY confidence DESC
  `;
  const result = await conn.query(sql);
  return result.toArray().map((row) => row.toJSON() as VesselRow);
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
