/**
 * Root application component.
 *
 * Layout mirrors the existing FastAPI/HTMX dashboard:
 *   header → kpi bar → sync status → main (sidebar table | map)
 *
 * Data flow:
 *   1. On mount: init DuckDB-WASM + sync OPFS from R2.
 *   2. After sync: query watchlist + metrics from OPFS-registered Parquet files.
 *   3. Vessels rendered in WatchlistTable (sidebar) and VesselMap (main area).
 */

import { useState, useEffect, useCallback, useRef } from "react";
import { initDuckDB, queryWatchlist, queryMetrics, queryRegions } from "./lib/duckdb";
import type { VesselRow, MetricsRow } from "./lib/duckdb";
import type { AsyncDuckDB, AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
import { syncAndLoad } from "./lib/opfs";
import type { SyncStatus } from "./lib/opfs";
import KpiBar from "./components/KpiBar";
import WatchlistTable from "./components/WatchlistTable";
import VesselDetail from "./components/VesselDetail";
import VesselMap from "./components/VesselMap";
import SyncStatusBar from "./components/SyncStatus";

export default function App() {
  const dbRef = useRef<AsyncDuckDB | null>(null);
  const connRef = useRef<AsyncDuckDBConnection | null>(null);

  const [initError, setInitError] = useState<string | null>(null);
  const [syncStatus, setSyncStatus] = useState<SyncStatus>({ phase: "idle" });
  const [vessels, setVessels] = useState<VesselRow[]>([]);
  const [metrics, setMetrics] = useState<MetricsRow | null>(null);
  const [regions, setRegions] = useState<string[]>([]);
  const [selectedRegion, setSelectedRegion] = useState<string>("all");
  const [selectedMmsi, setSelectedMmsi] = useState<string | null>(null);
  const [minConfidence, setMinConfidence] = useState(0.4);

  // ── Initialise DuckDB-WASM once ──────────────────────────────────────────
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const { db, conn } = await initDuckDB();
        if (cancelled) return;
        dbRef.current = db;
        connRef.current = conn;
        await doSync(db);
      } catch (err) {
        if (!cancelled) setInitError(String(err));
      }
    })();
    return () => { cancelled = true; };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const doSync = useCallback(async (db?: AsyncDuckDB) => {
    const target = db ?? dbRef.current;
    if (!target) return;
    setSyncStatus({ phase: "fetching_manifest" });
    const loaded = await syncAndLoad(target, setSyncStatus);
    if (loaded > 0 && connRef.current) {
      await refreshQuery();
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const refreshQuery = useCallback(async () => {
    const conn = connRef.current;
    if (!conn) return;
    const region = selectedRegion === "all" ? undefined : selectedRegion;
    const [vs, m, rs] = await Promise.all([
      queryWatchlist(conn, { minConfidence, region }),
      queryMetrics(conn),
      queryRegions(conn),
    ]);
    setVessels(vs);
    setMetrics(m);
    setRegions(rs);
  }, [minConfidence, selectedRegion]);

  // Re-query when filter changes (if data is already loaded)
  useEffect(() => {
    if (syncStatus.phase === "ready") {
      refreshQuery();
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [minConfidence, selectedRegion]);

  if (initError) {
    return (
      <div style={{ padding: "2rem", fontFamily: "ui-monospace,monospace", color: "#fc8181", background: "#0f1117", minHeight: "100vh" }}>
        <strong>DuckDB init failed</strong>
        <pre style={{ marginTop: "1rem", whiteSpace: "pre-wrap", fontSize: "0.8rem" }}>{initError}</pre>
      </div>
    );
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100vh", overflow: "hidden" }}>
      {/* Header */}
      <header
        style={{
          display: "flex",
          alignItems: "center",
          gap: "1.5rem",
          padding: "0.75rem 1.25rem",
          background: "#1a1f2e",
          borderBottom: "1px solid #2d3748",
          flexShrink: 0,
        }}
      >
        <h1 style={{ fontSize: "1.1rem", fontWeight: 600, letterSpacing: "0.05em", color: "#93c5fd" }}>
          MPOL Watchlist
        </h1>

        {/* Region selector */}
        <select
          value={selectedRegion}
          onChange={(e) => setSelectedRegion(e.target.value)}
          style={{
            background: "#0f1117",
            border: "1px solid #2d3748",
            color: "#e2e8f0",
            padding: "0.25rem 0.5rem",
            borderRadius: 4,
            fontSize: "0.78rem",
          }}
        >
          <option value="all">All regions</option>
          {regions.map((r) => (
            <option key={r} value={r}>{r}</option>
          ))}
        </select>

        {/* Confidence filter */}
        <label style={{ display: "flex", alignItems: "center", gap: "0.4rem", fontSize: "0.75rem", color: "#a0aec0" }}>
          Min confidence
          <input
            type="range"
            min={0}
            max={1}
            step={0.05}
            value={minConfidence}
            onChange={(e) => setMinConfidence(parseFloat(e.target.value))}
            style={{ width: 80 }}
          />
          <span style={{ color: "#e2e8f0", minWidth: "2.5rem" }}>{minConfidence.toFixed(2)}</span>
        </label>

        <span style={{ marginLeft: "auto", fontSize: "0.65rem", color: "#4a5568" }}>
          DuckDB-WASM · OPFS · R2
        </span>
      </header>

      {/* KPI bar */}
      <KpiBar vessels={vessels} metrics={metrics} />

      {/* Sync status */}
      <SyncStatusBar status={syncStatus} onSync={() => doSync()} />

      {/* Main content: sidebar + map */}
      <div style={{ display: "flex", flex: 1, overflow: "hidden", minHeight: 0 }}>
        {/* Sidebar */}
        <div
          style={{
            width: 320,
            flexShrink: 0,
            display: "flex",
            flexDirection: "column",
            background: "#1a1f2e",
            borderRight: "1px solid #2d3748",
            overflow: "hidden",
          }}
        >
          <WatchlistTable
            vessels={vessels}
            selectedMmsi={selectedMmsi}
            onSelect={setSelectedMmsi}
          />
          {selectedMmsi && (() => {
            const v = vessels.find((v) => v.mmsi === selectedMmsi);
            return v ? (
              <VesselDetail vessel={v} onClose={() => setSelectedMmsi(null)} />
            ) : null;
          })()}
        </div>

        {/* Map */}
        <VesselMap
          vessels={vessels}
          selectedMmsi={selectedMmsi}
          onSelect={setSelectedMmsi}
        />
      </div>
    </div>
  );
}
