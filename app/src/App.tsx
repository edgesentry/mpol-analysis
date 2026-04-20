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
import { initDuckDB, queryWatchlist, queryMetrics, queryRegions, queryScoreHistoryBulk } from "./lib/duckdb";
import { initReviewSchema, getBulkReviewStates, saveReview } from "./lib/reviews";
import { initBriefCache } from "./lib/briefCache";
import type { DecisionTier, HandoffState } from "./lib/reviews";
import type { VesselRow, MetricsRow } from "./lib/duckdb";
import type { AsyncDuckDB, AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
import { syncAndLoad } from "./lib/opfs";
import type { SyncStatus } from "./lib/opfs";
import { pushReviews, mergeDownloadedReviews } from "./lib/push";
import type { PushStatus } from "./lib/push";
import { checkPrivateAuth, login, logout, isPrivateModeEnabled, getAuthToken } from "./lib/auth";
import { loadConfig } from "./lib/config";
import type { AppConfig } from "./lib/config";
import { loadAlerts, diffAndAppend } from "./lib/alerts";
import type { AlertEntry } from "./lib/alerts";
import KpiBar from "./components/KpiBar";
import WatchlistTable from "./components/WatchlistTable";
import VesselDetail from "./components/VesselDetail";
import VesselMap from "./components/VesselMap";
import SyncStatusBar from "./components/SyncStatus";
import AlertDrawer from "./components/AlertDrawer";
import RegionPicker, { getStoredRegions, formatRegionLabel, DEFAULT_REGIONS } from "./components/RegionPicker";

export default function App() {
  const dbRef = useRef<AsyncDuckDB | null>(null);
  const connRef = useRef<AsyncDuckDBConnection | null>(null);

  const [initError, setInitError] = useState<string | null>(null);
  const [syncStatus, setSyncStatus] = useState<SyncStatus>({ phase: "idle" });
  const [vessels, setVessels] = useState<VesselRow[]>([]);
  const [metrics, setMetrics] = useState<MetricsRow | null>(null);
  const [, setRegions] = useState<string[]>([]);
  // Persist region list in localStorage; default to Singapore on first visit.
  const [selectedRegions, setSelectedRegions] = useState<string[]>(
    () => getStoredRegions() ?? DEFAULT_REGIONS
  );
  // Show region picker overlay on first visit (no stored preference yet).
  const [showRegionPicker, setShowRegionPicker] = useState<boolean>(
    () => getStoredRegions() === null
  );
  const [selectedMmsi, setSelectedMmsi] = useState<string | null>(null);
  const [minConfidence, setMinConfidence] = useState(0.4);
  const [reviewStates, setReviewStates] = useState<Map<string, { decision_tier: DecisionTier | null; handoff_state: HandoffState }>>(new Map());
  const [handoffFilter, setHandoffFilter] = useState<HandoffState | "all">("all");
  const [scoreHistory, setScoreHistory] = useState<Map<string, number[]>>(new Map());
  const [alerts, setAlerts] = useState<AlertEntry[]>(() => loadAlerts());
  const [alertDrawerOpen, setAlertDrawerOpen] = useState(false);
  const prevVesselsRef = useRef<VesselRow[]>([]);
  const [appConfig, setAppConfig] = useState<AppConfig | null>(null);
  const [userEmail, setUserEmail] = useState<string | null>(null);
  const [pushStatus, setPushStatus] = useState<PushStatus>({ phase: "idle" });
  const privateAuth = userEmail !== null;
  const privateMode = appConfig ? isPrivateModeEnabled(appConfig) : false;

  // ── Initialise DuckDB-WASM once ──────────────────────────────────────────
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const cfg = await loadConfig();
        if (cancelled) return;
        setAppConfig(cfg);
        const { db, conn } = await initDuckDB();
        if (cancelled) return;
        dbRef.current = db;
        connRef.current = conn;
        await initReviewSchema(conn);
        await initBriefCache(conn);
        const email = isPrivateModeEnabled(cfg) ? await checkPrivateAuth(cfg) : null;
        if (!cancelled) setUserEmail(email);
        // Skip auto-sync if the region picker is waiting for user input.
        if (!showRegionPicker) {
          await doSync(db, undefined, email !== null, cfg);
        }
      } catch (err) {
        if (!cancelled) setInitError(String(err));
      }
    })();
    return () => { cancelled = true; };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const doSync = useCallback(async (db?: AsyncDuckDB, regions?: string[], auth?: boolean, cfg?: AppConfig) => {
    const target = db ?? dbRef.current;
    if (!target) return;
    const activeCfg = cfg ?? appConfig ?? undefined;
    const activeRegions = regions ?? selectedRegions;
    const regionFilter = activeRegions.length > 0 ? activeRegions : undefined;
    const isAuthed = auth ?? privateAuth;
    const token = activeCfg && isAuthed ? await getAuthToken(activeCfg) : null;
    setSyncStatus({ phase: "fetching_manifest" });
    const loaded = await syncAndLoad(target, setSyncStatus, regionFilter, isAuthed, activeCfg, token);
    if (loaded > 0 && connRef.current) {
      if (isAuthed) {
        await mergeDownloadedReviews(connRef.current).catch((err) =>
          console.warn("[sync] mergeDownloadedReviews failed:", err)
        );
      }
      await refreshQuery();
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedRegions]);

  const refreshQuery = useCallback(async () => {
    const conn = connRef.current;
    if (!conn) return;
    const [vs, m, rs, sh] = await Promise.all([
      queryWatchlist(conn, { minConfidence, regions: selectedRegions.length > 0 ? selectedRegions : undefined }),
      queryMetrics(conn),
      queryRegions(conn),
      queryScoreHistoryBulk(conn),
    ]);
    setScoreHistory(sh);
    const updatedAlerts = diffAndAppend(prevVesselsRef.current, vs);
    prevVesselsRef.current = vs;
    setAlerts(updatedAlerts);
    setVessels(vs);
    setMetrics(m);
    setRegions(rs);
    const states = await getBulkReviewStates(conn, vs.map((v) => v.mmsi));
    setReviewStates(states);
  }, [minConfidence, selectedRegions]);

  const handlePush = useCallback(async () => {
    const db = dbRef.current;
    const conn = connRef.current;
    if (!db || !conn || !userEmail) return;
    try {
      await pushReviews(db, conn, setPushStatus);
    } catch (err) {
      setPushStatus({ phase: "error", message: String(err) });
    }
  }, [userEmail]);

  const handleClaim = useCallback(async (mmsi: string) => {
    const conn = connRef.current;
    if (!conn) return;
    // Optimistic update
    setReviewStates((prev) => {
      const next = new Map(prev);
      const existing = next.get(mmsi);
      next.set(mmsi, {
        decision_tier: existing?.decision_tier ?? null,
        handoff_state: "in_review",
      });
      return next;
    });
    try {
      await saveReview(conn, {
        mmsi,
        decision_tier: reviewStates.get(mmsi)?.decision_tier ?? null,
        handoff_state: "in_review",
        reviewer_id: "analyst",
        rationale: "",
        identifier_basis: "mmsi",
        evidence: [],
      });
    } catch {
      // Roll back on failure
      await refreshQuery();
    }
  }, [reviewStates, refreshQuery]);

  // Re-query when filter changes (if data is already loaded)
  useEffect(() => {
    if (syncStatus.phase === "ready") {
      refreshQuery();
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [minConfidence, selectedRegions]);

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

        {/* Region selector — shows active regions; click to edit */}
        <button
          onClick={() => setShowRegionPicker(true)}
          title="Change regions"
          style={{
            background: "#0f1117",
            border: "1px solid #2d3748",
            color: "#e2e8f0",
            padding: "0.25rem 0.6rem",
            borderRadius: 4,
            fontSize: "0.78rem",
            cursor: "pointer",
            display: "flex",
            alignItems: "center",
            gap: "0.35rem",
          }}
        >
          {formatRegionLabel(selectedRegions)}
          <span style={{ color: "#4a5568", fontSize: "0.65rem" }}>▾</span>
        </button>

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

        {privateMode && (
          privateAuth ? (
            <span style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
              <span style={{ color: "#a0aec0", fontSize: "0.75rem" }}>
                Logged in as {userEmail}
              </span>
              <button
                onClick={() => appConfig && logout(appConfig)}
                title="Sign out"
                style={{
                  background: "transparent",
                  border: "1px solid #2d3748",
                  color: "#a0aec0",
                  padding: "0.25rem 0.6rem",
                  borderRadius: 4,
                  fontSize: "0.75rem",
                  cursor: "pointer",
                }}
              >
                Sign out
              </button>
            </span>
          ) : (
            <button
              onClick={() => {
                if (!appConfig) return;
                const popup = login(appConfig);
                if (!popup) return;
                const timer = setInterval(async () => {
                  if (popup.closed) {
                    clearInterval(timer);
                    const email = await checkPrivateAuth(appConfig);
                    setUserEmail(email);
                  }
                }, 500);
              }}
              style={{
                background: "transparent",
                border: "1px solid #3b82f6",
                color: "#93c5fd",
                padding: "0.25rem 0.6rem",
                borderRadius: 4,
                fontSize: "0.75rem",
                cursor: "pointer",
              }}
            >
              Sign in
            </button>
          )
        )}
      </header>

      {/* KPI bar */}
      <KpiBar
        vessels={vessels}
        metrics={metrics}
        unreadAlerts={alerts.filter((a) => !a.read).length}
        onBellClick={() => setAlertDrawerOpen(true)}
      />

      {/* Sync status */}
      <SyncStatusBar
        status={syncStatus}
        onSync={() => doSync()}
        userEmail={userEmail}
        pushStatus={pushStatus}
        onPush={userEmail ? handlePush : undefined}
      />

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
            reviewStates={reviewStates}
            handoffFilter={handoffFilter}
            onHandoffFilterChange={setHandoffFilter}
            onClaim={handleClaim}
            exportRegion={selectedRegions.join("_") || "all"}
            scoreHistory={scoreHistory}
          />
          {selectedMmsi && (() => {
            const v = vessels.find((v) => v.mmsi === selectedMmsi);
            return v ? (
              <VesselDetail
                vessel={v}
                conn={connRef.current}
                onClose={() => setSelectedMmsi(null)}
                onReviewSaved={async () => {
                  await refreshQuery();
                  const conn = connRef.current;
                  if (conn) {
                    const states = await getBulkReviewStates(conn, vessels.map((v) => v.mmsi));
                    setReviewStates(states);
                  }
                }}
              />
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

      {/* Alert drawer */}
      {alertDrawerOpen && (
        <AlertDrawer
          alerts={alerts}
          onClose={() => setAlertDrawerOpen(false)}
          onSelectVessel={(mmsi) => setSelectedMmsi(mmsi)}
          onAlertsChange={setAlerts}
        />
      )}

      {/* Region picker — shown on first visit or when user clicks the header button */}
      {showRegionPicker && (
        <RegionPicker
          initial={selectedRegions}
          onConfirm={(regions) => {
            setSelectedRegions(regions);
            setShowRegionPicker(false);
            // Re-sync only if this is the first load (no data yet)
            if (syncStatus.phase === "idle") {
              doSync(undefined, regions);
            } else {
              refreshQuery();
            }
          }}
          onCancel={syncStatus.phase !== "idle" ? () => setShowRegionPicker(false) : undefined}
        />
      )}
    </div>
  );
}
