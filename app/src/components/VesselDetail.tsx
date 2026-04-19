import { useState, useEffect, useRef } from "react";
import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
import type { VesselRow } from "../lib/duckdb";
import { queryCausalEffect } from "../lib/duckdb";
import type { CausalEffectRow } from "../lib/duckdb";
import { getCachedBrief, saveCachedBrief } from "../lib/briefCache";
import { getAuditLog } from "../lib/reviews";
import {
  formatLastSeen,
  confidenceTier,
  confidenceTierColor,
  signalLabel,
  signalSeverity,
  severityColor,
} from "../lib/humanise";
import ReviewPanel from "./ReviewPanel";
import DispatchModal from "./DispatchModal";

interface Props {
  vessel: VesselRow;
  conn: AsyncDuckDBConnection | null;
  onClose: () => void;
  onReviewSaved?: () => void;
}

// ── LLM brief fetcher ────────────────────────────────────────────────────────

// VITE_LLM_ENDPOINT can be set in Cloudflare Pages environment variables to
// point at a remote HTTPS inference endpoint.  Falls back to the Caddy HTTPS
// proxy started by run_llama.sh (:8443 → :8080).  Using HTTPS for both Chrome
// and Safari avoids mixed-content issues entirely.
const LLM_ENDPOINT =
  import.meta.env.VITE_LLM_ENDPOINT ?? "https://localhost:8443/v1/chat/completions";
const LLM_TIMEOUT_MS = 45_000;

type BriefStatus = "idle" | "loading" | "cached" | "ready" | "offline" | "error";

function buildPrompt(v: VesselRow): string {
  const parts = [
    `Vessel: ${v.vessel_name || v.mmsi}`,
    `MMSI: ${v.mmsi}`,
    v.flag ? `Flag: ${v.flag}` : null,
    v.vessel_type ? `Type: ${v.vessel_type}` : null,
    v.region ? `Region: ${v.region}` : null,
    v.last_seen ? `Last seen: ${v.last_seen}` : null,
    v.last_lat != null && v.last_lon != null
      ? `Position: ${v.last_lat.toFixed(4)}°, ${v.last_lon.toFixed(4)}°`
      : null,
    `Anomaly confidence: ${v.confidence.toFixed(3)}`,
  ]
    .filter(Boolean)
    .join("\n");

  return (
    `You are a maritime intelligence analyst. Provide a concise 2-3 sentence risk assessment ` +
    `for the following vessel flagged by an anomaly-detection system. Focus on probable cause ` +
    `of the anomaly, regional context, and recommended follow-up action. Be direct — no preamble.\n\n` +
    parts
  );
}

async function fetchBrief(v: VesselRow, signal: AbortSignal): Promise<string> {
  // Default endpoint is https://localhost:8443 (Caddy proxy) so both Chrome
  // and Safari avoid mixed-content issues.  Network errors fall through to
  // the caller's "offline" handler.
  const res = await fetch(LLM_ENDPOINT, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: "local",
      max_tokens: 200,
      temperature: 0.3,
      messages: [{ role: "user", content: buildPrompt(v) }],
    }),
    signal,
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  return (data?.choices?.[0]?.message?.content ?? "").trim();
}

// ── SHAP signal bar chart ────────────────────────────────────────────────────

interface ShapSignal {
  feature: string;
  value: number | string | null;
  contribution: number;
}

function parseSignals(raw: string | null | undefined): ShapSignal[] {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed as ShapSignal[];
  } catch {
    return [];
  }
}

function ShapBarChart({ raw }: { raw: string | null | undefined }) {
  const signals = parseSignals(raw);
  if (!signals.length) return null;
  const maxContrib = Math.max(...signals.map((s) => s.contribution));

  return (
    <div style={{ marginTop: "0.75rem" }}>
      <div
        style={{
          fontSize: "0.65rem",
          textTransform: "uppercase",
          letterSpacing: "0.08em",
          color: "#4a5568",
          marginBottom: "0.4rem",
        }}
      >
        Top signals
      </div>
      {signals.map((s) => {
        const pct = maxContrib > 0 ? (s.contribution / maxContrib) * 100 : 0;
        const label = signalLabel(s.feature);
        const rawVal = s.value != null ? String(s.value) : "—";
        const sev = signalSeverity(s.feature, s.value);
        return (
          <div
            key={s.feature}
            title={`${s.feature}: ${rawVal}`}
            style={{ marginBottom: "0.35rem" }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: "0.3rem", marginBottom: "0.15rem" }}>
              <span style={{ fontSize: "0.65rem", color: "#a0aec0", flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                {label}
              </span>
              {sev && (
                <span style={{ fontSize: "0.55rem", fontWeight: 700, color: severityColor(sev), border: `1px solid ${severityColor(sev)}`, borderRadius: 2, padding: "0 0.25rem", flexShrink: 0, fontFamily: "ui-monospace,monospace" }}>
                  {sev}
                </span>
              )}
              <span style={{ fontSize: "0.65rem", color: "#718096", flexShrink: 0, fontFamily: "ui-monospace,monospace" }}>
                {rawVal}
              </span>
            </div>
            <div style={{ display: "flex", alignItems: "center", gap: "0.4rem" }}>
              <div style={{ flex: 1, background: "#1a1f2e", borderRadius: 2, height: 5, minWidth: 0 }}>
                <div style={{ width: `${pct}%`, background: sev ? severityColor(sev) : "#fc8181", height: "100%", borderRadius: 2 }} />
              </div>
              <span style={{ fontSize: "0.6rem", color: "#4a5568", minWidth: 24, textAlign: "right", fontFamily: "ui-monospace,monospace" }}>
                {(s.contribution * 100).toFixed(0)}%
              </span>
            </div>
          </div>
        );
      })}
    </div>
  );
}

const row = (label: string, value: string | number | null | undefined) => (
  <tr key={label}>
    <td
      style={{
        color: "#718096",
        paddingRight: "0.75rem",
        paddingBottom: "0.3rem",
        whiteSpace: "nowrap",
        fontSize: "0.72rem",
        verticalAlign: "top",
      }}
    >
      {label}
    </td>
    <td
      style={{
        color: "#e2e8f0",
        paddingBottom: "0.3rem",
        fontSize: "0.78rem",
        wordBreak: "break-all",
      }}
    >
      {value ?? "—"}
    </td>
  </tr>
);

function shadowSignalColor(att: number, significant: boolean): string {
  if (!significant) return "#4a5568";
  if (att >= 0.4) return "#fc8181";
  if (att >= 0.2) return "#f6ad55";
  return "#68d391";
}

export default function VesselDetail({ vessel, conn, onClose, onReviewSaved }: Props) {
  const [reviewOpen, setReviewOpen] = useState(false);
  const [dispatchOpen, setDispatchOpen] = useState(false);
  const [brief, setBrief] = useState<string>("");
  const [briefStatus, setBriefStatus] = useState<BriefStatus>("idle");
  const [causal, setCausal] = useState<CausalEffectRow | null | undefined>(undefined);
  const [shadowTooltip, setShadowTooltip] = useState(false);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [auditLog, setAuditLog] = useState<Awaited<ReturnType<typeof getAuditLog>>>([]);
  const [expandedRationale, setExpandedRationale] = useState<Set<number>>(new Set());
  const abortRef = useRef<AbortController | null>(null);

  // Load brief: serve from cache if available, otherwise call LLM
  useEffect(() => {
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;
    setBrief("");
    setBriefStatus("loading");

    async function loadBrief() {
      // 1. Check cache first
      if (conn) {
        const cached = await getCachedBrief(conn, vessel.mmsi);
        if (cached) {
          if (ac.signal.aborted) return;
          setBrief(cached);
          setBriefStatus("cached");
          return;
        }
      }
      // 2. Cache miss — call LLM
      const timeout = setTimeout(() => ac.abort(), LLM_TIMEOUT_MS);
      try {
        const text = await fetchBrief(vessel, ac.signal);
        clearTimeout(timeout);
        if (ac.signal.aborted) return;
        setBrief(text);
        setBriefStatus("ready");
        if (conn && text) await saveCachedBrief(conn, vessel.mmsi, text);
      } catch {
        clearTimeout(timeout);
        if (ac.signal.aborted) return;
        // Any error here is a connection failure (HTTP errors are thrown
        // explicitly by fetchBrief before this catch). Show offline for all
        // browsers — Safari throws "Load failed", Chrome "Failed to fetch",
        // Firefox "NetworkError"; string-matching is fragile and unnecessary.
        setBriefStatus("offline");
      }
    }

    loadBrief();
    return () => { ac.abort(); };
  // vessel.mmsi is intentional: re-fetch only when the vessel changes, not on
  // every confidence/position update that would recreate the vessel object.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [conn, vessel.mmsi]);

  async function handleRegenerate() {
    if (!conn) return;
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;
    setBrief("");
    setBriefStatus("loading");
    const timeout = setTimeout(() => ac.abort(), LLM_TIMEOUT_MS);
    try {
      const text = await fetchBrief(vessel, ac.signal);
      clearTimeout(timeout);
      if (ac.signal.aborted) return;
      setBrief(text);
      setBriefStatus("ready");
      await saveCachedBrief(conn, vessel.mmsi, text);
    } catch {
      clearTimeout(timeout);
      if (ac.signal.aborted) return;
      setBriefStatus("offline");
    }
  }

  // Reload audit log when vessel changes or review panel saves
  useEffect(() => {
    if (!conn) return;
    setAuditLog([]);
    setExpandedRationale(new Set());
    getAuditLog(conn, vessel.mmsi).then(setAuditLog).catch(() => {});
  }, [conn, vessel.mmsi]);

  // Fetch causal ATT on vessel change
  useEffect(() => {
    if (!conn) return;
    setCausal(undefined);
    queryCausalEffect(conn, vessel.mmsi).then(setCausal).catch(() => setCausal(null));
  }, [conn, vessel.mmsi]);

  return (
    <div
      style={{
        borderTop: "1px solid #2d3748",
        background: "#0f1117",
        padding: "0.75rem 1rem",
        flexShrink: 0,
        overflowY: "auto",
        maxHeight: "65vh",
      }}
    >
      {/* Title row */}
      <div
        style={{
          display: "flex",
          alignItems: "flex-start",
          justifyContent: "space-between",
          marginBottom: "0.6rem",
          gap: "0.5rem",
        }}
      >
        <div>
          <div
            style={{
              fontWeight: 600,
              fontSize: "0.85rem",
              color: "#93c5fd",
              lineHeight: 1.3,
            }}
          >
            {vessel.vessel_name || vessel.mmsi}
          </div>
          <div style={{ fontSize: "0.68rem", color: "#4a5568", marginTop: 2 }}>
            MMSI {vessel.mmsi}
          </div>
        </div>
        <div style={{ display: "flex", gap: "0.4rem", flexShrink: 0 }}>
          <button
            onClick={() => setDispatchOpen(true)}
            style={{
              background: "none",
              border: "1px solid #2d3748",
              borderRadius: 4,
              color: "#718096",
              cursor: "pointer",
              fontSize: "0.68rem",
              fontWeight: 600,
              padding: "0.15rem 0.5rem",
            }}
            aria-label="Open dispatch brief"
          >
            Dispatch
          </button>
          <button
            onClick={() => setReviewOpen((o) => !o)}
            style={{
              background: reviewOpen ? "#2b4a8a" : "none",
              border: "1px solid #2d3748",
              borderRadius: 4,
              color: reviewOpen ? "#93c5fd" : "#718096",
              cursor: "pointer",
              fontSize: "0.68rem",
              fontWeight: 600,
              padding: "0.15rem 0.5rem",
            }}
            aria-label="Toggle review panel"
          >
            Review
          </button>
          <button
            onClick={onClose}
            style={{
              background: "none",
              border: "none",
              color: "#4a5568",
              cursor: "pointer",
              fontSize: "1rem",
              lineHeight: 1,
              padding: "0 0.2rem",
            }}
            aria-label="Close detail panel"
          >
            ✕
          </button>
        </div>
      </div>

      {/* Confidence badge */}
      <div style={{ marginBottom: "0.75rem" }}>
        <span
          style={{
            display: "inline-block",
            padding: "0.2rem 0.6rem",
            borderRadius: 4,
            background: "#1a1f2e",
            border: `1px solid ${confidenceTierColor(vessel.confidence)}`,
            color: confidenceTierColor(vessel.confidence),
            fontSize: "0.78rem",
            fontWeight: 700,
            fontFamily: "ui-monospace, monospace",
          }}
        >
          {vessel.confidence.toFixed(3)} — {confidenceTier(vessel.confidence)}
        </span>
      </div>

      {/* Shadow Signal badge */}
      {causal !== undefined && (
        <div style={{ marginBottom: "0.75rem", position: "relative" }}>
          <button
            onMouseEnter={() => setShadowTooltip(true)}
            onMouseLeave={() => setShadowTooltip(false)}
            onFocus={() => setShadowTooltip(true)}
            onBlur={() => setShadowTooltip(false)}
            aria-label="Shadow Signal causal ATT score"
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: "0.4rem",
              padding: "0.2rem 0.6rem",
              borderRadius: 4,
              background: "none",
              border: `1px solid ${causal ? shadowSignalColor(causal.att_estimate, causal.is_significant) : "#2d3748"}`,
              color: causal ? shadowSignalColor(causal.att_estimate, causal.is_significant) : "#4a5568",
              fontSize: "0.72rem",
              fontWeight: 700,
              fontFamily: "ui-monospace, monospace",
              cursor: "default",
            }}
          >
            <span style={{ fontSize: "0.6rem", opacity: 0.7 }}>Shadow Signal</span>
            <span>
              {causal ? `ATT ${causal.att_estimate >= 0 ? "+" : ""}${causal.att_estimate.toFixed(3)}` : "—"}
            </span>
            {causal?.is_significant && (
              <span style={{ fontSize: "0.55rem", fontWeight: 700, opacity: 0.8 }}>★</span>
            )}
          </button>

          {/* Tooltip */}
          {shadowTooltip && (
            <div style={{
              position: "absolute",
              top: "calc(100% + 6px)",
              left: 0,
              zIndex: 50,
              background: "#1a1f2e",
              border: "1px solid #2d3748",
              borderRadius: 6,
              padding: "0.6rem 0.75rem",
              width: 260,
              fontSize: "0.68rem",
              lineHeight: 1.55,
              color: "#a0aec0",
              boxShadow: "0 4px 16px rgba(0,0,0,0.5)",
            }}>
              <div style={{ fontWeight: 700, color: "#e2e8f0", marginBottom: "0.35rem" }}>
                Causal Shadow Signal
              </div>
              <div style={{ marginBottom: "0.4rem" }}>
                Measures whether this vessel's behaviour changed significantly around a sanction
                announcement date — a causal indicator of evasion, not mere correlation.
              </div>
              {causal ? (
                <>
                  <div style={{ display: "flex", flexDirection: "column", gap: "0.15rem", marginBottom: "0.4rem" }}>
                    <div><span style={{ color: "#718096" }}>Regime: </span>{causal.regime}</div>
                    <div><span style={{ color: "#718096" }}>ATT: </span>
                      <span style={{ color: shadowSignalColor(causal.att_estimate, causal.is_significant) }}>
                        {causal.att_estimate >= 0 ? "+" : ""}{causal.att_estimate.toFixed(3)}
                      </span>
                      <span style={{ color: "#4a5568" }}> [{causal.att_ci_lower.toFixed(3)}, {causal.att_ci_upper.toFixed(3)}]</span>
                    </div>
                    <div><span style={{ color: "#718096" }}>p-value: </span>
                      <span style={{ color: causal.p_value < 0.05 ? "#68d391" : "#f6ad55" }}>
                        {causal.p_value.toFixed(4)}
                      </span>
                      {causal.is_significant
                        ? <span style={{ color: "#68d391" }}> ★ significant</span>
                        : <span style={{ color: "#4a5568" }}> not significant</span>}
                    </div>
                  </div>
                </>
              ) : (
                <div style={{ color: "#4a5568" }}>No causal record for this vessel.</div>
              )}
              <div style={{ borderTop: "1px solid #2d3748", paddingTop: "0.35rem", color: "#4a5568", fontSize: "0.62rem" }}>
                DiD model · docs/causal-analysis.md
              </div>
            </div>
          )}
        </div>
      )}

      {/* Details table */}
      <table style={{ borderCollapse: "collapse", width: "100%" }}>
        <tbody>
          {vessel.imo && row("IMO", vessel.imo)}
          {row("Flag", vessel.flag)}
          {row("Type", vessel.vessel_type)}
          {row("Region", vessel.region)}
          {row("Last seen", formatLastSeen(vessel.last_seen))}
          {vessel.last_lat != null &&
            vessel.last_lon != null &&
            row(
              "Position",
              `${vessel.last_lat.toFixed(4)}°, ${vessel.last_lon.toFixed(4)}°`
            )}
        </tbody>
      </table>

      {/* Analyst brief */}
      <div style={{ marginTop: "0.75rem" }}>
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: "0.35rem" }}>
          <div style={{ fontSize: "0.65rem", textTransform: "uppercase", letterSpacing: "0.08em", color: "#4a5568" }}>
            Analyst brief
            {briefStatus === "cached" && (
              <span style={{ marginLeft: "0.4rem", color: "#2d6a4f", fontWeight: 600 }}>· cached</span>
            )}
          </div>
          {(briefStatus === "cached" || briefStatus === "ready") && conn && (
            <button
              onClick={handleRegenerate}
              style={{ background: "none", border: "none", color: "#4a5568", cursor: "pointer", fontSize: "0.62rem", padding: 0, textDecoration: "underline" }}
            >
              Regenerate
            </button>
          )}
        </div>

        {briefStatus === "loading" && (
          <div style={{ fontSize: "0.72rem", color: "#4a5568", fontStyle: "italic" }}>
            Generating…
          </div>
        )}

        {briefStatus === "offline" && (
          <div role="status" style={{ display: "flex", alignItems: "center", gap: "0.4rem", padding: "0.35rem 0.6rem", borderRadius: 4, background: "#1a1f2e", border: "1px solid #4a5568", fontSize: "0.72rem", color: "#718096" }}>
            <span style={{ width: 6, height: 6, borderRadius: "50%", background: "#4a5568", flexShrink: 0 }} />
            {LLM_ENDPOINT.startsWith("http://localhost")
              ? "Local LLM offline — start llama-server on :8080"
              : "LLM offline"}
          </div>
        )}

        {briefStatus === "error" && (
          <div role="status" style={{ padding: "0.35rem 0.6rem", borderRadius: 4, background: "#1a1f2e", border: "1px solid #744210", fontSize: "0.72rem", color: "#f6ad55" }}>
            Brief unavailable
          </div>
        )}

        {(briefStatus === "ready" || briefStatus === "cached") && brief && (
          <div style={{ fontSize: "0.75rem", color: "#cbd5e0", lineHeight: 1.5, padding: "0.4rem 0.6rem", background: "#1a1f2e", borderRadius: 4, border: "1px solid #2d3748", borderLeft: "3px solid #93c5fd" }}>
            {brief}
          </div>
        )}
      </div>

      {/* SHAP bar chart */}
      <ShapBarChart raw={vessel.top_signals} />

      {/* Review panel */}
      {reviewOpen && conn && (
        <ReviewPanel
          vessel={vessel}
          conn={conn}
          onSaved={() => {
            onReviewSaved?.();
            // Refresh audit log after save
            if (conn) getAuditLog(conn, vessel.mmsi).then(setAuditLog).catch(() => {});
          }}
        />
      )}
      {reviewOpen && !conn && (
        <div style={{ padding: "0.5rem 1rem", fontSize: "0.72rem", color: "#4a5568", fontStyle: "italic" }}>
          DuckDB not ready.
        </div>
      )}

      {/* ── History / audit trail ────────────────────────────────────── */}
      <div style={{ marginTop: "0.5rem", borderTop: "1px solid #2d3748", paddingTop: "0.5rem" }}>
        <div
          onClick={() => setHistoryOpen((o) => !o)}
          style={{ display: "flex", alignItems: "center", justifyContent: "space-between", cursor: "pointer", userSelect: "none" }}
        >
          <div style={{ fontSize: "0.65rem", textTransform: "uppercase", letterSpacing: "0.08em", color: "#4a5568" }}>
            History
            {auditLog.length > 0 && (
              <span style={{ marginLeft: "0.4rem", color: "#718096" }}>({auditLog.length})</span>
            )}
          </div>
          <span style={{ fontSize: "0.6rem", color: "#4a5568" }}>{historyOpen ? "▲" : "▼"}</span>
        </div>

        {historyOpen && (
          <div style={{ marginTop: "0.4rem" }}>
            {auditLog.length === 0 ? (
              <div style={{ fontSize: "0.68rem", color: "#4a5568", fontStyle: "italic", padding: "0.35rem 0" }}>
                No review history yet.
              </div>
            ) : (
              auditLog.map((entry, idx) => {
                const isExpanded = expandedRationale.has(idx);
                const hasRationale = entry.rationale?.trim();
                return (
                  <div
                    key={idx}
                    style={{
                      borderLeft: "2px solid #2d3748",
                      paddingLeft: "0.6rem",
                      marginBottom: "0.5rem",
                    }}
                  >
                    {/* Timestamp + reviewer */}
                    <div style={{ display: "flex", alignItems: "center", gap: "0.4rem", marginBottom: "0.15rem" }}>
                      <span style={{ fontSize: "0.62rem", color: "#4a5568", fontFamily: "ui-monospace,monospace" }}>
                        {new Date(entry.changed_at).toLocaleString("en-GB", {
                          day: "numeric", month: "short", year: "numeric",
                          hour: "2-digit", minute: "2-digit",
                        })}
                      </span>
                      <span style={{ fontSize: "0.62rem", color: "#718096" }}>
                        {entry.reviewer_id || "—"}
                      </span>
                    </div>
                    {/* State transition */}
                    <div style={{ display: "flex", alignItems: "center", gap: "0.3rem", fontSize: "0.68rem", marginBottom: hasRationale ? "0.2rem" : 0 }}>
                      {entry.from_state ? (
                        <span style={{ color: "#4a5568" }}>{entry.from_state.replace(/_/g, " ")}</span>
                      ) : (
                        <span style={{ color: "#4a5568" }}>—</span>
                      )}
                      <span style={{ color: "#2d3748" }}>→</span>
                      <span style={{ color: "#93c5fd", fontWeight: 600 }}>
                        {entry.to_state.replace(/_/g, " ")}
                      </span>
                    </div>
                    {/* Rationale */}
                    {hasRationale && (
                      <div style={{ fontSize: "0.65rem", color: "#718096", lineHeight: 1.4 }}>
                        <span style={{
                          display: isExpanded ? "block" : "-webkit-box",
                          WebkitLineClamp: isExpanded ? undefined : 1,
                          WebkitBoxOrient: isExpanded ? undefined : "vertical" as const,
                          overflow: isExpanded ? "visible" : "hidden",
                        }}>
                          {entry.rationale}
                        </span>
                        {entry.rationale.length > 60 && (
                          <button
                            onClick={() => setExpandedRationale((s) => {
                              const next = new Set(s);
                              if (isExpanded) { next.delete(idx); } else { next.add(idx); }
                              return next;
                            })}
                            style={{ background: "none", border: "none", color: "#4a5568", cursor: "pointer", fontSize: "0.6rem", padding: 0, textDecoration: "underline" }}
                          >
                            {isExpanded ? "show less" : "show more"}
                          </button>
                        )}
                      </div>
                    )}
                  </div>
                );
              })
            )}
          </div>
        )}
      </div>

      {/* Dispatch modal — rendered outside the scrollable div via portal would be ideal,
          but mounting here works since the parent sidebar is overflow:hidden */}
      {dispatchOpen && (
        <DispatchModal
          vessel={vessel}
          brief={brief}
          conn={conn}
          onClose={() => setDispatchOpen(false)}
        />
      )}
    </div>
  );
}
