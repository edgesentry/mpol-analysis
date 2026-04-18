import { useState, useEffect, useRef } from "react";
import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
import type { VesselRow } from "../lib/duckdb";
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

const LLM_ENDPOINT = "http://localhost:8080/v1/chat/completions";
const LLM_TIMEOUT_MS = 10_000;

type BriefStatus = "idle" | "loading" | "ready" | "offline" | "error";

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

export default function VesselDetail({ vessel, conn, onClose, onReviewSaved }: Props) {
  const [reviewOpen, setReviewOpen] = useState(false);
  const [dispatchOpen, setDispatchOpen] = useState(false);
  const [brief, setBrief] = useState<string>("");
  const [briefStatus, setBriefStatus] = useState<BriefStatus>("idle");
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    // Cancel any in-flight request from the previous vessel
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;

    setBrief("");
    setBriefStatus("loading");

    const timeout = setTimeout(() => ac.abort(), LLM_TIMEOUT_MS);

    fetchBrief(vessel, ac.signal)
      .then((text) => {
        if (ac.signal.aborted) return;
        setBrief(text);
        setBriefStatus("ready");
      })
      .catch((err: unknown) => {
        if (ac.signal.aborted) return;
        const msg = err instanceof Error ? err.message : String(err);
        // Connection refused / network error → treat as offline
        const isOffline =
          msg.includes("Failed to fetch") ||
          msg.includes("fetch") ||
          msg.includes("ECONNREFUSED") ||
          msg.includes("NetworkError");
        setBriefStatus(isOffline ? "offline" : "error");
      })
      .finally(() => clearTimeout(timeout));

    return () => {
      ac.abort();
      clearTimeout(timeout);
    };
  }, [vessel.mmsi]);

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
        <div
          style={{
            fontSize: "0.65rem",
            textTransform: "uppercase",
            letterSpacing: "0.08em",
            color: "#4a5568",
            marginBottom: "0.35rem",
          }}
        >
          Analyst brief
        </div>

        {briefStatus === "loading" && (
          <div
            style={{
              fontSize: "0.72rem",
              color: "#4a5568",
              fontStyle: "italic",
            }}
          >
            Generating…
          </div>
        )}

        {briefStatus === "offline" && (
          <div
            role="status"
            style={{
              display: "flex",
              alignItems: "center",
              gap: "0.4rem",
              padding: "0.35rem 0.6rem",
              borderRadius: 4,
              background: "#1a1f2e",
              border: "1px solid #4a5568",
              fontSize: "0.72rem",
              color: "#718096",
            }}
          >
            <span
              style={{
                width: 6,
                height: 6,
                borderRadius: "50%",
                background: "#4a5568",
                flexShrink: 0,
              }}
            />
            Local LLM offline — start llama-server on :8080
          </div>
        )}

        {briefStatus === "error" && (
          <div
            role="status"
            style={{
              padding: "0.35rem 0.6rem",
              borderRadius: 4,
              background: "#1a1f2e",
              border: "1px solid #744210",
              fontSize: "0.72rem",
              color: "#f6ad55",
            }}
          >
            Brief unavailable
          </div>
        )}

        {briefStatus === "ready" && brief && (
          <div
            style={{
              fontSize: "0.75rem",
              color: "#cbd5e0",
              lineHeight: 1.5,
              padding: "0.4rem 0.6rem",
              background: "#1a1f2e",
              borderRadius: 4,
              border: "1px solid #2d3748",
              borderLeft: "3px solid #93c5fd",
            }}
          >
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
          }}
        />
      )}
      {reviewOpen && !conn && (
        <div style={{ padding: "0.5rem 1rem", fontSize: "0.72rem", color: "#4a5568", fontStyle: "italic" }}>
          DuckDB not ready.
        </div>
      )}

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
