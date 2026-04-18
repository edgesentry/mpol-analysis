import { useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
import type { VesselRow } from "../lib/duckdb";
import { getReview, tierColor, handoffLabel } from "../lib/reviews";
import type { VesselReview } from "../lib/reviews";
import {
  formatLastSeen,
  confidenceTier,
  confidenceTierColor,
  signalLabel,
  signalSeverity,
  severityColor,
} from "../lib/humanise";

interface ShapSignal {
  feature: string;
  value: number | string | null;
  contribution: number;
}

function parseSignals(raw: string | null | undefined): ShapSignal[] {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? (parsed as ShapSignal[]) : [];
  } catch {
    return [];
  }
}

interface Props {
  vessel: VesselRow;
  brief: string;
  conn: AsyncDuckDBConnection | null;
  onClose: () => void;
}

export default function DispatchModal({ vessel, brief, conn, onClose }: Props) {
  const signals = parseSignals(vessel.top_signals);
  const maxContrib = signals.length ? Math.max(...signals.map((s) => s.contribution)) : 1;
  const closeRef = useRef<HTMLButtonElement | null>(null);
  const [review, setReview] = useState<VesselReview | null>(null);

  // Fetch review record on open
  useEffect(() => {
    if (!conn) return;
    getReview(conn, vessel.mmsi).then(setReview).catch(() => {});
  }, [conn, vessel.mmsi]);

  // Focus close button on open; close on Escape; inject print stylesheet
  useEffect(() => {
    closeRef.current?.focus();
    const handler = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", handler);

    const style = document.createElement("style");
    style.id = "__dispatch-print-style";
    style.textContent = `
      @media print {
        body > *:not(#dispatch-print-root) { display: none !important; }
        #dispatch-print-root {
          position: static !important;
          transform: none !important;
          width: 100% !important;
          max-height: none !important;
          background: white !important;
          border: none !important;
          box-shadow: none !important;
          padding: 2rem !important;
          font-family: system-ui, sans-serif !important;
          color: black !important;
        }
        #dispatch-print-root * {
          color: black !important;
          background: transparent !important;
          border-color: #ccc !important;
        }
        #dispatch-print-footer { display: none !important; }
      }
    `;
    document.head.appendChild(style);
    return () => {
      window.removeEventListener("keydown", handler);
      document.getElementById("__dispatch-print-style")?.remove();
    };
  }, [onClose]);

  // ── Export JSON ────────────────────────────────────────────────────────────
  function handleExport() {
    const payload = {
      exported_at: new Date().toISOString(),
      mmsi: vessel.mmsi,
      imo: vessel.imo ?? null,
      vessel_name: vessel.vessel_name || null,
      flag: vessel.flag || null,
      vessel_type: vessel.vessel_type || null,
      confidence: vessel.confidence,
      confidence_tier: confidenceTier(vessel.confidence),
      region: vessel.region || null,
      last_lat: vessel.last_lat ?? null,
      last_lon: vessel.last_lon ?? null,
      last_seen: vessel.last_seen ?? null,
      top_signals: signals.map((s) => ({
        feature: s.feature,
        label: signalLabel(s.feature),
        value: s.value,
        severity: signalSeverity(s.feature, s.value),
        contribution: s.contribution,
      })),
      analyst_brief: brief || null,
      review: review ? {
        decision_tier: review.decision_tier,
        handoff_state: review.handoff_state,
        reviewer_id: review.reviewer_id || null,
        rationale: review.rationale || null,
        updated_at: review.updated_at,
      } : null,
    };
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `patrol_task_${vessel.mmsi}_${Date.now()}.json`;
    a.click();
    URL.revokeObjectURL(url);
  }

  // ── Copy to clipboard ─────────────────────────────────────────────────────
  function handleCopy() {
    const signalLines = signals
      .map((s) => {
        const sev = signalSeverity(s.feature, s.value);
        return `- **${signalLabel(s.feature)}**: ${s.value ?? "—"}${sev ? ` [${sev}]` : ""} (${(s.contribution * 100).toFixed(0)}%)`;
      })
      .join("\n");

    const md = [
      `# Patrol Dispatch Brief`,
      `**Vessel:** ${vessel.vessel_name || vessel.mmsi}`,
      `**MMSI:** ${vessel.mmsi}`,
      vessel.imo ? `**IMO:** ${vessel.imo}` : null,
      `**Flag:** ${vessel.flag || "—"}`,
      `**Type:** ${vessel.vessel_type || "—"}`,
      `**Region:** ${vessel.region || "—"}`,
      `**Last seen:** ${formatLastSeen(vessel.last_seen)}`,
      vessel.last_lat != null && vessel.last_lon != null
        ? `**Position:** ${vessel.last_lat.toFixed(4)}°, ${vessel.last_lon.toFixed(4)}°`
        : null,
      `**Anomaly confidence:** ${vessel.confidence.toFixed(3)} — ${confidenceTier(vessel.confidence)}`,
      "",
      signals.length ? `## Top signals\n${signalLines}` : null,
      "",
      brief ? `## Analyst brief\n${brief}` : null,
      "",
      review ? [
        `## Review decision`,
        review.decision_tier ? `**Tier:** ${review.decision_tier}` : null,
        `**Status:** ${handoffLabel(review.handoff_state)}`,
        review.reviewer_id ? `**Reviewer:** ${review.reviewer_id}` : null,
        review.rationale ? `**Rationale:** ${review.rationale}` : null,
      ].filter(Boolean).join("\n") : null,
      "",
      `---`,
      `*Generated ${new Date().toISOString()}*`,
    ]
      .filter((l) => l !== null)
      .join("\n");

    navigator.clipboard.writeText(md).catch(() => {/* ignore */});
  }

  const color = confidenceTierColor(vessel.confidence);
  const tier = confidenceTier(vessel.confidence);

  return createPortal(
    <>
      {/* Backdrop */}
      <div
        onClick={onClose}
        style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.6)", zIndex: 100 }}
      />

      {/* Modal */}
      <div
        id="dispatch-print-root"
        role="dialog"
        aria-modal="true"
        aria-label="Dispatch brief"
        style={{
          position: "fixed",
          top: "50%",
          left: "50%",
          transform: "translate(-50%, -50%)",
          zIndex: 101,
          background: "#0f1117",
          border: "1px solid #2d3748",
          borderRadius: 6,
          width: "min(640px, 94vw)",
          maxHeight: "85vh",
          display: "flex",
          flexDirection: "column",
          fontFamily: "ui-monospace,monospace",
        }}
      >
        {/* Header */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "0.75rem 1rem", borderBottom: "1px solid #2d3748", flexShrink: 0 }}>
          <div>
            <div style={{ fontWeight: 700, fontSize: "0.9rem", color: "#93c5fd" }}>
              Dispatch Brief
            </div>
            <div style={{ fontSize: "0.68rem", color: "#4a5568", marginTop: 2 }}>
              {vessel.vessel_name || vessel.mmsi} · MMSI {vessel.mmsi}
              {vessel.imo && <span style={{ marginLeft: "0.5rem" }}>· IMO {vessel.imo}</span>}
            </div>
          </div>
          <button ref={closeRef} onClick={onClose} aria-label="Close dispatch modal"
            style={{ background: "none", border: "none", color: "#4a5568", cursor: "pointer", fontSize: "1.1rem", lineHeight: 1, padding: "0.2rem 0.3rem" }}>
            ✕
          </button>
        </div>

        {/* Body */}
        <div style={{ overflowY: "auto", padding: "0.85rem 1rem", flex: 1 }}>

          {/* Confidence */}
          <div style={{ marginBottom: "0.75rem" }}>
            <span style={{
              display: "inline-block",
              padding: "0.2rem 0.6rem",
              borderRadius: 4,
              background: "#1a1f2e",
              border: `1px solid ${color}`,
              color,
              fontSize: "0.78rem",
              fontWeight: 700,
            }}>
              {vessel.confidence.toFixed(3)} — {tier}
            </span>
          </div>

          {/* Vessel details */}
          <table style={{ borderCollapse: "collapse", width: "100%", marginBottom: "0.75rem" }}>
            <tbody>
              {[
                vessel.imo ? ["IMO", vessel.imo] : null,
                ["Flag", vessel.flag],
                ["Type", vessel.vessel_type],
                ["Region", vessel.region],
                ["Last seen", formatLastSeen(vessel.last_seen)],
                vessel.last_lat != null && vessel.last_lon != null
                  ? ["Position", `${vessel.last_lat.toFixed(4)}°, ${vessel.last_lon.toFixed(4)}°`]
                  : null,
              ]
                .filter((r): r is [string, string] => r !== null)
                .map(([label, val]) => (
                  <tr key={label}>
                    <td style={{ color: "#718096", paddingRight: "0.75rem", paddingBottom: "0.25rem", fontSize: "0.72rem", whiteSpace: "nowrap", verticalAlign: "top" }}>
                      {label}
                    </td>
                    <td style={{ color: "#e2e8f0", paddingBottom: "0.25rem", fontSize: "0.78rem" }}>
                      {val || "—"}
                    </td>
                  </tr>
                ))}
            </tbody>
          </table>

          {/* Top signals */}
          {signals.length > 0 && (
            <div style={{ marginBottom: "0.75rem" }}>
              <div style={{ fontSize: "0.65rem", textTransform: "uppercase", letterSpacing: "0.08em", color: "#4a5568", marginBottom: "0.5rem" }}>
                Top signals
              </div>
              {signals.map((s) => {
                const pct = (s.contribution / maxContrib) * 100;
                const label = signalLabel(s.feature);
                const sev = signalSeverity(s.feature, s.value);
                const barColor = sev ? severityColor(sev) : "#fc8181";
                const rawVal = s.value != null ? String(s.value) : "—";
                return (
                  <div key={s.feature} title={`${s.feature}: ${rawVal}`} style={{ marginBottom: "0.4rem" }}>
                    <div style={{ display: "flex", alignItems: "center", gap: "0.3rem", marginBottom: "0.15rem" }}>
                      <span style={{ fontSize: "0.68rem", color: "#a0aec0", flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                        {label}
                      </span>
                      {sev && (
                        <span style={{ fontSize: "0.55rem", fontWeight: 700, color: severityColor(sev), border: `1px solid ${severityColor(sev)}`, borderRadius: 2, padding: "0 0.25rem", flexShrink: 0 }}>
                          {sev}
                        </span>
                      )}
                      <span style={{ fontSize: "0.65rem", color: "#718096", flexShrink: 0 }}>
                        {rawVal}
                      </span>
                    </div>
                    <div style={{ display: "flex", alignItems: "center", gap: "0.4rem" }}>
                      <div style={{ flex: 1, background: "#1a1f2e", borderRadius: 2, height: 5, minWidth: 0 }}>
                        <div style={{ width: `${pct}%`, background: barColor, height: "100%", borderRadius: 2 }} />
                      </div>
                      <span style={{ fontSize: "0.6rem", color: "#4a5568", minWidth: 24, textAlign: "right" }}>
                        {(s.contribution * 100).toFixed(0)}%
                      </span>
                    </div>
                  </div>
                );
              })}
            </div>
          )}

          {/* Analyst brief */}
          {brief && (
            <div style={{ marginBottom: "0.75rem" }}>
              <div style={{ fontSize: "0.65rem", textTransform: "uppercase", letterSpacing: "0.08em", color: "#4a5568", marginBottom: "0.35rem" }}>
                Analyst brief
              </div>
              <div style={{ fontSize: "0.75rem", color: "#cbd5e0", lineHeight: 1.6, padding: "0.5rem 0.7rem", background: "#1a1f2e", borderRadius: 4, border: "1px solid #2d3748", borderLeft: "3px solid #93c5fd" }}>
                {brief}
              </div>
            </div>
          )}

          {/* Review decision */}
          {review && (
            <div>
              <div style={{ fontSize: "0.65rem", textTransform: "uppercase", letterSpacing: "0.08em", color: "#4a5568", marginBottom: "0.4rem" }}>
                Review decision
              </div>
              <div style={{ background: "#1a1f2e", border: "1px solid #2d3748", borderRadius: 4, padding: "0.5rem 0.7rem" }}>
                <div style={{ display: "flex", alignItems: "center", gap: "0.5rem", marginBottom: review.rationale ? "0.4rem" : 0 }}>
                  {review.decision_tier && (
                    <span style={{
                      display: "inline-block",
                      padding: "0.1rem 0.4rem",
                      borderRadius: 3,
                      fontSize: "0.62rem",
                      fontWeight: 700,
                      fontFamily: "ui-monospace,monospace",
                      background: tierColor(review.decision_tier) + "22",
                      border: `1px solid ${tierColor(review.decision_tier)}`,
                      color: tierColor(review.decision_tier),
                    }}>
                      {review.decision_tier.toUpperCase()}
                    </span>
                  )}
                  <span style={{ fontSize: "0.72rem", color: "#a0aec0" }}>
                    {handoffLabel(review.handoff_state)}
                  </span>
                  {review.reviewer_id && (
                    <span style={{ fontSize: "0.65rem", color: "#4a5568", marginLeft: "auto" }}>
                      {review.reviewer_id}
                    </span>
                  )}
                </div>
                {review.rationale && (
                  <div style={{ fontSize: "0.72rem", color: "#cbd5e0", lineHeight: 1.5, borderTop: "1px solid #2d3748", paddingTop: "0.35rem", marginTop: "0.1rem" }}>
                    {review.rationale}
                  </div>
                )}
              </div>
            </div>
          )}
        </div>

        {/* Footer actions — hidden in print */}
        <div id="dispatch-print-footer" style={{ display: "flex", gap: "0.5rem", padding: "0.65rem 1rem", borderTop: "1px solid #2d3748", flexShrink: 0 }}>
          <button onClick={handleExport} style={{ background: "#1a3a5c", border: "1px solid #2b5a8a", borderRadius: 4, color: "#93c5fd", cursor: "pointer", fontSize: "0.72rem", fontWeight: 600, padding: "0.3rem 0.75rem" }}>
            Export JSON
          </button>
          <button onClick={handleCopy} style={{ background: "none", border: "1px solid #2d3748", borderRadius: 4, color: "#718096", cursor: "pointer", fontSize: "0.72rem", fontWeight: 600, padding: "0.3rem 0.75rem" }}>
            Copy brief
          </button>
          <button
            onClick={() => {
              const prev = document.title;
              document.title = `Patrol Dispatch — ${vessel.vessel_name || vessel.mmsi} (${vessel.mmsi})`;
              window.print();
              document.title = prev;
            }}
            style={{ background: "none", border: "1px solid #2d3748", borderRadius: 4, color: "#718096", cursor: "pointer", fontSize: "0.72rem", fontWeight: 600, padding: "0.3rem 0.75rem" }}
          >
            Print
          </button>
          <button onClick={onClose} style={{ marginLeft: "auto", background: "none", border: "1px solid #2d3748", borderRadius: 4, color: "#4a5568", cursor: "pointer", fontSize: "0.72rem", padding: "0.3rem 0.75rem" }}>
            Close
          </button>
        </div>
      </div>
    </>,
    document.body
  );
}
