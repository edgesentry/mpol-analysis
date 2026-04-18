import { useEffect, useRef } from "react";
import type { VesselRow } from "../lib/duckdb";

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

function confidenceColor(c: number): string {
  if (c >= 0.75) return "#fc8181";
  if (c >= 0.5) return "#f6ad55";
  return "#68d391";
}

interface Props {
  vessel: VesselRow;
  brief: string;
  onClose: () => void;
}

export default function DispatchModal({ vessel, brief, onClose }: Props) {
  const signals = parseSignals(vessel.top_signals);
  const maxContrib = signals.length ? Math.max(...signals.map((s) => s.contribution)) : 1;
  const closeRef = useRef<HTMLButtonElement | null>(null);

  // Focus close button on open; close on Escape
  useEffect(() => {
    closeRef.current?.focus();
    const handler = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
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
      region: vessel.region || null,
      last_lat: vessel.last_lat ?? null,
      last_lon: vessel.last_lon ?? null,
      last_seen: vessel.last_seen ?? null,
      top_signals: signals,
      analyst_brief: brief || null,
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
      .map((s) => `- **${s.feature.replace(/_/g, " ")}**: ${s.value ?? "—"} (${(s.contribution * 100).toFixed(0)}%)`)
      .join("\n");

    const md = [
      `# Patrol Dispatch Brief`,
      `**Vessel:** ${vessel.vessel_name || vessel.mmsi}`,
      `**MMSI:** ${vessel.mmsi}`,
      vessel.imo ? `**IMO:** ${vessel.imo}` : null,
      `**Flag:** ${vessel.flag || "—"}`,
      `**Type:** ${vessel.vessel_type || "—"}`,
      `**Region:** ${vessel.region || "—"}`,
      `**Last seen:** ${vessel.last_seen || "—"}`,
      vessel.last_lat != null && vessel.last_lon != null
        ? `**Position:** ${vessel.last_lat.toFixed(4)}°, ${vessel.last_lon.toFixed(4)}°`
        : null,
      `**Anomaly confidence:** ${vessel.confidence.toFixed(3)}`,
      "",
      signals.length ? `## Top signals\n${signalLines}` : null,
      "",
      brief ? `## Analyst brief\n${brief}` : null,
      "",
      `---`,
      `*Generated ${new Date().toISOString()}*`,
    ]
      .filter((l) => l !== null)
      .join("\n");

    navigator.clipboard.writeText(md).catch(() => {/* ignore */});
  }

  return (
    <>
      {/* Backdrop */}
      <div
        onClick={onClose}
        style={{
          position: "fixed",
          inset: 0,
          background: "rgba(0,0,0,0.6)",
          zIndex: 100,
        }}
      />

      {/* Modal */}
      <div
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
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            padding: "0.75rem 1rem",
            borderBottom: "1px solid #2d3748",
            flexShrink: 0,
          }}
        >
          <div>
            <div style={{ fontWeight: 700, fontSize: "0.9rem", color: "#93c5fd" }}>
              Dispatch Brief
            </div>
            <div style={{ fontSize: "0.68rem", color: "#4a5568", marginTop: 2 }}>
              {vessel.vessel_name || vessel.mmsi} · MMSI {vessel.mmsi}
            </div>
          </div>
          <button
            ref={closeRef}
            onClick={onClose}
            aria-label="Close dispatch modal"
            style={{
              background: "none",
              border: "none",
              color: "#4a5568",
              cursor: "pointer",
              fontSize: "1.1rem",
              lineHeight: 1,
              padding: "0.2rem 0.3rem",
            }}
          >
            ✕
          </button>
        </div>

        {/* Body */}
        <div style={{ overflowY: "auto", padding: "0.85rem 1rem", flex: 1 }}>

          {/* Confidence */}
          <div style={{ marginBottom: "0.75rem" }}>
            <span
              style={{
                display: "inline-block",
                padding: "0.2rem 0.6rem",
                borderRadius: 4,
                background: "#1a1f2e",
                border: `1px solid ${confidenceColor(vessel.confidence)}`,
                color: confidenceColor(vessel.confidence),
                fontSize: "0.78rem",
                fontWeight: 700,
              }}
            >
              confidence {vessel.confidence.toFixed(3)}
            </span>
          </div>

          {/* Vessel details */}
          <table style={{ borderCollapse: "collapse", width: "100%", marginBottom: "0.75rem" }}>
            <tbody>
              {[
                ["Flag", vessel.flag],
                ["Type", vessel.vessel_type],
                ["Region", vessel.region],
                ["Last seen", vessel.last_seen],
                vessel.last_lat != null && vessel.last_lon != null
                  ? ["Position", `${vessel.last_lat.toFixed(4)}°, ${vessel.last_lon.toFixed(4)}°`]
                  : null,
                vessel.imo ? ["IMO", vessel.imo] : null,
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
              <div style={{ fontSize: "0.65rem", textTransform: "uppercase", letterSpacing: "0.08em", color: "#4a5568", marginBottom: "0.4rem" }}>
                Top signals
              </div>
              {signals.map((s) => {
                const pct = (s.contribution / maxContrib) * 100;
                const label = s.feature.replace(/_/g, " ");
                return (
                  <div key={s.feature} title={`${s.feature}: ${s.value ?? "—"}`} style={{ marginBottom: "0.3rem" }}>
                    <div style={{ display: "flex", alignItems: "center", gap: "0.4rem" }}>
                      <span style={{ fontSize: "0.65rem", color: "#a0aec0", width: 160, flexShrink: 0, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                        {label}
                      </span>
                      <div style={{ flex: 1, background: "#1a1f2e", borderRadius: 2, height: 6, minWidth: 0 }}>
                        <div style={{ width: `${pct}%`, background: "#fc8181", height: "100%", borderRadius: 2 }} />
                      </div>
                      <span style={{ fontSize: "0.65rem", color: "#718096", minWidth: 28, textAlign: "right" }}>
                        {(s.contribution * 100).toFixed(0)}%
                      </span>
                      <span style={{ fontSize: "0.65rem", color: "#4a5568", minWidth: 32, textAlign: "right" }}>
                        {s.value ?? "—"}
                      </span>
                    </div>
                  </div>
                );
              })}
            </div>
          )}

          {/* Analyst brief */}
          {brief && (
            <div>
              <div style={{ fontSize: "0.65rem", textTransform: "uppercase", letterSpacing: "0.08em", color: "#4a5568", marginBottom: "0.35rem" }}>
                Analyst brief
              </div>
              <div
                style={{
                  fontSize: "0.75rem",
                  color: "#cbd5e0",
                  lineHeight: 1.6,
                  padding: "0.5rem 0.7rem",
                  background: "#1a1f2e",
                  borderRadius: 4,
                  border: "1px solid #2d3748",
                  borderLeft: "3px solid #93c5fd",
                }}
              >
                {brief}
              </div>
            </div>
          )}
        </div>

        {/* Footer actions */}
        <div
          style={{
            display: "flex",
            gap: "0.5rem",
            padding: "0.65rem 1rem",
            borderTop: "1px solid #2d3748",
            flexShrink: 0,
          }}
        >
          <button
            onClick={handleExport}
            style={{
              background: "#1a3a5c",
              border: "1px solid #2b5a8a",
              borderRadius: 4,
              color: "#93c5fd",
              cursor: "pointer",
              fontSize: "0.72rem",
              fontWeight: 600,
              padding: "0.3rem 0.75rem",
            }}
          >
            Export JSON
          </button>
          <button
            onClick={handleCopy}
            style={{
              background: "none",
              border: "1px solid #2d3748",
              borderRadius: 4,
              color: "#718096",
              cursor: "pointer",
              fontSize: "0.72rem",
              fontWeight: 600,
              padding: "0.3rem 0.75rem",
            }}
          >
            Copy brief
          </button>
          <button
            onClick={() => window.print()}
            style={{
              background: "none",
              border: "1px solid #2d3748",
              borderRadius: 4,
              color: "#718096",
              cursor: "pointer",
              fontSize: "0.72rem",
              fontWeight: 600,
              padding: "0.3rem 0.75rem",
            }}
          >
            Print
          </button>
          <button
            onClick={onClose}
            style={{
              marginLeft: "auto",
              background: "none",
              border: "1px solid #2d3748",
              borderRadius: 4,
              color: "#4a5568",
              cursor: "pointer",
              fontSize: "0.72rem",
              padding: "0.3rem 0.75rem",
            }}
          >
            Close
          </button>
        </div>
      </div>
    </>
  );
}
