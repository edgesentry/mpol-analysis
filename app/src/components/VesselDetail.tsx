import { useState, useEffect, useRef } from "react";
import type { VesselRow } from "../lib/duckdb";

interface Props {
  vessel: VesselRow;
  onClose: () => void;
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

function confidenceColor(c: number): string {
  if (c >= 0.75) return "#fc8181";
  if (c >= 0.5) return "#f6ad55";
  return "#68d391";
}

export default function VesselDetail({ vessel, onClose }: Props) {
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
            flexShrink: 0,
          }}
          aria-label="Close detail panel"
        >
          ✕
        </button>
      </div>

      {/* Confidence badge */}
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
            fontWeight: 600,
            fontFamily: "ui-monospace, monospace",
          }}
        >
          confidence {vessel.confidence.toFixed(3)}
        </span>
      </div>

      {/* Details table */}
      <table style={{ borderCollapse: "collapse", width: "100%" }}>
        <tbody>
          {row("Flag", vessel.flag)}
          {row("Type", vessel.vessel_type)}
          {row("Region", vessel.region)}
          {row("Last seen", vessel.last_seen)}
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
    </div>
  );
}
