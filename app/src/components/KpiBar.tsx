import type { VesselRow, MetricsRow } from "../lib/duckdb";

interface Props {
  vessels: VesselRow[];
  metrics: MetricsRow | null;
  unreadAlerts?: number;
  onBellClick?: () => void;
}

function Kpi({ label, value }: { label: string; value: string }) {
  return (
    <div
      style={{ display: "flex", flexDirection: "column", gap: "0.1rem" }}
    >
      <span
        style={{
          fontSize: "0.65rem",
          textTransform: "uppercase",
          letterSpacing: "0.08em",
          color: "#718096",
        }}
      >
        {label}
      </span>
      <span style={{ fontSize: "1.1rem", fontWeight: 700, color: "#e2e8f0" }}>
        {value}
      </span>
    </div>
  );
}

export default function KpiBar({ vessels, metrics, unreadAlerts = 0, onBellClick }: Props) {
  const total = vessels.length;
  const high = vessels.filter((v) => v.confidence >= 0.75).length;
  const avg =
    total > 0
      ? (vessels.reduce((s, v) => s + v.confidence, 0) / total).toFixed(3)
      : "—";

  const p50 = metrics?.precision_at_50 ?? metrics?.backtest_summary_p_at_50 ?? null;
  const auroc = metrics?.auroc ?? metrics?.backtest_summary_auroc ?? null;

  return (
    <div
      style={{
        display: "flex",
        gap: "1.5rem",
        padding: "0.6rem 1.25rem",
        background: "#161b27",
        borderBottom: "1px solid #2d3748",
        flexShrink: 0,
      }}
    >
      <Kpi label="Candidates" value={total > 0 ? String(total) : "—"} />
      <Kpi label="High (≥0.75)" value={total > 0 ? String(high) : "—"} />
      <Kpi label="Avg confidence" value={total > 0 ? avg : "—"} />
      {p50 != null && (
        <Kpi
          label="Precision@50"
          value={
            typeof p50 === "number" ? `${(p50 * 100).toFixed(1)}%` : String(p50)
          }
        />
      )}
      {auroc != null && (
        <Kpi
          label="AUROC"
          value={typeof auroc === "number" ? auroc.toFixed(4) : String(auroc)}
        />
      )}

      {/* Bell icon — right-aligned */}
      <button
        onClick={onBellClick}
        aria-label={`Alert history${unreadAlerts > 0 ? ` (${unreadAlerts} unread)` : ""}`}
        style={{
          marginLeft: "auto",
          position: "relative",
          background: "none",
          border: "none",
          cursor: "pointer",
          padding: "0.2rem 0.3rem",
          color: unreadAlerts > 0 ? "#93c5fd" : "#4a5568",
          fontSize: "1rem",
          lineHeight: 1,
          alignSelf: "center",
        }}
      >
        🔔
        {unreadAlerts > 0 && (
          <span style={{
            position: "absolute",
            top: -2,
            right: -4,
            background: "#fc8181",
            color: "#0f1117",
            borderRadius: "50%",
            fontSize: "0.5rem",
            fontWeight: 700,
            minWidth: 14,
            height: 14,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            fontFamily: "ui-monospace, monospace",
            padding: "0 2px",
          }}>
            {unreadAlerts > 9 ? "9+" : unreadAlerts}
          </span>
        )}
      </button>
    </div>
  );
}
