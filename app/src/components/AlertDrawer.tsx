import { createPortal } from "react-dom";
import type { AlertEntry } from "../lib/alerts";
import { markRead, markAllRead } from "../lib/alerts";

interface Props {
  alerts: AlertEntry[];
  onClose: () => void;
  onSelectVessel: (mmsi: string) => void;
  onAlertsChange: (alerts: AlertEntry[]) => void;
}

function timeLabel(iso: string): string {
  const d = new Date(iso);
  const now = new Date();
  const diffMs = now.getTime() - d.getTime();
  const diffMin = Math.floor(diffMs / 60_000);
  if (diffMin < 1) return "just now";
  if (diffMin < 60) return `${diffMin}m ago`;
  const diffH = Math.floor(diffMin / 60);
  if (diffH < 24) return `${diffH}h ago`;
  return d.toLocaleDateString("en-GB", { day: "numeric", month: "short" });
}

function confidenceColor(c: number): string {
  if (c >= 0.75) return "#fc8181";
  if (c >= 0.5) return "#f6ad55";
  return "#68d391";
}

export default function AlertDrawer({ alerts, onClose, onSelectVessel, onAlertsChange }: Props) {
  function handleClick(entry: AlertEntry) {
    const updated = markRead(entry.id);
    onAlertsChange(updated);
    onSelectVessel(entry.mmsi);
    onClose();
  }

  function handleMarkAllRead() {
    onAlertsChange(markAllRead());
  }

  return createPortal(
    <>
      {/* Backdrop */}
      <div
        onClick={onClose}
        style={{ position: "fixed", inset: 0, background: "rgba(0,0,0,0.5)", zIndex: 200 }}
      />

      {/* Drawer */}
      <div
        role="dialog"
        aria-modal="true"
        aria-label="Alert history"
        style={{
          position: "fixed",
          top: 0,
          right: 0,
          bottom: 0,
          width: "min(360px, 92vw)",
          zIndex: 201,
          background: "#0f1117",
          borderLeft: "1px solid #2d3748",
          display: "flex",
          flexDirection: "column",
          fontFamily: "ui-monospace, monospace",
        }}
      >
        {/* Header */}
        <div style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "0.75rem 1rem",
          borderBottom: "1px solid #2d3748",
          flexShrink: 0,
        }}>
          <div style={{ fontWeight: 700, fontSize: "0.85rem", color: "#93c5fd" }}>
            Alert history
          </div>
          <div style={{ display: "flex", gap: "0.5rem", alignItems: "center" }}>
            {alerts.some((a) => !a.read) && (
              <button
                onClick={handleMarkAllRead}
                style={{
                  background: "none",
                  border: "1px solid #2d3748",
                  borderRadius: 4,
                  color: "#718096",
                  cursor: "pointer",
                  fontSize: "0.65rem",
                  padding: "0.2rem 0.5rem",
                }}
              >
                Mark all read
              </button>
            )}
            <button
              onClick={onClose}
              aria-label="Close alert drawer"
              style={{ background: "none", border: "none", color: "#4a5568", cursor: "pointer", fontSize: "1.1rem", lineHeight: 1, padding: "0.2rem 0.3rem" }}
            >
              ✕
            </button>
          </div>
        </div>

        {/* List */}
        <div style={{ overflowY: "auto", flex: 1 }}>
          {alerts.length === 0 ? (
            <div style={{ padding: "2rem 1rem", textAlign: "center", fontSize: "0.75rem", color: "#4a5568" }}>
              No alerts yet — alerts appear after sync when new vessels or confidence increases are detected.
            </div>
          ) : (
            alerts.map((entry) => (
              <div
                key={entry.id}
                onClick={() => handleClick(entry)}
                style={{
                  display: "flex",
                  alignItems: "flex-start",
                  gap: "0.6rem",
                  padding: "0.6rem 1rem",
                  borderBottom: "1px solid #1a1f2e",
                  cursor: "pointer",
                  background: entry.read ? "transparent" : "#111827",
                  transition: "background 0.1s",
                }}
                onMouseEnter={(e) => { (e.currentTarget as HTMLElement).style.background = "#1e2a3a"; }}
                onMouseLeave={(e) => { (e.currentTarget as HTMLElement).style.background = entry.read ? "transparent" : "#111827"; }}
              >
                {/* Unread dot */}
                <div style={{
                  width: 6,
                  height: 6,
                  borderRadius: "50%",
                  background: entry.read ? "transparent" : "#93c5fd",
                  flexShrink: 0,
                  marginTop: "0.35rem",
                }} />

                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ display: "flex", alignItems: "center", gap: "0.4rem", marginBottom: "0.15rem" }}>
                    <span style={{
                      fontSize: "0.72rem",
                      fontWeight: 600,
                      color: "#e2e8f0",
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                      flex: 1,
                    }}>
                      {entry.vessel_name || entry.mmsi}
                    </span>
                    <span style={{
                      fontSize: "0.65rem",
                      fontWeight: 700,
                      color: confidenceColor(entry.confidence),
                      flexShrink: 0,
                    }}>
                      {entry.confidence.toFixed(3)}
                    </span>
                  </div>
                  <div style={{ display: "flex", alignItems: "center", gap: "0.4rem" }}>
                    <span style={{
                      fontSize: "0.6rem",
                      padding: "0 0.3rem",
                      borderRadius: 2,
                      background: entry.kind === "new" ? "#1a3a5c" : "#2a1a3a",
                      border: `1px solid ${entry.kind === "new" ? "#2b5a8a" : "#5a2b8a"}`,
                      color: entry.kind === "new" ? "#93c5fd" : "#c084fc",
                      fontWeight: 600,
                    }}>
                      {entry.kind === "new" ? "NEW" : `+${(entry.delta! * 100).toFixed(0)}%`}
                    </span>
                    <span style={{ fontSize: "0.6rem", color: "#4a5568" }}>
                      MMSI {entry.mmsi}
                    </span>
                    <span style={{ fontSize: "0.6rem", color: "#4a5568", marginLeft: "auto", flexShrink: 0 }}>
                      {timeLabel(entry.timestamp)}
                    </span>
                  </div>
                </div>
              </div>
            ))
          )}
        </div>

        <div style={{ padding: "0.5rem 1rem", borderTop: "1px solid #2d3748", fontSize: "0.62rem", color: "#4a5568", flexShrink: 0 }}>
          Last 50 alerts · persisted in localStorage
        </div>
      </div>
    </>,
    document.body
  );
}
