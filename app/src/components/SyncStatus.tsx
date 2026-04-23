import type { SyncStatus } from "../lib/opfs";

interface Props {
  status: SyncStatus;
  onSync: () => void;
}

function formatCacheAge(iso: string): string {
  const days = Math.floor((Date.now() - new Date(iso).getTime()) / 86_400_000);
  if (days === 0) return "today";
  if (days === 1) return "1 day ago";
  return `${days} days ago`;
}

const BTN: React.CSSProperties = {
  background: "#2d3748",
  border: "none",
  color: "#e2e8f0",
  padding: "0.2rem 0.6rem",
  borderRadius: 4,
  cursor: "pointer",
  fontSize: "0.7rem",
};

const BTN_SMALL: React.CSSProperties = {
  marginLeft: "auto",
  background: "none",
  border: "1px solid #2d3748",
  color: "#718096",
  padding: "0.15rem 0.5rem",
  borderRadius: 3,
  cursor: "pointer",
  fontSize: "0.65rem",
};

export default function SyncStatusBar({ status, onSync }: Props) {
  const base: React.CSSProperties = {
    display: "flex",
    alignItems: "center",
    gap: "0.75rem",
    padding: "0.35rem 1.25rem",
    fontSize: "0.7rem",
    background: "#0f1117",
    borderBottom: "1px solid #2d3748",
    color: "#718096",
    flexShrink: 0,
  };

  if (status.phase === "idle") {
    return (
      <div style={base}>
        <span>Not synced.</span>
        <button onClick={onSync} style={BTN}>Sync from R2</button>
      </div>
    );
  }

  if (status.phase === "fetching_manifest") {
    return <div style={base}>Fetching manifest…</div>;
  }

  if (status.phase === "syncing") {
    const pct = Math.round((status.done / Math.max(status.total, 1)) * 100);
    return (
      <div style={base}>
        <div
          style={{
            width: 80, height: 4, background: "#2d3748",
            borderRadius: 2, overflow: "hidden",
          }}
        >
          <div
            style={{
              width: `${pct}%`, height: "100%",
              background: "#3b82f6", transition: "width 0.2s",
            }}
          />
        </div>
        <span>
          Syncing {status.done}/{status.total} — {status.current}
        </span>
      </div>
    );
  }

  if (status.phase === "loading") {
    return <div style={base}>Loading into DuckDB-WASM…</div>;
  }

  if (status.phase === "ready") {
    const ageLabel = status.oldestCacheDate
      ? ` · Data cached ${formatCacheAge(status.oldestCacheDate)}`
      : "";
    return (
      <div style={{ ...base, color: "#48bb78" }}>
        ✓ {status.filesLoaded} file{status.filesLoaded !== 1 ? "s" : ""} loaded
        {status.fromFixtures
          ? " (demo fixtures — sync for live data)"
          : status.fromCache
          ? ` (from OPFS cache${ageLabel})`
          : ` (synced from R2${ageLabel})`}
        <button onClick={onSync} style={BTN_SMALL}>Re-sync</button>
      </div>
    );
  }

  if (status.phase === "error") {
    return (
      <div style={{ ...base, color: "#fc8181" }}>
        ✗ {status.message}
        <button onClick={onSync} style={{ ...BTN_SMALL, marginLeft: "auto" }}>
          Retry
        </button>
      </div>
    );
  }

  return null;
}
