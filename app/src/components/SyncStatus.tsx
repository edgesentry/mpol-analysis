import type { SyncStatus } from "../lib/opfs";
import type { PushStatus } from "../lib/push";

interface Props {
  status: SyncStatus;
  onSync: () => void;
  userEmail?: string | null;
  pushStatus?: PushStatus;
  onPush?: () => void;
}

function formatCacheAge(iso: string): string {
  const days = Math.floor((Date.now() - new Date(iso).getTime()) / 86_400_000);
  if (days === 0) return "today";
  if (days === 1) return "1 day ago";
  return `${days} days ago`;
}

function formatPushedAt(iso: string): string {
  const mins = Math.floor((Date.now() - new Date(iso).getTime()) / 60_000);
  if (mins < 1) return "just now";
  if (mins === 1) return "1 min ago";
  if (mins < 60) return `${mins} mins ago`;
  return new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
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

const BTN_PUSH: React.CSSProperties = {
  ...BTN,
  background: "#1e3a5f",
  border: "1px solid #2b6cb0",
  color: "#90cdf4",
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

export default function SyncStatusBar({ status, onSync, userEmail, pushStatus, onPush }: Props) {
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

  // Push button — shown only when a user is logged in and sync is ready/idle
  const showPush =
    !!userEmail &&
    !!onPush &&
    (status.phase === "ready" || status.phase === "idle");

  function PushBtn() {
    if (!showPush) return null;
    const phase = pushStatus?.phase ?? "idle";
    const pushing = phase === "exporting" || phase === "uploading";
    const label =
      phase === "exporting" ? "Exporting…"
      : phase === "uploading" ? "Uploading…"
      : phase === "done"     ? `Pushed ${formatPushedAt((pushStatus as { phase: "done"; pushedAt: string }).pushedAt)}`
      : phase === "error"    ? "Push failed — retry"
      : "Push reviews";

    return (
      <button
        onClick={onPush}
        disabled={pushing}
        title={userEmail ?? undefined}
        style={{
          ...BTN_PUSH,
          opacity: pushing ? 0.6 : 1,
          cursor: pushing ? "default" : "pointer",
          color: phase === "error" ? "#fc8181"
               : phase === "done"  ? "#68d391"
               : "#90cdf4",
        }}
      >
        ↑ {label}
      </button>
    );
  }

  if (status.phase === "idle") {
    return (
      <div style={base}>
        <span>Not synced.</span>
        <button onClick={onSync} style={BTN}>Sync from R2</button>
        <PushBtn />
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
        <PushBtn />
        <button onClick={onSync} style={BTN_SMALL}>Re-sync</button>
      </div>
    );
  }

  if (status.phase === "error") {
    return (
      <div style={{ ...base, color: "#fc8181" }}>
        ✗ {status.message}
        <PushBtn />
        <button onClick={onSync} style={{ ...BTN_SMALL, marginLeft: showPush ? "0" : "auto" }}>
          Retry
        </button>
      </div>
    );
  }

  return null;
}
