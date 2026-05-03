import { useState, useEffect, useRef } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import type { VesselRow } from "../lib/duckdb";
import { tierColor, HANDOFF_STATES, handoffLabel } from "../lib/reviews";
import type { DecisionTier, HandoffState } from "../lib/reviews";

interface Props {
  vessels: VesselRow[];
  statelessVessels?: VesselRow[];
  selectedMmsi: string | null;
  onSelect: (mmsi: string) => void;
  reviewStates?: Map<string, { decision_tier: DecisionTier | null; handoff_state: HandoffState }>;
  handoffFilter?: HandoffState | "all";
  onHandoffFilterChange?: (v: HandoffState | "all") => void;
  onClaim?: (mmsi: string) => void;
  exportRegion?: string;
  scoreHistory?: Map<string, number[]>;
}

// ── Sparkline ────────────────────────────────────────────────────────────────

function Sparkline({ scores }: { scores: number[] }) {
  if (scores.length < 2) return null;
  const W = 56, H = 14;
  const min = Math.min(...scores);
  const max = Math.max(...scores);
  const range = max - min || 0.01;
  const pts = scores.map((s, i) => {
    const x = (i / (scores.length - 1)) * W;
    const y = H - ((s - min) / range) * H;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });
  // Trend: compare last 5 vs first 5
  const first = scores.slice(0, 5).reduce((a, b) => a + b, 0) / 5;
  const last  = scores.slice(-5).reduce((a, b) => a + b, 0) / 5;
  const color = last > first + 0.02 ? "#fc8181" : last < first - 0.02 ? "#68d391" : "#4a5568";

  // Tooltip: last value + date label approximation
  const lastScore = scores[scores.length - 1];

  const trendLabel = last > first + 0.02 ? "↑" : last < first - 0.02 ? "↓" : "→";
  return (
    <span title={`Latest: ${lastScore.toFixed(3)} · 30d trend: ${trendLabel}`} style={{ display: "inline-block", lineHeight: 0 }}>
      <svg width={W} height={H} style={{ display: "block", overflow: "visible" }}>
        <polyline
          points={pts.join(" ")}
          fill="none"
          stroke={color}
          strokeWidth="1.2"
          strokeLinejoin="round"
          strokeLinecap="round"
        />
        <circle
          cx={parseFloat(pts[pts.length - 1].split(",")[0])}
          cy={parseFloat(pts[pts.length - 1].split(",")[1])}
          r="1.8"
          fill={color}
        />
      </svg>
    </span>
  );
}

const ROW_HEIGHT = 30; // px — keep in sync with row padding

function confidenceColor(c: number): string {
  if (c >= 0.75) return "#fc8181";
  if (c >= 0.5) return "#f6ad55";
  return "#68d391";
}

function TierBadge({
  tier,
  handoffState,
}: {
  tier: DecisionTier | null;
  handoffState: HandoffState;
}) {
  const color = tierColor(tier);
  const label = tier ? tier.slice(0, 3).toUpperCase() : "—";
  const showHandoff =
    handoffState === "handoff_recommended" ||
    handoffState === "handoff_accepted" ||
    handoffState === "handoff_completed";

  return (
    <span style={{ display: "inline-flex", alignItems: "center", gap: "0.2rem", marginRight: "0.35rem", flexShrink: 0 }}>
      <span
        title={tier ?? "unreviewed"}
        style={{
          display: "inline-block",
          padding: "0 0.3rem",
          borderRadius: 3,
          fontSize: "0.6rem",
          fontWeight: 700,
          fontFamily: "ui-monospace,monospace",
          letterSpacing: "0.03em",
          background: tier ? color + "22" : "#1a1f2e",
          border: `1px solid ${tier ? color : "#2d3748"}`,
          color: tier ? color : "#4a5568",
          lineHeight: "1.5",
        }}
      >
        {label}
      </span>
      {showHandoff && (
        <span title={handoffState.replace(/_/g, " ")} style={{ fontSize: "0.6rem", color: "#93c5fd" }}>→</span>
      )}
    </span>
  );
}

function StatelessRow({
  v,
  isSelected,
  onSelect,
}: {
  v: VesselRow;
  isSelected: boolean;
  onSelect: (mmsi: string) => void;
}) {
  const mid = v.mmsi.slice(0, 3);
  return (
    <tr
      onClick={() => onSelect(v.mmsi)}
      style={{
        height: ROW_HEIGHT,
        cursor: "pointer",
        background: isSelected ? "#1e3a5a" : "#1a1200",
        borderBottom: "1px solid #2d2800",
        borderLeft: "3px solid #f6ad55",
      }}
      onMouseEnter={(e) => { if (!isSelected) (e.currentTarget as HTMLElement).style.background = "#2a2000"; }}
      onMouseLeave={(e) => { if (!isSelected) (e.currentTarget as HTMLElement).style.background = "#1a1200"; }}
    >
      <td style={{ padding: "0.35rem 0.5rem", maxWidth: 140, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
          title={`MMSI ${v.mmsi} — MID ${mid} unallocated (ITU)`}>
        <span style={{ fontFamily: "ui-monospace,monospace", color: "#f6ad55", fontSize: "0.7rem" }}>
          ⚠ {v.mmsi}
        </span>
      </td>
      <td style={{ padding: "0.35rem 0.5rem", color: "#718096", fontFamily: "ui-monospace,monospace", fontSize: "0.65rem" }}>
        MID {mid}
      </td>
      <td style={{ padding: "0.35rem 0.5rem", color: "#a0aec0" }}>
        {v.vessel_type || "—"}
      </td>
      <td style={{ padding: "0.35rem 0.5rem", fontWeight: 700, color: "#f6ad55" }}>
        {v.confidence.toFixed(3)}
      </td>
      <td style={{ padding: "0.35rem 0.5rem", color: "#718096", fontSize: "0.65rem" }}>
        {v.last_seen ? v.last_seen.slice(0, 10) : "—"}
      </td>
    </tr>
  );
}

export default function WatchlistTable({
  vessels,
  statelessVessels = [],
  selectedMmsi,
  onSelect,
  reviewStates,
  handoffFilter = "all",
  onHandoffFilterChange,
  onClaim,
  exportRegion = "all",
  scoreHistory,
}: Props) {
  const [search, setSearch] = useState("");
  const [hovered, setHovered] = useState<string | null>(null);
  const scrollRef = useRef<HTMLDivElement | null>(null);

  const filtered = vessels.filter((v) => {
    if (search) {
      const q = search.toLowerCase();
      const match =
        v.vessel_name?.toLowerCase().includes(q) ||
        v.mmsi?.includes(q) ||
        v.imo?.toLowerCase().includes(q) ||
        v.flag?.toLowerCase().includes(q);
      if (!match) return false;
    }
    if (handoffFilter !== "all") {
      const state = reviewStates?.get(v.mmsi)?.handoff_state ?? "queued_review";
      if (state !== handoffFilter) return false;
    }
    return true;
  });

  const virtualizer = useVirtualizer({
    count: filtered.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 8,
  });

  // Auto-select when search narrows to exactly one result
  useEffect(() => {
    if (search && filtered.length === 1 && filtered[0].mmsi !== selectedMmsi) {
      onSelect(filtered[0].mmsi);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [search, filtered.length]);

  // Scroll selected row into view
  useEffect(() => {
    if (!selectedMmsi) return;
    const idx = filtered.findIndex((v) => v.mmsi === selectedMmsi);
    if (idx >= 0) virtualizer.scrollToIndex(idx, { align: "auto" });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedMmsi]);

  // Keyboard navigation: ↑ / ↓ move selection within filtered list
  function handleKeyDown(e: React.KeyboardEvent) {
    if (e.key !== "ArrowDown" && e.key !== "ArrowUp") return;
    e.preventDefault();
    const idx = filtered.findIndex((v) => v.mmsi === selectedMmsi);
    const next =
      e.key === "ArrowDown"
        ? Math.min(idx + 1, filtered.length - 1)
        : Math.max(idx - 1, 0);
    if (filtered[next]) {
      onSelect(filtered[next].mmsi);
      virtualizer.scrollToIndex(next, { align: "auto" });
    }
  }

  const virtualItems = virtualizer.getVirtualItems();
  const totalSize = virtualizer.getTotalSize();
  const paddingTop = virtualItems.length > 0 ? virtualItems[0].start : 0;
  const paddingBottom =
    virtualItems.length > 0 ? totalSize - virtualItems[virtualItems.length - 1].end : 0;

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" }}>
      {/* Search + handoff filter */}
      <div style={{ padding: "0.5rem 0.75rem", borderBottom: "1px solid #2d3748", display: "flex", flexDirection: "column", gap: "0.4rem" }}>
        <input
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Search name / MMSI / IMO / flag…"
          style={{
            width: "100%",
            background: "#0f1117",
            border: "1px solid #2d3748",
            borderRadius: 4,
            color: "#e2e8f0",
            padding: "0.3rem 0.5rem",
            fontSize: "0.75rem",
            outline: "none",
            boxSizing: "border-box",
          }}
        />
        <select
          value={handoffFilter}
          onChange={(e) => onHandoffFilterChange?.(e.target.value as HandoffState | "all")}
          style={{
            width: "100%",
            background: "#0f1117",
            border: handoffFilter !== "all" ? "1px solid #93c5fd" : "1px solid #2d3748",
            borderRadius: 4,
            color: handoffFilter !== "all" ? "#93c5fd" : "#718096",
            padding: "0.3rem 0.5rem",
            fontSize: "0.72rem",
            outline: "none",
            boxSizing: "border-box",
          }}
        >
          <option value="all">All handoff states</option>
          {HANDOFF_STATES.map((s) => (
            <option key={s} value={s}>{handoffLabel(s)}</option>
          ))}
        </select>
      </div>

      {/* Stateless MMSI section — pinned above ranked list */}
      {statelessVessels.length > 0 && (
        <div style={{ borderBottom: "1px solid #2d3748", flexShrink: 0 }}>
          <div style={{
            padding: "0.25rem 0.75rem",
            fontSize: "0.6rem",
            fontWeight: 700,
            textTransform: "uppercase",
            letterSpacing: "0.08em",
            color: "#f6ad55",
            background: "#1a1200",
            display: "flex",
            alignItems: "center",
            gap: "0.4rem",
          }}>
            <span>⚠ Stateless MMSI</span>
            <span style={{ color: "#718096", fontWeight: 400 }}>— ITU-unallocated MID · not visible on MarineTraffic / VesselFinder</span>
          </div>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.72rem" }}>
            <tbody>
              {statelessVessels.map((v) => (
                <StatelessRow
                  key={v.mmsi}
                  v={v}
                  isSelected={selectedMmsi === v.mmsi}
                  onSelect={onSelect}
                />
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Virtual table */}
      <div
        ref={scrollRef}
        onKeyDown={handleKeyDown}
        tabIndex={0}
        style={{ overflowY: "auto", flex: 1, outline: "none" }}
      >
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.72rem" }}>
          <thead>
            <tr style={{ background: "#1a1f2e", position: "sticky", top: 0, zIndex: 1 }}>
              {["Vessel", "Flag", "Type", "Conf", scoreHistory?.size ? "Trend" : "Region"].map((h) => (
                <th
                  key={h}
                  style={{
                    padding: "0.4rem 0.5rem",
                    textAlign: "left",
                    color: "#718096",
                    fontWeight: 600,
                    fontSize: "0.65rem",
                    textTransform: "uppercase",
                    letterSpacing: "0.05em",
                    borderBottom: "1px solid #2d3748",
                    whiteSpace: "nowrap",
                  }}
                >
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {/* Spacer — rows above viewport */}
            {paddingTop > 0 && (
              <tr><td colSpan={5} style={{ height: paddingTop, padding: 0, border: "none" }} /></tr>
            )}

            {virtualItems.map((vRow) => {
              const v = filtered[vRow.index];
              const rs = reviewStates?.get(v.mmsi);
              const isSelected = selectedMmsi === v.mmsi;
              return (
                <tr
                  key={v.mmsi}
                  data-index={vRow.index}
                  onClick={() => onSelect(v.mmsi)}
                  style={{
                    height: ROW_HEIGHT,
                    cursor: "pointer",
                    background: isSelected ? "#1e3a5a" : "transparent",
                    borderBottom: "1px solid #1a1f2e",
                    borderLeft: rs?.decision_tier
                      ? `3px solid ${tierColor(rs.decision_tier)}`
                      : "3px solid transparent",
                  }}
                  onMouseEnter={(e) => {
                    if (!isSelected) (e.currentTarget as HTMLElement).style.background = "#1e2a3a";
                    setHovered(v.mmsi);
                  }}
                  onMouseLeave={(e) => {
                    if (!isSelected) (e.currentTarget as HTMLElement).style.background = "transparent";
                    setHovered(null);
                  }}
                >
                  <td
                    style={{ padding: "0.35rem 0.5rem", maxWidth: 140, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
                    title={v.vessel_name}
                  >
                    <span style={{ display: "inline-flex", alignItems: "center", maxWidth: "100%" }}>
                      {reviewStates && (
                        <TierBadge
                          tier={rs?.decision_tier ?? null}
                          handoffState={rs?.handoff_state ?? "queued_review"}
                        />
                      )}
                      <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                        {v.vessel_name || v.mmsi}
                      </span>
                    </span>
                  </td>
                  <td style={{ padding: "0.35rem 0.5rem", color: "#a0aec0" }}>{v.flag || "—"}</td>
                  <td
                    style={{ padding: "0.35rem 0.5rem", color: "#a0aec0", maxWidth: 80, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}
                    title={v.vessel_type}
                  >
                    {v.vessel_type || "—"}
                  </td>
                  <td style={{ padding: "0.35rem 0.5rem", fontWeight: 700, color: confidenceColor(v.confidence) }}>
                    {v.confidence.toFixed(3)}
                  </td>
                  <td style={{ padding: "0.2rem 0.5rem", color: "#718096", whiteSpace: "nowrap" }}>
                    {onClaim && hovered === v.mmsi ? (
                      (() => {
                        const isClaimed = rs?.handoff_state === "in_review";
                        return (
                          <button
                            onClick={(e) => { e.stopPropagation(); if (!isClaimed) onClaim(v.mmsi); }}
                            disabled={isClaimed}
                            style={{
                              background: isClaimed ? "none" : "#1a3a5c",
                              border: `1px solid ${isClaimed ? "#2d3748" : "#2b5a8a"}`,
                              borderRadius: 3,
                              color: isClaimed ? "#4a5568" : "#93c5fd",
                              cursor: isClaimed ? "default" : "pointer",
                              fontSize: "0.6rem",
                              fontWeight: 600,
                              padding: "0.1rem 0.4rem",
                              fontFamily: "ui-monospace,monospace",
                            }}
                          >
                            {isClaimed ? "claimed" : "claim"}
                          </button>
                        );
                      })()
                    ) : scoreHistory?.size ? (
                      <Sparkline scores={scoreHistory.get(v.mmsi) ?? []} />
                    ) : (
                      v.region || "—"
                    )}
                  </td>
                </tr>
              );
            })}

            {/* Spacer — rows below viewport */}
            {paddingBottom > 0 && (
              <tr><td colSpan={5} style={{ height: paddingBottom, padding: 0, border: "none" }} /></tr>
            )}

            {filtered.length === 0 && (
              <tr>
                <td colSpan={5} style={{ padding: "2rem", textAlign: "center", color: "#4a5568" }}>
                  {vessels.length === 0 ? "No data — sync from R2 first." : "No results."}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Footer */}
      <div style={{
        padding: "0.35rem 0.75rem",
        fontSize: "0.65rem",
        color: "#4a5568",
        borderTop: "1px solid #2d3748",
        flexShrink: 0,
        display: "flex",
        alignItems: "center",
        gap: "0.5rem",
      }}>
        <span>
          {filtered.length} / {vessels.length} vessels
          {handoffFilter !== "all" && (
            <span style={{ color: "#93c5fd", marginLeft: "0.4rem" }}>
              · {handoffLabel(handoffFilter as HandoffState)}
            </span>
          )}
        </span>
        {vessels.length > 0 && (
          <button
            onClick={() => {
              const payload = filtered.map((v) => ({
                mmsi: v.mmsi,
                imo: v.imo ?? null,
                vessel_name: v.vessel_name || null,
                flag: v.flag || null,
                vessel_type: v.vessel_type || null,
                confidence: v.confidence,
                region: v.region || null,
                last_lat: v.last_lat ?? null,
                last_lon: v.last_lon ?? null,
                last_seen: v.last_seen ?? null,
                top_signals: v.top_signals ? JSON.parse(v.top_signals) : [],
              }));
              const ts = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
              const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
              const url = URL.createObjectURL(blob);
              const a = document.createElement("a");
              a.href = url;
              a.download = `watchlist_${exportRegion}_${ts}.json`;
              a.click();
              URL.revokeObjectURL(url);
            }}
            style={{
              marginLeft: "auto",
              background: "#1a1f2e",
              border: "1px solid #4a5568",
              borderRadius: 3,
              color: "#a0aec0",
              cursor: "pointer",
              fontSize: "0.65rem",
              fontWeight: 600,
              padding: "0.15rem 0.5rem",
              fontFamily: "ui-monospace,monospace",
              flexShrink: 0,
            }}
          >
            export
          </button>
        )}
      </div>
    </div>
  );
}
