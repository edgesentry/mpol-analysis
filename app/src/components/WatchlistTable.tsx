import { useState, useEffect, useRef } from "react";
import type { VesselRow } from "../lib/duckdb";
import { tierColor, HANDOFF_STATES, handoffLabel } from "../lib/reviews";
import type { DecisionTier, HandoffState } from "../lib/reviews";

interface Props {
  vessels: VesselRow[];
  selectedMmsi: string | null;
  onSelect: (mmsi: string) => void;
  reviewStates?: Map<string, { decision_tier: DecisionTier | null; handoff_state: HandoffState }>;
  handoffFilter?: HandoffState | "all";
  onHandoffFilterChange?: (v: HandoffState | "all") => void;
  onClaim?: (mmsi: string) => void;
  exportRegion?: string;
}

function confidenceColor(c: number): string {
  if (c >= 0.75) return "#fc8181";
  if (c >= 0.5) return "#f6ad55";
  return "#68d391";
}

/** Compact pill shown in the vessel name cell. */
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
        <span
          title={handoffState.replace(/_/g, " ")}
          style={{ fontSize: "0.6rem", color: "#93c5fd" }}
        >
          →
        </span>
      )}
    </span>
  );
}

export default function WatchlistTable({
  vessels,
  selectedMmsi,
  onSelect,
  reviewStates,
  handoffFilter = "all",
  onHandoffFilterChange,
  onClaim,
  exportRegion = "all",
}: Props) {
  const [search, setSearch] = useState("");
  const [hovered, setHovered] = useState<string | null>(null);
  const selectedRowRef = useRef<HTMLTableRowElement | null>(null);

  const filtered = vessels.filter((v) => {
    // Text search — vessel_name, mmsi, imo, flag
    if (search) {
      const q = search.toLowerCase();
      const match =
        v.vessel_name?.toLowerCase().includes(q) ||
        v.mmsi?.includes(q) ||
        v.imo?.toLowerCase().includes(q) ||
        v.flag?.toLowerCase().includes(q);
      if (!match) return false;
    }
    // Handoff-state filter
    if (handoffFilter !== "all") {
      const rs = reviewStates?.get(v.mmsi);
      const state = rs?.handoff_state ?? "queued_review";
      if (state !== handoffFilter) return false;
    }
    return true;
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
    selectedRowRef.current?.scrollIntoView({ block: "nearest" });
  }, [selectedMmsi]);

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        height: "100%",
        overflow: "hidden",
      }}
    >
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
            <option key={s} value={s}>
              {handoffLabel(s)}
            </option>
          ))}
        </select>
      </div>

      {/* Table */}
      <div style={{ overflowY: "auto", flex: 1 }}>
        <table
          style={{
            width: "100%",
            borderCollapse: "collapse",
            fontSize: "0.72rem",
          }}
        >
          <thead>
            <tr
              style={{
                background: "#1a1f2e",
                position: "sticky",
                top: 0,
                zIndex: 1,
              }}
            >
              {["Vessel", "Flag", "Type", "Conf", "Region"].map((h) => (
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
            {filtered.map((v) => {
              const rs = reviewStates?.get(v.mmsi);
              const isSelected = selectedMmsi === v.mmsi;
              return (
                <tr
                  key={v.mmsi}
                  ref={isSelected ? selectedRowRef : null}
                  onClick={() => onSelect(v.mmsi)}
                  style={{
                    cursor: "pointer",
                    background: isSelected ? "#1e3a5a" : "transparent",
                    borderBottom: "1px solid #1a1f2e",
                    borderLeft: rs?.decision_tier
                      ? `3px solid ${tierColor(rs.decision_tier)}`
                      : "3px solid transparent",
                  }}
                  onMouseEnter={(e) => {
                    if (!isSelected)
                      (e.currentTarget as HTMLElement).style.background = "#1e2a3a";
                    setHovered(v.mmsi);
                  }}
                  onMouseLeave={(e) => {
                    if (!isSelected)
                      (e.currentTarget as HTMLElement).style.background = "transparent";
                    setHovered(null);
                  }}
                >
                  <td
                    style={{
                      padding: "0.35rem 0.5rem",
                      maxWidth: 140,
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}
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
                  <td style={{ padding: "0.35rem 0.5rem", color: "#a0aec0" }}>
                    {v.flag || "—"}
                  </td>
                  <td
                    style={{
                      padding: "0.35rem 0.5rem",
                      color: "#a0aec0",
                      maxWidth: 80,
                      overflow: "hidden",
                      textOverflow: "ellipsis",
                      whiteSpace: "nowrap",
                    }}
                    title={v.vessel_type}
                  >
                    {v.vessel_type || "—"}
                  </td>
                  <td
                    style={{
                      padding: "0.35rem 0.5rem",
                      fontWeight: 700,
                      color: confidenceColor(v.confidence),
                    }}
                  >
                    {v.confidence.toFixed(3)}
                  </td>
                  <td style={{ padding: "0.35rem 0.5rem", color: "#718096", whiteSpace: "nowrap" }}>
                    {onClaim && hovered === v.mmsi ? (
                      (() => {
                        const isClaimed = rs?.handoff_state === "in_review";
                        return (
                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              if (!isClaimed) onClaim(v.mmsi);
                            }}
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
                    ) : (
                      v.region || "—"
                    )}
                  </td>
                </tr>
              );
            })}
            {filtered.length === 0 && (
              <tr>
                <td
                  colSpan={5}
                  style={{
                    padding: "2rem",
                    textAlign: "center",
                    color: "#4a5568",
                  }}
                >
                  {vessels.length === 0
                    ? "No data — sync from R2 first."
                    : "No results."}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      <div
        style={{
          padding: "0.35rem 0.75rem",
          fontSize: "0.65rem",
          color: "#4a5568",
          borderTop: "1px solid #2d3748",
          flexShrink: 0,
          display: "flex",
          alignItems: "center",
          gap: "0.5rem",
        }}
      >
        <span>
          {filtered.length} / {vessels.length} vessels
          {handoffFilter !== "all" && (
            <span style={{ color: "#93c5fd", marginLeft: "0.4rem" }}>
              · {handoffLabel(handoffFilter as HandoffState)}
            </span>
          )}
        </span>
        {filtered.length > 0 && (
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
              const filename = `watchlist_${exportRegion}_${ts}.json`;
              const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
              const url = URL.createObjectURL(blob);
              const a = document.createElement("a");
              a.href = url;
              a.download = filename;
              a.click();
              URL.revokeObjectURL(url);
            }}
            style={{
              marginLeft: "auto",
              background: "none",
              border: "1px solid #2d3748",
              borderRadius: 3,
              color: "#718096",
              cursor: "pointer",
              fontSize: "0.6rem",
              fontWeight: 600,
              padding: "0.1rem 0.4rem",
              fontFamily: "ui-monospace,monospace",
            }}
          >
            export
          </button>
        )}
      </div>
    </div>
  );
}
