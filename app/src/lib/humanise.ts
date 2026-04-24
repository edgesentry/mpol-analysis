/**
 * Human-readable formatting helpers for maritime operational display.
 * Used in VesselDetail, DispatchModal, and export output.
 */

// ── Timestamp ────────────────────────────────────────────────────────────────

/** "2026-04-17T12:00:00Z" → "17 Apr 2026 12:00 UTC" */
export function formatLastSeen(iso: string | null | undefined): string {
  if (!iso) return "—";
  try {
    const d = new Date(iso);
    if (isNaN(d.getTime())) return iso;
    return d.toUTCString().replace(/:\d\d GMT$/, " UTC").replace(/^.*, /, "");
  } catch {
    return iso;
  }
}

// ── Confidence label ─────────────────────────────────────────────────────────

export type ConfidenceTier = "CRITICAL" | "ELEVATED" | "WATCH";

export function confidenceTier(c: number): ConfidenceTier {
  if (c >= 0.75) return "CRITICAL";
  if (c >= 0.5) return "ELEVATED";
  return "WATCH";
}

export function confidenceTierColor(c: number): string {
  if (c >= 0.75) return "#fc8181";
  if (c >= 0.5) return "#f6ad55";
  return "#68d391";
}

// ── Signal display names ─────────────────────────────────────────────────────

const SIGNAL_LABELS: Record<string, string> = {
  high_risk_flag_ratio:            "High-risk flag state",
  ais_gap_count_30d:               "AIS dark periods (30d)",
  ais_gap_max_hours:               "Longest AIS dark window (hours)",
  loitering_hours_30d:             "Loitering hours (30d)",
  sanctions_distance:              "Proximity to sanctioned vessel",
  route_cargo_mismatch:            "Route / cargo mismatch",
  position_jump_count:             "GPS position jumps",
  flag_changes_2y:                 "Flag changes (2 years)",
  sts_hub_degree:                  "Ship-to-ship transfer hub",
  sts_candidate_count:             "STS candidates",
  shared_address_centrality:       "Shared registration address",
  dark_activity_days:              "AIS dark days",
  port_state_control_deficiencies: "Port state control deficiencies",
  port_call_ratio:                 "Port call frequency ratio",
  behavioral_deviation_score:      "Behavioural deviation",
  graph_risk_score:                "Network risk score",
  identity_score:                  "Identity risk score",
};

export function signalLabel(feature: string): string {
  return (
    SIGNAL_LABELS[feature] ??
    feature
      .replace(/_/g, " ")
      .replace(/\b\w/g, (c) => c.toUpperCase())
  );
}

// ── Signal severity bands ────────────────────────────────────────────────────

export type Severity = "HIGH" | "ELEVATED" | "LOW";

type SeverityThreshold = { high: number; elevated: number; invert?: boolean };

/** invert=true means lower value = higher risk (e.g. sanctions_distance) */
const SEVERITY_THRESHOLDS: Record<string, SeverityThreshold> = {
  high_risk_flag_ratio:            { high: 0.6,  elevated: 0.3 },
  ais_gap_count_30d:               { high: 8,    elevated: 3 },
  sanctions_distance:              { high: 1,    elevated: 3,  invert: true },
  route_cargo_mismatch:            { high: 0.7,  elevated: 0.4 },
  position_jump_count:             { high: 5,    elevated: 2 },
  flag_changes_2y:                 { high: 3,    elevated: 1 },
  sts_hub_degree:                  { high: 5,    elevated: 2 },
  shared_address_centrality:       { high: 4,    elevated: 2 },
  dark_activity_days:              { high: 10,   elevated: 4 },
  port_state_control_deficiencies: { high: 3,    elevated: 1 },
};

export function signalSeverity(feature: string, value: number | string | null): Severity | null {
  const thresh = SEVERITY_THRESHOLDS[feature];
  if (!thresh || value == null) return null;
  const n = typeof value === "number" ? value : parseFloat(String(value));
  if (isNaN(n)) return null;

  if (thresh.invert) {
    if (n <= thresh.high) return "HIGH";
    if (n <= thresh.elevated) return "ELEVATED";
    return "LOW";
  }
  if (n >= thresh.high) return "HIGH";
  if (n >= thresh.elevated) return "ELEVATED";
  return "LOW";
}

export function severityColor(s: Severity): string {
  if (s === "HIGH") return "#fc8181";
  if (s === "ELEVATED") return "#f6ad55";
  return "#68d391";
}
