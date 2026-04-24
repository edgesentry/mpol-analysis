import type { VesselRow } from "../lib/duckdb";
import type { VesselReview } from "../lib/reviews";
import { confidenceTier, formatLastSeen, signalLabel, signalSeverity } from "../lib/humanise";
import { handoffLabel } from "../lib/reviews";

export interface ShapSignal {
  feature: string;
  value: number | string | null;
  contribution: number;
}

export function parseSignals(raw: string | null | undefined): ShapSignal[] {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? (parsed as ShapSignal[]) : [];
  } catch {
    return [];
  }
}

export function buildPdfFilename(mmsi: string, date?: Date): string {
  const d = (date ?? new Date()).toISOString().slice(0, 10);
  return `dispatch_brief_${mmsi}_${d}`;
}

export function buildExportPayload(
  vessel: VesselRow,
  signals: ShapSignal[],
  brief: string,
  review: VesselReview | null,
  now?: Date,
) {
  return {
    exported_at: (now ?? new Date()).toISOString(),
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
}

export function buildCopyMarkdown(
  vessel: VesselRow,
  signals: ShapSignal[],
  brief: string,
  review: VesselReview | null,
  now?: Date,
): string {
  const signalLines = signals
    .map((s) => {
      const sev = signalSeverity(s.feature, s.value);
      return `- **${signalLabel(s.feature)}**: ${s.value ?? "—"}${sev ? ` [${sev}]` : ""} (${(s.contribution * 100).toFixed(0)}%)`;
    })
    .join("\n");

  return [
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
    `*Generated ${(now ?? new Date()).toISOString()}*`,
  ]
    .filter((l) => l !== null)
    .join("\n");
}
