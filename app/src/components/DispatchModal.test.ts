import { describe, it, expect, vi } from "vitest";

vi.mock("../lib/reviews", () => ({
  getReview: vi.fn(),
  tierColor: vi.fn(),
  handoffLabel: vi.fn((s: string) => s),
}));
vi.mock("../lib/humanise", () => ({
  formatLastSeen: vi.fn((v: string | null) => v ?? "—"),
  confidenceTier: vi.fn((c: number) => (c >= 0.75 ? "CRITICAL" : c >= 0.5 ? "ELEVATED" : "WATCH")),
  confidenceTierColor: vi.fn(() => "#fc8181"),
  signalLabel: vi.fn((f: string) => f),
  signalSeverity: vi.fn(() => "HIGH"),
  severityColor: vi.fn(() => "#fc8181"),
}));

import {
  parseSignals,
  buildPdfFilename,
  buildExportPayload,
  buildCopyMarkdown,
  type ShapSignal,
} from "./dispatchBriefUtils";
import type { VesselRow } from "../lib/duckdb";
import type { VesselReview } from "../lib/reviews";

const BASE_VESSEL: VesselRow = {
  mmsi: "123456789",
  imo: "9305609",
  vessel_name: "TEST VESSEL",
  flag: "PA",
  vessel_type: "Tanker",
  region: "singapore",
  last_seen: "2026-04-24T08:00:00Z",
  last_lat: 1.3521,
  last_lon: 103.8198,
  confidence: 0.87,
  top_signals: null,
};

const SIGNALS: ShapSignal[] = [
  { feature: "ais_gap_count_30d", value: 4, contribution: 0.42 },
  { feature: "high_risk_flag_ratio", value: 0.8, contribution: 0.31 },
];

const REVIEW: VesselReview = {
  mmsi: "123456789",
  decision_tier: "Confirmed",
  handoff_state: "handoff_recommended",
  reviewer_id: "analyst_01",
  rationale: "Multiple AIS gaps near known STS zone.",
  identifier_basis: "MMSI",
  outcome: null,
  outcome_notes: null,
  officer_id: null,
  created_at: "2026-04-24T07:00:00Z",
  updated_at: "2026-04-24T08:00:00Z",
};

const FIXED_DATE = new Date("2026-04-24T09:00:00Z");

// ── parseSignals ─────────────────────────────────────────────────────────────

describe("parseSignals", () => {
  it("returns empty array for null input", () => {
    expect(parseSignals(null)).toEqual([]);
  });

  it("returns empty array for empty string", () => {
    expect(parseSignals("")).toEqual([]);
  });

  it("returns empty array for invalid JSON", () => {
    expect(parseSignals("{bad json")).toEqual([]);
  });

  it("returns empty array when JSON is not an array", () => {
    expect(parseSignals('{"feature":"x"}')).toEqual([]);
  });

  it("parses a valid signals array", () => {
    const raw = JSON.stringify(SIGNALS);
    expect(parseSignals(raw)).toEqual(SIGNALS);
  });
});

// ── buildPdfFilename ─────────────────────────────────────────────────────────

describe("buildPdfFilename", () => {
  it("produces correct format", () => {
    expect(buildPdfFilename("123456789", FIXED_DATE)).toBe("dispatch_brief_123456789_2026-04-24");
  });

  it("uses today's date when no date supplied", () => {
    const result = buildPdfFilename("999000001");
    expect(result).toMatch(/^dispatch_brief_999000001_\d{4}-\d{2}-\d{2}$/);
  });

  it("uses the MMSI verbatim", () => {
    expect(buildPdfFilename("000000001", FIXED_DATE)).toContain("000000001");
  });
});

// ── buildExportPayload ───────────────────────────────────────────────────────

describe("buildExportPayload", () => {
  it("includes core vessel fields", () => {
    const p = buildExportPayload(BASE_VESSEL, SIGNALS, "brief text", null, FIXED_DATE);
    expect(p.mmsi).toBe("123456789");
    expect(p.imo).toBe("9305609");
    expect(p.vessel_name).toBe("TEST VESSEL");
    expect(p.flag).toBe("PA");
    expect(p.confidence).toBe(0.87);
  });

  it("sets exported_at from the supplied date", () => {
    const p = buildExportPayload(BASE_VESSEL, SIGNALS, "", null, FIXED_DATE);
    expect(p.exported_at).toBe("2026-04-24T09:00:00.000Z");
  });

  it("includes analyst_brief when provided", () => {
    const p = buildExportPayload(BASE_VESSEL, SIGNALS, "summary here", null, FIXED_DATE);
    expect(p.analyst_brief).toBe("summary here");
  });

  it("sets analyst_brief to null for empty string", () => {
    const p = buildExportPayload(BASE_VESSEL, SIGNALS, "", null, FIXED_DATE);
    expect(p.analyst_brief).toBeNull();
  });

  it("maps signals with feature, label, value, contribution", () => {
    const p = buildExportPayload(BASE_VESSEL, SIGNALS, "", null, FIXED_DATE);
    expect(p.top_signals).toHaveLength(2);
    expect(p.top_signals[0].feature).toBe("ais_gap_count_30d");
    expect(p.top_signals[0].contribution).toBe(0.42);
  });

  it("sets review to null when not provided", () => {
    const p = buildExportPayload(BASE_VESSEL, [], "", null, FIXED_DATE);
    expect(p.review).toBeNull();
  });

  it("includes review fields when provided", () => {
    const p = buildExportPayload(BASE_VESSEL, [], "", REVIEW, FIXED_DATE);
    expect(p.review).not.toBeNull();
    expect(p.review!.decision_tier).toBe("Confirmed");
    expect(p.review!.handoff_state).toBe("handoff_recommended");
    expect(p.review!.reviewer_id).toBe("analyst_01");
    expect(p.review!.rationale).toBe("Multiple AIS gaps near known STS zone.");
  });

  it("sets lat/lon to null when absent", () => {
    const p = buildExportPayload({ ...BASE_VESSEL, last_lat: null, last_lon: null }, [], "", null, FIXED_DATE);
    expect(p.last_lat).toBeNull();
    expect(p.last_lon).toBeNull();
  });
});

// ── buildCopyMarkdown ────────────────────────────────────────────────────────

describe("buildCopyMarkdown", () => {
  it("starts with the patrol dispatch heading", () => {
    const md = buildCopyMarkdown(BASE_VESSEL, [], "", null, FIXED_DATE);
    expect(md).toMatch(/^# Patrol Dispatch Brief/);
  });

  it("includes MMSI and vessel name", () => {
    const md = buildCopyMarkdown(BASE_VESSEL, [], "", null, FIXED_DATE);
    expect(md).toContain("**MMSI:** 123456789");
    expect(md).toContain("**Vessel:** TEST VESSEL");
  });

  it("includes IMO when present", () => {
    const md = buildCopyMarkdown(BASE_VESSEL, [], "", null, FIXED_DATE);
    expect(md).toContain("**IMO:** 9305609");
  });

  it("omits IMO line when null", () => {
    const md = buildCopyMarkdown({ ...BASE_VESSEL, imo: null }, [], "", null, FIXED_DATE);
    expect(md).not.toContain("**IMO:**");
  });

  it("includes confidence score and tier", () => {
    const md = buildCopyMarkdown(BASE_VESSEL, [], "", null, FIXED_DATE);
    expect(md).toContain("0.870");
    expect(md).toContain("CRITICAL");
  });

  it("includes position when lat/lon present", () => {
    const md = buildCopyMarkdown(BASE_VESSEL, [], "", null, FIXED_DATE);
    expect(md).toContain("**Position:**");
    expect(md).toContain("1.3521");
  });

  it("omits position when lat/lon null", () => {
    const md = buildCopyMarkdown({ ...BASE_VESSEL, last_lat: null, last_lon: null }, [], "", null, FIXED_DATE);
    expect(md).not.toContain("**Position:**");
  });

  it("includes top signals section when signals present", () => {
    const md = buildCopyMarkdown(BASE_VESSEL, SIGNALS, "", null, FIXED_DATE);
    expect(md).toContain("## Top signals");
    expect(md).toContain("ais_gap_count_30d");
    expect(md).toContain("42%");
  });

  it("omits signals section when empty", () => {
    const md = buildCopyMarkdown(BASE_VESSEL, [], "", null, FIXED_DATE);
    expect(md).not.toContain("## Top signals");
  });

  it("includes analyst brief when provided", () => {
    const md = buildCopyMarkdown(BASE_VESSEL, [], "vessel is suspicious", null, FIXED_DATE);
    expect(md).toContain("## Analyst brief");
    expect(md).toContain("vessel is suspicious");
  });

  it("omits analyst brief section when empty", () => {
    const md = buildCopyMarkdown(BASE_VESSEL, [], "", null, FIXED_DATE);
    expect(md).not.toContain("## Analyst brief");
  });

  it("includes review decision when provided", () => {
    const md = buildCopyMarkdown(BASE_VESSEL, [], "", REVIEW, FIXED_DATE);
    expect(md).toContain("## Review decision");
    expect(md).toContain("Confirmed");
    expect(md).toContain("analyst_01");
    expect(md).toContain("Multiple AIS gaps");
  });

  it("omits review section when null", () => {
    const md = buildCopyMarkdown(BASE_VESSEL, [], "", null, FIXED_DATE);
    expect(md).not.toContain("## Review decision");
  });

  it("ends with a generation timestamp", () => {
    const md = buildCopyMarkdown(BASE_VESSEL, [], "", null, FIXED_DATE);
    expect(md).toContain("*Generated 2026-04-24T09:00:00.000Z*");
  });

  it("falls back to MMSI when vessel_name is empty", () => {
    const md = buildCopyMarkdown({ ...BASE_VESSEL, vessel_name: "" }, [], "", null, FIXED_DATE);
    expect(md).toContain("**Vessel:** 123456789");
  });
});
