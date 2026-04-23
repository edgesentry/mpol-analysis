import { describe, it, expect, vi } from "vitest";

// Stub heavy browser dependencies before importing the component.
vi.mock("../lib/duckdb", () => ({
  isParquetRegistered: vi.fn(),
}));
vi.mock("../lib/briefCache", () => ({
  getCachedBrief: vi.fn(),
  saveCachedBrief: vi.fn(),
  initBriefCache: vi.fn(),
}));
vi.mock("../lib/reviews", () => ({
  getReview: vi.fn(),
  tierColor: vi.fn(),
  handoffLabel: vi.fn(),
}));
vi.mock("../lib/humanise", () => ({
  formatLastSeen: vi.fn((v) => v ?? "—"),
  confidenceTier: vi.fn(() => "HIGH"),
  confidenceTierColor: vi.fn(() => "#f00"),
  signalLabel: vi.fn((f: string) => f),
  signalSeverity: vi.fn(() => null),
  severityColor: vi.fn(() => "#f00"),
}));

import { SYSTEM_PROMPT, buildUserContent } from "./VesselDetail";
import type { VesselRow } from "../lib/duckdb";

const BASE_VESSEL: VesselRow = {
  mmsi: "123456789",
  imo: null,
  vessel_name: "TEST VESSEL",
  flag: "SG",
  vessel_type: "Tanker",
  region: "singapore",
  last_seen: "2026-04-22T10:00:00Z",
  last_lat: 1.3521,
  last_lon: 103.8198,
  confidence: 0.87,
  top_signals: null,
};

// ── SYSTEM_PROMPT constraints ────────────────────────────────────────────────

describe("SYSTEM_PROMPT", () => {
  it("prohibits inventing MMSIs and vessel identifiers", () => {
    expect(SYSTEM_PROMPT).toContain("Do NOT invent");
    expect(SYSTEM_PROMPT).toContain("MMSI");
    expect(SYSTEM_PROMPT).toContain("IMO number");
    expect(SYSTEM_PROMPT).toContain("vessel name");
  });

  it("prohibits adding unverified sanctions or ownership claims", () => {
    expect(SYSTEM_PROMPT).toContain("sanctions designations");
    expect(SYSTEM_PROMPT).toContain("ownership links");
  });

  it("requires plain text output only", () => {
    expect(SYSTEM_PROMPT).toContain("plain text only");
    expect(SYSTEM_PROMPT).toContain("no markdown");
  });

  it("enforces a 3-sentence maximum", () => {
    expect(SYSTEM_PROMPT).toContain("Maximum 3 sentences");
  });

  it("references the context block as the sole source of truth", () => {
    expect(SYSTEM_PROMPT).toContain("context block");
  });
});

// ── buildUserContent ─────────────────────────────────────────────────────────

describe("buildUserContent", () => {
  it("includes MMSI and confidence", () => {
    const content = buildUserContent(BASE_VESSEL);
    expect(content).toContain("MMSI: 123456789");
    expect(content).toContain("Anomaly confidence: 0.870");
  });

  it("includes vessel name when present", () => {
    const content = buildUserContent(BASE_VESSEL);
    expect(content).toContain("Vessel: TEST VESSEL");
  });

  it("falls back to MMSI in vessel line when name is absent", () => {
    const content = buildUserContent({ ...BASE_VESSEL, vessel_name: "" });
    expect(content).toContain("Vessel: 123456789");
  });

  it("omits empty fields — flag", () => {
    const content = buildUserContent({ ...BASE_VESSEL, flag: "" });
    expect(content).not.toContain("Flag:");
  });

  it("omits empty fields — vessel type", () => {
    const content = buildUserContent({ ...BASE_VESSEL, vessel_type: "" });
    expect(content).not.toContain("Type:");
  });

  it("omits empty fields — region", () => {
    const content = buildUserContent({ ...BASE_VESSEL, region: "" });
    expect(content).not.toContain("Region:");
  });

  it("omits null fields — last_seen", () => {
    const content = buildUserContent({ ...BASE_VESSEL, last_seen: null });
    expect(content).not.toContain("Last seen:");
  });

  it("omits position when lat/lon are null", () => {
    const content = buildUserContent({ ...BASE_VESSEL, last_lat: null, last_lon: null });
    expect(content).not.toContain("Position:");
  });

  it("includes position when both lat and lon are present", () => {
    const content = buildUserContent(BASE_VESSEL);
    expect(content).toContain("Position:");
    expect(content).toContain("1.3521");
    expect(content).toContain("103.8198");
  });

  it("does not include any field not in the vessel row", () => {
    const content = buildUserContent(BASE_VESSEL);
    // Ensure no invented data markers leak in
    expect(content).not.toContain("undefined");
    expect(content).not.toContain("null");
    expect(content).not.toContain("NaN");
  });
});
