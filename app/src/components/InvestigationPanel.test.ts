import { describe, it, expect, vi } from "vitest";

vi.mock("../lib/investigationStore", () => ({
  getInvestigationSession: vi.fn(),
  saveInvestigationField: vi.fn(),
  resetInvestigationSession: vi.fn(),
  initInvestigationStore: vi.fn(),
}));

import {
  TRIAGE_SYSTEM,
  SYNTHESIS_SYSTEM,
  BRIEFING_SYSTEM,
  vesselContext,
  triagePrompt,
  synthesisPrompt,
  briefingPrompt,
  osintLinks,
  toMarkdown,
} from "./InvestigationPanel";
import type { VesselRow } from "../lib/duckdb";

const BASE: VesselRow = {
  mmsi: "273449240",
  imo: "9354521",
  vessel_name: "PIONEER 92",
  flag: "MN",
  vessel_type: "Tug",
  region: "singapore",
  last_seen: "2026-05-02T00:23:42Z",
  last_lat: 1.25,
  last_lon: 103.9,
  confidence: 0.489,
  top_signals: JSON.stringify([
    { feature: "sts_candidate_count", value: 100, contribution: 1.0 },
    { feature: "ais_gap_count_30d", value: 5, contribution: 0.5 },
    { feature: "position_jump_count", value: 2, contribution: 0.2 },
  ]),
  ais_gap_count_30d: 5,
  sts_candidate_count: 100,
};

// ── System prompt constraints ─────────────────────────────────────────────────

describe("TRIAGE_SYSTEM", () => {
  it("requires exactly 3 indicators", () => {
    expect(TRIAGE_SYSTEM).toContain("exactly 3");
  });

  it("enforces 10-word limit per indicator", () => {
    expect(TRIAGE_SYSTEM).toContain("10 words");
  });

  it("prohibits referencing fields not in vessel data", () => {
    expect(TRIAGE_SYSTEM).toContain("Only reference fields present");
  });

  it("prohibits markdown", () => {
    expect(TRIAGE_SYSTEM.toLowerCase()).toContain("no markdown");
  });
});

describe("SYNTHESIS_SYSTEM", () => {
  it("requires exactly 2 sentences", () => {
    expect(SYNTHESIS_SYSTEM).toContain("Exactly 2 sentences");
  });

  it("prohibits inventing facts", () => {
    expect(SYNTHESIS_SYSTEM).toContain("Do NOT invent");
  });

  it("references both vessel data and OSINT notes as sources", () => {
    expect(SYNTHESIS_SYSTEM).toContain("vessel data");
    expect(SYNTHESIS_SYSTEM).toContain("OSINT notes");
  });
});

describe("BRIEFING_SYSTEM", () => {
  it("requires exactly 3 sentences", () => {
    expect(BRIEFING_SYSTEM).toContain("Exactly 3 sentences");
  });

  it("targets DSTA/MPA government audience", () => {
    expect(BRIEFING_SYSTEM).toContain("government audience");
  });

  it("prohibits markdown and bullet points", () => {
    expect(BRIEFING_SYSTEM).toContain("no markdown");
    expect(BRIEFING_SYSTEM).toContain("no bullet points");
  });
});

// ── vesselContext ─────────────────────────────────────────────────────────────

describe("vesselContext", () => {
  it("includes MMSI", () => {
    expect(vesselContext(BASE)).toContain("MMSI: 273449240");
  });

  it("includes IMO when present", () => {
    expect(vesselContext(BASE)).toContain("IMO: 9354521");
  });

  it("omits IMO when absent", () => {
    expect(vesselContext({ ...BASE, imo: null })).not.toContain("IMO:");
  });

  it("includes vessel name when different from MMSI", () => {
    expect(vesselContext(BASE)).toContain("Name: PIONEER 92");
  });

  it("omits vessel name when it equals MMSI", () => {
    expect(vesselContext({ ...BASE, vessel_name: BASE.mmsi })).not.toContain("Name:");
  });

  it("includes confidence", () => {
    expect(vesselContext(BASE)).toContain("Confidence: 0.489");
  });

  it("includes top 3 signals from JSON when present", () => {
    const ctx = vesselContext(BASE);
    expect(ctx).toContain("sts_candidate_count");
    expect(ctx).toContain("ais_gap_count_30d");
    expect(ctx).toContain("position_jump_count");
  });

  it("handles malformed top_signals gracefully", () => {
    expect(() => vesselContext({ ...BASE, top_signals: "not json" })).not.toThrow();
    expect(vesselContext({ ...BASE, top_signals: "not json" })).not.toContain("Top signals");
  });

  it("handles null top_signals", () => {
    expect(vesselContext({ ...BASE, top_signals: null })).not.toContain("Top signals");
  });

  it("includes AIS gap count when present", () => {
    expect(vesselContext(BASE)).toContain("AIS gaps (30d): 5");
  });

  it("omits AIS gap when null", () => {
    expect(vesselContext({ ...BASE, ais_gap_count_30d: null })).not.toContain("AIS gaps");
  });

  it("does not produce undefined or null strings", () => {
    const ctx = vesselContext(BASE);
    expect(ctx).not.toContain("undefined");
    expect(ctx).not.toContain("null");
  });
});

// ── triagePrompt ──────────────────────────────────────────────────────────────

describe("triagePrompt", () => {
  it("includes vessel context", () => {
    const prompt = triagePrompt(BASE);
    expect(prompt).toContain("273449240");
    expect(prompt).toContain("PIONEER 92");
  });

  it("asks for top 3 evasion indicators", () => {
    expect(triagePrompt(BASE)).toContain("top 3 evasion indicators");
  });
});

// ── synthesisPrompt ───────────────────────────────────────────────────────────

describe("synthesisPrompt", () => {
  it("includes vessel context", () => {
    const prompt = synthesisPrompt(BASE, "Vessel seen near Hormuz");
    expect(prompt).toContain("273449240");
  });

  it("includes analyst OSINT notes", () => {
    const prompt = synthesisPrompt(BASE, "Vessel seen near Hormuz");
    expect(prompt).toContain("Vessel seen near Hormuz");
  });

  it("includes fallback when notes are empty", () => {
    const prompt = synthesisPrompt(BASE, "");
    expect(prompt).toContain("no notes provided");
  });

  it("separates vessel data and OSINT notes with headers", () => {
    const prompt = synthesisPrompt(BASE, "some notes");
    expect(prompt).toContain("VESSEL DATA");
    expect(prompt).toContain("OSINT NOTES");
  });
});

// ── briefingPrompt ────────────────────────────────────────────────────────────

describe("briefingPrompt", () => {
  it("includes vessel context", () => {
    const prompt = briefingPrompt(BASE, "Assessed as high-risk STS operator");
    expect(prompt).toContain("273449240");
  });

  it("includes the synthesis / threat assessment", () => {
    const prompt = briefingPrompt(BASE, "Assessed as high-risk STS operator");
    expect(prompt).toContain("Assessed as high-risk STS operator");
  });

  it("targets DSTA/MPA audience", () => {
    expect(briefingPrompt(BASE, "synthesis")).toContain("DSTA/MPA");
  });

  it("separates vessel data and threat assessment with headers", () => {
    const prompt = briefingPrompt(BASE, "synthesis");
    expect(prompt).toContain("VESSEL DATA");
    expect(prompt).toContain("THREAT ASSESSMENT");
  });
});

// ── osintLinks ────────────────────────────────────────────────────────────────

describe("osintLinks", () => {
  it("includes MarineTraffic MMSI link", () => {
    const links = osintLinks(BASE);
    const mt = links.find((l) => l.label === "MarineTraffic (MMSI)");
    expect(mt).toBeDefined();
    expect(mt!.url).toContain("273449240");
    expect(mt!.url).toContain("marinetraffic.com");
  });

  it("includes MarineTraffic IMO link when IMO is present", () => {
    const links = osintLinks(BASE);
    const mt = links.find((l) => l.label === "MarineTraffic (IMO)");
    expect(mt).toBeDefined();
    expect(mt!.url).toContain("9354521");
  });

  it("omits MarineTraffic IMO link when IMO is absent", () => {
    const links = osintLinks({ ...BASE, imo: null });
    expect(links.find((l) => l.label === "MarineTraffic (IMO)")).toBeUndefined();
  });

  it("includes VesselFinder link", () => {
    const links = osintLinks(BASE);
    const vf = links.find((l) => l.label === "VesselFinder");
    expect(vf).toBeDefined();
    expect(vf!.url).toContain("273449240");
    expect(vf!.url).toContain("vesselfinder.com");
  });

  it("includes OFAC search link with vessel name when name differs from MMSI", () => {
    const links = osintLinks(BASE);
    const ofac = links.find((l) => l.label === "OFAC Search");
    expect(ofac).toBeDefined();
    expect(ofac!.url).toContain("PIONEER");
  });

  it("uses MMSI in OFAC search when vessel name equals MMSI", () => {
    const links = osintLinks({ ...BASE, vessel_name: BASE.mmsi });
    const ofac = links.find((l) => l.label === "OFAC Search");
    expect(ofac!.url).toContain("MMSI");
  });
});

// ── toMarkdown ────────────────────────────────────────────────────────────────

describe("toMarkdown", () => {
  const session = {
    triage: "1. High AIS gap count\n2. STS candidate\n3. Flag of convenience",
    osint_notes: "Vessel spotted near EOPL 2026-04-30",
    synthesis: "PIONEER 92 is an STS support vessel linked to Iran crude transfers.",
    briefing: "PIONEER 92 is a Mongolia-flagged tug operating at Singapore EOPL.",
    approved: true,
  };

  it("includes vessel name in heading", () => {
    expect(toMarkdown(BASE, session)).toContain("PIONEER 92");
  });

  it("includes MMSI in table", () => {
    expect(toMarkdown(BASE, session)).toContain("273449240");
  });

  it("includes triage output", () => {
    expect(toMarkdown(BASE, session)).toContain("High AIS gap count");
  });

  it("includes analyst OSINT notes", () => {
    expect(toMarkdown(BASE, session)).toContain("Vessel spotted near EOPL");
  });

  it("includes synthesis", () => {
    expect(toMarkdown(BASE, session)).toContain("STS support vessel");
  });

  it("includes briefing", () => {
    expect(toMarkdown(BASE, session)).toContain("Mongolia-flagged tug");
  });

  it("omits triage section when triage is absent", () => {
    expect(toMarkdown(BASE, { ...session, triage: undefined })).not.toContain("Triage");
  });

  it("omits OSINT notes section when notes are absent", () => {
    expect(toMarkdown(BASE, { ...session, osint_notes: undefined })).not.toContain("OSINT findings");
  });

  it("includes analyst-approved footer", () => {
    expect(toMarkdown(BASE, session)).toContain("analyst-approved");
  });
});
