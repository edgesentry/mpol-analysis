/**
 * Structured OSINT investigation panel — local LLM-assisted analyst workflow.
 *
 * Five steps, each with a single bounded LLM call or analyst input.
 * Designed for small local models (Qwen2.5-7B, Llama 3.1-8B): every prompt
 * is narrowly scoped with an explicit output format requirement.
 *
 * Step 1 — Triage          LLM: identify top 3 evasion indicators from vessel data
 * Step 2 — OSINT links     No LLM: pre-built MarineTraffic / VesselFinder / OFAC URLs
 * Step 3 — Analyst notes   Human pastes findings from OSINT sources
 * Step 4 — Synthesis       LLM: combine vessel data + analyst notes into threat assessment
 * Step 5 — Briefing        LLM: draft 3-sentence DSTA/MPA briefing; approve to save
 */

import { useState, useEffect, useRef } from "react";
import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
import type { VesselRow } from "../lib/duckdb";
import {
  getInvestigationSession,
  saveInvestigationField,
  resetInvestigationSession,
  type InvestigationSession,
} from "../lib/investigationStore";

// ── LLM config (shared with VesselDetail) ────────────────────────────────────

const LLM_ENDPOINT =
  import.meta.env.VITE_LLM_ENDPOINT ?? "https://localhost:8443/v1/chat/completions";
const LLM_TIMEOUT_MS = 60_000;
const LLM_MODEL =
  import.meta.env.VITE_LLM_MODEL ?? "bartowski/Qwen2.5-7B-Instruct-GGUF:Q4_K_M";

type StepStatus = "idle" | "loading" | "done" | "offline" | "error";

// ── Prompt builders ───────────────────────────────────────────────────────────

export function vesselContext(v: VesselRow): string {
  const signals = (() => {
    try {
      const parsed = JSON.parse(v.top_signals ?? "[]");
      if (!Array.isArray(parsed)) return "";
      return parsed
        .slice(0, 3)
        .map((s: { feature: string; value: unknown; contribution: number }) =>
          `${s.feature}=${s.value} (weight ${(s.contribution * 100).toFixed(0)}%)`
        )
        .join(", ");
    } catch {
      return "";
    }
  })();

  return [
    `MMSI: ${v.mmsi}`,
    v.imo ? `IMO: ${v.imo}` : null,
    v.vessel_name && v.vessel_name !== v.mmsi ? `Name: ${v.vessel_name}` : null,
    v.flag ? `Flag: ${v.flag}` : null,
    v.vessel_type ? `Type: ${v.vessel_type}` : null,
    v.region ? `Region: ${v.region}` : null,
    `Confidence: ${v.confidence.toFixed(3)}`,
    v.ais_gap_count_30d != null ? `AIS gaps (30d): ${v.ais_gap_count_30d}` : null,
    v.sts_candidate_count != null ? `STS candidates: ${v.sts_candidate_count}` : null,
    signals ? `Top signals: ${signals}` : null,
    v.last_seen ? `Last seen: ${v.last_seen}` : null,
  ]
    .filter(Boolean)
    .join("\n");
}

export const TRIAGE_SYSTEM =
  "You are a maritime threat analyst. You will receive structured vessel data. " +
  "Your task: identify the top 3 evasion indicators. " +
  "STRICT CONSTRAINTS: " +
  "- List exactly 3 indicators, each on its own line starting with a number and period. " +
  "- Each indicator must be 10 words or fewer. " +
  "- Only reference fields present in the vessel data. " +
  "- No markdown, no headers, no extra text before or after the list.";

export function triagePrompt(v: VesselRow): string {
  return `Identify the top 3 evasion indicators for this vessel.\n\n${vesselContext(v)}`;
}

export const SYNTHESIS_SYSTEM =
  "You are a maritime intelligence analyst. You will receive structured vessel data and " +
  "OSINT notes gathered by the analyst. " +
  "Your task: synthesise both into a threat assessment. " +
  "STRICT CONSTRAINTS: " +
  "- Exactly 2 sentences. " +
  "- Sentence 1: describe the evasion scenario supported by the data. " +
  "- Sentence 2: state the most likely next action or risk. " +
  "- Do NOT invent facts not present in the provided data or OSINT notes. " +
  "- Plain text only, no markdown.";

export function synthesisPrompt(v: VesselRow, notes: string): string {
  return (
    `Synthesise the vessel data and OSINT findings into a 2-sentence threat assessment.\n\n` +
    `--- VESSEL DATA ---\n${vesselContext(v)}\n\n` +
    `--- OSINT NOTES ---\n${notes.trim() || "(no notes provided)"}`
  );
}

export const BRIEFING_SYSTEM =
  "You are a maritime intelligence analyst writing a briefing for a government audience. " +
  "You will receive vessel data and a threat assessment. " +
  "Your task: draft a 3-sentence briefing note. " +
  "STRICT CONSTRAINTS: " +
  "- Exactly 3 sentences. " +
  "- Sentence 1: what the vessel is and what it is doing. " +
  "- Sentence 2: why it poses a risk (link to sanctions, evasion behaviour, or shadow fleet indicators). " +
  "- Sentence 3: recommended action (monitor / escalate / cross-reference with named agency). " +
  "- Plain text only, no markdown, no bullet points.";

export function briefingPrompt(v: VesselRow, synthesis: string): string {
  return (
    `Draft a 3-sentence briefing note for a DSTA/MPA audience.\n\n` +
    `--- VESSEL DATA ---\n${vesselContext(v)}\n\n` +
    `--- THREAT ASSESSMENT ---\n${synthesis}`
  );
}

// ── LLM call ─────────────────────────────────────────────────────────────────

async function callLLM(
  systemPrompt: string,
  userPrompt: string,
  maxTokens: number,
  signal: AbortSignal
): Promise<string> {
  const res = await fetch(LLM_ENDPOINT, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      model: LLM_MODEL,
      max_tokens: maxTokens,
      temperature: 0.2,
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userPrompt },
      ],
    }),
    signal,
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const data = await res.json();
  return (data?.choices?.[0]?.message?.content ?? "").trim();
}

// ── OSINT links ───────────────────────────────────────────────────────────────

export function osintLinks(v: VesselRow) {
  const links: { label: string; url: string }[] = [
    {
      label: "MarineTraffic (MMSI)",
      url: `https://www.marinetraffic.com/en/ais/details/ships/mmsi:${v.mmsi}`,
    },
  ];
  if (v.imo) {
    links.push({
      label: "MarineTraffic (IMO)",
      url: `https://www.marinetraffic.com/en/ais/details/ships/imo:${v.imo}`,
    });
  }
  links.push({
    label: "VesselFinder",
    url: `https://www.vesselfinder.com/?mmsi=${v.mmsi}`,
  });
  const query = v.vessel_name && v.vessel_name !== v.mmsi ? v.vessel_name : `MMSI ${v.mmsi}`;
  links.push({
    label: "OFAC Search",
    url: `https://sanctionssearch.ofac.treas.gov/?searchText=${encodeURIComponent(query)}`,
  });
  if (v.imo) {
    links.push({
      label: "ITU MMSI Lookup",
      url: `https://www.itu.int/mmsapp/SearchStation/search`,
    });
  }
  return links;
}

// ── Markdown export ───────────────────────────────────────────────────────────

export function toMarkdown(v: VesselRow, session: Partial<InvestigationSession>): string {
  const name = v.vessel_name && v.vessel_name !== v.mmsi ? v.vessel_name : v.mmsi;
  const date = new Date().toISOString().slice(0, 10);
  const lines = [
    `## OSINT Investigation — ${name} (${date})`,
    "",
    `| Field | Value |`,
    `|---|---|`,
    `| MMSI | ${v.mmsi} |`,
    v.imo ? `| IMO | ${v.imo} |` : null,
    `| Flag | ${v.flag || "—"} |`,
    `| Type | ${v.vessel_type || "—"} |`,
    `| Region | ${v.region || "—"} |`,
    `| Confidence | ${v.confidence.toFixed(3)} |`,
    "",
    session.triage
      ? `### Triage — Top 3 evasion indicators\n\n${session.triage}`
      : null,
    session.osint_notes?.trim()
      ? `\n### OSINT findings (analyst)\n\n${session.osint_notes}`
      : null,
    session.synthesis
      ? `\n### Threat assessment\n\n${session.synthesis}`
      : null,
    session.briefing
      ? `\n### Briefing note\n\n${session.briefing}`
      : null,
    "",
    `---`,
    `*Generated by arktrace OSINT investigation workflow — analyst-approved*`,
  ]
    .filter((l) => l !== null)
    .join("\n");
  return lines;
}

// ── Shared styles ─────────────────────────────────────────────────────────────

const sectionLabel: React.CSSProperties = {
  fontSize: "0.65rem",
  textTransform: "uppercase",
  letterSpacing: "0.08em",
  color: "#4a5568",
  marginBottom: "0.35rem",
};

const outputBox: React.CSSProperties = {
  fontSize: "0.75rem",
  color: "#cbd5e0",
  lineHeight: 1.5,
  padding: "0.4rem 0.6rem",
  background: "#1a1f2e",
  borderRadius: 4,
  border: "1px solid #2d3748",
  borderLeft: "3px solid #68d391",
  whiteSpace: "pre-wrap",
};

const stepBtn = (disabled: boolean): React.CSSProperties => ({
  background: disabled ? "#1a1f2e" : "#2b4a8a",
  border: "1px solid #2d3748",
  borderRadius: 4,
  color: disabled ? "#4a5568" : "#93c5fd",
  cursor: disabled ? "default" : "pointer",
  fontSize: "0.7rem",
  fontWeight: 600,
  padding: "0.25rem 0.7rem",
  marginTop: "0.5rem",
});

const offlineMsg: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: "0.4rem",
  padding: "0.35rem 0.6rem",
  borderRadius: 4,
  background: "#1a1f2e",
  border: "1px solid #4a5568",
  fontSize: "0.72rem",
  color: "#718096",
};

// ── Component ─────────────────────────────────────────────────────────────────

interface Props {
  vessel: VesselRow;
  conn: AsyncDuckDBConnection | null;
}

type Step = 1 | 2 | 3 | 4 | 5;

export default function InvestigationPanel({ vessel, conn }: Props) {
  const [step, setStep] = useState<Step>(1);
  const [session, setSession] = useState<Partial<InvestigationSession>>({});
  const [status, setStatus] = useState<StepStatus>("idle");
  const [notes, setNotes] = useState("");
  const [briefingEdit, setBriefingEdit] = useState("");
  const [copied, setCopied] = useState(false);
  const abortRef = useRef<AbortController | null>(null);

  // Load any existing session when vessel changes
  useEffect(() => {
    setStep(1);
    setStatus("idle");
    setNotes("");
    setBriefingEdit("");
    if (!conn) return;
    getInvestigationSession(conn, vessel.mmsi).then((s) => {
      if (s) {
        setSession(s);
        setNotes(s.osint_notes ?? "");
        setBriefingEdit(s.briefing ?? "");
        // Resume at the furthest completed step
        if (s.briefing) setStep(5);
        else if (s.synthesis) setStep(4);
        else if (s.osint_notes) setStep(3);
        else if (s.triage) setStep(2);
      }
    });
    return () => abortRef.current?.abort();
  }, [conn, vessel.mmsi]);

  async function runTriage() {
    if (!conn) return;
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;
    setStatus("loading");
    const timeout = setTimeout(() => ac.abort(), LLM_TIMEOUT_MS);
    try {
      const text = await callLLM(TRIAGE_SYSTEM, triagePrompt(vessel), 160, ac.signal);
      clearTimeout(timeout);
      if (ac.signal.aborted) return;
      await saveInvestigationField(conn, vessel.mmsi, "triage", text);
      setSession((s) => ({ ...s, triage: text }));
      setStatus("done");
      setStep(2);
    } catch {
      clearTimeout(timeout);
      if (ac.signal.aborted) return;
      setStatus("offline");
    }
  }

  async function runSynthesis() {
    if (!conn) return;
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;
    setStatus("loading");
    const savedNotes = notes;
    await saveInvestigationField(conn, vessel.mmsi, "osint_notes", savedNotes);
    const timeout = setTimeout(() => ac.abort(), LLM_TIMEOUT_MS);
    try {
      const text = await callLLM(SYNTHESIS_SYSTEM, synthesisPrompt(vessel, savedNotes), 150, ac.signal);
      clearTimeout(timeout);
      if (ac.signal.aborted) return;
      await saveInvestigationField(conn, vessel.mmsi, "synthesis", text);
      setSession((s) => ({ ...s, osint_notes: savedNotes, synthesis: text }));
      setStatus("done");
      setStep(4);
    } catch {
      clearTimeout(timeout);
      if (ac.signal.aborted) return;
      setStatus("offline");
    }
  }

  async function runBriefing() {
    if (!conn || !session.synthesis) return;
    abortRef.current?.abort();
    const ac = new AbortController();
    abortRef.current = ac;
    setStatus("loading");
    const timeout = setTimeout(() => ac.abort(), LLM_TIMEOUT_MS);
    try {
      const text = await callLLM(
        BRIEFING_SYSTEM,
        briefingPrompt(vessel, session.synthesis),
        220,
        ac.signal
      );
      clearTimeout(timeout);
      if (ac.signal.aborted) return;
      setBriefingEdit(text);
      await saveInvestigationField(conn, vessel.mmsi, "briefing", text);
      setSession((s) => ({ ...s, briefing: text }));
      setStatus("done");
      setStep(5);
    } catch {
      clearTimeout(timeout);
      if (ac.signal.aborted) return;
      setStatus("offline");
    }
  }

  async function handleApprove() {
    if (!conn) return;
    const finalBriefing = briefingEdit.trim();
    if (finalBriefing !== session.briefing) {
      await saveInvestigationField(conn, vessel.mmsi, "briefing", finalBriefing);
    }
    await saveInvestigationField(conn, vessel.mmsi, "approved", true);
    setSession((s) => ({ ...s, briefing: finalBriefing, approved: true }));
  }

  async function handleReset() {
    if (!conn) return;
    await resetInvestigationSession(conn, vessel.mmsi);
    setSession({});
    setNotes("");
    setBriefingEdit("");
    setStep(1);
    setStatus("idle");
  }

  function handleCopyMarkdown() {
    const md = toMarkdown(vessel, { ...session, briefing: briefingEdit || session.briefing });
    navigator.clipboard.writeText(md).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  }

  const isLoading = status === "loading";
  const isOffline = status === "offline";

  return (
    <div style={{ marginTop: "0.75rem", borderTop: "1px solid #2d3748", paddingTop: "0.75rem" }}>
      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: "0.75rem" }}>
        <div style={{ ...sectionLabel, marginBottom: 0 }}>
          OSINT Investigation
          <span style={{ marginLeft: "0.5rem", color: "#2d6a4f", fontWeight: 600 }}>
            Step {step}/5
          </span>
        </div>
        {(session.triage || session.synthesis || session.briefing) && (
          <button
            onClick={handleReset}
            style={{ background: "none", border: "none", color: "#4a5568", cursor: "pointer", fontSize: "0.62rem", textDecoration: "underline", padding: 0 }}
          >
            Reset
          </button>
        )}
      </div>

      {/* Step progress bar */}
      <div style={{ display: "flex", gap: 3, marginBottom: "0.75rem" }}>
        {([1, 2, 3, 4, 5] as Step[]).map((s) => (
          <div
            key={s}
            style={{
              flex: 1,
              height: 3,
              borderRadius: 2,
              background: s < step ? "#68d391" : s === step ? "#93c5fd" : "#2d3748",
            }}
          />
        ))}
      </div>

      {/* ── Step 1: Triage ── */}
      {step >= 1 && (
        <div style={{ marginBottom: "0.75rem" }}>
          <div style={sectionLabel}>1 — Triage</div>
          {session.triage ? (
            <div style={outputBox}>{session.triage}</div>
          ) : (
            <>
              <div style={{ fontSize: "0.72rem", color: "#718096", marginBottom: "0.4rem" }}>
                LLM identifies the top 3 evasion indicators from vessel data.
              </div>
              {isOffline && step === 1 && (
                <div style={offlineMsg}>
                  <span style={{ width: 6, height: 6, borderRadius: "50%", background: "#4a5568", flexShrink: 0 }} />
                  Local LLM offline — start llama-server on :8080
                </div>
              )}
              <button
                disabled={isLoading}
                onClick={runTriage}
                style={stepBtn(isLoading)}
              >
                {isLoading ? "Analysing…" : "Run triage"}
              </button>
            </>
          )}
        </div>
      )}

      {/* ── Step 2: OSINT links ── */}
      {step >= 2 && (
        <div style={{ marginBottom: "0.75rem" }}>
          <div style={sectionLabel}>2 — OSINT sources</div>
          <div style={{ fontSize: "0.68rem", color: "#718096", marginBottom: "0.4rem" }}>
            Open each link and note findings below.
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: "0.3rem" }}>
            {osintLinks(vessel).map((link) => (
              <a
                key={link.label}
                href={link.url}
                target="_blank"
                rel="noopener noreferrer"
                style={{
                  fontSize: "0.72rem",
                  color: "#93c5fd",
                  textDecoration: "none",
                  display: "flex",
                  alignItems: "center",
                  gap: "0.3rem",
                }}
              >
                <span style={{ fontSize: "0.6rem", opacity: 0.5 }}>↗</span>
                {link.label}
              </a>
            ))}
          </div>
          {step === 2 && (
            <button onClick={() => setStep(3)} style={stepBtn(false)}>
              Continue to notes →
            </button>
          )}
        </div>
      )}

      {/* ── Step 3: Analyst notes ── */}
      {step >= 3 && (
        <div style={{ marginBottom: "0.75rem" }}>
          <div style={sectionLabel}>3 — Analyst findings</div>
          {session.approved && session.osint_notes ? (
            <div style={{ ...outputBox, borderLeft: "3px solid #4a5568" }}>
              {session.osint_notes}
            </div>
          ) : (
            <>
              <div style={{ fontSize: "0.68rem", color: "#718096", marginBottom: "0.4rem" }}>
                Paste relevant OSINT findings from the links above. Leave blank if nothing found.
              </div>
              <textarea
                value={notes}
                onChange={(e) => setNotes(e.target.value)}
                placeholder="e.g. Vessel last seen near Hormuz 2026-04-28. Operator linked to UAE shell company. No current MT position."
                rows={4}
                style={{
                  width: "100%",
                  boxSizing: "border-box",
                  background: "#1a1f2e",
                  border: "1px solid #2d3748",
                  borderRadius: 4,
                  color: "#e2e8f0",
                  fontSize: "0.72rem",
                  lineHeight: 1.45,
                  padding: "0.4rem 0.6rem",
                  resize: "vertical",
                  fontFamily: "inherit",
                }}
              />
              {isOffline && step === 3 && (
                <div style={{ ...offlineMsg, marginTop: "0.4rem" }}>
                  <span style={{ width: 6, height: 6, borderRadius: "50%", background: "#4a5568", flexShrink: 0 }} />
                  Local LLM offline — start llama-server on :8080
                </div>
              )}
              <button
                disabled={isLoading}
                onClick={runSynthesis}
                style={stepBtn(isLoading)}
              >
                {isLoading ? "Synthesising…" : "Synthesise →"}
              </button>
            </>
          )}
        </div>
      )}

      {/* ── Step 4: Synthesis ── */}
      {step >= 4 && (
        <div style={{ marginBottom: "0.75rem" }}>
          <div style={sectionLabel}>4 — Threat assessment</div>
          {session.synthesis ? (
            <>
              <div style={outputBox}>{session.synthesis}</div>
              {!session.briefing && (
                <>
                  {isOffline && step === 4 && (
                    <div style={{ ...offlineMsg, marginTop: "0.4rem" }}>
                      <span style={{ width: 6, height: 6, borderRadius: "50%", background: "#4a5568", flexShrink: 0 }} />
                      Local LLM offline — start llama-server on :8080
                    </div>
                  )}
                  <button
                    disabled={isLoading}
                    onClick={runBriefing}
                    style={stepBtn(isLoading)}
                  >
                    {isLoading ? "Drafting…" : "Draft briefing →"}
                  </button>
                </>
              )}
            </>
          ) : (
            <div style={{ fontSize: "0.72rem", color: "#4a5568", fontStyle: "italic" }}>
              {isLoading ? "Synthesising…" : "Waiting for synthesis…"}
            </div>
          )}
        </div>
      )}

      {/* ── Step 5: Briefing + approve ── */}
      {step >= 5 && (
        <div style={{ marginBottom: "0.5rem" }}>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: "0.35rem" }}>
            <div style={sectionLabel}>5 — Briefing note</div>
            {session.approved && (
              <span style={{ fontSize: "0.62rem", color: "#68d391", fontWeight: 600 }}>✓ approved</span>
            )}
          </div>
          {session.approved ? (
            <div style={outputBox}>{briefingEdit || session.briefing}</div>
          ) : (
            <>
              <div style={{ fontSize: "0.68rem", color: "#718096", marginBottom: "0.4rem" }}>
                Edit if needed, then approve to save.
              </div>
              <textarea
                value={briefingEdit}
                onChange={(e) => setBriefingEdit(e.target.value)}
                rows={5}
                style={{
                  width: "100%",
                  boxSizing: "border-box",
                  background: "#1a1f2e",
                  border: "1px solid #2d3748",
                  borderRadius: 4,
                  color: "#e2e8f0",
                  fontSize: "0.75rem",
                  lineHeight: 1.5,
                  padding: "0.4rem 0.6rem",
                  resize: "vertical",
                  fontFamily: "inherit",
                }}
              />
              <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.5rem" }}>
                <button
                  disabled={!briefingEdit.trim()}
                  onClick={handleApprove}
                  style={{
                    ...stepBtn(!briefingEdit.trim()),
                    marginTop: 0,
                    background: briefingEdit.trim() ? "#1a4731" : "#1a1f2e",
                    border: `1px solid ${briefingEdit.trim() ? "#68d391" : "#2d3748"}`,
                    color: briefingEdit.trim() ? "#68d391" : "#4a5568",
                  }}
                >
                  Approve
                </button>
                <button
                  disabled={isLoading}
                  onClick={runBriefing}
                  style={{ ...stepBtn(isLoading), marginTop: 0, background: "none" }}
                >
                  Regenerate
                </button>
              </div>
            </>
          )}

          {/* Copy as Markdown — available once briefing exists */}
          {(session.briefing || briefingEdit) && (
            <button
              onClick={handleCopyMarkdown}
              style={{
                background: "none",
                border: "none",
                color: copied ? "#68d391" : "#4a5568",
                cursor: "pointer",
                fontSize: "0.62rem",
                marginTop: "0.5rem",
                padding: 0,
                textDecoration: "underline",
              }}
            >
              {copied ? "Copied!" : "Copy as Markdown (paste to GitHub Issue)"}
            </button>
          )}
        </div>
      )}
    </div>
  );
}
