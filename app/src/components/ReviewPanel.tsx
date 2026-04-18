import { useState, useEffect, useCallback } from "react";
import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
import type { VesselRow } from "../lib/duckdb";
import {
  getReview,
  getEvidence,
  saveReview,
  saveOutcome,
  tierColor,
  handoffLabel,
  validateTransition,
  TIERS,
  HANDOFF_STATES,
  type DecisionTier,
  type HandoffState,
  type OutcomeValue,
  type EvidenceRef,
} from "../lib/reviews";

interface Props {
  vessel: VesselRow;
  conn: AsyncDuckDBConnection;
  /** Called after a successful save so the parent can refresh badges. */
  onSaved?: () => void;
}

const REVIEWER_ID_KEY = "arktrace_reviewer_id";

function getReviewerId(): string {
  let id = localStorage.getItem(REVIEWER_ID_KEY);
  if (!id) {
    id = `analyst-${Math.random().toString(36).slice(2, 8)}`;
    localStorage.setItem(REVIEWER_ID_KEY, id);
  }
  return id;
}

// ── Sub-components ────────────────────────────────────────────────────────────

const label = (text: string) => (
  <div
    style={{
      fontSize: "0.65rem",
      textTransform: "uppercase",
      letterSpacing: "0.07em",
      color: "#4a5568",
      marginBottom: "0.25rem",
    }}
  >
    {text}
  </div>
);

const selectStyle: React.CSSProperties = {
  width: "100%",
  background: "#0f1117",
  border: "1px solid #2d3748",
  borderRadius: 4,
  color: "#e2e8f0",
  padding: "0.3rem 0.5rem",
  fontSize: "0.75rem",
};

const inputStyle: React.CSSProperties = {
  ...selectStyle,
};

// ── Main component ────────────────────────────────────────────────────────────

export default function ReviewPanel({ vessel, conn, onSaved }: Props) {
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [success, setSuccess] = useState(false);

  // Outcome form state (visible only when handoff_state === 'handoff_completed')
  const [outcome, setOutcome] = useState<OutcomeValue>("Confirmed");
  const [outcomeNotes, setOutcomeNotes] = useState("");
  const [officerId, setOfficerId] = useState(getReviewerId());
  const [outcomeError, setOutcomeError] = useState<string | null>(null);
  const [outcomeSuccess, setOutcomeSuccess] = useState(false);
  const [savingOutcome, setSavingOutcome] = useState(false);

  // Form state
  const [tier, setTier] = useState<DecisionTier | "">("");
  const [handoffState, setHandoffState] = useState<HandoffState>("queued_review");
  const [reviewerId, setReviewerId] = useState(getReviewerId());
  const [rationale, setRationale] = useState("");
  const [identifierBasis, setIdentifierBasis] = useState("MMSI");
  const [evidence, setEvidence] = useState<
    Array<{ id: string; source: string; url: string; publication_date: string; credibility: EvidenceRef["credibility"] }>
  >([]);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const [review, evs] = await Promise.all([
        getReview(conn, vessel.mmsi),
        getEvidence(conn, vessel.mmsi),
      ]);
      if (review) {
        setTier((review.decision_tier as DecisionTier) ?? "");
        setHandoffState(review.handoff_state as HandoffState);
        setReviewerId(review.reviewer_id || getReviewerId());
        setRationale(review.rationale);
        setIdentifierBasis(review.identifier_basis);
      } else {
        setTier("");
        setHandoffState("queued_review");
        setRationale("");
        setIdentifierBasis("MMSI");
      }
      setEvidence(
        evs.map((e) => ({
          id: e.id,
          source: e.source,
          url: e.url,
          publication_date: e.publication_date,
          credibility: e.credibility,
        }))
      );
    } finally {
      setLoading(false);
    }
  }, [conn, vessel.mmsi]);

  useEffect(() => {
    load();
    setError(null);
    setSuccess(false);
    setOutcomeError(null);
    setOutcomeSuccess(false);
    setOutcomeNotes("");
  }, [load]);

  const handleLogOutcome = async () => {
    setOutcomeError(null);
    if (!outcomeNotes.trim()) {
      setOutcomeError("Outcome notes are required before logging.");
      return;
    }
    setSavingOutcome(true);
    try {
      await saveOutcome(conn, {
        mmsi: vessel.mmsi,
        outcome,
        outcome_notes: outcomeNotes,
        officer_id: officerId,
      });
      setHandoffState("closed");
      setOutcomeSuccess(true);
      setTimeout(() => setOutcomeSuccess(false), 2500);
      onSaved?.();
    } catch (err) {
      setOutcomeError(err instanceof Error ? err.message : "Save failed");
    } finally {
      setSavingOutcome(false);
    }
  };

  const addEvidence = () =>
    setEvidence((ev) => [
      ...ev,
      { id: crypto.randomUUID(), source: "", url: "", publication_date: "", credibility: "medium" },
    ]);

  const removeEvidence = (id: string) =>
    setEvidence((ev) => ev.filter((e) => e.id !== id));

  const updateEvidence = (
    id: string,
    field: keyof (typeof evidence)[0],
    value: string
  ) =>
    setEvidence((ev) =>
      ev.map((e) => (e.id === id ? { ...e, [field]: value } : e))
    );

  const handleSave = async () => {
    setError(null);
    const validationError = validateTransition(
      undefined,
      handoffState,
      tier || null,
      rationale
    );
    if (validationError) {
      setError(validationError);
      return;
    }

    setSaving(true);
    try {
      await saveReview(conn, {
        mmsi: vessel.mmsi,
        decision_tier: (tier as DecisionTier) || null,
        handoff_state: handoffState,
        reviewer_id: reviewerId,
        rationale,
        identifier_basis: identifierBasis,
        evidence,
      });
      setSuccess(true);
      setTimeout(() => setSuccess(false), 2500);
      onSaved?.();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Save failed");
    } finally {
      setSaving(false);
    }
  };

  if (loading) {
    return (
      <div style={{ padding: "0.75rem 1rem", fontSize: "0.72rem", color: "#4a5568", fontStyle: "italic" }}>
        Loading review…
      </div>
    );
  }

  return (
    <div
      style={{
        borderTop: "1px solid #2d3748",
        background: "#0d1117",
        padding: "0.75rem 1rem",
      }}
    >
      {/* Header */}
      <div
        style={{
          fontSize: "0.65rem",
          textTransform: "uppercase",
          letterSpacing: "0.08em",
          color: "#4a5568",
          marginBottom: "0.75rem",
        }}
      >
        Review
      </div>

      {/* Tier + Handoff state */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0.6rem", marginBottom: "0.6rem" }}>
        <div>
          {label("Decision tier")}
          <select
            value={tier}
            onChange={(e) => setTier(e.target.value as DecisionTier | "")}
            style={{
              ...selectStyle,
              color: tier ? tierColor(tier as DecisionTier) : "#718096",
              borderColor: tier ? tierColor(tier as DecisionTier) : "#2d3748",
            }}
          >
            <option value="">— unreviewed —</option>
            {TIERS.map((t) => (
              <option key={t} value={t} style={{ color: tierColor(t) }}>
                {t}
              </option>
            ))}
          </select>
        </div>
        <div>
          {label("Handoff state")}
          <select
            value={handoffState}
            onChange={(e) => setHandoffState(e.target.value as HandoffState)}
            style={selectStyle}
          >
            {HANDOFF_STATES.map((s) => (
              <option key={s} value={s}>
                {handoffLabel(s)}
              </option>
            ))}
          </select>
        </div>
      </div>

      {/* Reviewer + Identifier basis */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0.6rem", marginBottom: "0.6rem" }}>
        <div>
          {label("Reviewer ID")}
          <input
            value={reviewerId}
            onChange={(e) => setReviewerId(e.target.value)}
            style={inputStyle}
            placeholder="analyst-id"
          />
        </div>
        <div>
          {label("Identifier basis")}
          <select
            value={identifierBasis}
            onChange={(e) => setIdentifierBasis(e.target.value)}
            style={selectStyle}
          >
            {["MMSI", "IMO", "Name", "MMSI+IMO"].map((b) => (
              <option key={b} value={b}>{b}</option>
            ))}
          </select>
        </div>
      </div>

      {/* Rationale */}
      <div style={{ marginBottom: "0.6rem" }}>
        {label("Rationale")}
        <textarea
          value={rationale}
          onChange={(e) => setRationale(e.target.value)}
          rows={3}
          placeholder="Why this tier? What signals support this decision?"
          style={{
            ...inputStyle,
            resize: "vertical",
            minHeight: "3.5rem",
            fontFamily: "inherit",
            lineHeight: 1.5,
          }}
        />
      </div>

      {/* Evidence references */}
      <div style={{ marginBottom: "0.75rem" }}>
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            marginBottom: "0.35rem",
          }}
        >
          {label("Evidence references")}
          <button
            onClick={addEvidence}
            style={{
              background: "none",
              border: "1px solid #2d3748",
              borderRadius: 3,
              color: "#718096",
              cursor: "pointer",
              fontSize: "0.65rem",
              padding: "0.15rem 0.4rem",
              marginBottom: "0.25rem",
            }}
          >
            + Add
          </button>
        </div>

        {evidence.length === 0 && (
          <div style={{ fontSize: "0.68rem", color: "#4a5568", fontStyle: "italic" }}>
            No evidence refs — required for Confirmed / Probable.
          </div>
        )}

        {evidence.map((ev, i) => (
          <div
            key={ev.id}
            style={{
              border: "1px solid #2d3748",
              borderRadius: 4,
              padding: "0.4rem 0.5rem",
              marginBottom: "0.4rem",
              background: "#111827",
            }}
          >
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "0.3rem" }}>
              <span style={{ fontSize: "0.65rem", color: "#4a5568" }}>#{i + 1}</span>
              <button
                onClick={() => removeEvidence(ev.id)}
                style={{
                  background: "none",
                  border: "none",
                  color: "#4a5568",
                  cursor: "pointer",
                  fontSize: "0.7rem",
                  padding: 0,
                }}
              >
                ✕
              </button>
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0.3rem", marginBottom: "0.3rem" }}>
              <input
                value={ev.source}
                onChange={(e) => updateEvidence(ev.id, "source", e.target.value)}
                placeholder="Source name"
                style={{ ...inputStyle, fontSize: "0.7rem" }}
              />
              <select
                value={ev.credibility}
                onChange={(e) => updateEvidence(ev.id, "credibility", e.target.value)}
                style={{ ...selectStyle, fontSize: "0.7rem" }}
              >
                <option value="high">High credibility</option>
                <option value="medium">Medium credibility</option>
                <option value="weak">Weak credibility</option>
              </select>
            </div>
            <input
              value={ev.url}
              onChange={(e) => updateEvidence(ev.id, "url", e.target.value)}
              placeholder="URL (optional)"
              style={{ ...inputStyle, fontSize: "0.7rem", marginBottom: "0.3rem" }}
            />
            <input
              value={ev.publication_date}
              onChange={(e) => updateEvidence(ev.id, "publication_date", e.target.value)}
              placeholder="Publication date (YYYY-MM-DD)"
              style={{ ...inputStyle, fontSize: "0.7rem" }}
            />
          </div>
        ))}
      </div>

      {/* Error / success */}
      {error && (
        <div
          style={{
            fontSize: "0.72rem",
            color: "#f6ad55",
            background: "#1a1209",
            border: "1px solid #744210",
            borderRadius: 4,
            padding: "0.3rem 0.5rem",
            marginBottom: "0.5rem",
          }}
        >
          {error}
        </div>
      )}
      {success && (
        <div
          style={{
            fontSize: "0.72rem",
            color: "#68d391",
            background: "#0a1f0f",
            border: "1px solid #276749",
            borderRadius: 4,
            padding: "0.3rem 0.5rem",
            marginBottom: "0.5rem",
          }}
        >
          Review saved.
        </div>
      )}

      {/* Save */}
      <button
        onClick={handleSave}
        disabled={saving}
        style={{
          width: "100%",
          background: saving ? "#2d3748" : "#2b4a8a",
          border: "1px solid #3b5fc0",
          borderRadius: 4,
          color: saving ? "#718096" : "#93c5fd",
          cursor: saving ? "not-allowed" : "pointer",
          fontSize: "0.75rem",
          fontWeight: 600,
          padding: "0.4rem",
        }}
      >
        {saving ? "Saving…" : "Save review"}
      </button>

      {/* ── Patrol outcome (handoff_completed only) ─────────────────────── */}
      {handoffState === "handoff_completed" && (
        <div style={{ marginTop: "0.85rem", borderTop: "1px solid #2d3748", paddingTop: "0.75rem" }}>
          <div style={{
            fontSize: "0.65rem",
            textTransform: "uppercase",
            letterSpacing: "0.08em",
            color: "#f6ad55",
            marginBottom: "0.6rem",
            display: "flex",
            alignItems: "center",
            gap: "0.4rem",
          }}>
            <span style={{ width: 6, height: 6, borderRadius: "50%", background: "#f6ad55", display: "inline-block" }} />
            Patrol outcome
          </div>

          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "0.6rem", marginBottom: "0.6rem" }}>
            <div>
              {label("Outcome")}
              <select
                value={outcome}
                onChange={(e) => setOutcome(e.target.value as OutcomeValue)}
                style={{
                  ...selectStyle,
                  color: outcome === "Confirmed" ? "#fc8181" : outcome === "Cleared" ? "#68d391" : "#718096",
                  borderColor: outcome === "Confirmed" ? "#fc8181" : outcome === "Cleared" ? "#68d391" : "#2d3748",
                }}
              >
                <option value="Confirmed">Confirmed</option>
                <option value="Cleared">Cleared</option>
                <option value="Inconclusive">Inconclusive</option>
              </select>
            </div>
            <div>
              {label("Officer ID")}
              <input
                value={officerId}
                onChange={(e) => setOfficerId(e.target.value)}
                style={inputStyle}
                placeholder="officer-id"
              />
            </div>
          </div>

          <div style={{ marginBottom: "0.6rem" }}>
            {label("Outcome notes (required)")}
            <textarea
              value={outcomeNotes}
              onChange={(e) => setOutcomeNotes(e.target.value)}
              rows={3}
              placeholder="Describe the patrol findings and result…"
              style={{
                ...inputStyle,
                resize: "vertical",
                minHeight: "3.5rem",
                fontFamily: "inherit",
                lineHeight: 1.5,
                borderColor: !outcomeNotes.trim() && outcomeError ? "#fc8181" : "#2d3748",
              }}
            />
          </div>

          {outcomeError && (
            <div style={{
              fontSize: "0.72rem",
              color: "#f6ad55",
              background: "#1a1209",
              border: "1px solid #744210",
              borderRadius: 4,
              padding: "0.3rem 0.5rem",
              marginBottom: "0.5rem",
            }}>
              {outcomeError}
            </div>
          )}
          {outcomeSuccess && (
            <div style={{
              fontSize: "0.72rem",
              color: "#68d391",
              background: "#0a1f0f",
              border: "1px solid #276749",
              borderRadius: 4,
              padding: "0.3rem 0.5rem",
              marginBottom: "0.5rem",
            }}>
              Outcome logged — vessel closed.
            </div>
          )}

          <button
            onClick={handleLogOutcome}
            disabled={savingOutcome}
            style={{
              width: "100%",
              background: savingOutcome ? "#2d3748" : "#1a2a0a",
              border: "1px solid #4a6a1a",
              borderRadius: 4,
              color: savingOutcome ? "#718096" : "#a0d060",
              cursor: savingOutcome ? "not-allowed" : "pointer",
              fontSize: "0.75rem",
              fontWeight: 600,
              padding: "0.4rem",
            }}
          >
            {savingOutcome ? "Logging…" : "Log outcome → close case"}
          </button>
        </div>
      )}
    </div>
  );
}
