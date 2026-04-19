/**
 * RegionPicker — full-screen overlay shown on first install (empty OPFS).
 * User picks one or more regions to download; defaults to Singapore.
 * Choice is persisted to localStorage and not shown again unless reset.
 */

import { useState } from "react";

export const KNOWN_REGIONS = [
  { id: "singapore",    label: "Singapore Strait" },
  { id: "japansea",     label: "Japan Sea" },
  { id: "persiangulf",  label: "Persian Gulf" },
  { id: "europe",       label: "Europe" },
  { id: "middleeast",   label: "Middle East" },
  { id: "blacksea",     label: "Black Sea" },
  { id: "hornofafrica", label: "Horn of Africa" },
  { id: "gulfofguinea", label: "Gulf of Guinea" },
  { id: "gulfofaden",   label: "Gulf of Aden" },
  { id: "gulfofmexico", label: "Gulf of Mexico" },
] as const;

export type RegionId = (typeof KNOWN_REGIONS)[number]["id"];

export const STORAGE_KEY = "arktrace.regions";
export const DEFAULT_REGIONS: RegionId[] = ["singapore"];

/** Read persisted region list, or null if not yet chosen. */
export function getStoredRegions(): string[] | null {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) && parsed.length > 0 ? parsed : null;
  } catch {
    return null;
  }
}

/** Persist region list. */
export function setStoredRegions(regions: string[]): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(regions));
  } catch {
    // localStorage unavailable — ignore
  }
}

/** Format a region list for display in the header (max 2 names then "+N"). */
export function formatRegionLabel(regions: string[]): string {
  if (regions.length === 0) return "No region";
  const names = regions.map(
    (id) => KNOWN_REGIONS.find((r) => r.id === id)?.label ?? id
  );
  if (names.length <= 2) return names.join(", ");
  return `${names.slice(0, 2).join(", ")} +${names.length - 2}`;
}

interface Props {
  initial?: string[];
  onConfirm: (regions: string[]) => void;
  onCancel?: () => void;
}

export default function RegionPicker({ initial, onConfirm, onCancel }: Props) {
  const [selected, setSelected] = useState<Set<string>>(
    new Set(initial ?? DEFAULT_REGIONS)
  );

  function toggle(id: string) {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) {
        // Keep at least one region selected
        if (next.size > 1) next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  }

  const selectedList = Array.from(selected);

  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(15,17,23,0.92)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        zIndex: 1000,
      }}
    >
      <div
        style={{
          background: "#1a1f2e",
          border: "1px solid #2d3748",
          borderRadius: 8,
          padding: "2rem",
          width: 420,
          maxWidth: "90vw",
        }}
      >
        <h2
          style={{
            fontSize: "1rem",
            fontWeight: 600,
            color: "#93c5fd",
            marginBottom: "0.25rem",
          }}
        >
          Select regions
        </h2>
        <p
          style={{
            fontSize: "0.75rem",
            color: "#718096",
            marginBottom: "1.25rem",
          }}
        >
          Choose one or more regions to download. You can change this later.
        </p>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "1fr 1fr",
            gap: "0.5rem",
            marginBottom: "1.5rem",
          }}
        >
          {KNOWN_REGIONS.map((r) => {
            const active = selected.has(r.id);
            return (
              <button
                key={r.id}
                onClick={() => toggle(r.id)}
                style={{
                  padding: "0.5rem 0.75rem",
                  borderRadius: 4,
                  border: `1px solid ${active ? "#3b82f6" : "#2d3748"}`,
                  background: active ? "#1e3a5f" : "#0f1117",
                  color: active ? "#93c5fd" : "#a0aec0",
                  fontSize: "0.75rem",
                  cursor: "pointer",
                  textAlign: "left",
                  display: "flex",
                  alignItems: "center",
                  gap: "0.4rem",
                }}
              >
                <span
                  style={{
                    width: 10,
                    height: 10,
                    borderRadius: 2,
                    border: `1px solid ${active ? "#3b82f6" : "#4a5568"}`,
                    background: active ? "#3b82f6" : "transparent",
                    flexShrink: 0,
                  }}
                />
                {r.label}
              </button>
            );
          })}
        </div>

        <div style={{ display: "flex", gap: "0.5rem" }}>
          {onCancel && (
            <button
              onClick={onCancel}
              style={{
                flex: 1,
                padding: "0.6rem",
                borderRadius: 4,
                border: "1px solid #2d3748",
                background: "transparent",
                color: "#a0aec0",
                fontSize: "0.85rem",
                cursor: "pointer",
              }}
            >
              Cancel
            </button>
          )}
          <button
            onClick={() => {
              setStoredRegions(selectedList);
              onConfirm(selectedList);
            }}
            style={{
              flex: 2,
              padding: "0.6rem",
              borderRadius: 4,
              border: "none",
              background: "#3b82f6",
              color: "#fff",
              fontSize: "0.85rem",
              fontWeight: 600,
              cursor: "pointer",
            }}
          >
            Download {selectedList.length === 1
              ? (KNOWN_REGIONS.find((r) => r.id === selectedList[0])?.label ?? selectedList[0])
              : `${selectedList.length} regions`}
          </button>
        </div>
      </div>
    </div>
  );
}
