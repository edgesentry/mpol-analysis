/**
 * RegionPicker — full-screen overlay shown on first install (empty OPFS).
 * User picks which region to download; defaults to Singapore.
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

export const STORAGE_KEY = "arktrace.region";
export const DEFAULT_REGION: RegionId = "singapore";

/** Read persisted region preference, or null if not yet chosen. */
export function getStoredRegion(): string | null {
  try {
    return localStorage.getItem(STORAGE_KEY);
  } catch {
    return null;
  }
}

/** Persist region preference. */
export function setStoredRegion(region: string): void {
  try {
    localStorage.setItem(STORAGE_KEY, region);
  } catch {
    // localStorage unavailable — ignore
  }
}

interface Props {
  onConfirm: (region: string) => void;
}

export default function RegionPicker({ onConfirm }: Props) {
  const [selected, setSelected] = useState<string>(DEFAULT_REGION);

  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        background: "#0f1117",
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
          width: 400,
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
          Select region
        </h2>
        <p
          style={{
            fontSize: "0.75rem",
            color: "#718096",
            marginBottom: "1.25rem",
          }}
        >
          Choose which region to download. You can change this later.
        </p>

        <div
          style={{
            display: "grid",
            gridTemplateColumns: "1fr 1fr",
            gap: "0.5rem",
            marginBottom: "1.5rem",
          }}
        >
          {KNOWN_REGIONS.map((r) => (
            <button
              key={r.id}
              onClick={() => setSelected(r.id)}
              style={{
                padding: "0.5rem 0.75rem",
                borderRadius: 4,
                border: `1px solid ${selected === r.id ? "#3b82f6" : "#2d3748"}`,
                background: selected === r.id ? "#1e3a5f" : "#0f1117",
                color: selected === r.id ? "#93c5fd" : "#a0aec0",
                fontSize: "0.75rem",
                cursor: "pointer",
                textAlign: "left",
              }}
            >
              {r.label}
            </button>
          ))}
        </div>

        <button
          onClick={() => {
            setStoredRegion(selected);
            onConfirm(selected);
          }}
          style={{
            width: "100%",
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
          Download {KNOWN_REGIONS.find((r) => r.id === selected)?.label ?? selected}
        </button>
      </div>
    </div>
  );
}
