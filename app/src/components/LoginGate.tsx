import { useState } from "react";
import { setToken } from "../lib/auth";

interface Props {
  onLogin: () => void;
}

export default function LoginGate({ onLogin }: Props) {
  const [value, setValue] = useState("");
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const trimmed = value.trim();
    if (!trimmed) {
      setError("Access token required");
      return;
    }
    setToken(trimmed);
    onLogin();
  };

  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(0,0,0,0.75)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        zIndex: 1000,
      }}
    >
      <form
        onSubmit={handleSubmit}
        style={{
          background: "#1a1f2e",
          border: "1px solid #2d3748",
          borderRadius: 8,
          padding: "2rem",
          display: "flex",
          flexDirection: "column",
          gap: "1rem",
          minWidth: 320,
        }}
      >
        <h2 style={{ margin: 0, fontSize: "1rem", fontWeight: 600, color: "#93c5fd" }}>
          Private Data Access
        </h2>
        <p style={{ margin: 0, fontSize: "0.8rem", color: "#a0aec0" }}>
          Enter your Cap Vista access token to load private vessel data.
        </p>
        <input
          type="password"
          autoFocus
          placeholder="Access token"
          value={value}
          onChange={(e) => { setValue(e.target.value); setError(null); }}
          style={{
            background: "#0f1117",
            border: "1px solid #2d3748",
            borderRadius: 4,
            color: "#e2e8f0",
            padding: "0.5rem 0.75rem",
            fontSize: "0.875rem",
            outline: "none",
          }}
        />
        {error && (
          <span style={{ fontSize: "0.75rem", color: "#fc8181" }}>{error}</span>
        )}
        <button
          type="submit"
          style={{
            background: "#3b82f6",
            border: "none",
            borderRadius: 4,
            color: "#fff",
            padding: "0.5rem",
            fontSize: "0.875rem",
            cursor: "pointer",
            fontWeight: 600,
          }}
        >
          Sign in
        </button>
      </form>
    </div>
  );
}
