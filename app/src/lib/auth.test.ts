import { describe, it, expect, vi, beforeEach } from "vitest";

// VITE_PRIVATE_MANIFEST_URL must be set before the module is imported because
// auth.ts reads it at module-load time into a module-level constant.
// Vitest injects import.meta.env from process.env for VITE_* vars.
process.env.VITE_PRIVATE_MANIFEST_URL = "https://worker.example.com/outputs/manifest.json";

const { checkPrivateAuth, isPrivateModeEnabled } = await import("./auth");

beforeEach(() => {
  vi.restoreAllMocks();
});

describe("isPrivateModeEnabled", () => {
  it("returns true when VITE_PRIVATE_MANIFEST_URL is set", () => {
    expect(isPrivateModeEnabled()).toBe(true);
  });
});

describe("checkPrivateAuth", () => {
  it("returns email when /whoami responds with a non-empty email", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ email: "user@example.com" }),
    }));
    const result = await checkPrivateAuth();
    expect(result).toBe("user@example.com");
    expect(fetch).toHaveBeenCalledWith(
      "https://worker.example.com/whoami",
      { credentials: "include" }
    );
  });

  it("returns null when /whoami responds with an empty email (unauthenticated)", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ email: "" }),
    }));
    expect(await checkPrivateAuth()).toBeNull();
  });

  it("returns null when /whoami returns a non-ok status", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: false }));
    expect(await checkPrivateAuth()).toBeNull();
  });

  it("returns null when fetch throws (network error / CORS block)", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("CORS")));
    expect(await checkPrivateAuth()).toBeNull();
  });
});
