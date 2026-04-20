import { describe, it, expect, vi, beforeEach } from "vitest";
import type { AppConfig } from "./config";

const CF_CONFIG: AppConfig = {
  publicBucketUrl: "https://arktrace-public.edgesentry.io",
  privateManifestUrl: "https://worker.example.com/outputs/manifest.json",
  authProvider: "cloudflare-access",
};

const OIDC_CONFIG: AppConfig = {
  publicBucketUrl: "https://arktrace-public.edgesentry.io",
  privateSigningEndpoint: "https://api.example.com/sign",
  authProvider: "oidc",
  oidcLoginUrl: "https://keycloak.example.com/login",
};

const NONE_CONFIG: AppConfig = {
  publicBucketUrl: "https://arktrace-public.edgesentry.io",
  privateSigningEndpoint: "https://api.example.com/sign",
  authProvider: "none",
};

const NO_PRIVATE_CONFIG: AppConfig = {
  publicBucketUrl: "https://arktrace-public.edgesentry.io",
  authProvider: "cloudflare-access",
};

beforeEach(() => {
  vi.restoreAllMocks();
  sessionStorage.clear();
});

// Import after setup so module constants are correct
const { checkPrivateAuth, isPrivateModeEnabled, getAuthToken } = await import("./auth");

describe("isPrivateModeEnabled", () => {
  it("returns true when privateManifestUrl is set", () => {
    expect(isPrivateModeEnabled(CF_CONFIG)).toBe(true);
  });
  it("returns true when privateSigningEndpoint is set", () => {
    expect(isPrivateModeEnabled(OIDC_CONFIG)).toBe(true);
  });
  it("returns false when neither is set", () => {
    expect(isPrivateModeEnabled(NO_PRIVATE_CONFIG)).toBe(false);
  });
});

describe("checkPrivateAuth — cloudflare-access", () => {
  it("returns email from /whoami when authenticated", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ email: "user@example.com" }),
    }));
    expect(await checkPrivateAuth(CF_CONFIG)).toBe("user@example.com");
    expect(fetch).toHaveBeenCalledWith(
      "https://worker.example.com/whoami",
      { credentials: "include" }
    );
  });

  it("returns null when /whoami returns empty email", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ email: "" }),
    }));
    expect(await checkPrivateAuth(CF_CONFIG)).toBeNull();
  });

  it("returns null on non-ok response", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: false }));
    expect(await checkPrivateAuth(CF_CONFIG)).toBeNull();
  });

  it("returns null on network error", async () => {
    vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("CORS")));
    expect(await checkPrivateAuth(CF_CONFIG)).toBeNull();
  });

  it("returns null when private mode not configured", async () => {
    expect(await checkPrivateAuth(NO_PRIVATE_CONFIG)).toBeNull();
  });
});

describe("checkPrivateAuth — oidc", () => {
  it("returns email from JWT payload when token is stored", async () => {
    const payload = btoa(JSON.stringify({ email: "oidc@example.com" }));
    sessionStorage.setItem("arktrace_oidc_token", `header.${payload}.sig`);
    expect(await checkPrivateAuth(OIDC_CONFIG)).toBe("oidc@example.com");
  });

  it("returns null when no token in sessionStorage", async () => {
    expect(await checkPrivateAuth(OIDC_CONFIG)).toBeNull();
  });
});

describe("checkPrivateAuth — none", () => {
  it("returns 'trusted' regardless of state", async () => {
    expect(await checkPrivateAuth(NONE_CONFIG)).toBe("trusted");
  });
});

describe("getAuthToken", () => {
  it("returns null for cloudflare-access (uses cookies)", async () => {
    expect(await getAuthToken(CF_CONFIG)).toBeNull();
  });
  it("returns token from sessionStorage for oidc", async () => {
    sessionStorage.setItem("arktrace_oidc_token", "my-token");
    expect(await getAuthToken(OIDC_CONFIG)).toBe("my-token");
  });
  it("returns 'trusted' for none", async () => {
    expect(await getAuthToken(NONE_CONFIG)).toBe("trusted");
  });
});
