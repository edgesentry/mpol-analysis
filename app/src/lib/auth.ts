import type { AppConfig } from "./config";

export function isPrivateModeEnabled(config: AppConfig): boolean {
  return Boolean(config.privateManifestUrl || config.privateSigningEndpoint);
}

/**
 * Check authentication and return the user's identity string, or null if
 * unauthenticated / private mode not configured.
 *
 * - cloudflare-access: calls /whoami on the Worker (uses CF_Authorization cookie)
 * - oidc: reads token from sessionStorage and decodes the email claim
 * - none: always returns "trusted" (on-prem trusted-network deployments)
 */
export async function checkPrivateAuth(config: AppConfig): Promise<string | null> {
  if (!isPrivateModeEnabled(config)) return null;

  if (config.authProvider === "none") return "trusted";

  if (config.authProvider === "oidc") {
    const token = sessionStorage.getItem("arktrace_oidc_token");
    if (!token) return null;
    try {
      const payload = JSON.parse(atob(token.split(".")[1])) as Record<string, unknown>;
      return (payload.email as string) ?? (payload.sub as string) ?? "oidc-user";
    } catch {
      return null;
    }
  }

  // cloudflare-access: probe /whoami on the Worker domain
  const origin = privateOrigin(config);
  if (!origin) return null;
  try {
    const resp = await fetch(`${origin}/whoami`, { credentials: "include", redirect: "manual" });
    if (!resp.ok) return null;
    const { email } = (await resp.json()) as { email: string };
    return email || null;
  } catch {
    return null;
  }
}

/**
 * Return a Bearer token for signing-endpoint deployments (oidc / none).
 * Returns null for cloudflare-access (uses cookies instead).
 */
export async function getAuthToken(config: AppConfig): Promise<string | null> {
  if (config.authProvider === "none") return "trusted";
  if (config.authProvider === "oidc") {
    return sessionStorage.getItem("arktrace_oidc_token");
  }
  return null; // cloudflare-access uses CF_Authorization cookie
}

/**
 * Trigger login. Opens a popup for cloudflare-access; redirects for oidc.
 * Returns the popup Window for cloudflare-access so the caller can poll for close.
 */
export function login(config: AppConfig): Window | null {
  if (config.authProvider === "oidc") {
    if (!config.oidcLoginUrl) return null;
    window.location.href =
      `${config.oidcLoginUrl}?redirect_uri=${encodeURIComponent(window.location.href)}`;
    return null;
  }
  // cloudflare-access — popup that auto-closes after login
  const origin = privateOrigin(config);
  if (!origin) return null;
  return window.open(
    `${origin}/auth-success`,
    "cf-access-login",
    "width=520,height=620,noopener"
  );
}

/** Logout the user from whichever provider is active. */
export function logout(config: AppConfig): void {
  if (config.authProvider === "cloudflare-access") {
    const origin = privateOrigin(config);
    if (origin) window.location.href = `${origin}/cdn-cgi/access/logout`;
  } else if (config.authProvider === "oidc") {
    sessionStorage.removeItem("arktrace_oidc_token");
    window.location.reload();
  }
  // "none" — no-op
}

function privateOrigin(config: AppConfig): string | null {
  const url = config.privateManifestUrl || config.privateSigningEndpoint;
  if (!url) return null;
  try {
    return new URL(url).origin;
  } catch {
    return null;
  }
}
