const PRIVATE_MANIFEST_URL = import.meta.env.VITE_PRIVATE_MANIFEST_URL as string | undefined;

/** True when a private manifest URL is configured in the build. */
export const isPrivateModeEnabled = (): boolean => Boolean(PRIVATE_MANIFEST_URL);

/**
 * Check CF Access authentication by calling /whoami on the Worker.
 * Returns the authenticated user's email, or null if not authenticated.
 */
export async function checkPrivateAuth(): Promise<string | null> {
  if (!PRIVATE_MANIFEST_URL) return null;
  try {
    const origin = new URL(PRIVATE_MANIFEST_URL).origin;
    const resp = await fetch(`${origin}/whoami`, { credentials: "include" });
    if (!resp.ok) return null;
    const { email } = await resp.json() as { email: string };
    return email || null;
  } catch {
    return null;
  }
}

/**
 * Open /auth-success on the Worker domain in a popup. CF Access intercepts it,
 * redirects to login, then back to /auth-success which auto-closes the popup.
 * Once the popup closes the CF_Authorization cookie is set; caller re-checks auth.
 */
export function loginWithCFAccess(): Window | null {
  const origin = new URL(PRIVATE_MANIFEST_URL!).origin;
  return window.open(`${origin}/auth-success`, "cf-access-login", "width=520,height=620,noopener");
}

/** Redirect to Cloudflare Access logout. */
export function logoutFromCFAccess(): void {
  const origin = new URL(PRIVATE_MANIFEST_URL!).origin;
  window.location.href = `${origin}/cdn-cgi/access/logout`;
}
