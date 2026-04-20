const PRIVATE_MANIFEST_URL = import.meta.env.VITE_PRIVATE_MANIFEST_URL as string | undefined;

/** True when a private manifest URL is configured in the build. */
export const isPrivateModeEnabled = (): boolean => Boolean(PRIVATE_MANIFEST_URL);

/**
 * Check whether the user is authenticated with Cloudflare Access by probing
 * the private manifest with credentials. Returns true if the request succeeds
 * (Access cookie present), false if redirected to login or blocked.
 */
export async function checkPrivateAuth(): Promise<boolean> {
  if (!PRIVATE_MANIFEST_URL) return false;
  try {
    const resp = await fetch(PRIVATE_MANIFEST_URL, {
      method: "HEAD",
      credentials: "include",
    });
    return resp.ok;
  } catch {
    return false;
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
