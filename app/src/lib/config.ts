/**
 * Runtime deployment config — loaded from /config.json at startup.
 *
 * Cloudflare deployment: /config.json is empty {}; values come from Vite
 * build-time env vars (VITE_PRIVATE_MANIFEST_URL etc.).
 *
 * On-prem / custom deployments: populate /config.json (e.g. via Docker
 * entrypoint sed) to set privateManifestUrl, privateSigningEndpoint and
 * authProvider without rebuilding the app.
 *
 * Example on-prem config.json:
 * {
 *   "privateSigningEndpoint": "https://api.example.com/sign",
 *   "authProvider": "oidc",
 *   "oidcLoginUrl": "https://keycloak.example.com/realms/arktrace/protocol/openid-connect/auth"
 * }
 *
 * Example AWS config.json:
 * {
 *   "privateSigningEndpoint": "https://lambda.example.com/sign",
 *   "authProvider": "oidc",
 *   "oidcLoginUrl": "https://arktrace.auth.us-east-1.amazoncognito.com/login"
 * }
 */

export type AuthProvider = "cloudflare-access" | "oidc" | "none";

export interface AppConfig {
  /** Base URL of the public R2 bucket (anonymous access). */
  publicBucketUrl: string;
  /**
   * Full URL of the private ducklake manifest.
   * Used by cloudflare-access deployments (Worker proxies R2 directly).
   */
  privateManifestUrl?: string;
  /**
   * Signing endpoint for presigned URL generation.
   * Used by on-prem / AWS deployments.
   * Contract: GET /sign?key=<r2-key>  Authorization: Bearer <token>
   *           → 302 redirect to presigned URL valid for 15 min
   */
  privateSigningEndpoint?: string;
  /** Auth provider for private data. */
  authProvider: AuthProvider;
  /** OIDC login redirect URL (when authProvider = "oidc"). */
  oidcLoginUrl?: string;
}

const PUBLIC_BUCKET_URL = "https://arktrace-public.edgesentry.io";

let _config: AppConfig | null = null;

/** Load runtime config from /config.json, falling back to build-time env vars. */
export async function loadConfig(): Promise<AppConfig> {
  if (_config) return _config;
  let overrides: Partial<AppConfig> = {};
  try {
    const resp = await fetch("/config.json", { cache: "no-store" });
    if (resp.ok) overrides = await resp.json();
  } catch {
    // config.json absent or network error — use env var fallback below
  }
  _config = {
    publicBucketUrl: overrides.publicBucketUrl ?? PUBLIC_BUCKET_URL,
    // Env var takes precedence when config.json doesn't specify private URL.
    privateManifestUrl:
      overrides.privateManifestUrl ??
      (import.meta.env.VITE_PRIVATE_MANIFEST_URL || undefined),
    privateSigningEndpoint: overrides.privateSigningEndpoint,
    authProvider: overrides.authProvider ?? "cloudflare-access",
    oidcLoginUrl: overrides.oidcLoginUrl,
  };
  return _config;
}

/** Reset cached config (for testing). */
export function _resetConfig(): void {
  _config = null;
}
