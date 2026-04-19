const TOKEN_KEY = "arktrace_access_token";

export const getToken = (): string | null => localStorage.getItem(TOKEN_KEY);
export const setToken = (token: string): void => localStorage.setItem(TOKEN_KEY, token);
export const clearToken = (): void => localStorage.removeItem(TOKEN_KEY);

/** True when a private manifest URL is configured in the build. */
export const isPrivateModeEnabled = (): boolean =>
  Boolean(import.meta.env.VITE_PRIVATE_MANIFEST_URL);
