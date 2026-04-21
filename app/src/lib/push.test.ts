import { describe, it, expect, vi, beforeEach } from "vitest";
import type { AppConfig } from "./config";

vi.mock("./duckdb", () => ({
  isParquetRegistered: vi.fn(),
}));

import { isParquetRegistered } from "./duckdb";

const CF_CONFIG: AppConfig = {
  publicBucketUrl: "https://arktrace-public.edgesentry.io",
  privateManifestUrl: "https://worker.example.com/outputs/manifest.json",
  authProvider: "cloudflare-access",
};

const CF_NO_PRIVATE_CONFIG: AppConfig = {
  publicBucketUrl: "https://arktrace-public.edgesentry.io",
  authProvider: "cloudflare-access",
};

const OIDC_CONFIG: AppConfig = {
  publicBucketUrl: "https://arktrace-public.edgesentry.io",
  privateSigningEndpoint: "https://api.example.com/sign",
  authProvider: "oidc",
};

const NONE_CONFIG: AppConfig = {
  publicBucketUrl: "https://arktrace-public.edgesentry.io",
  authProvider: "none",
};

function makeDb() {
  return {
    copyFileToBuffer: vi.fn().mockResolvedValue(new Uint8Array([1, 2, 3])),
    dropFile: vi.fn().mockResolvedValue(undefined),
  };
}

function makeConn() {
  return { query: vi.fn().mockResolvedValue(undefined) };
}

beforeEach(() => {
  vi.restoreAllMocks();
});

const { pushReviews, mergeDownloadedReviews } = await import("./push");

describe("pushReviews — endpoint routing", () => {
  it("POSTs to private worker /push-reviews for cloudflare-access with privateManifestUrl", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ updatedAt: "2026-04-21T00:00:00.000Z" }),
    }));
    const statuses: string[] = [];
    await pushReviews(makeDb() as never, makeConn() as never, CF_CONFIG, (s) => statuses.push(s.phase));
    expect(fetch).toHaveBeenCalledWith(
      "https://worker.example.com/push-reviews",
      expect.objectContaining({ method: "POST", credentials: "include" })
    );
  });

  it("falls back to /api/reviews/push for cloudflare-access without privateManifestUrl", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ updatedAt: "2026-04-21T00:00:00.000Z" }),
    }));
    await pushReviews(makeDb() as never, makeConn() as never, CF_NO_PRIVATE_CONFIG, () => {});
    expect(fetch).toHaveBeenCalledWith(
      "/api/reviews/push",
      expect.objectContaining({ method: "POST" })
    );
  });

  it("uses /api/reviews/push for oidc deployments", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ updatedAt: "2026-04-21T00:00:00.000Z" }),
    }));
    await pushReviews(makeDb() as never, makeConn() as never, OIDC_CONFIG, () => {});
    expect(fetch).toHaveBeenCalledWith("/api/reviews/push", expect.anything());
  });

  it("uses /api/reviews/push for none deployments", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ updatedAt: "2026-04-21T00:00:00.000Z" }),
    }));
    await pushReviews(makeDb() as never, makeConn() as never, NONE_CONFIG, () => {});
    expect(fetch).toHaveBeenCalledWith("/api/reviews/push", expect.anything());
  });
});

describe("pushReviews — status progression", () => {
  it("emits exporting → uploading → done on success", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ updatedAt: "2026-04-21T00:00:00.000Z" }),
    }));
    const phases: string[] = [];
    await pushReviews(makeDb() as never, makeConn() as never, CF_CONFIG, (s) => phases.push(s.phase));
    expect(phases).toEqual(["exporting", "uploading", "done"]);
  });

  it("throws on 401 with a sign-in message", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: false, status: 401, text: async () => "" }));
    await expect(
      pushReviews(makeDb() as never, makeConn() as never, CF_CONFIG, () => {})
    ).rejects.toThrow("Sign in required to push changes");
  });

  it("throws on non-ok response with server message", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({
      ok: false,
      status: 500,
      text: async () => "internal error",
    }));
    await expect(
      pushReviews(makeDb() as never, makeConn() as never, CF_CONFIG, () => {})
    ).rejects.toThrow("Push failed: internal error");
  });
});

describe("mergeDownloadedReviews — parquet guard", () => {
  beforeEach(() => {
    vi.mocked(isParquetRegistered).mockReturnValue(false);
  });

  it("skips all queries and returns 0 when no merged files are registered", async () => {
    const conn = makeConn();
    const count = await mergeDownloadedReviews(conn as never);
    expect(conn.query).not.toHaveBeenCalled();
    expect(count).toBe(0);
  });

  it("runs all three queries and returns 3 when all files are registered", async () => {
    vi.mocked(isParquetRegistered).mockReturnValue(true);
    const conn = makeConn();
    const count = await mergeDownloadedReviews(conn as never);
    expect(conn.query).toHaveBeenCalledTimes(3);
    expect(count).toBe(3);
  });

  it("runs only the query for the one registered file", async () => {
    vi.mocked(isParquetRegistered).mockImplementation(
      (name) => name === "reviews_merged.parquet"
    );
    const conn = makeConn();
    const count = await mergeDownloadedReviews(conn as never);
    expect(conn.query).toHaveBeenCalledTimes(1);
    expect(count).toBe(1);
  });

  it("does not throw and returns 0 when no files registered", async () => {
    const conn = makeConn();
    await expect(mergeDownloadedReviews(conn as never)).resolves.toBe(0);
  });

  it("returns 2 and skips the failed query on unexpected query error", async () => {
    vi.mocked(isParquetRegistered).mockReturnValue(true);
    const conn = makeConn();
    conn.query
      .mockResolvedValueOnce(undefined)
      .mockRejectedValueOnce(new Error("schema mismatch"))
      .mockResolvedValueOnce(undefined);
    const count = await mergeDownloadedReviews(conn as never);
    expect(count).toBe(2);
  });
});
