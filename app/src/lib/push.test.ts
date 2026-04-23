import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("./duckdb", () => ({
  isParquetRegistered: vi.fn(),
}));

import { isParquetRegistered } from "./duckdb";

function makeConn() {
  return { query: vi.fn().mockResolvedValue(undefined) };
}

beforeEach(() => {
  vi.restoreAllMocks();
});

const { mergeDownloadedReviews } = await import("./push");

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
