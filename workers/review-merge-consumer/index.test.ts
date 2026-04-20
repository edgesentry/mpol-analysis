/**
 * Tests for the CF Queue consumer batch-collapse logic.
 *
 * The consumer must:
 *   1. Collapse the entire batch into ONE pipeline call (no matter how many messages).
 *   2. ackAll() on pipeline success.
 *   3. retry() every message on pipeline failure or network error.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import worker from "./index";
import type { Env } from "./index";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const ENV: Env = {
  PIPELINE_URL: "https://pipeline.example.com",
  PIPELINE_SECRET: "test-secret",
};

function makeMsg(email = "analyst@example.com") {
  return {
    id: crypto.randomUUID(),
    timestamp: new Date(),
    attempts: 1,
    body: { email, triggeredAt: new Date().toISOString() },
    ack: vi.fn(),
    retry: vi.fn(),
  };
}

function makeBatch(msgs: ReturnType<typeof makeMsg>[]) {
  return {
    queue: "arktrace-review-merge",
    messages: msgs,
    ackAll: vi.fn(),
    retryAll: vi.fn(),
  } as unknown as MessageBatch<{ email: string; triggeredAt: string }>;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("review-merge-consumer queue handler", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("makes exactly ONE fetch call regardless of batch size", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response('{"status":"accepted"}', { status: 202 })
    );
    const msgs = [makeMsg("a@x.com"), makeMsg("b@x.com"), makeMsg("c@x.com")];
    const batch = makeBatch(msgs);

    await worker.queue(batch, ENV);

    expect(fetchSpy).toHaveBeenCalledTimes(1);
  });

  it("calls the correct pipeline URL with the shared secret", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("{}", { status: 202 })
    );
    const batch = makeBatch([makeMsg()]);

    await worker.queue(batch, ENV);

    const [url, init] = fetchSpy.mock.calls[0] as [string, RequestInit];
    expect(url).toBe("https://pipeline.example.com/api/reviews/merge");
    expect((init.headers as Record<string, string>)["X-Pipeline-Secret"]).toBe("test-secret");
    expect(init.method).toBe("POST");
  });

  it("ackAll() on pipeline 202", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("{}", { status: 202 })
    );
    const batch = makeBatch([makeMsg(), makeMsg()]);

    await worker.queue(batch, ENV);

    expect(batch.ackAll).toHaveBeenCalledOnce();
    for (const msg of batch.messages) {
      expect(msg.retry).not.toHaveBeenCalled();
    }
  });

  it("retries all messages on non-2xx pipeline response", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("Internal Server Error", { status: 500 })
    );
    const msgs = [makeMsg(), makeMsg()];
    const batch = makeBatch(msgs);

    await worker.queue(batch, ENV);

    expect(batch.ackAll).not.toHaveBeenCalled();
    for (const msg of msgs) {
      expect(msg.retry).toHaveBeenCalledWith({ delaySeconds: 30 });
    }
  });

  it("retries all messages on network error (fetch throws)", async () => {
    vi.spyOn(globalThis, "fetch").mockRejectedValue(new TypeError("network error"));
    const msgs = [makeMsg(), makeMsg()];
    const batch = makeBatch(msgs);

    await worker.queue(batch, ENV);

    expect(batch.ackAll).not.toHaveBeenCalled();
    for (const msg of msgs) {
      expect(msg.retry).toHaveBeenCalledWith({ delaySeconds: 30 });
    }
  });

  it("single-message batch still only one fetch call", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("{}", { status: 202 })
    );
    const batch = makeBatch([makeMsg()]);

    await worker.queue(batch, ENV);

    expect(fetchSpy).toHaveBeenCalledTimes(1);
    expect(batch.ackAll).toHaveBeenCalledOnce();
  });
});
