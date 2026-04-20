/**
 * POST /api/reviews/push
 *
 * Accepts vessel_reviews, vessel_reviews_audit, and analyst_briefs as Parquet
 * files in a multipart FormData body, then writes them to R2 under a per-user
 * prefix.  After writing, it updates reviews/index.json so other clients can
 * discover and pull the changes on their next sync.
 *
 * Auth: Cloudflare Access injects Cf-Access-Authenticated-User-Email
 * automatically before the request reaches this function.  Requests that
 * lack the header (unauthenticated) receive a 401.
 */

interface Env {
  ARKTRACE_PUBLIC: R2Bucket;
}

interface ReviewsIndex {
  users: { email: string; updatedAt: string }[];
}

export const onRequestPost: PagesFunction<Env> = async (ctx) => {
  const email = ctx.request.headers.get("Cf-Access-Authenticated-User-Email");
  if (!email) {
    return json({ error: "Sign in required to push changes" }, 401);
  }

  let formData: FormData;
  try {
    formData = await ctx.request.formData();
  } catch {
    return json({ error: "Invalid request body" }, 400);
  }

  const reviews = formData.get("reviews") as File | null;
  const audit   = formData.get("audit")   as File | null;
  const briefs  = formData.get("briefs")  as File | null;

  if (!reviews || !audit || !briefs) {
    return json({ error: "Missing files: expected reviews, audit, briefs" }, 400);
  }

  const prefix  = `reviews/${encodeURIComponent(email)}`;
  const now     = new Date().toISOString();
  const putOpts = { httpMetadata: { contentType: "application/octet-stream" } };

  await Promise.all([
    ctx.env.ARKTRACE_PUBLIC.put(`${prefix}/reviews.parquet`, await reviews.arrayBuffer(), putOpts),
    ctx.env.ARKTRACE_PUBLIC.put(`${prefix}/audit.parquet`,   await audit.arrayBuffer(),   putOpts),
    ctx.env.ARKTRACE_PUBLIC.put(`${prefix}/briefs.parquet`,  await briefs.arrayBuffer(),  putOpts),
  ]);

  // Update reviews/index.json — upsert this user's entry
  const indexKey = "reviews/index.json";
  let index: ReviewsIndex = { users: [] };
  const existing = await ctx.env.ARKTRACE_PUBLIC.get(indexKey);
  if (existing) {
    try { index = await existing.json<ReviewsIndex>(); } catch { /* reset on corrupt */ }
  }
  index.users = [
    ...index.users.filter((u) => u.email !== email),
    { email, updatedAt: now },
  ];
  await ctx.env.ARKTRACE_PUBLIC.put(indexKey, JSON.stringify(index), {
    httpMetadata: { contentType: "application/json" },
  });

  return json({ ok: true, email, updatedAt: now }, 200);
};

function json(body: unknown, status: number): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}
