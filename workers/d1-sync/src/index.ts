export interface Env {
  sub2api_db: D1Database;
  API_KEY: string;
}

type AccountPayload = {
  email: string;
  password: string;
};

const UPSERT_SQL = `
  INSERT INTO accounts (email, password, created_at, updated_at)
  VALUES (?, ?, unixepoch(), unixepoch())
  ON CONFLICT(email) DO UPDATE SET
    password = excluded.password,
    updated_at = excluded.updated_at
`;

const LIST_SQL = `
  SELECT email, password, created_at, updated_at
  FROM accounts
  ORDER BY updated_at DESC
  LIMIT ?
`;

const GET_SQL = `
  SELECT email, password, created_at, updated_at
  FROM accounts
  WHERE email = ?
  LIMIT 1
`;

const MAX_BATCH_SIZE = 50;
const MAX_LIST_LIMIT = 100;

function json(data: unknown, init?: ResponseInit): Response {
  const headers = new Headers(init?.headers);
  headers.set("Content-Type", "application/json; charset=utf-8");
  return new Response(JSON.stringify(data), {
    ...init,
    headers,
  });
}

function error(status: number, message: string): Response {
  return json({ success: false, error: message }, { status });
}

function getBearerToken(request: Request): string {
  const header = request.headers.get("Authorization") || "";
  const prefix = "Bearer ";
  if (!header.startsWith(prefix)) {
    return "";
  }
  return header.slice(prefix.length).trim();
}

function isAuthorized(request: Request, env: Env): boolean {
  const expected = String(env.API_KEY || "").trim();
  return Boolean(expected) && getBearerToken(request) === expected;
}

function normalizeEmail(raw: unknown): string {
  if (typeof raw !== "string") {
    return "";
  }
  return raw.trim().toLowerCase();
}

function parseAccountPayload(raw: unknown): AccountPayload | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }

  const payload = raw as Record<string, unknown>;
  const email = normalizeEmail(payload.email);
  const password = typeof payload.password === "string" ? payload.password : "";
  if (!email || !password) {
    return null;
  }

  return { email, password };
}

async function readJson(request: Request): Promise<unknown> {
  try {
    return await request.json();
  } catch {
    throw new Error("Invalid JSON body");
  }
}

async function upsertOne(db: D1Database, account: AccountPayload): Promise<void> {
  await db.prepare(UPSERT_SQL).bind(account.email, account.password).run();
}

function dedupeAccounts(rawAccounts: unknown[]): AccountPayload[] {
  const deduped = new Map<string, AccountPayload>();
  for (const raw of rawAccounts) {
    const account = parseAccountPayload(raw);
    if (!account) {
      throw new Error("Each account requires non-empty email and password");
    }
    deduped.set(account.email, account);
  }
  return Array.from(deduped.values());
}

async function handleUpsert(request: Request, env: Env): Promise<Response> {
  const payload = parseAccountPayload(await readJson(request));
  if (!payload) {
    return error(400, "email and password are required");
  }

  await upsertOne(env.sub2api_db, payload);
  return json({ success: true, account: payload });
}

async function handleBatch(request: Request, env: Env): Promise<Response> {
  const body = await readJson(request);
  const rawAccounts = body && typeof body === "object" ? (body as Record<string, unknown>).accounts : null;
  if (!Array.isArray(rawAccounts)) {
    return error(400, "accounts must be an array");
  }
  if (rawAccounts.length < 1 || rawAccounts.length > MAX_BATCH_SIZE) {
    return error(400, `accounts length must be between 1 and ${MAX_BATCH_SIZE}`);
  }

  const accounts = dedupeAccounts(rawAccounts);
  if (accounts.length < 1 || accounts.length > MAX_BATCH_SIZE) {
    return error(400, `accounts length must be between 1 and ${MAX_BATCH_SIZE}`);
  }

  const statements = accounts.map((account) =>
    env.sub2api_db.prepare(UPSERT_SQL).bind(account.email, account.password),
  );
  await env.sub2api_db.batch(statements);

  return json({
    success: true,
    processed: accounts.length,
    accounts: accounts.map((account) => account.email),
  });
}

async function handleGetAccount(url: URL, env: Env): Promise<Response> {
  const encodedEmail = url.pathname.slice("/v1/accounts/".length);
  const email = normalizeEmail(decodeURIComponent(encodedEmail));
  if (!email) {
    return error(400, "email is required");
  }

  const result = await env.sub2api_db.prepare(GET_SQL).bind(email).first<AccountPayload & {
    created_at: number;
    updated_at: number;
  }>();

  return json({
    success: true,
    account: result ?? null,
  });
}

async function handleList(url: URL, env: Env): Promise<Response> {
  const rawLimit = Number(url.searchParams.get("limit") || "20");
  const limit = Number.isFinite(rawLimit) && rawLimit > 0 ? Math.min(Math.floor(rawLimit), MAX_LIST_LIMIT) : 20;

  const result = await env.sub2api_db.prepare(LIST_SQL).bind(limit).all<{
    email: string;
    password: string;
    created_at: number;
    updated_at: number;
  }>();

  return json({
    success: true,
    limit,
    accounts: Array.isArray(result.results) ? result.results : [],
  });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (!isAuthorized(request, env)) {
      return error(401, "Unauthorized");
    }

    const url = new URL(request.url);
    const pathname = url.pathname.replace(/\/+$/, "") || "/";

    try {
      if (pathname === "/v1/accounts" && request.method === "PUT") {
        return await handleUpsert(request, env);
      }

      if (pathname === "/v1/accounts/batch" && request.method === "POST") {
        return await handleBatch(request, env);
      }

      if (pathname === "/v1/accounts" && request.method === "GET") {
        return await handleList(url, env);
      }

      if (pathname.startsWith("/v1/accounts/") && request.method === "GET") {
        return await handleGetAccount(url, env);
      }

      if (pathname === "/healthz" && request.method === "GET") {
        return json({ success: true });
      }

      return error(404, "Not found");
    } catch (err) {
      const message = err instanceof Error ? err.message : "Internal Server Error";
      return error(500, message);
    }
  },
} satisfies ExportedHandler<Env>;
