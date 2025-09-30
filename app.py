from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import sqlite3, time, hashlib, hmac, os, math, json, random

DB_PATH = os.environ.get("DB_PATH", "ctr.db")
SECRET = os.environ.get("SECRET", "changeme")

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"]
)

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def safe_alter(con, sql):
    try:
        con.execute(sql)
    except sqlite3.OperationalError:
        pass  # column already exists, etc.

def init_db():
    with db() as con:
        con.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS users(
          user_id TEXT PRIMARY KEY,
          ua TEXT, created_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS sessions(
          session_id TEXT PRIMARY KEY,
          user_id TEXT, referrer TEXT, created_at INTEGER
        );
        CREATE TABLE IF NOT EXISTS impressions(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts INTEGER, user_id TEXT, session_id TEXT,
          variant TEXT, creative_id TEXT, visible_ms INTEGER,
          viewport_w INTEGER, viewport_h INTEGER, ua TEXT, ip TEXT
        );
        CREATE TABLE IF NOT EXISTS clicks(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts INTEGER, user_id TEXT, session_id TEXT,
          variant TEXT, creative_id TEXT, ua TEXT, ip TEXT
        );
        CREATE TABLE IF NOT EXISTS searches(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts INTEGER, user_id TEXT, session_id TEXT,
          query_text TEXT, result_count INTEGER
        );
        """)
        # add placement cols if missing
        safe_alter(con, "ALTER TABLE impressions ADD COLUMN placement TEXT")
        safe_alter(con, "ALTER TABLE clicks ADD COLUMN placement TEXT")
init_db()

# ---------- Experiment config ----------
VARIANTS   = ["A", "B", "C"]         # content style
PLACEMENTS = ["P1", "P2", "P3"]      # Top / Side / Inline

def assign_variant_and_placement(user_id: str):
    """Deterministic assignment per user (stable across sessions)."""
    digest = hmac.new(SECRET.encode(), user_id.encode(), hashlib.sha256).hexdigest()
    bucket1 = int(digest[:8], 16) / 0xFFFFFFFF
    bucket2 = int(digest[8:16], 16) / 0xFFFFFFFF
    v = VARIANTS[int(bucket1 * len(VARIANTS)) % len(VARIANTS)]
    p = PLACEMENTS[int(bucket2 * len(PLACEMENTS)) % len(PLACEMENTS)]
    return v, p

# ---------- API: identity/session ----------
@app.post("/api/register_user")
async def register_user(req: Request):
    data = await req.json()
    user_id = data.get("user_id")
    if not user_id:
        raise HTTPException(400, "user_id required")
    with db() as con:
        con.execute(
            "INSERT OR IGNORE INTO users(user_id, ua, created_at) VALUES(?,?,?)",
            (user_id, req.headers.get("user-agent",""), int(time.time()*1000))
        )
    v, p = assign_variant_and_placement(user_id)
    return {"ok": True, "variant": v, "placement": p}

@app.post("/api/start_session")
async def start_session(req: Request):
    data = await req.json()
    user_id = data.get("user_id"); session_id = data.get("session_id")
    ref = data.get("referrer","")
    if not user_id or not session_id:
        raise HTTPException(400, "ids required")
    with db() as con:
        con.execute(
            "INSERT OR IGNORE INTO sessions(session_id, user_id, referrer, created_at) VALUES(?,?,?,?)",
            (session_id, user_id, ref, int(time.time()*1000))
        )
    v, p = assign_variant_and_placement(user_id)
    return {"ok": True, "variant": v, "placement": p}

# ---------- API: search (mock results) ----------
MOCK_DOCS = [
    {"id":"r1","title":"무선 이어폰 가성비 TOP10", "desc":"가격대별 추천과 배터리 비교", "domain":"soundpick.example"},
    {"id":"r2","title":"블루투스 이어폰 할인 모음", "desc":"이번 주 특가 한눈에 보기", "domain":"salehub.example"},
    {"id":"r3","title":"노이즈캔슬링 입문 가이드", "desc":"ANC 기본 원리와 추천 모델", "domain":"techwiki.example"},
    {"id":"r4","title":"땀에도 강한 운동용 이어폰", "desc":"방수 등급 IPX와 착용감", "domain":"fitgear.example"},
    {"id":"r5","title":"친구 선물 베스트 20", "desc":"예산 3~5만원 선물 추천", "domain":"giftmap.example"},
    {"id":"r6","title":"이어폰 배터리 오래 쓰는 법", "desc":"충전 팁과 보관 요령", "domain":"caretips.example"},
    {"id":"r7","title":"최신 코드리스 비교표", "desc":"칩셋/코덱/지연시간 비교", "domain":"specs.example"},
    {"id":"r8","title":"오픈형 vs 커널형", "desc":"장단점, 상황별 선택", "domain":"audio101.example"},
]

@app.post("/api/search")
async def api_search(req: Request):
    data = await req.json()
    user_id = data.get("user_id"); session_id = data.get("session_id"); q = (data.get("q") or "").strip()
    if not (user_id and session_id):
        raise HTTPException(400, "ids required")
    # very simple filter/scoring
    toks = [t for t in q.lower().split() if t]
    def score(doc):
        base = 1
        text = (doc["title"] + " " + doc["desc"]).lower()
        base += sum(1 for t in toks if t in text)
        return base + random.random()*0.01
    docs = sorted(MOCK_DOCS, key=score, reverse=True)[:8]
    with db() as con:
        con.execute(
            "INSERT INTO searches(ts,user_id,session_id,query_text,result_count) VALUES(?,?,?,?,?)",
            (int(time.time()*1000), user_id, session_id, q, len(docs))
        )
    return {"ok": True, "results": docs}

# ---------- API: events ----------
@app.post("/api/imp")
async def log_impression(req: Request):
    data = await req.json()
    must = ["user_id","session_id","variant","placement","creative_id","visible_ms","viewport_w","viewport_h"]
    if any(k not in data for k in must):
        raise HTTPException(400, "missing fields")
    ip = req.client.host if req.client else ""
    with db() as con:
        con.execute("""INSERT INTO impressions(
            ts,user_id,session_id,variant,placement,creative_id,visible_ms,viewport_w,viewport_h,ua,ip
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
        (int(time.time()*1000), data["user_id"], data["session_id"], data["variant"], data["placement"],
         data["creative_id"], int(data["visible_ms"]), int(data["viewport_w"]), int(data["viewport_h"]),
         req.headers.get("user-agent",""), ip))
    return {"ok": True}

@app.post("/api/click")
async def log_click(req: Request):
    data = await req.json()
    must = ["user_id","session_id","variant","placement","creative_id"]
    if any(k not in data for k in must):
        raise HTTPException(400, "missing fields")
    ip = req.client.host if req.client else ""
    with db() as con:
        con.execute("""INSERT INTO clicks(
            ts,user_id,session_id,variant,placement,creative_id,ua,ip
        ) VALUES(?,?,?,?,?,?,?,?)""",
        (int(time.time()*1000), data["user_id"], data["session_id"], data["variant"], data["placement"],
         data["creative_id"], req.headers.get("user-agent",""), ip))
    return {"ok": True}

# ---------- stats ----------
def wilson_interval(k, n, z=1.96):
    if n == 0: return (0.0, 0.0, 0.0)
    phat = k/n
    denom = 1 + z**2/n
    center = (phat + z*z/(2*n)) / denom
    half = (z*math.sqrt((phat*(1-phat)+z*z/(4*n))/n)) / denom
    return (phat, max(0.0, center-half), min(1.0, center+half))

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    with db() as con:
        # only sessions with at least 1 search (valid sessions)
        rows = con.execute("""
        WITH valid_sessions AS (
          SELECT DISTINCT session_id FROM searches
        ),
        imp AS (
          SELECT i.variant, i.placement, i.creative_id, COUNT(*) AS imps
          FROM impressions i
          WHERE i.session_id IN (SELECT session_id FROM valid_sessions)
          GROUP BY i.variant, i.placement, i.creative_id
        ),
        clk AS (
          SELECT c.variant, c.placement, c.creative_id, COUNT(*) AS clicks
          FROM clicks c
          WHERE c.session_id IN (SELECT session_id FROM valid_sessions)
          GROUP BY c.variant, c.placement, c.creative_id
        )
        SELECT imp.variant, imp.placement, imp.creative_id, imp.imps, COALESCE(clk.clicks,0) AS clicks
        FROM imp
        LEFT JOIN clk ON imp.variant=clk.variant AND imp.placement=clk.placement AND imp.creative_id=clk.creative_id
        ORDER BY imp.variant, imp.placement, imp.creative_id
        """).fetchall()

        v_sessions = con.execute("SELECT COUNT(DISTINCT session_id) AS n FROM searches").fetchone()["n"]

    html = ["<html><head><meta charset='utf-8'><title>CTR Dashboard</title>",
            "<style>body{font-family:system-ui;padding:24px} table{border-collapse:collapse} td,th{padding:8px 10px;border:1px solid #ddd} .mono{font-variant-numeric:tabular-nums} .badge{color:#666}</style>",
            "</head><body><h1>CTR Dashboard</h1>",
            f"<p class='badge'>Valid sessions (>=1 search): <b>{v_sessions}</b></p>",
            "<table><tr><th>Variant</th><th>Placement</th><th>Creative</th><th>Imps</th><th>Clicks</th><th>CTR</th><th>95% CI</th></tr>"]
    for r in rows:
        ctr, lo, hi = wilson_interval(r["clicks"], r["imps"])
        html.append(
            f"<tr><td>{r['variant']}</td><td>{r['placement']}</td><td>{r['creative_id']}</td>"
            f"<td class='mono'>{r['imps']}</td><td class='mono'>{r['clicks']}</td>"
            f"<td class='mono'>{ctr*100:.2f}%</td><td class='mono'>[{lo*100:.2f}%, {hi*100:.2f}%]</td></tr>"
        )
    html.append("</table><p class='badge' style='margin-top:16px'>Wilson interval. Refresh to update.</p></body></html>")
    return "\n".join(html)

@app.get("/", include_in_schema=False)
def serve_index():
    path = os.path.join(os.path.dirname(__file__), "index.html")
    return FileResponse(path)

@app.get("/healthz", include_in_schema=False)
def healthz():
    return {"ok": True}
