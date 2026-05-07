import os
import json
import re
import secrets
import logging
import time as _time
import pandas as pd
from functools import wraps
from flask import Flask, render_template, jsonify, request, redirect, session, url_for
import google.generativeai as genai
import psycopg2
from psycopg2.extras import RealDictCursor
from urllib.parse import urlencode

# ---------- Logging ----------
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("alfaleus")

import threading
_classify_lock = threading.Lock()

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", secrets.token_hex(32))

OAUTH_CLIENT_ID     = os.environ.get("GOOGLE_OAUTH_CLIENT_ID", "")
OAUTH_CLIENT_SECRET = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "")
OAUTH_REDIRECT_URI  = os.environ.get("GOOGLE_OAUTH_REDIRECT_URI", "http://localhost:8080/api/gdrive/callback")
# Separate OAuth for user login (identity) — can reuse same client ID
LOGIN_REDIRECT_URI  = os.environ.get("GOOGLE_LOGIN_REDIRECT_URI",
                                     "http://localhost:8080/auth/google/callback")

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
EXCEL_PATH     = os.environ.get("EXCEL_PATH", "Query_sheet_alfaleus.xlsx")
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
DATA_DIR       = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

DATABASE_URL = os.environ.get("DATABASE_URL", "")


# ---------- Postgres helpers ----------

def get_db():
    """Return a new psycopg2 connection. Caller must close."""
    url = DATABASE_URL
    if not url:
        raise RuntimeError("DATABASE_URL env var not set")
    # Railway sometimes gives postgres:// — psycopg2 needs postgresql://
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    return psycopg2.connect(url, cursor_factory=RealDictCursor)


def init_db():
    conn = get_db()
    c = conn.cursor()

    # ── users ──────────────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id          SERIAL PRIMARY KEY,
            google_id   TEXT UNIQUE NOT NULL,
            email       TEXT UNIQUE NOT NULL,
            name        TEXT,
            picture     TEXT,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    # ── per-user settings (key/value) ──────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            user_id  INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            key      TEXT    NOT NULL,
            value    TEXT,
            PRIMARY KEY (user_id, key)
        )
    """)

    # ── per-user feedback ──────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS feedback (
            id                       SERIAL PRIMARY KEY,
            user_id                  INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            txn_key                  TEXT NOT NULL,
            original_classification  TEXT,
            corrected_classification TEXT,
            should_learn             INTEGER DEFAULT 1,
            created_at               TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    # ── per-user learned rules ─────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS learned_rules (
            id                 SERIAL PRIMARY KEY,
            user_id            INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            narration_pattern  TEXT    NOT NULL,
            classification     TEXT    NOT NULL,
            created_at         TIMESTAMPTZ DEFAULT NOW(),
            source_txn_key     TEXT
        )
    """)

    conn.commit()
    conn.close()
    log.info("Database initialised")


try:
    init_db()
except Exception as e:
    log.error("DB init failed (will retry on first request): %s", e)


# ---------- Auth helpers ----------

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if "user_id" not in session:
            if request.is_json or request.path.startswith("/api/"):
                return jsonify({"error": "unauthenticated", "login_url": "/auth/google"}), 401
            return redirect("/auth/google")
        return f(*args, **kwargs)
    return decorated


def current_user_id():
    return session.get("user_id")


# ---------- User DB helpers ----------

def upsert_user(google_id, email, name, picture):
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        INSERT INTO users (google_id, email, name, picture)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (google_id) DO UPDATE
            SET email = EXCLUDED.email,
                name  = EXCLUDED.name,
                picture = EXCLUDED.picture
        RETURNING id, email, name, picture
    """, (google_id, email, name, picture))
    row = c.fetchone()
    conn.commit()
    conn.close()
    return row


def get_user_by_id(user_id):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT id, email, name, picture FROM users WHERE id=%s", (user_id,))
    row = c.fetchone()
    conn.close()
    return row


# ---------- Settings helpers (per-user) ----------

def get_setting(key, default=None, user_id=None):
    uid = user_id or current_user_id()
    if not uid:
        return default
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT value FROM settings WHERE user_id=%s AND key=%s", (uid, key))
    row = c.fetchone()
    conn.close()
    return row["value"] if row else default


def set_setting(key, value, user_id=None):
    uid = user_id or current_user_id()
    if not uid:
        return
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        INSERT INTO settings (user_id, key, value) VALUES (%s, %s, %s)
        ON CONFLICT (user_id, key) DO UPDATE SET value = EXCLUDED.value
    """, (uid, key, value))
    conn.commit()
    conn.close()


def get_learned_rules(user_id=None):
    uid = user_id or current_user_id()
    if not uid:
        return []
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT narration_pattern, classification FROM learned_rules WHERE user_id=%s", (uid,))
    rows = c.fetchall()
    conn.close()
    return [{"pattern": r["narration_pattern"], "classification": r["classification"]} for r in rows]


def save_feedback(txn_key, original, corrected, should_learn, narration="", user_id=None):
    uid = user_id or current_user_id()
    if not uid:
        return
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        INSERT INTO feedback (user_id, txn_key, original_classification, corrected_classification, should_learn)
        VALUES (%s, %s, %s, %s, %s)
    """, (uid, txn_key, original, corrected, int(should_learn)))
    if should_learn and narration:
        pattern = narration.strip()[:60]
        c.execute("""
            SELECT id FROM learned_rules
            WHERE user_id=%s AND narration_pattern=%s AND classification=%s
        """, (uid, pattern, corrected))
        if not c.fetchone():
            c.execute("""
                INSERT INTO learned_rules (user_id, narration_pattern, classification, source_txn_key)
                VALUES (%s, %s, %s, %s)
            """, (uid, pattern, corrected, txn_key))
    conn.commit()
    conn.close()


# ---------- Google OAuth2 — Login (identity) ----------

@app.route("/auth/google")
def google_login():
    if not OAUTH_CLIENT_ID:
        return "GOOGLE_OAUTH_CLIENT_ID not set", 500
    state = secrets.token_urlsafe(16)
    session["login_state"] = state
    params = {
        "client_id": OAUTH_CLIENT_ID,
        "redirect_uri": LOGIN_REDIRECT_URI,
        "response_type": "code",
        "scope": "openid email profile",
        "access_type": "offline",
        "prompt": "select_account",
        "state": state,
    }
    return redirect("https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params))


@app.route("/auth/google/callback")
def google_callback():
    import requests as req_lib
    error = request.args.get("error")
    if error:
        return f"Login failed: {error}", 400

    code  = request.args.get("code", "")
    state = request.args.get("state", "")
    if state != session.pop("login_state", None):
        return "State mismatch — possible CSRF", 400

    # Exchange code for tokens
    resp = req_lib.post("https://oauth2.googleapis.com/token", data={
        "code": code,
        "client_id": OAUTH_CLIENT_ID,
        "client_secret": OAUTH_CLIENT_SECRET,
        "redirect_uri": LOGIN_REDIRECT_URI,
        "grant_type": "authorization_code",
    })
    tokens = resp.json()
    if "access_token" not in tokens:
        return f"Token exchange failed: {tokens}", 400

    # Fetch user info
    user_resp = req_lib.get("https://www.googleapis.com/oauth2/v2/userinfo",
                            headers={"Authorization": f"Bearer {tokens['access_token']}"})
    info = user_resp.json()
    google_id = info.get("id", "")
    email     = info.get("email", "")
    name      = info.get("name", "")
    picture   = info.get("picture", "")

    if not google_id or not email:
        return "Could not retrieve user info from Google", 400

    user = upsert_user(google_id, email, name, picture)
    session["user_id"]   = user["id"]
    session["user_email"] = user["email"]
    session["user_name"]  = user["name"]
    session["user_pic"]   = user["picture"]
    session.permanent = True

    log.info("User logged in: %s (id=%s)", email, user["id"])
    return redirect("/")


@app.route("/auth/logout", methods=["POST", "GET"])
def logout():
    session.clear()
    return redirect("/auth/google")


@app.route("/api/me")
@login_required
def me():
    return jsonify({
        "id":      session["user_id"],
        "email":   session.get("user_email"),
        "name":    session.get("user_name"),
        "picture": session.get("user_pic"),
    })


# ---------- Google Sheets helpers ----------

def gsheets_available():
    try:
        import gspread  # noqa
        from google.oauth2 import service_account  # noqa
        return True
    except ImportError:
        return False


def _oauth_creds(tokens: dict):
    from google.oauth2.credentials import Credentials
    return Credentials(
        token=tokens.get("access_token"),
        refresh_token=tokens.get("refresh_token"),
        token_uri="https://oauth2.googleapis.com/token",
        client_id=OAUTH_CLIENT_ID,
        client_secret=OAUTH_CLIENT_SECRET,
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.readonly"],
    )


def _refresh_oauth_creds(tokens: dict):
    from google.auth.transport.requests import Request as GRequest
    creds = _oauth_creds(tokens)
    if not creds.valid:
        creds.refresh(GRequest())
        updated = dict(tokens)
        updated["access_token"] = creds.token
        if creds.refresh_token:
            updated["refresh_token"] = creds.refresh_token
        set_setting("oauth_tokens", json.dumps(updated))
    return creds


def load_from_gsheets_oauth(sheet_id, sheet_name, tokens):
    import gspread
    creds  = _refresh_oauth_creds(tokens)
    client = gspread.authorize(creds)
    sh     = client.open_by_key(sheet_id)
    ws     = sh.worksheet(sheet_name)
    rows   = ws.get_all_values()
    if not rows:
        return pd.DataFrame()
    header_row = 0
    for i, row in enumerate(rows):
        if row and str(row[0]).strip() == "Date":
            header_row = i
            break
    df = pd.DataFrame(rows[header_row + 1:], columns=rows[header_row])
    df.columns = [str(c).strip() for c in df.columns]
    df = df[df["Date"].notna()]
    df = df[df["Date"].astype(str).str.strip() != ""]
    df = df[~df["Narration"].astype(str).str.lower().str.contains("grand total", na=False)]
    return df


def load_sheet_as_context_oauth(sheet_id, tab_name, tokens):
    import gspread
    creds  = _refresh_oauth_creds(tokens)
    client = gspread.authorize(creds)
    sh     = client.open_by_key(sheet_id)
    ws     = sh.worksheet(tab_name)
    rows   = ws.get_all_values()
    if len(rows) < 2:
        return pd.DataFrame()
    df = pd.DataFrame(rows[1:], columns=rows[0])
    df.columns = [str(c).strip() for c in df.columns]
    return df


def list_drive_sheets(tokens):
    import requests as req_lib
    from google.auth.transport.requests import Request as GRequest
    creds = _oauth_creds(tokens)
    if not creds.valid:
        creds.refresh(GRequest())
        updated = dict(tokens)
        updated["access_token"] = creds.token
        if creds.refresh_token:
            updated["refresh_token"] = creds.refresh_token
        set_setting("oauth_tokens", json.dumps(updated))
    headers = {"Authorization": f"Bearer {creds.token}"}
    url = ("https://www.googleapis.com/drive/v3/files"
           "?q=mimeType%3D%27application%2Fvnd.google-apps.spreadsheet%27"
           "+and+%27me%27+in+owners"
           "&fields=files(id,name,modifiedTime)&orderBy=modifiedTime+desc&pageSize=50")
    resp = req_lib.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json().get("files", [])


# ---------- Data Loading (scoped to current user) ----------

def load_transactions():
    active_sheet_id = get_setting("active_sheet_id")
    active_tab_name = get_setting("active_tab_name") or "query_sk"
    active_creds    = get_setting("oauth_tokens")

    if active_sheet_id and active_creds:
        try:
            return load_from_gsheets_oauth(
                active_sheet_id, active_tab_name, json.loads(active_creds))
        except Exception as e:
            log.warning("GSheets load failed: %s", e)

    df = pd.read_excel(EXCEL_PATH, sheet_name="query_sk", header=None)
    header_row = next((i for i, row in df.iterrows() if str(row[0]).strip() == "Date"), 3)
    transactions = pd.read_excel(EXCEL_PATH, sheet_name="query_sk", header=header_row)
    transactions.columns = [str(c).strip() for c in transactions.columns]
    transactions = transactions[transactions["Date"].notna()]
    transactions = transactions[transactions["Date"].astype(str).str.strip() != "nan"]
    transactions = transactions[~transactions["Narration"].astype(str).str.lower().str.contains(
        "grand total", na=False)]
    return transactions


def load_context_sheets():
    active_sheet_id   = get_setting("active_sheet_id")
    active_creds      = get_setting("oauth_tokens")
    context_tabs_json = get_setting("context_tab_names", "[]")
    try:
        context_tabs = json.loads(context_tabs_json)
    except Exception:
        context_tabs = []

    if not active_sheet_id or not active_creds or not context_tabs:
        return []

    results = []
    tokens  = json.loads(active_creds)
    for tab in context_tabs:
        try:
            df = load_sheet_as_context_oauth(active_sheet_id, tab, tokens)
            results.append({"tab_name": tab, "df": df})
        except Exception as e:
            log.warning("Failed to load context tab '%s': %s", tab, e)
    return results


def _load_excel_fallback():
    df = pd.read_excel(EXCEL_PATH, sheet_name="query_sk", header=None)
    header_row = next((i for i, row in df.iterrows() if str(row[0]).strip() == "Date"), 3)
    transactions = pd.read_excel(EXCEL_PATH, sheet_name="query_sk", header=header_row)
    transactions.columns = [str(c).strip() for c in transactions.columns]
    transactions = transactions[transactions["Date"].notna()]
    transactions = transactions[transactions["Date"].astype(str).str.strip() != "nan"]
    transactions = transactions[~transactions["Narration"].astype(str).str.lower().str.contains(
        "grand total", na=False)]
    return transactions


# ---------- Duplicate Detection ----------

def detect_duplicates(transactions_df):
    from datetime import datetime
    payments = []
    for i, (_, row) in enumerate(transactions_df.iterrows()):
        if str(row.get("Voucher Type", "")).strip() == "Payment":
            try:
                amt = float(str(row.get("Gross Total", "0")).replace(",", "").strip())
            except ValueError:
                amt = 0
            date_str = str(row.get("Date", ""))[:10].replace("/", "-")
            narration = str(row.get("Narration", "")).strip().upper()
            vendor = narration.split()[0] if narration else ""
            payments.append({"idx": i, "amt": amt, "date": date_str,
                             "narration": narration, "vendor": vendor})
    duplicates = {}
    for j, p in enumerate(payments):
        for k, q in enumerate(payments):
            if k <= j:
                continue
            if not p["vendor"] or p["vendor"] != q["vendor"]:
                continue
            if p["amt"] == 0:
                continue
            if abs(p["amt"] - q["amt"]) / p["amt"] > 0.01:
                continue
            try:
                d1 = datetime.fromisoformat(p["date"])
                d2 = datetime.fromisoformat(q["date"])
                if abs((d1 - d2).days) > 90:
                    continue
            except Exception:
                pass
            duplicates.setdefault(str(p["idx"]), []).append(q["idx"])
            duplicates.setdefault(str(q["idx"]), []).append(p["idx"])
    return duplicates


# ---------- Classification ----------

def build_context_sheet_summary(context_sheets):
    if not context_sheets:
        return ""
    parts = []
    for ctx in context_sheets:
        tab = ctx["tab_name"]
        df  = ctx["df"]
        if df.empty:
            continue
        parts.append(f"\n=== CONTEXT SHEET: {tab} ===")
        for _, row in df.head(300).iterrows():
            row_str = " | ".join(
                f"{k}: {str(v).strip()}" for k, v in row.items()
                if str(v).strip() and str(v).strip().lower() not in ("nan", "none", ""))
            if row_str:
                parts.append(f"  {row_str}")
    return "\n".join(parts)


def apply_learned_rules(item, learned_rules):
    narration = str(item.get("narration", "")).upper()
    for rule in learned_rules:
        if rule["pattern"].upper() in narration:
            return rule["classification"]
    return None


_CLASSIFY_PROMPT_BODY = """You are an AI assistant classifying bank transactions for Alfaleus Technology Pvt Ltd, a medical device company selling ophthalmic (eye care) devices.
{context_block}
{learned_rules_text}

=== YOUR TASK ===
Classify each transaction below. Use the context sheets above to match transactions to known records wherever possible.
"Other Expense" and "Other Income" are LAST RESORTS — use specific categories first.

PAYMENTS (outgoing) — pick the MOST SPECIFIC matching category:
- "Exhibition/Conference Expense" — AIOC, stall, fabrication, passes, AIOYV, GENERAL FUND, AIOC PROMOTION, AIOC EXPENSE, any AIOC-related spend
- "Salary/Freelance Payment" — FREELANCING, FREELANCE, WEBDEV CONSULTANCY, or any payment clearly to an individual for services/work
- "Hotel Booking" — hotel name in narration, or match amount+date to any hotel records in context sheets
- "Flight Booking" — airline name or PNR in narration, or match amount+date to flight records in context sheets
- "Cab/Transport Booking" — QUICKRIDE, SAVAARI, cab, taxi, or match to cab/transport records in context sheets
- "Bus Booking" — bus operator name or PNR, or match to bus records in context sheets
- "Courier/Logistics" — BLUE DART, BLUEDART, GENERATING DYNAMIC (Blue Dart code), courier, shipping
- "Sales Incentive Payment" — Incentive in narration, MMT/IMPS to field agents
- "Office/Admin Expense" — car wash, PAYTM jio/utility, AWFIS coworking, ROBSOAP, DAZZLE ROBOTICS, IOCL fuel/petrol, small tools/supplies
- "Tax Payment" — CBDT, GST, TDS, income tax
- "Bank/Finance Transaction" — LITE (UPI Lite add money), ADD MONEY, wallet top-up, UPIRET refund, PHONEPE REVERSE
- "Business Travel/Logistics" — DELHI EXPENSES, city name + EXPENSES, travel reimbursement, petrol/toll, any travel cost without a specific booking match
- "Sales Consultant Payment" — SALES CONSULTANT, commission payment
- "Other Expense" — ONLY if truly none of the above fit

RECEIPTS (incoming) — pick the MOST SPECIFIC matching category:
- "Device Payment Receipt" — incoming UPI/NEFT/IMPS from any doctor, hospital, or person whose name appears in context sheets
- "EMI/Installment Receipt" — same sender appearing more than once, or narration says EMI
- "Card Settlement" — TERMINAL CARDS SETTL, CARDS SETTL (daily POS batch)
- "Payment Gateway Receipt" — PAYUFLI, REF-PAYUFLI (online payment gateway)
- "Other Income" — ONLY if truly none of the above fit

CRITICAL RULES:
1. NEVER use "Other Expense" if ANY keyword above matches the narration
2. NEVER use "Other Income" if the sender name appears anywhere in the context sheets
3. Payments to named individuals for city/travel costs = "Business Travel/Logistics"
4. Payments to named individuals for work/services = "Salary/Freelance Payment"
5. Any narration containing AIOC = "Exhibition/Conference Expense" always
6. Match hotel/flight/bus/cab by amount AND approximate date using context sheets if provided

TRANSACTIONS:
{batch_json}

Return a JSON array (same length, same order):
[
  {{
    "idx": <same idx>,
    "classification": "<category>",
    "reference_id": "<reference ID from context sheet or empty string>",
    "matched_detail": "<what was matched>",
    "confidence": "high|medium|low",
    "reasoning": "<1-2 sentence explanation>"
  }}
]

Return ONLY the JSON array. No markdown. No extra text."""


def _make_model_runner():
    genai.configure(api_key=GEMINI_API_KEY)
    MODEL_CHAIN = ["gemini-2.5-flash", "gemini-2.0-flash", "gemini-2.5-flash-lite"]
    state = {"idx": 0, "cache": {}}

    def _get_model(name):
        if name not in state["cache"]:
            state["cache"][name] = genai.GenerativeModel(
                name, generation_config=genai.types.GenerationConfig())
        return state["cache"][name]

    def _generate(prompt_text, max_attempts=6):
        for attempt in range(max_attempts):
            model_name = MODEL_CHAIN[state["idx"]]
            model = _get_model(model_name)
            try:
                response = model.generate_content(prompt_text)
                if state["idx"] > 0:
                    state["idx"] -= 1
                return response
            except Exception as exc:
                err = str(exc)
                is_transient = any(kw in err for kw in [
                    "503", "UNAVAILABLE", "high demand", "ServiceUnavailable",
                    "429", "ResourceExhausted", "quota", "rate"])
                log.warning("Gemini error attempt %d/%d model=%s: %s",
                            attempt + 1, max_attempts, model_name, err[:200])
                if is_transient and attempt < max_attempts - 1:
                    nxt = state["idx"] + 1
                    if nxt < len(MODEL_CHAIN):
                        state["idx"] = nxt
                    _time.sleep(min(5 * (2 ** attempt), 60))
                else:
                    raise
    return _generate


def _parse_gemini_json(text):
    text = re.sub(r"^```[a-z]*\n?", "", text.strip())
    text = re.sub(r"\n?```$", "", text).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    last = text.rfind("}")
    if last != -1:
        try:
            return json.loads(re.sub(r",\s*$", "", text[:last + 1].strip()) + "]")
        except json.JSONDecodeError:
            pass
    objects = re.findall(r'\{[^{}]*\}', text, re.DOTALL)
    parsed  = []
    for o in objects:
        try:
            parsed.append(json.loads(o))
        except json.JSONDecodeError:
            pass
    return parsed if parsed else None


def classify_transactions_batch(transactions_df, context_sheets, user_id=None):
    uid = user_id or current_user_id()
    if not GEMINI_API_KEY:
        return [{"classification": "API key missing", "reference_id": "", "matched_detail": "",
                 "confidence": "low", "reasoning": "No Gemini API key set."}
                for _ in range(len(transactions_df))]

    _generate = _make_model_runner()
    context_section = build_context_sheet_summary(context_sheets)
    learned_rules   = get_learned_rules(uid)

    txn_list = [{"idx": i,
                 "date": str(row.get("Date", "")).strip(),
                 "particulars": str(row.get("Particulars", "")).strip(),
                 "voucher_type": str(row.get("Voucher Type", "")).strip(),
                 "voucher_no": str(row.get("Voucher No.", "")).strip(),
                 "narration": str(row.get("Narration", "")).strip(),
                 "gross_total": str(row.get("Gross Total", "")).strip()}
                for i, (_, row) in enumerate(transactions_df.iterrows())]

    results   = [None] * len(txn_list)
    needs_ai  = []
    for item in txn_list:
        learned = apply_learned_rules(item, learned_rules)
        if learned:
            results[item["idx"]] = {"idx": item["idx"], "classification": learned,
                                    "reference_id": "", "matched_detail": "Auto-classified",
                                    "confidence": "high", "reasoning": f"Learned rule: '{learned}'."}
        else:
            needs_ai.append(item)

    learned_rules_text = ""
    if learned_rules:
        learned_rules_text = "\n=== LEARNED RULES (OVERRIDE your judgement) ===\n"
        for r in learned_rules:
            learned_rules_text += f'  If narration contains "{r["pattern"]}" → classify as "{r["classification"]}"\n'

    context_block = (f"\n=== ADDITIONAL CONTEXT SHEETS ===\n{context_section}\n"
                     if context_section else "")

    batch_size = 20
    for start in range(0, len(needs_ai), batch_size):
        batch = needs_ai[start:start + batch_size]
        prompt = _CLASSIFY_PROMPT_BODY.format(
            context_block=context_block,
            learned_rules_text=learned_rules_text,
            batch_json=json.dumps(batch, indent=2))
        try:
            response     = _generate(prompt)
            batch_results = _parse_gemini_json(response.text)
            if batch_results:
                for r in batch_results:
                    if "idx" in r:
                        results[r["idx"]] = r
            else:
                raise ValueError("JSON parse failed")
        except Exception as e:
            log.error("Batch failed: %s", str(e)[:200])
            for item in batch:
                results[item["idx"]] = {"idx": item["idx"], "classification": "Error",
                                        "reference_id": "", "matched_detail": "",
                                        "confidence": "low", "reasoning": str(e)[:100]}
    return results


def _classify_transactions_stream(transactions_df, context_sheets, user_id=None):
    """Generator: yields list of result dicts per sub-batch."""
    uid = user_id or current_user_id()
    if not GEMINI_API_KEY:
        yield [{"idx": i, "classification": "API key missing", "reference_id": "",
                "matched_detail": "", "confidence": "low",
                "reasoning": "No Gemini API key set."} for i in range(len(transactions_df))]
        return

    _generate = _make_model_runner()
    context_section = build_context_sheet_summary(context_sheets)
    learned_rules   = get_learned_rules(uid)

    txn_list = [{"idx": i,
                 "date": str(row.get("Date", "")).strip(),
                 "particulars": str(row.get("Particulars", "")).strip(),
                 "voucher_type": str(row.get("Voucher Type", "")).strip(),
                 "voucher_no": str(row.get("Voucher No.", "")).strip(),
                 "narration": str(row.get("Narration", "")).strip(),
                 "gross_total": str(row.get("Gross Total", "")).strip()}
                for i, (_, row) in enumerate(transactions_df.iterrows())]

    needs_ai    = []
    rule_matches = []
    for item in txn_list:
        learned = apply_learned_rules(item, learned_rules)
        if learned:
            rule_matches.append({"idx": item["idx"], "classification": learned,
                                 "reference_id": "", "matched_detail": "Auto-classified",
                                 "confidence": "high", "reasoning": f"Learned rule: '{learned}'."})
        else:
            needs_ai.append(item)

    if rule_matches:
        yield rule_matches

    if not needs_ai:
        return

    learned_rules_text = ""
    if learned_rules:
        learned_rules_text = "\n=== LEARNED RULES (OVERRIDE your judgement) ===\n"
        for r in learned_rules:
            learned_rules_text += f'  If narration contains "{r["pattern"]}" → classify as "{r["classification"]}"\n'

    context_block = (f"\n=== ADDITIONAL CONTEXT SHEETS ===\n{context_section}\n"
                     if context_section else "")

    batch_size = 20
    for start in range(0, len(needs_ai), batch_size):
        batch  = needs_ai[start:start + batch_size]
        prompt = _CLASSIFY_PROMPT_BODY.format(
            context_block=context_block,
            learned_rules_text=learned_rules_text,
            batch_json=json.dumps(batch, indent=2))
        try:
            response      = _generate(prompt)
            batch_results = _parse_gemini_json(response.text)
            if batch_results:
                yield batch_results
            else:
                yield [{"idx": item["idx"], "classification": "Error", "reference_id": "",
                        "matched_detail": "", "confidence": "low",
                        "reasoning": "JSON parse failed."} for item in batch]
        except Exception as e:
            log.error("[stream] sub-batch failed: %s", str(e)[:200])
            yield [{"idx": item["idx"], "classification": "Error", "reference_id": "",
                    "matched_detail": "", "confidence": "low",
                    "reasoning": f"Failed: {str(e)[:80]}"} for item in batch]


def _safe_json(s):
    try:
        json.loads(s)
        return True
    except Exception:
        return False


# ============================================================
# Routes
# ============================================================

@app.route("/")
@login_required
def index():
    return render_template("index.html")


@app.route("/api/data")
@login_required
def get_data():
    try:
        transactions    = load_transactions()
        context_sheets  = load_context_sheets()

        txns = [{"date": str(row.get("Date", "")).strip(),
                 "particulars": str(row.get("Particulars", "")).strip(),
                 "voucher_type": str(row.get("Voucher Type", "")).strip(),
                 "voucher_no": str(row.get("Voucher No.", "")).strip(),
                 "narration": str(row.get("Narration", "")).strip(),
                 "gross_total": str(row.get("Gross Total", "")).strip()}
                for _, row in transactions.iterrows()]

        duplicates = detect_duplicates(transactions)

        context_tab_names = []
        try:
            context_tab_names = json.loads(get_setting("context_tab_names", "[]"))
        except Exception:
            pass

        return jsonify({"transactions": txns, "total": len(txns),
                        "duplicates": duplicates, "data_source": "google_sheets",
                        "context_tabs": context_tab_names})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/classify", methods=["POST"])
@login_required
def classify():
    if not _classify_lock.acquire(blocking=False):
        return jsonify({"error": "A classification is already running. Please wait."}), 429
    try:
        data    = request.json
        indices = data.get("indices", None)
        transactions    = load_transactions()
        context_sheets  = load_context_sheets()

        if indices is not None:
            subset = transactions.iloc[indices].copy()
        else:
            subset  = transactions.copy()
            indices = list(range(len(transactions)))

        results = classify_transactions_batch(subset, context_sheets)
        output  = [{"original_index": indices[i], **res} for i, res in enumerate(results) if res]
        return jsonify({"results": output})
    except Exception as e:
        log.exception("/api/classify error")
        return jsonify({"error": str(e)}), 500
    finally:
        _classify_lock.release()


@app.route("/api/classify_stream", methods=["POST"])
@login_required
def classify_stream():
    if not _classify_lock.acquire(blocking=False):
        def _busy():
            yield "data: " + json.dumps({"error": "A classification is already running.", "done": True}) + "\n\n"
        return app.response_class(_busy(), mimetype="text/event-stream")

    data    = request.json or {}
    indices = data.get("indices", None)

    # Snapshot everything from session NOW (generator runs after response starts)
    uid              = current_user_id()
    snap_sheet_id    = get_setting("active_sheet_id",   user_id=uid) or ""
    snap_tab_name    = get_setting("active_tab_name",   user_id=uid) or "query_sk"
    snap_tokens_str  = get_setting("oauth_tokens",      user_id=uid)
    snap_ctx_tabs    = get_setting("context_tab_names", user_id=uid) or "[]"

    def _generate():
        try:
            if snap_sheet_id and snap_tokens_str:
                transactions = load_from_gsheets_oauth(
                    snap_sheet_id, snap_tab_name, json.loads(snap_tokens_str))
            else:
                transactions = _load_excel_fallback()

            context_sheets = []
            if snap_sheet_id and snap_tokens_str:
                try:
                    ctx_tabs = json.loads(snap_ctx_tabs) if snap_ctx_tabs else []
                    tokens   = json.loads(snap_tokens_str)
                    for tab in ctx_tabs:
                        try:
                            df = load_sheet_as_context_oauth(snap_sheet_id, tab, tokens)
                            context_sheets.append({"tab_name": tab, "df": df})
                        except Exception as e:
                            log.warning("Context tab '%s' failed: %s", tab, e)
                except Exception as e:
                    log.warning("Context load failed: %s", e)

            if indices is not None:
                req_indices = indices
                subset      = transactions.iloc[req_indices].copy()
            else:
                subset      = transactions.copy()
                req_indices = list(range(len(transactions)))

            total     = len(req_indices)
            completed = 0

            for batch_results in _classify_transactions_stream(subset, context_sheets, uid):
                output = []
                for res in batch_results:
                    if res and "idx" in res:
                        original_idx = req_indices[res["idx"]]
                        output.append({"original_index": original_idx, **res})
                completed += len(output)
                yield f"data: {json.dumps({'results': output, 'done': False, 'completed': completed, 'total': total})}\n\n"

            yield f"data: {json.dumps({'results': [], 'done': True, 'completed': completed, 'total': total})}\n\n"

        except Exception as e:
            log.exception("classify_stream error")
            yield f"data: {json.dumps({'error': str(e), 'done': True})}\n\n"
        finally:
            _classify_lock.release()

    return app.response_class(
        _generate(), mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/classify_single", methods=["POST"])
@login_required
def classify_single():
    try:
        data   = request.json
        idx    = data.get("index", 0)
        transactions   = load_transactions()
        context_sheets = load_context_sheets()
        subset  = transactions.iloc[[idx]]
        results = classify_transactions_batch(subset, context_sheets)
        return jsonify({"result": results[0] if results else None})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/feedback", methods=["POST"])
@login_required
def submit_feedback():
    try:
        data         = request.json
        idx          = data.get("index")
        original     = data.get("original_classification", "")
        corrected    = data.get("corrected_classification", "")
        should_learn = data.get("should_learn", True)
        narration    = data.get("narration", "")
        key = f"{data.get('date','')[:10]}|{narration[:80]}|{data.get('gross_total','')}"

        save_feedback(key, original, corrected, should_learn,
                      narration if should_learn else "")

        affected = []
        if should_learn and narration:
            pattern = narration.strip()[:60].upper()
            try:
                transactions = load_transactions()
                for i, (_, row) in enumerate(transactions.iterrows()):
                    if i == idx:
                        continue
                    if pattern in str(row.get("Narration", "")).strip().upper():
                        affected.append(i)
            except Exception:
                pass

        return jsonify({"ok": True, "affected_indices": affected})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/feedback/rules", methods=["GET"])
@login_required
def get_rules():
    uid  = current_user_id()
    conn = get_db()
    c    = conn.cursor()
    c.execute("""
        SELECT id, narration_pattern, classification, created_at
        FROM learned_rules WHERE user_id=%s ORDER BY created_at DESC
    """, (uid,))
    rows = c.fetchall()
    conn.close()
    return jsonify({"rules": [{"id": r["id"], "pattern": r["narration_pattern"],
                               "classification": r["classification"],
                               "created_at": str(r["created_at"])} for r in rows]})


@app.route("/api/feedback/rules/<int:rule_id>", methods=["DELETE"])
@login_required
def delete_rule(rule_id):
    uid  = current_user_id()
    conn = get_db()
    c    = conn.cursor()
    # Only delete rules belonging to this user
    c.execute("DELETE FROM learned_rules WHERE id=%s AND user_id=%s", (rule_id, uid))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/status")
@login_required
def status():
    has_key = bool(GEMINI_API_KEY)
    try:
        transactions = load_transactions()
        rules        = get_learned_rules()
        context_tabs = json.loads(get_setting("context_tab_names", "[]") or "[]")
        return jsonify({
            "api_key_set":            has_key,
            "excel_loaded":           True,
            "transaction_count":      len(transactions),
            "google_sheets_connected": bool(get_setting("active_sheet_id")),
            "learned_rules_count":    len(rules),
            "context_tab_count":      len(context_tabs),
            "data_source":            "google_sheets" if get_setting("active_sheet_id") else "excel",
        })
    except Exception as e:
        return jsonify({"api_key_set": has_key, "excel_loaded": False, "error": str(e)})


# ---------- Google Drive OAuth2 — Sheet picker ----------

@app.route("/api/gdrive/auth")
@login_required
def gdrive_auth():
    if not OAUTH_CLIENT_ID or not OAUTH_CLIENT_SECRET:
        return jsonify({"error": "GOOGLE_OAUTH_CLIENT_ID / GOOGLE_OAUTH_CLIENT_SECRET not set"}), 400
    state = secrets.token_urlsafe(16)
    session["oauth_state"] = state
    params = {
        "client_id":     OAUTH_CLIENT_ID,
        "redirect_uri":  OAUTH_REDIRECT_URI,
        "response_type": "code",
        "scope": ("https://www.googleapis.com/auth/spreadsheets "
                  "https://www.googleapis.com/auth/drive.readonly"),
        "access_type": "offline",
        "prompt":      "consent",
        "state":       state,
    }
    return redirect("https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params))


@app.route("/api/gdrive/callback")
def gdrive_callback():
    import requests as req_lib
    error = request.args.get("error")
    if error:
        return (f"<script>window.opener.postMessage("
                f"{{type:'gdrive_error',error:{json.dumps(error)}}}, '*'); window.close();</script>")

    code  = request.args.get("code", "")
    state = request.args.get("state", "")
    if state != session.get("oauth_state", ""):
        return "State mismatch — possible CSRF", 400

    resp = req_lib.post("https://oauth2.googleapis.com/token", data={
        "code":          code,
        "client_id":     OAUTH_CLIENT_ID,
        "client_secret": OAUTH_CLIENT_SECRET,
        "redirect_uri":  OAUTH_REDIRECT_URI,
        "grant_type":    "authorization_code",
    })
    tokens = resp.json()
    if "access_token" not in tokens:
        return f"Token exchange failed: {tokens}", 400

    # Store tokens scoped to this user
    set_setting("oauth_tokens", json.dumps(tokens))
    return "<script>window.opener.postMessage({type:'gdrive_connected'}, '*'); window.close();</script>"


@app.route("/api/gdrive/sheets")
@login_required
def gdrive_sheets():
    tokens_str = get_setting("oauth_tokens")
    if not tokens_str:
        return jsonify({"error": "not_connected"}), 401
    try:
        sheets = list_drive_sheets(json.loads(tokens_str))
        return jsonify({"sheets": sheets})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/gdrive/select", methods=["POST"])
@login_required
def gdrive_select():
    data       = request.json or {}
    sheet_id   = data.get("sheet_id", "").strip()
    sheet_name = data.get("sheet_name", "").strip()
    tab_name   = data.get("tab_name", "").strip()
    if not sheet_id:
        return jsonify({"error": "sheet_id required"}), 400
    set_setting("active_sheet_id",   sheet_id)
    set_setting("active_sheet_name", sheet_name)
    if tab_name:
        set_setting("active_tab_name", tab_name)
    set_setting("context_tab_names", "[]")
    return jsonify({"ok": True, "sheet_id": sheet_id,
                    "sheet_name": sheet_name, "tab_name": tab_name})


@app.route("/api/gdrive/set_context_tabs", methods=["POST"])
@login_required
def set_context_tabs():
    data = request.json or {}
    tabs = data.get("tabs", [])
    if not isinstance(tabs, list):
        return jsonify({"error": "tabs must be an array"}), 400
    set_setting("context_tab_names", json.dumps(tabs))
    return jsonify({"ok": True, "tabs": tabs})


@app.route("/api/gdrive/worksheets")
@login_required
def gdrive_worksheets():
    sheet_id   = request.args.get("sheet_id", "").strip()
    if not sheet_id:
        return jsonify({"error": "sheet_id required"}), 400
    tokens_str = get_setting("oauth_tokens")
    if not tokens_str:
        return jsonify({"error": "not_connected"}), 401
    try:
        import gspread
        creds  = _refresh_oauth_creds(json.loads(tokens_str))
        client = gspread.authorize(creds)
        sh     = client.open_by_key(sheet_id)
        tabs   = [ws.title for ws in sh.worksheets()]
        return jsonify({"tabs": tabs})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/gdrive/write_results", methods=["POST"])
@login_required
def gdrive_write_results():
    tokens_str = get_setting("oauth_tokens")
    sheet_id   = get_setting("active_sheet_id")
    if not tokens_str or not sheet_id:
        return jsonify({"error": "Not connected or no sheet selected"}), 400
    try:
        import gspread
        data   = request.json or {}
        rows   = data.get("results", [])
        if not rows:
            return jsonify({"error": "No results provided"}), 400
        creds  = _refresh_oauth_creds(json.loads(tokens_str))
        client = gspread.authorize(creds)
        sh     = client.open_by_key(sheet_id)
        OUTPUT_TAB = "Classified"
        try:
            ws = sh.worksheet(OUTPUT_TAB)
            ws.clear()
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=OUTPUT_TAB, rows=len(rows) + 10, cols=10)
        header = ["Date", "Narration", "Voucher Type", "Gross Total",
                  "Classification", "Reference ID", "Confidence", "Reasoning"]
        body   = [[r.get("date",""), r.get("narration",""), r.get("voucher_type",""),
                   r.get("gross_total",""), r.get("classification",""),
                   r.get("reference_id",""), r.get("confidence",""), r.get("reasoning","")]
                  for r in rows]
        ws.update([header] + body)
        ws.format("A1:H1", {"textFormat": {"bold": True},
                             "backgroundColor": {"red": 0.2, "green": 0.2, "blue": 0.5}})
        return jsonify({"ok": True, "tab": OUTPUT_TAB, "rows_written": len(body)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/gdrive/status")
@login_required
def gdrive_status():
    tokens_str   = get_setting("oauth_tokens")
    sheet_id     = get_setting("active_sheet_id") or ""
    sheet_name   = get_setting("active_sheet_name") or ""
    tab_name     = get_setting("active_tab_name") or ""
    context_tabs = []
    try:
        context_tabs = json.loads(get_setting("context_tab_names", "[]") or "[]")
    except Exception:
        pass
    return jsonify({
        "connected":         bool(tokens_str),
        "active_sheet_id":   sheet_id,
        "active_sheet_name": sheet_name,
        "active_tab_name":   tab_name,
        "context_tabs":      context_tabs,
        "oauth_configured":  bool(OAUTH_CLIENT_ID and OAUTH_CLIENT_SECRET),
    })


@app.route("/api/gdrive/disconnect", methods=["POST"])
@login_required
def gdrive_disconnect():
    set_setting("oauth_tokens",      "")
    set_setting("active_sheet_id",   "")
    set_setting("active_sheet_name", "")
    set_setting("active_tab_name",   "")
    set_setting("context_tab_names", "[]")
    return jsonify({"ok": True})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)