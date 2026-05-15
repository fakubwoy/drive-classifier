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
_secret = os.environ.get("FLASK_SECRET_KEY")
if not _secret:
    raise RuntimeError(
        "FLASK_SECRET_KEY env var is not set. "
        "A random key causes session loss between workers/restarts and breaks OAuth. "
        "Set a fixed value in your .env file."
    )
app.secret_key = _secret

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

    # ── workspace nodes (folder tree + sheet attachments, per user) ───────────
    # source_type tells us where the transaction sheet lives:
    #   'gsheet' (default, legacy) — sheet_id is a Google Sheet id
    #   'excel'                    — excel_file_id points to workspace_excel_files
    c.execute("""
        CREATE TABLE IF NOT EXISTS workspace_nodes (
            id            SERIAL PRIMARY KEY,
            user_id       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            parent_id     INTEGER REFERENCES workspace_nodes(id) ON DELETE CASCADE,
            name          TEXT NOT NULL,
            node_type     TEXT NOT NULL DEFAULT 'folder',  -- 'folder' | 'sheet'
            sheet_id      TEXT,
            sheet_name    TEXT,
            tab_name      TEXT,
            context_tabs  TEXT DEFAULT '[]',
            source_type   TEXT NOT NULL DEFAULT 'gsheet',  -- 'gsheet' | 'excel'
            excel_file_id INTEGER,
            sort_order    INTEGER DEFAULT 0,
            created_at    TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    # Migration for existing installations — add the new columns if missing.
    c.execute("""
        ALTER TABLE workspace_nodes
        ADD COLUMN IF NOT EXISTS source_type TEXT NOT NULL DEFAULT 'gsheet'
    """)
    c.execute("""
        ALTER TABLE workspace_nodes
        ADD COLUMN IF NOT EXISTS excel_file_id INTEGER
    """)

    # ── workspace excel files — uploaded Excel statements per workspace node ──
    # An alternative to attaching a Google Sheet. Stores parsed tab data so the
    # classifier and viewer can read transactions without re-parsing the bytes.
    c.execute("""
        CREATE TABLE IF NOT EXISTS workspace_excel_files (
            id          SERIAL PRIMARY KEY,
            user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            filename    TEXT NOT NULL,
            file_data   BYTEA,
            parsed_json TEXT NOT NULL DEFAULT '{}',
            created_at  TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_workspace_excel_files_user
        ON workspace_excel_files(user_id)
    """)

    # ── per-node classifications (scoped to user + node) ─────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS node_classifications (
            id              SERIAL PRIMARY KEY,
            user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            node_id         INTEGER NOT NULL REFERENCES workspace_nodes(id) ON DELETE CASCADE,
            txn_key         TEXT NOT NULL,
            classification  TEXT,
            reference_id    TEXT DEFAULT '',
            matched_detail  TEXT DEFAULT '',
            confidence      TEXT DEFAULT 'high',
            reasoning       TEXT DEFAULT '',
            review_decision TEXT DEFAULT '',
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            updated_at      TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(user_id, node_id, txn_key)
        )
    """)

    # ── account-wide classification cache (scoped to user, NOT node) ─────────
    # Mirrors tax_classifications: keyed by (user_id, txn_key) only, with no
    # FK to workspace_nodes. Lets classifications survive node deletes, sheet
    # re-uploads, and node recreations after a redeploy — same persistence
    # behavior the GST/TDS table already gives us. node_classifications is
    # still the per-sheet source of truth (so the same txn can be classified
    # differently in two sheets), and this table is consulted only as a
    # fallback when a sheet has no per-node row for a given txn_key.
    c.execute("""
        CREATE TABLE IF NOT EXISTS account_classifications (
            id              SERIAL PRIMARY KEY,
            user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            txn_key         TEXT NOT NULL,
            classification  TEXT,
            reference_id    TEXT DEFAULT '',
            matched_detail  TEXT DEFAULT '',
            confidence      TEXT DEFAULT 'high',
            reasoning       TEXT DEFAULT '',
            review_decision TEXT DEFAULT '',
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            updated_at      TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(user_id, txn_key)
        )
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_account_classifications_user
        ON account_classifications(user_id)
    """)

    # ── uploaded excel sheets (per user, multi-sheet combine) ─────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS uploaded_sheets (
            id          SERIAL PRIMARY KEY,
            user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            filename    TEXT NOT NULL,
            label       TEXT,
            file_data   BYTEA NOT NULL,
            row_count   INTEGER DEFAULT 0,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    # ── per-node uploaded context files (xlsx / xls / csv) ───────────────
    # Each row = one uploaded file attached to a workspace sheet node.
    # `parsed_json` is a JSON object mapping tab-name -> {headers, rows}
    # so the viewer + classifier can read without re-parsing the bytes.
    # `selected_tabs` is a JSON array of tab names the user wants the
    # classifier to actually use (defaults to all tabs at upload time).
    c.execute("""
        CREATE TABLE IF NOT EXISTS node_context_files (
            id              SERIAL PRIMARY KEY,
            user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            node_id         INTEGER NOT NULL REFERENCES workspace_nodes(id) ON DELETE CASCADE,
            filename        TEXT NOT NULL,
            file_data       BYTEA,
            parsed_json     TEXT NOT NULL DEFAULT '{}',
            selected_tabs   TEXT NOT NULL DEFAULT '[]',
            row_count_total INTEGER DEFAULT 0,
            created_at      TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_node_context_files_user_node
        ON node_context_files(user_id, node_id)
    """)

    # ── GST input credit sheet (government GSTR-2B style) ─────────────────
    # NOTE: now scoped to a parent expense sheet via sheet_id (nullable for
    # legacy global rows). One GST sheet per (user_id, sheet_id).
    c.execute("""
        CREATE TABLE IF NOT EXISTS gst_input_sheets (
            id          SERIAL PRIMARY KEY,
            user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            sheet_id    INTEGER REFERENCES uploaded_sheets(id) ON DELETE CASCADE,
            filename    TEXT NOT NULL,
            file_data   BYTEA NOT NULL,
            row_count   INTEGER DEFAULT 0,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    # Migration: if the column was missing from a previous install, add it.
    c.execute("""
        ALTER TABLE gst_input_sheets
        ADD COLUMN IF NOT EXISTS sheet_id INTEGER
          REFERENCES uploaded_sheets(id) ON DELETE CASCADE
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_gst_input_sheets_user_sheet
        ON gst_input_sheets(user_id, sheet_id)
    """)

    # ── TDS input credit sheet (e.g. Form 26AS / TDS reconciliation) ──────
    # Mirrors gst_input_sheets — one TDS file per (user_id, sheet_id).
    c.execute("""
        CREATE TABLE IF NOT EXISTS tds_input_sheets (
            id          SERIAL PRIMARY KEY,
            user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            sheet_id    INTEGER REFERENCES uploaded_sheets(id) ON DELETE CASCADE,
            filename    TEXT NOT NULL,
            file_data   BYTEA NOT NULL,
            row_count   INTEGER DEFAULT 0,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_tds_input_sheets_user_sheet
        ON tds_input_sheets(user_id, sheet_id)
    """)

    # ── per-transaction tax classifications ────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS tax_classifications (
            id              SERIAL PRIMARY KEY,
            user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            txn_key         TEXT NOT NULL,
            gst_applicable  TEXT DEFAULT 'no',   -- yes | no | reverse_charge | exempt
            gst_direction   TEXT DEFAULT '',      -- in | out
            gst_rate        TEXT DEFAULT '',      -- 5 | 12 | 18 | 28 | 0
            tds_applicable  TEXT DEFAULT 'no',   -- yes | no
            tds_direction   TEXT DEFAULT '',      -- in | out
            tds_section     TEXT DEFAULT '',      -- 194C | 194J | 194I | 192 | 194Q …
            tds_rate        TEXT DEFAULT '',      -- e.g. 1 | 2 | 10
            ai_reasoning    TEXT DEFAULT '',
            confirmed       INTEGER DEFAULT 0,
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            updated_at      TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(user_id, txn_key)
        )
    """)

    # ── GST ITC match results (deterministic pre-match, per user+node) ────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS gst_itc_matches (
            id              SERIAL PRIMARY KEY,
            user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            node_id         INTEGER NOT NULL REFERENCES workspace_nodes(id) ON DELETE CASCADE,
            txn_key         TEXT NOT NULL,
            itc_status      TEXT NOT NULL DEFAULT 'unverifiable',
            confidence      TEXT NOT NULL DEFAULT 'low',
            signals         TEXT NOT NULL DEFAULT '{}',
            invoice_no      TEXT DEFAULT '',
            vendor_name     TEXT DEFAULT '',
            invoice_date    TEXT DEFAULT '',
            invoice_value   TEXT DEFAULT '',
            taxable_value   TEXT DEFAULT '',
            igst            TEXT DEFAULT '',
            cgst            TEXT DEFAULT '',
            sgst            TEXT DEFAULT '',
            source_sheet    TEXT DEFAULT '',
            created_at      TIMESTAMPTZ DEFAULT NOW(),
            updated_at      TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(user_id, node_id, txn_key)
        )
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_gst_itc_matches_user_node
        ON gst_itc_matches(user_id, node_id)
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
    # Request Drive + Sheets scopes at login so no second OAuth popup is needed
    params = {
        "client_id": OAUTH_CLIENT_ID,
        "redirect_uri": LOGIN_REDIRECT_URI,
        "response_type": "code",
        "scope": (
            "openid email profile "
            "https://www.googleapis.com/auth/spreadsheets "
            "https://www.googleapis.com/auth/drive.readonly"
        ),
        "access_type": "offline",
        "prompt": "consent",   # always show consent so we get a refresh_token
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

    # Exchange code for tokens (includes Drive/Sheets access + refresh token)
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
    session["user_id"]    = user["id"]
    session["user_email"] = user["email"]
    session["user_name"]  = user["name"]
    session["user_pic"]   = user["picture"]
    session.permanent = True

    # Save Drive/Sheets tokens immediately — no second "Connect Drive" OAuth needed
    # Only overwrite if we got a refresh_token (first login always has one due to prompt=consent)
    uid = user["id"]
    existing_tokens_str = get_setting("oauth_tokens", user_id=uid)
    if tokens.get("refresh_token"):
        set_setting("oauth_tokens", json.dumps(tokens), user_id=uid)
    elif not existing_tokens_str:
        # No refresh token this time and nothing saved yet — store what we have
        set_setting("oauth_tokens", json.dumps(tokens), user_id=uid)

    log.info("User logged in: %s (id=%s), drive_tokens_saved=%s",
             email, uid, bool(tokens.get("refresh_token") or existing_tokens_str))
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

class NoSheetConnectedError(Exception):
    pass


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
            raise

    # No sheet connected — give a clear actionable error instead of crashing on missing xlsx
    raise NoSheetConnectedError(
        "No Google Sheet connected. Click 'Connect Drive' in the top-right to link your sheet."
    )


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
    raise NoSheetConnectedError(
        "No Google Sheet connected. Click 'Connect Drive' in the top-right to link your sheet."
    )


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

_REF_COL_KEYWORDS = {
    "invoice", "invoice no", "invoice number", "invoice #", "inv no", "inv #",
    "invoice details invoice number",   # GSTR-2B merged header after _load_and_fix
    "bill no", "bill number", "bill #", "order no", "order number", "po no",
    "po number", "purchase order", "booking ref", "booking id", "booking no",
    "pnr", "reference", "ref no", "ref #", "ref id", "receipt no", "receipt #",
    "voucher", "voucher no", "doc no", "document no", "txn id", "transaction id",
}

# Fuzzy sub-strings ranked by specificity — more-specific phrases tried first
# so "Invoice Details Invoice number" matches "invoice number" before bare "inv",
# preventing "Invoice type" or "Invoice Date" from winning.
_REF_FUZZY_PRIORITY = [
    "invoice number", "invoice no", "inv no",
    "ref no", "ref id", "reference no",
    "booking ref", "booking id",
    "order no", "po no",
    "bill no",
    "pnr",
    "voucher no",
    "inv",
    "ref",
]

def _detect_ref_column(df):
    """Return the column name most likely to hold an invoice/reference number, or None."""
    # 1. Exact match
    for col in df.columns:
        if col.strip().lower() in _REF_COL_KEYWORDS:
            return col
    # 2. Ranked fuzzy — iterate priority phrases, return first column containing it
    for phrase in _REF_FUZZY_PRIORITY:
        for col in df.columns:
            if phrase in col.strip().lower():
                return col
    return None


def build_context_sheet_summary(context_sheets):
    if not context_sheets:
        return ""
    parts = []
    for ctx in context_sheets:
        tab     = ctx["tab_name"]
        df      = ctx["df"]
        if df.empty:
            continue
        ref_col = _detect_ref_column(df)
        cols_lower = [str(c).strip().lower() for c in df.columns]
        # Detect if this is a GSTR-2B / purchase register sheet vs a bank statement
        is_gst_sheet = any(k in cols_lower for k in (
            "gstin of supplier", "gstin", "trade/legal name", "invoice value(₹)",
            "invoice value", "taxable value (₹)", "taxable value"
        ))
        header  = f"\n=== CONTEXT SHEET: {tab} ==="
        if ref_col:
            header += (
                f'  [REFERENCE ID COLUMN: "{ref_col}"'
                f' — use this value as reference_id when you match a row from this sheet]'
            )
        if is_gst_sheet:
            header += (
                "\n  [GST/PURCHASE REGISTER: Match by vendor/trade name appearing in the bank narration"
                " AND/OR Invoice Value matching bank amount. DO NOT require invoice date to match"
                " the bank transaction date — invoices are often paid weeks/months after issue.]"
            )
        parts.append(header)
        for _, row in df.head(300).iterrows():
            cells = []
            for k, v in row.items():
                vs = str(v).strip()
                if not vs or vs.lower() in ("nan", "none", ""):
                    continue
                label = f"[REF] {k}" if k == ref_col else k
                cells.append(f"{label}: {vs}")
            row_str = " | ".join(cells)
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

GST & TDS DETERMINATION:
For each transaction ALSO set gst_applicable, gst_direction, tds_applicable, tds_direction.
If GST or TDS context sheets were uploaded, cross-reference vendors/amounts against them.
- If a vendor appears in the uploaded GST sheet → gst_applicable="yes" (or "reverse_charge" for imports/GTA)
- If the transaction involves a vendor NOT in the GST sheet → gst_applicable="no" or flag it
- Outgoing payments for services/goods → gst_direction="in" (ITC), incoming sales → gst_direction="out"
- Salary/wages/bank transfers with no goods/services → gst_applicable="no", tds_applicable="no"
- Professional services → tds_applicable="yes", tds_direction="out" (we deduct), section 194J
- Contractor/fabrication → tds_applicable="yes", tds_direction="out", section 194C
- Customer paying us (receipt) and TDS deducted → tds_direction="in"

STRICT GST/TDS RULES (override general rules above when they conflict):

RULE-G1 REIMBURSEMENTS: Any narration containing "REIMBURSE", "REIMBURSEMENT", or category is "Business Travel/Logistics" where the payment is to an individual (single person name, not a company) → gst_applicable="no", tds_applicable="no". Reimbursements are not supply of goods/services; no ITC can be claimed.

RULE-G2 RATE REQUIRED: If you set gst_applicable="yes" or "reverse_charge", you MUST also set a non-empty gst_rate. Never leave gst_rate blank when GST applies. If you are unsure of the exact rate, pick the most likely rate (18 for most services, 12 for medical devices, 5 for basic travel/food) and flag it with low confidence.

RULE-G3 UPI TO INDIVIDUAL = NO GST: A UPI/IMPS/NEFT payment to an individual person (not a registered business) cannot have GST applicable. If the narration contains a person's name and no business/trade name, set gst_applicable="no". Individuals are not GST registrants.

RULE-G4 CATEGORY-TAX CONSISTENCY: Category and tax must be consistent:
  - "Salary/Freelance Payment" → gst_applicable="no", tds_applicable="yes", tds_section="192" (salary) or "194J" (freelance/consulting). NEVER set gst_applicable="yes" for salary.
  - "Tax Payment" (CBDT/GST/TDS) → gst_applicable="no", tds_applicable="no". Tax payments are not taxable.
  - "Bank/Finance Transaction" → gst_applicable="exempt", tds_applicable="no".
  - "Office/Admin Expense" for small amounts (<5000) → tds_applicable="no" (below threshold).

RULE-T1 TDS THRESHOLDS: TDS is NOT applicable below these annual thresholds. For a single transaction, apply TDS only if the per-transaction amount or running total is likely to cross the threshold:
  - 194C (contractor): threshold ₹30,000 per transaction OR ₹1,00,000 annual aggregate. Single transactions below ₹30,000 to a contractor → tds_applicable="no" unless it is clearly a recurring vendor.
  - 194J (professional): threshold ₹30,000 per year. Transactions below ₹3,000 → tds_applicable="no".
  - 194I (rent): threshold ₹2,40,000 per year. Monthly rent below ₹20,000 → tds_applicable="no".
  - 192 (salary): TDS applicable only on salary exceeding basic exemption (₹3L/year = ~₹25,000/month). One-off small payments to an individual below ₹5,000 → tds_applicable="no".

RULE-R1 REFERENCE ID STRICT: The "reference_id" field must contain ONLY a genuine invoice number, purchase order number, or booking reference from the context sheets that directly matches this transaction. Do NOT put: vendor names, bank names, payment descriptions, narration text, or UPI transaction IDs in reference_id. If no invoice/PO/booking reference can be matched from the context sheets, set reference_id="".

RULE-R2 AMOUNT+DATE MATCHING: Matching rules differ by context sheet type:
  - For BANK STATEMENT context sheets (columns: Date, Narration, Gross Total): date must be within ±3 days AND amount must match exactly (or within 1%).
  - For PURCHASE REGISTER / GSTR-2B context sheets (columns: GSTIN, Trade/Legal name, Invoice number, Invoice Date, Invoice Value): the invoice date is when the invoice was issued, NOT when it was paid — do NOT require the bank transaction date to match the invoice date. Instead match by: (a) vendor/trade name appearing in the narration OR (b) invoice value matching the bank transaction amount within 1%. A vendor name match alone (without amount) is sufficient when the vendor name is distinctive and specific (not a generic name like "HDFC BANK"). Amount-only matches without any vendor name signal should still set reference_id="".
  - If multiple records match, pick the one whose invoice value is closest to the bank amount; if still ambiguous, set reference_id="" and note ambiguity in reasoning.

RULE-B1 LARGE UNMATCHED RECEIPTS: For incoming receipts (positive amounts) above ₹10,000 from an individual or entity not identifiable as a known customer in the context sheets, set confidence="low" and note "unmatched receipt — verify if GST output liability applies" in reasoning. Do not silently classify these as "Other Income" without flagging.

{prior_decisions_block}
TRANSACTIONS:
{batch_json}

Return a JSON array (same length, same order):
[
  {{
    "idx": <same idx>,
    "classification": "<category>",
    "reference_id": "<the exact value from the [REF] column of the matched context sheet row — NOT a vendor name, bank name, or narration text. Empty string if no row was matched>",
    "matched_detail": "<what was matched>",
    "confidence": "high|medium|low",
    "reasoning": "<1-2 sentence explanation>",
    "gst_applicable": "yes|no|reverse_charge|exempt",
    "gst_direction": "in|out|",
    "gst_rate": "5|12|18|28|0|",
    "tds_applicable": "yes|no",
    "tds_direction": "in|out|",
    "tds_section": "192|194C|194J|194I(a)|194I(b)|194H|194Q|194A|"
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


def _extract_vendor_key(narration: str) -> str:
    """Return a short normalised vendor signature from a narration string."""
    import re as _re
    s = narration.upper().strip()
    s = _re.sub(r'\b(UPI|NEFT|RTGS|IMPS|NACH|ECS|ACH|MMT|TRANSFER|CR|DR|REF)\b[/\-]?', '', s)
    s = _re.sub(r'\b\d[\d\-/]*\b', '', s)
    s = _re.sub(r'\b(LTD|PVT|LIMITED|PRIVATE|CO|INDIA|PAYMENT|PAY|TO|FROM|BY|THE|AND|OF|FOR)\b', '', s)
    s = _re.sub(r'[^A-Z\s]', ' ', s)
    s = _re.sub(r'\s+', ' ', s).strip()
    tokens = [t for t in s.split() if len(t) >= 4]
    return tokens[0] if tokens else s[:20]


def _build_prior_decisions_block(prior: dict) -> str:
    """Render the prior-decisions dict as a compact prompt block."""
    if not prior:
        return ""
    lines = ["=== PRIOR DECISIONS FOR KNOWN VENDORS (maintain consistency) ==="]
    for vendor, d in list(prior.items())[:40]:
        parts = []
        if d.get("classification"):
            parts.append(f'category="{d["classification"]}"'  )
        if d.get("gst_applicable"):
            rate = f'@{d["gst_rate"]}%' if d.get("gst_rate") else ""
            parts.append(f'gst={d["gst_applicable"]}{rate}')
        if d.get("tds_applicable") == "yes":
            sec = d.get("tds_section", "")
            parts.append(f"tds=yes({sec})")
        else:
            parts.append("tds=no")
        lines.append(f"  {vendor}: {', '.join(parts)}")
    lines.append("If the current transaction matches one of these vendors, use the same "
                 "GST rate, TDS section, and category unless there is a clear reason not to.\n")
    return "\n".join(lines)


def _update_prior_decisions(prior: dict, batch_results: list, batch_items: list) -> None:
    """Mutate *prior* in-place with decisions from a completed batch."""
    idx_to_item = {item["idx"]: item for item in batch_items}
    for res in batch_results:
        item = idx_to_item.get(res.get("idx", -1))
        if not item:
            continue
        vendor_key = _extract_vendor_key(item.get("narration", ""))
        if not vendor_key:
            continue
        entry = {
            "classification": res.get("classification", ""),
            "gst_applicable": res.get("gst_applicable", ""),
            "gst_rate":       res.get("gst_rate", ""),
            "tds_applicable": res.get("tds_applicable", ""),
            "tds_section":    res.get("tds_section", ""),
        }
        if any(entry.values()):
            if vendor_key not in prior or res.get("confidence", "high") != "low":
                prior[vendor_key] = entry


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

    prior_decisions: dict = {}
    batch_size = 20
    for start in range(0, len(needs_ai), batch_size):
        batch = needs_ai[start:start + batch_size]
        prompt = _CLASSIFY_PROMPT_BODY.format(
            context_block=context_block,
            learned_rules_text=learned_rules_text,
            prior_decisions_block=_build_prior_decisions_block(prior_decisions),
            batch_json=json.dumps(batch, indent=2))
        try:
            response     = _generate(prompt)
            batch_results = _parse_gemini_json(response.text)
            if batch_results:
                _update_prior_decisions(prior_decisions, batch_results, batch)
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

    prior_decisions: dict = {}
    batch_size = 20
    for start in range(0, len(needs_ai), batch_size):
        batch  = needs_ai[start:start + batch_size]
        prompt = _CLASSIFY_PROMPT_BODY.format(
            context_block=context_block,
            learned_rules_text=learned_rules_text,
            prior_decisions_block=_build_prior_decisions_block(prior_decisions),
            batch_json=json.dumps(batch, indent=2))
        try:
            response      = _generate(prompt)
            batch_results = _parse_gemini_json(response.text)
            if batch_results:
                _update_prior_decisions(prior_decisions, batch_results, batch)
                # Persist any GST/TDS fields the model returned alongside classification
                _persist_tax_from_classify_results(batch_results, needs_ai[start:start+batch_size], uid)
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


def _persist_tax_from_classify_results(batch_results, batch_items, uid):
    """Save GST/TDS fields returned by the classify prompt into tax_classifications."""
    tax_rows = []
    idx_to_item = {item["idx"]: item for item in batch_items}
    for res in (batch_results or []):
        gst = res.get("gst_applicable")
        tds = res.get("tds_applicable")
        if not gst and not tds:
            continue  # model didn't return tax fields
        item = idx_to_item.get(res.get("idx", -1))
        if not item:
            continue
        key = f"{item.get('date','')[:10]}|{item.get('narration','')[:80]}|{item.get('gross_total','')}"
        tax_rows.append((
            uid, key,
            gst or "no", res.get("gst_direction", ""), res.get("gst_rate", ""),
            tds or "no", res.get("tds_direction", ""), res.get("tds_section", ""), res.get("tds_rate", ""),
            res.get("reasoning", ""),
        ))
    if not tax_rows:
        return
    try:
        conn = get_db()
        c = conn.cursor()
        for row in tax_rows:
            c.execute("""
                INSERT INTO tax_classifications
                  (user_id, txn_key, gst_applicable, gst_direction, gst_rate,
                   tds_applicable, tds_direction, tds_section, tds_rate, ai_reasoning, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (user_id, txn_key) DO UPDATE SET
                  gst_applicable=EXCLUDED.gst_applicable,
                  gst_direction=EXCLUDED.gst_direction,
                  gst_rate=EXCLUDED.gst_rate,
                  tds_applicable=EXCLUDED.tds_applicable,
                  tds_direction=EXCLUDED.tds_direction,
                  tds_section=EXCLUDED.tds_section,
                  tds_rate=EXCLUDED.tds_rate,
                  ai_reasoning=EXCLUDED.ai_reasoning,
                  updated_at=NOW()
            """, row)
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning("Could not persist tax results from classify: %s", e)


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
    except NoSheetConnectedError as e:
        return jsonify({"error": str(e), "needs_setup": True}), 200
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


# ---------- Google Drive — Sheet picker ----------
# Note: Drive access is granted at login time (no separate OAuth popup needed).
# /api/gdrive/auth is kept as a fallback re-auth in case the refresh token was revoked.


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
    # Connected = we have tokens from login (Drive scope was requested at sign-in)
    return jsonify({
        "connected":         bool(tokens_str),
        "active_sheet_id":   sheet_id,
        "active_sheet_name": sheet_name,
        "active_tab_name":   tab_name,
        "context_tabs":      context_tabs,
        "oauth_configured":  bool(OAUTH_CLIENT_ID and OAUTH_CLIENT_SECRET),
        "drive_granted_at_login": True,
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


# ============================================================
# Workspace (folder tree) routes
# ============================================================

def _node_to_dict(r):
    # source_type / excel_file_id are newer columns; tolerate older rows that
    # may not have them set (defensive .get-style access via try/except).
    try:
        source_type = r["source_type"] or "gsheet"
    except (KeyError, IndexError):
        source_type = "gsheet"
    try:
        excel_file_id = r["excel_file_id"] or 0
    except (KeyError, IndexError):
        excel_file_id = 0
    return {
        "id":            r["id"],
        "parent_id":     r["parent_id"],
        "name":          r["name"],
        "node_type":     r["node_type"],
        "sheet_id":      r["sheet_id"] or "",
        "sheet_name":    r["sheet_name"] or "",
        "tab_name":      r["tab_name"] or "",
        "context_tabs":  json.loads(r["context_tabs"] or "[]"),
        "source_type":   source_type,
        "excel_file_id": excel_file_id,
        "sort_order":    r["sort_order"],
        "created_at":    str(r["created_at"]),
    }


@app.route("/api/workspace/nodes", methods=["GET"])
@login_required
def workspace_list_nodes():
    uid = current_user_id()
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT * FROM workspace_nodes WHERE user_id=%s ORDER BY parent_id NULLS FIRST, sort_order, name
    """, (uid,))
    rows = c.fetchall()
    conn.close()
    return jsonify({"nodes": [_node_to_dict(r) for r in rows]})


@app.route("/api/workspace/nodes", methods=["POST"])
@login_required
def workspace_create_node():
    uid  = current_user_id()
    data = request.json or {}
    name      = data.get("name", "").strip()
    node_type = data.get("node_type", "folder")
    parent_id = data.get("parent_id")  # None = root
    if not name:
        return jsonify({"error": "name required"}), 400
    if node_type not in ("folder", "sheet"):
        return jsonify({"error": "node_type must be folder or sheet"}), 400
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        INSERT INTO workspace_nodes (user_id, parent_id, name, node_type, sheet_id, sheet_name, tab_name, context_tabs, sort_order)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
            (SELECT COALESCE(MAX(sort_order),0)+1 FROM workspace_nodes WHERE user_id=%s AND parent_id IS NOT DISTINCT FROM %s))
        RETURNING *
    """, (uid, parent_id, name, node_type,
          data.get("sheet_id",""), data.get("sheet_name",""), data.get("tab_name",""),
          json.dumps(data.get("context_tabs",[])),
          uid, parent_id))
    row = c.fetchone()
    conn.commit()
    conn.close()
    return jsonify({"node": _node_to_dict(row)})


@app.route("/api/workspace/nodes/<int:node_id>", methods=["PATCH"])
@login_required
def workspace_update_node(node_id):
    uid  = current_user_id()
    data = request.json or {}
    fields = []
    vals   = []
    for col in ("name", "sheet_id", "sheet_name", "tab_name", "parent_id", "sort_order",
                "source_type", "excel_file_id"):
        if col in data:
            fields.append(f"{col} = %s")
            vals.append(data[col])
    if "context_tabs" in data:
        fields.append("context_tabs = %s")
        vals.append(json.dumps(data["context_tabs"]))
    if not fields:
        return jsonify({"ok": True})
    vals += [node_id, uid]
    conn = get_db()
    c = conn.cursor()
    c.execute(f"UPDATE workspace_nodes SET {', '.join(fields)} WHERE id=%s AND user_id=%s RETURNING *", vals)
    row = c.fetchone()
    conn.commit()
    conn.close()
    if not row:
        return jsonify({"error": "not found"}), 404
    return jsonify({"node": _node_to_dict(row)})


@app.route("/api/workspace/nodes/<int:node_id>", methods=["DELETE"])
@login_required
def workspace_delete_node(node_id):
    uid  = current_user_id()
    conn = get_db()
    c = conn.cursor()
    # Cascade deletes children via FK; also deletes node_classifications via FK
    c.execute("DELETE FROM workspace_nodes WHERE id=%s AND user_id=%s", (node_id, uid))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


# ────────────────────────────────────────────────────────────────────────
# Worksheet listing for an already-attached node — used by the "Edit
# context sheets" flow so the user can pick / change context tabs at any
# time after the initial link.
# ────────────────────────────────────────────────────────────────────────
@app.route("/api/workspace/nodes/<int:node_id>/worksheets", methods=["GET"])
@login_required
def workspace_node_worksheets(node_id):
    uid = current_user_id()
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM workspace_nodes WHERE id=%s AND user_id=%s", (node_id, uid))
    row = c.fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "not found"}), 404

    source_type = (row["source_type"] if "source_type" in row.keys() else None) or "gsheet"

    # Excel-sourced node: read tabs from the parsed Excel file.
    if source_type == "excel":
        excel_id = row["excel_file_id"] if "excel_file_id" in row.keys() else None
        if not excel_id:
            return jsonify({"error": "no excel file attached"}), 400
        erow = _get_excel_file(int(excel_id), uid)
        if not erow:
            return jsonify({"error": "excel file missing"}), 404
        try:
            parsed = json.loads(erow["parsed_json"] or "{}")
        except Exception:
            parsed = {}
        return jsonify({
            "tabs":         list(parsed.keys()),
            "primary_tab":  row["tab_name"] or "",
            "context_tabs": json.loads(row["context_tabs"] or "[]"),
            "sheet_name":   erow["filename"],
            "source_type":  "excel",
        })

    # Default: Google Sheets source.
    sheet_id = row["sheet_id"] or ""
    if not sheet_id:
        return jsonify({"error": "no sheet attached to this node"}), 400
    tokens_str = get_setting("oauth_tokens")
    if not tokens_str:
        return jsonify({"error": "not_connected"}), 401
    try:
        import gspread
        creds  = _refresh_oauth_creds(json.loads(tokens_str))
        client = gspread.authorize(creds)
        sh     = client.open_by_key(sheet_id)
        tabs   = [ws.title for ws in sh.worksheets()]
        primary_tab = row["tab_name"] or ""
        context_tabs = json.loads(row["context_tabs"] or "[]")
        return jsonify({
            "tabs": tabs,
            "primary_tab": primary_tab,
            "context_tabs": context_tabs,
            "sheet_name": row["sheet_name"] or "",
            "source_type": "gsheet",
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ────────────────────────────────────────────────────────────────────────
# Read a single context-tab's contents (rows + headers) so the UI can
# render a viewer letting the user inspect what context the AI is using.
# ────────────────────────────────────────────────────────────────────────
@app.route("/api/workspace/nodes/<int:node_id>/context_tab_data", methods=["GET"])
@login_required
def workspace_node_context_tab_data(node_id):
    uid = current_user_id()
    tab = (request.args.get("tab") or "").strip()
    if not tab:
        return jsonify({"error": "tab required"}), 400

    # Optional row cap — keep responses small for big sheets.
    try:
        limit = int(request.args.get("limit", "500"))
    except Exception:
        limit = 500
    limit = max(1, min(limit, 5000))

    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM workspace_nodes WHERE id=%s AND user_id=%s", (node_id, uid))
    row = c.fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "not found"}), 404

    source_type = (row["source_type"] if "source_type" in row.keys() else None) or "gsheet"

    # Excel source: serve the tab from the uploaded file's parsed_json.
    if source_type == "excel":
        excel_id = row["excel_file_id"] if "excel_file_id" in row.keys() else None
        if not excel_id:
            return jsonify({"error": "no excel file attached"}), 400
        erow = _get_excel_file(int(excel_id), uid)
        if not erow:
            return jsonify({"error": "excel file missing"}), 404
        try:
            parsed = json.loads(erow["parsed_json"] or "{}")
        except Exception:
            parsed = {}
        payload = parsed.get(tab)
        if payload is None:
            for k, v in parsed.items():
                if str(k).strip().lower() == tab.strip().lower():
                    payload = v; tab = k; break
        if payload is None:
            return jsonify({"error": "tab not found"}), 404
        headers = payload.get("headers", []) or []
        data_rows = payload.get("rows", []) or []
        total = payload.get("total_rows", len(data_rows))
        truncated = len(data_rows) > limit
        if truncated:
            data_rows = data_rows[:limit]
        return jsonify({
            "tab":        tab,
            "headers":    headers,
            "rows":       data_rows,
            "total_rows": total,
            "truncated":  truncated or bool(payload.get("truncated", False)),
            "limit":      limit,
        })

    sheet_id = row["sheet_id"] or ""
    if not sheet_id:
        return jsonify({"error": "no sheet attached to this node"}), 400

    tokens_str = get_setting("oauth_tokens")
    if not tokens_str:
        return jsonify({"error": "not_connected"}), 401

    try:
        import gspread
        creds  = _refresh_oauth_creds(json.loads(tokens_str))
        client = gspread.authorize(creds)
        sh     = client.open_by_key(sheet_id)
        ws     = sh.worksheet(tab)
        rows   = ws.get_all_values()
        if not rows:
            return jsonify({"tab": tab, "headers": [], "rows": [],
                            "total_rows": 0, "truncated": False})
        headers = [str(h).strip() for h in rows[0]]
        data_rows = rows[1:]
        total = len(data_rows)
        truncated = total > limit
        if truncated:
            data_rows = data_rows[:limit]
        return jsonify({
            "tab":        tab,
            "headers":    headers,
            "rows":       data_rows,
            "total_rows": total,
            "truncated":  truncated,
            "limit":      limit,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ════════════════════════════════════════════════════════════════════════
# Uploaded context files (xlsx / xls / csv) per workspace sheet node.
# Lets a user attach context that lives outside their primary Google
# Sheet — e.g. a vendor master, a list of clients, a pricing sheet, etc.
# Multiple files per node, multiple tabs per file, each tab individually
# selectable for inclusion in the classifier prompt.
# ════════════════════════════════════════════════════════════════════════

# Tabs / sheets larger than this are truncated when stored, to keep
# parsed_json under a reasonable size. Viewer + classifier both see
# the truncated set.
_CTX_FILE_MAX_ROWS_PER_TAB = 2000


def _find_header_row(raw_df: pd.DataFrame) -> int:
    """Scan up to the first 50 rows for the real header row.

    Handles both bank-statement style headers (Date, Narration, Amount …)
    and GSTR-2B / GST-style headers (GSTIN, Trade/Legal name, Invoice …).
    Also handles merged-cell two-row headers by returning the first of the
    two rows.  Falls back to the first row that has at least 3 non-blank
    distinct values so we never silently treat a metadata row as the header.
    """
    BANK_SIGNALS = {"date", "narration", "description", "particulars",
                    "withdrawal", "deposit", "debit", "credit",
                    "amount", "balance", "chq", "ref"}

    GST_SIGNALS  = {"gstin", "gstin of supplier", "trade/legal name",
                    "trade", "legal name", "invoice", "invoice number",
                    "invoice details", "taxable", "taxable value",
                    "integrated tax", "central tax", "state", "igst",
                    "cgst", "sgst", "place of supply", "reverse charge",
                    "supplier", "hsn", "invoice value", "invoice date"}

    ALL_SIGNALS  = BANK_SIGNALS | GST_SIGNALS

    # Header cells in real statements are short column-name labels (rarely
    # longer than ~30 chars). Metadata rows above the header — things like
    # "Statement From : 01/03/2026 To : 31/03/2026" or
    # "A/C Open Date :23/04/2018" — also happen to contain signal words
    # like "date", so we cap cell length on the substring path to stop
    # those long sentences from being mistaken for the header row.
    SHORT_CELL_MAX_LEN = 30

    for i in range(min(50, len(raw_df))):
        row_vals = [str(v).strip().lower() for v in raw_df.iloc[i].values
                    if str(v).strip() not in ("", "nan", "none")]
        row_set  = set(row_vals)
        # Strong match: 2+ known column-name tokens (exact cell == signal).
        if len(row_set & ALL_SIGNALS) >= 2:
            return i
        # Partial match: a cell *contains* a signal as a substring. Only
        # count short cells so a long metadata sentence that mentions
        # "date" or "amount" in passing isn't treated as a header row.
        # Require ≥3 short hits (vs. 2) so a row with one or two stray
        # short signal words doesn't outrank a later real header row.
        short_signal_hits = sum(
            1 for v in row_vals
            if len(v) <= SHORT_CELL_MAX_LEN
            and any(sig in v for sig in ALL_SIGNALS)
        )
        if short_signal_hits >= 3:
            return i

    # Fallback: first row with ≥3 non-blank distinct values that isn't
    # obviously a single-cell title (company name, report name, etc.)
    for i in range(min(50, len(raw_df))):
        non_blank = [str(v).strip() for v in raw_df.iloc[i].values
                     if str(v).strip() not in ("", "nan", "none")]
        if len(non_blank) >= 3 and len(set(non_blank)) >= 3:
            return i

    return 0


def _normalise_bank_statement(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise bank-statement column names to the canonical schema.

    Maps Withdrawal Amt. / Deposit Amt. (and similar variants from HDFC,
    ICICI, SBI, Axis …) to a single Gross Total column and derives a
    Voucher Type (Payment / Receipt) from whichever side has a value.
    Standard column names (Date, Narration, Voucher No., etc.) are also
    normalised so the rest of the pipeline never sees bank-specific names.
    """
    col_map: dict = {}
    _mapped_targets: set = set()  # prevent duplicate target columns (e.g. Date + Value Dt both → Date)
    for orig in df.columns:
        lc = str(orig).lower().strip()
        if lc in ("date", "txn date", "transaction date"):
            target = "Date"
        elif lc in ("value date", "value dt"):
            # Only use value-date as a Date fallback when the primary date column wasn't found yet
            target = "Date" if "Date" not in _mapped_targets else None
        elif lc in ("narration", "description", "particulars", "remarks",
                    "txn remarks", "transaction remarks"):
            target = "Narration"
        elif any(k in lc for k in ("withdrawal", "debit amount", "debit amt",
                                    "dr amount", "dr amt")):
            target = "_Debit"
        elif any(k in lc for k in ("deposit", "credit amount", "credit amt",
                                    "cr amount", "cr amt")):
            target = "_Credit"
        elif any(k in lc for k in ("chq", "ref.no", "ref no", "cheque no",
                                    "reference", "voucher no")):
            target = "Voucher No."
        elif lc in ("amount", "gross total", "txn amount"):
            target = "Gross Total"
        elif any(k in lc for k in ("voucher type", "txn type", "transaction type",
                                    "cr/dr")):
            target = "Voucher Type"
        else:
            target = None

        if target is not None and target not in _mapped_targets:
            col_map[orig] = target
            _mapped_targets.add(target)

    df = df.rename(columns=col_map)

    # Merge separate debit/credit columns into Gross Total + Voucher Type.
    has_debit  = "_Debit"  in df.columns
    has_credit = "_Credit" in df.columns
    if (has_debit or has_credit) and "Gross Total" not in df.columns:
        def _to_float(s):
            try:
                v = float(str(s).replace(",", "").strip())
                return v if v > 0 else 0.0
            except Exception:
                return 0.0

        debit_vals  = df["_Debit"].apply(_to_float)  if has_debit  else pd.Series([0.0] * len(df))
        credit_vals = df["_Credit"].apply(_to_float) if has_credit else pd.Series([0.0] * len(df))

        if "Voucher Type" not in df.columns:
            df["Voucher Type"] = ["Payment" if d > 0 else ("Receipt" if c > 0 else "")
                                  for d, c in zip(debit_vals, credit_vals)]

        df["Gross Total"] = [str(d) if d > 0 else str(c)
                              for d, c in zip(debit_vals, credit_vals)]

        df = df.drop(columns=[c for c in ("_Debit", "_Credit") if c in df.columns])

    # Ensure all mandatory columns exist (as empty strings).
    for col in ("Date", "Narration", "Gross Total", "Voucher Type", "Voucher No."):
        if col not in df.columns:
            df[col] = ""

    return df


def _parse_context_file_to_tabs(filename: str, raw: bytes):
    """Parse an uploaded file into {tab_name: {"headers": [...], "rows": [[...]]}}.

    Supports .xlsx, .xls (single or multi-tab) and .csv (single tab named
    after the file). Returns the dict plus the total row count across
    all tabs. Raises ValueError on unsupported formats.

    Auto-detects the real header row so bank statements that have metadata
    rows before the column headers are handled correctly, and normalises
    bank-statement column names (Withdrawal Amt., Deposit Amt., etc.) to the
    canonical names used throughout the app.
    """
    import io
    name_lower = (filename or "").lower()
    tabs_out: dict = {}
    total_rows = 0

    def _df_to_payload(df: pd.DataFrame):
        if df is None or df.empty:
            return {"headers": [], "rows": []}, 0
        df = df.fillna("")
        headers = [str(c).strip() for c in df.columns]
        rows = df.astype(str).values.tolist()
        truncated = len(rows) > _CTX_FILE_MAX_ROWS_PER_TAB
        if truncated:
            rows = rows[:_CTX_FILE_MAX_ROWS_PER_TAB]
        return {"headers": headers, "rows": rows, "truncated": truncated,
                "total_rows": int(df.shape[0])}, len(rows)

    def _load_and_fix(raw_df: pd.DataFrame) -> pd.DataFrame:
        """Detect the real header row, re-build the DataFrame from there, then
        normalise column names and strip blank / masked rows.

        For GSTR-2B and other non-bank-statement files we skip the bank-style
        date-filter — those files use columns like 'Invoice Date', not 'Date',
        so filtering on a missing 'Date' column would silently drop everything.
        """
        header_row = _find_header_row(raw_df)

        # GSTR-2B / government sheets often use TWO merged header rows.
        # E.g. row N has 'GSTIN of supplier | Invoice Details | None | None …'
        # and row N+1 has 'None | None | Invoice number | Invoice type | …'
        # Detect this by checking if the row immediately after the header has
        # mostly None/blank in the same positions the header row had values,
        # and values where the header had None — a classic merged-cell pattern.
        use_header_row = header_row
        if header_row + 1 < len(raw_df):
            h1 = [str(v).strip() for v in raw_df.iloc[header_row].values]
            h2 = [str(v).strip() for v in raw_df.iloc[header_row + 1].values]
            h1_blanks = sum(1 for v in h1 if v in ("", "nan", "none", "None"))
            h2_non_blanks = sum(1 for v in h2 if v not in ("", "nan", "none", "None"))
            # If first header has many blanks AND second row fills some of them → merge
            if h1_blanks >= 2 and h2_non_blanks >= 2:
                merged = []
                for a, b in zip(h1, h2):
                    a_blank = a in ("", "nan", "none", "None")
                    b_blank = b in ("", "nan", "none", "None")
                    if not a_blank and not b_blank:
                        # Both rows have a value — spanning group label + sub-column name
                        # (e.g. "Invoice Details" / "Invoice number"). Concatenate so
                        # the column name is unambiguous and _detect_ref_column finds
                        # "Invoice Details Invoice number" reliably.
                        merged.append(f"{a} {b}")
                    elif not a_blank:
                        merged.append(a)
                    elif not b_blank:
                        merged.append(b)
                    else:
                        merged.append("")
                new_headers = merged
                data_rows   = raw_df.iloc[header_row + 2:].values.tolist()
                df = pd.DataFrame(data_rows, columns=new_headers)
                # Drop empty-name columns
                df = df[[c for c in df.columns if str(c).strip() not in ("", "nan")]]
                df = df[df.apply(
                    lambda r: any(str(v).strip() not in ("", "nan", "none") for v in r), axis=1
                )].reset_index(drop=True)
                df = _normalise_bank_statement(df)
                if "Date" in df.columns:
                    date_filtered = df[df["Date"].astype(str).str.strip().str.match(
                        r"^\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4}$"
                        r"|^\d{4}[/\-\.]\d{1,2}[/\-\.]\d{1,2}$", na=False)]
                    if len(date_filtered) > 0:
                        df = date_filtered.reset_index(drop=True)
                return df

        if header_row > 0:
            new_headers = [str(v).strip() for v in raw_df.iloc[header_row].values]
            data_rows   = raw_df.iloc[header_row + 1:].values.tolist()
            df = pd.DataFrame(data_rows, columns=new_headers)
        else:
            df = raw_df.copy()

        # Drop rows that are entirely empty or bank-export padding (e.g. "****")
        df = df[df.apply(
            lambda r: any(str(v).strip() not in ("", "nan", "none") for v in r), axis=1
        )].reset_index(drop=True)

        df = _normalise_bank_statement(df)

        # Only apply the date-format filter for bank statements (files that have
        # a 'Date' column after normalisation). GSTR-2B / GST reference sheets
        # use 'Invoice Date' and similar — never filter those or all rows vanish.
        if "Date" in df.columns:
            date_filtered = df[df["Date"].astype(str).str.strip().str.match(
                r"^\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4}$"
                r"|^\d{4}[/\-\.]\d{1,2}[/\-\.]\d{1,2}$",
                na=False
            )]
            # Only apply if it doesn't wipe the whole frame
            if len(date_filtered) > 0:
                df = date_filtered.reset_index(drop=True)

        return df

    if name_lower.endswith(".csv"):
        try:
            raw_df = pd.read_csv(io.BytesIO(raw), header=None)
        except Exception:
            raw_df = pd.read_csv(io.BytesIO(raw), encoding="latin-1", header=None)
        df = _load_and_fix(raw_df)
        tab_name = os.path.splitext(os.path.basename(filename))[0] or "csv"
        payload, count = _df_to_payload(df)
        tabs_out[tab_name] = payload
        total_rows += count
        return tabs_out, total_rows

    if name_lower.endswith(".xlsx") or name_lower.endswith(".xls") or name_lower.endswith(".xlsm"):
        is_xls = name_lower.endswith(".xls")
        engine = "xlrd" if is_xls else "openpyxl"
        try:
            xls = pd.ExcelFile(io.BytesIO(raw), engine=engine)
        except ImportError as e:
            if is_xls:
                raise ValueError(
                    "This is a legacy .xls file and the xlrd library isn't installed "
                    "on the server. Either ask the admin to add `xlrd>=2.0.1` to "
                    "requirements.txt, or open this file in Excel and re-save it as "
                    "a .xlsx workbook (File → Save As → Excel Workbook)."
                ) from e
            raise ValueError(f"Could not read Excel file: {e}") from e
        except Exception:
            try:
                xls = pd.ExcelFile(io.BytesIO(raw))
            except ImportError as e:
                if is_xls:
                    raise ValueError(
                        "This is a legacy .xls file and the xlrd library isn't "
                        "installed on the server. Re-save the file as .xlsx in Excel "
                        "(File → Save As → Excel Workbook) and upload again."
                    ) from e
                raise ValueError(f"Could not read Excel file: {e}") from e
        for sheet_name in xls.sheet_names:
            try:
                # Read without assuming row 0 is the header — detect it ourselves.
                raw_df = xls.parse(sheet_name, header=None)
                df     = _load_and_fix(raw_df)
            except Exception as e:
                log.warning("Skip tab '%s' in %s: %s", sheet_name, filename, e)
                continue
            payload, count = _df_to_payload(df)
            tabs_out[str(sheet_name)] = payload
            total_rows += count
        return tabs_out, total_rows

    raise ValueError(f"Unsupported file type: {filename}")


def _ctx_file_to_dict(row, include_data=False):
    """Shape a node_context_files row for JSON responses. Skips raw bytes
    and the full parsed payload unless explicitly asked for, since a single
    file's payload can be large (thousands of rows)."""
    parsed = {}
    try:
        parsed = json.loads(row["parsed_json"] or "{}")
    except Exception:
        parsed = {}
    try:
        selected = json.loads(row["selected_tabs"] or "[]")
    except Exception:
        selected = []
    # Light per-tab summary (no rows) — enough for the file-list UI.
    tabs_summary = []
    for tab_name, payload in parsed.items():
        tabs_summary.append({
            "tab_name":   tab_name,
            "row_count":  payload.get("total_rows", len(payload.get("rows", []))),
            "col_count":  len(payload.get("headers", [])),
            "selected":   tab_name in selected,
            "truncated":  bool(payload.get("truncated", False)),
        })
    out = {
        "id":              row["id"],
        "filename":        row["filename"],
        "row_count_total": row["row_count_total"],
        "tabs":            tabs_summary,
        "selected_tabs":   selected,
        "created_at":      str(row["created_at"]),
    }
    if include_data:
        out["parsed"] = parsed
    return out


@app.route("/api/workspace/nodes/<int:node_id>/context_files", methods=["GET"])
@login_required
def workspace_list_context_files(node_id):
    uid = current_user_id()
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT id FROM workspace_nodes WHERE id=%s AND user_id=%s", (node_id, uid))
    if not c.fetchone():
        conn.close()
        return jsonify({"error": "not found"}), 404
    c.execute("""SELECT id, filename, parsed_json, selected_tabs, row_count_total, created_at
                 FROM node_context_files
                 WHERE user_id=%s AND node_id=%s
                 ORDER BY created_at DESC""", (uid, node_id))
    rows = c.fetchall()
    conn.close()
    return jsonify({"files": [_ctx_file_to_dict(r) for r in rows]})


@app.route("/api/workspace/nodes/<int:node_id>/context_files", methods=["POST"])
@login_required
def workspace_upload_context_files(node_id):
    uid = current_user_id()
    files = request.files.getlist("files")
    if not files:
        return jsonify({"error": "No files provided"}), 400

    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT id FROM workspace_nodes WHERE id=%s AND user_id=%s", (node_id, uid))
    if not c.fetchone():
        conn.close()
        return jsonify({"error": "not found"}), 404

    saved = []
    errors = []
    for f in files:
        if not f.filename:
            continue
        raw = f.read()
        try:
            tabs_dict, total = _parse_context_file_to_tabs(f.filename, raw)
        except Exception as e:
            errors.append({"filename": f.filename, "error": str(e)})
            continue
        if not tabs_dict:
            errors.append({"filename": f.filename, "error": "no readable tabs found"})
            continue
        # Default: all tabs selected for use as context.
        selected = list(tabs_dict.keys())
        c.execute("""
            INSERT INTO node_context_files
              (user_id, node_id, filename, file_data, parsed_json, selected_tabs, row_count_total)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id, filename, parsed_json, selected_tabs, row_count_total, created_at
        """, (uid, node_id, f.filename, psycopg2.Binary(raw),
              json.dumps(tabs_dict), json.dumps(selected), total))
        row = c.fetchone()
        saved.append(_ctx_file_to_dict(row))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "uploaded": saved, "errors": errors})


@app.route("/api/workspace/nodes/<int:node_id>/context_files/<int:file_id>",
           methods=["PATCH"])
@login_required
def workspace_update_context_file(node_id, file_id):
    """Update which tabs of an uploaded file are selected as classifier context."""
    uid  = current_user_id()
    data = request.json or {}
    if "selected_tabs" not in data or not isinstance(data["selected_tabs"], list):
        return jsonify({"error": "selected_tabs (array) required"}), 400

    conn = get_db()
    c = conn.cursor()
    c.execute("""SELECT id, parsed_json FROM node_context_files
                 WHERE id=%s AND user_id=%s AND node_id=%s""", (file_id, uid, node_id))
    row = c.fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "not found"}), 404
    try:
        parsed = json.loads(row["parsed_json"] or "{}")
    except Exception:
        parsed = {}
    # Keep only requested tabs that actually exist in the file.
    valid_tabs = [t for t in data["selected_tabs"] if t in parsed]
    c.execute("""UPDATE node_context_files SET selected_tabs=%s
                 WHERE id=%s AND user_id=%s AND node_id=%s
                 RETURNING id, filename, parsed_json, selected_tabs, row_count_total, created_at""",
              (json.dumps(valid_tabs), file_id, uid, node_id))
    updated = c.fetchone()
    conn.commit()
    conn.close()
    return jsonify({"file": _ctx_file_to_dict(updated)})


@app.route("/api/workspace/nodes/<int:node_id>/context_files/<int:file_id>",
           methods=["DELETE"])
@login_required
def workspace_delete_context_file(node_id, file_id):
    uid = current_user_id()
    conn = get_db()
    c = conn.cursor()
    c.execute("""DELETE FROM node_context_files
                 WHERE id=%s AND user_id=%s AND node_id=%s""",
              (file_id, uid, node_id))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/workspace/nodes/<int:node_id>/context_files/<int:file_id>/tab_data",
           methods=["GET"])
@login_required
def workspace_context_file_tab_data(node_id, file_id):
    """Return a single tab's rows from an uploaded context file (for the viewer)."""
    uid = current_user_id()
    tab = (request.args.get("tab") or "").strip()
    try:
        limit = int(request.args.get("limit", "500"))
    except Exception:
        limit = 500
    limit = max(1, min(limit, _CTX_FILE_MAX_ROWS_PER_TAB))

    conn = get_db()
    c = conn.cursor()
    c.execute("""SELECT filename, parsed_json FROM node_context_files
                 WHERE id=%s AND user_id=%s AND node_id=%s""",
              (file_id, uid, node_id))
    row = c.fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "not found"}), 404

    try:
        parsed = json.loads(row["parsed_json"] or "{}")
    except Exception:
        parsed = {}
    if tab and tab not in parsed:
        return jsonify({"error": f"tab '{tab}' not found in file"}), 404
    # Default to the first tab if none specified.
    if not tab:
        tab = next(iter(parsed.keys()), "")
        if not tab:
            return jsonify({"error": "file has no readable tabs"}), 404

    payload = parsed[tab]
    headers = payload.get("headers", [])
    rows = payload.get("rows", [])
    total = payload.get("total_rows", len(rows))
    truncated_at_parse = bool(payload.get("truncated", False))
    truncated_at_view  = len(rows) > limit
    if truncated_at_view:
        rows = rows[:limit]
    return jsonify({
        "filename":  row["filename"],
        "tab":       tab,
        "headers":   headers,
        "rows":      rows,
        "total_rows": total,
        "truncated": truncated_at_parse or truncated_at_view,
        "limit":     limit,
    })


def load_node_uploaded_context(node_id: int, user_id: int):
    """Load all uploaded-file context tabs for a node as [{tab_name, df}] entries
    in the same shape as load_context_sheets() — so classify code can splice them
    into the existing context_sheets list with no other changes."""
    conn = get_db()
    c = conn.cursor()
    c.execute("""SELECT id, filename, parsed_json, selected_tabs
                 FROM node_context_files
                 WHERE user_id=%s AND node_id=%s""", (user_id, node_id))
    rows = c.fetchall()
    conn.close()
    out = []
    for r in rows:
        try:
            parsed = json.loads(r["parsed_json"] or "{}")
            selected = json.loads(r["selected_tabs"] or "[]")
        except Exception:
            continue
        for tab_name in selected:
            payload = parsed.get(tab_name)
            if not payload:
                continue
            headers = payload.get("headers", [])
            data_rows = payload.get("rows", [])
            if not headers and not data_rows:
                continue
            try:
                df = pd.DataFrame(data_rows, columns=headers) if headers \
                     else pd.DataFrame(data_rows)
            except Exception as e:
                log.warning("Could not build DF for %s:%s — %s",
                            r["filename"], tab_name, e)
                continue
            # Label includes filename so the AI prompt distinguishes
            # multiple files that happen to have a 'Sheet1' tab.
            label = f"{r['filename']} › {tab_name}"
            out.append({"tab_name": label, "df": df})
    return out


@app.route("/api/workspace/nodes/<int:node_id>/classifications", methods=["GET"])
@login_required
def workspace_get_classifications(node_id):
    uid  = current_user_id()
    conn = get_db()
    c = conn.cursor()
    # Verify ownership
    c.execute("SELECT id FROM workspace_nodes WHERE id=%s AND user_id=%s", (node_id, uid))
    if not c.fetchone():
        conn.close()
        return jsonify({"error": "not found"}), 404
    c.execute("""
        SELECT txn_key, classification, reference_id, matched_detail, confidence, reasoning, review_decision
        FROM node_classifications WHERE node_id=%s AND user_id=%s
    """, (node_id, uid))
    node_rows = c.fetchall()
    seen_keys = {r["txn_key"] for r in node_rows}
    # Fallback: pull any account-wide classifications for txn_keys this
    # node hasn't classified yet. Lets a re-uploaded statement (same txn
    # values, fresh node row) inherit the prior classifications instead
    # of starting blank — same persistence behavior tax_classifications
    # already provides for GST/TDS.
    c.execute("""
        SELECT txn_key, classification, reference_id, matched_detail, confidence, reasoning, review_decision
        FROM account_classifications WHERE user_id=%s
    """, (uid,))
    # Filter out empty/degenerate keys defensively — they can't match a
    # real transaction's txn_key anyway, and they'd be a sign of stale
    # garbage from before validation was added. Mirrors the POST guard.
    fallback_rows = [
        dict(r) for r in c.fetchall()
        if r["txn_key"]
        and r["txn_key"].replace("|", "").strip()
        and r["txn_key"] not in seen_keys
    ]
    conn.close()
    return jsonify({"classifications": [dict(r) for r in node_rows] + fallback_rows})


@app.route("/api/workspace/nodes/<int:node_id>/classifications", methods=["POST"])
@login_required
def workspace_save_classifications(node_id):
    uid  = current_user_id()
    data = request.json or {}
    records = data.get("records", [])
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT id FROM workspace_nodes WHERE id=%s AND user_id=%s", (node_id, uid))
    if not c.fetchone():
        conn.close()
        return jsonify({"error": "not found"}), 404
    # Defensive: skip records with an empty or meaningless txn_key. The
    # UNIQUE constraint in account_classifications would collapse them all
    # into one row, which would then poison the fallback for unrelated
    # transactions. Empty keys can leak in if a transaction is missing a
    # date/amount, so we drop them silently and report the count.
    # We deliberately do NOT strip/normalize the rest of the key — the
    # frontend builds it as `${date}|${narration[:80]}|${gross_total}` with
    # no trimming, so any normalization here would cause silent mismatches
    # on load. We only reject the degenerate cases (empty, or all three
    # components empty → just pipes).
    skipped = 0
    for rec in records:
        key = rec.get("txn_key") or ""
        # Reject empty, or the degenerate "||" case where every component
        # of the frontend's `date|narration|gross_total` was empty.
        if not key or not key.replace("|", "").strip():
            skipped += 1
            continue
        c.execute("""
            INSERT INTO node_classifications
              (user_id, node_id, txn_key, classification, reference_id, matched_detail, confidence, reasoning, review_decision, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s, NOW())
            ON CONFLICT (user_id, node_id, txn_key) DO UPDATE SET
              classification  = EXCLUDED.classification,
              reference_id    = EXCLUDED.reference_id,
              matched_detail  = EXCLUDED.matched_detail,
              confidence      = EXCLUDED.confidence,
              reasoning       = EXCLUDED.reasoning,
              review_decision = EXCLUDED.review_decision,
              updated_at      = NOW()
        """, (uid, node_id,
              key,
              rec.get("classification",""),
              rec.get("reference_id",""),
              rec.get("matched_detail",""),
              rec.get("confidence","high"),
              rec.get("reasoning",""),
              rec.get("review_decision","")))
        # Mirror to the account-wide cache so this classification persists
        # across node deletes, sheet re-uploads, and node recreations. Same
        # txn_key shape, same fields — node_classifications stays the
        # authoritative per-sheet record, this is a recovery cache.
        c.execute("""
            INSERT INTO account_classifications
              (user_id, txn_key, classification, reference_id, matched_detail, confidence, reasoning, review_decision, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s, NOW())
            ON CONFLICT (user_id, txn_key) DO UPDATE SET
              classification  = EXCLUDED.classification,
              reference_id    = EXCLUDED.reference_id,
              matched_detail  = EXCLUDED.matched_detail,
              confidence      = EXCLUDED.confidence,
              reasoning       = EXCLUDED.reasoning,
              review_decision = EXCLUDED.review_decision,
              updated_at      = NOW()
        """, (uid,
              key,
              rec.get("classification",""),
              rec.get("reference_id",""),
              rec.get("matched_detail",""),
              rec.get("confidence","high"),
              rec.get("reasoning",""),
              rec.get("review_decision","")))
    conn.commit()
    conn.close()
    saved = len(records) - skipped
    resp = {"ok": True, "saved": saved}
    if skipped:
        resp["skipped_empty_keys"] = skipped
    return jsonify(resp)


# ════════════════════════════════════════════════════════════════════════
# Excel-source helpers for workspace nodes.
# A workspace sheet node can have its transactions come from either a
# Google Sheet (source_type='gsheet') or an uploaded Excel file
# (source_type='excel'). The helpers below mirror the Google-Sheets helpers
# (load_from_gsheets_oauth / load_sheet_as_context_oauth) so the rest of
# the classification pipeline doesn't need to care about the source.
# ════════════════════════════════════════════════════════════════════════

def _get_excel_file(excel_file_id: int, user_id: int):
    """Fetch an uploaded excel file row for this user, or None."""
    conn = get_db()
    c = conn.cursor()
    c.execute("""SELECT id, filename, parsed_json
                 FROM workspace_excel_files
                 WHERE id=%s AND user_id=%s""", (excel_file_id, user_id))
    row = c.fetchone()
    conn.close()
    return row


def _excel_tab_payload_to_df(payload: dict) -> pd.DataFrame:
    """Convert a parsed_json tab payload {headers, rows} to a pandas DataFrame."""
    headers = payload.get("headers", []) or []
    data_rows = payload.get("rows", []) or []
    if headers:
        # Pad/trim rows to header length so DataFrame construction never fails.
        n = len(headers)
        norm_rows = []
        for r in data_rows:
            if len(r) < n:
                r = list(r) + [""] * (n - len(r))
            elif len(r) > n:
                r = list(r)[:n]
            norm_rows.append(r)
        return pd.DataFrame(norm_rows, columns=headers)
    return pd.DataFrame(data_rows)


def load_excel_source_tab(excel_file_id: int, tab_name: str, user_id: int) -> pd.DataFrame:
    """Load a single tab from an uploaded Excel transaction file as a DataFrame.
    Raises ValueError if the file or tab is missing."""
    row = _get_excel_file(excel_file_id, user_id)
    if not row:
        raise ValueError("Excel file not found")
    try:
        parsed = json.loads(row["parsed_json"] or "{}")
    except Exception:
        parsed = {}
    payload = parsed.get(tab_name)
    if payload is None:
        # Try a case-insensitive match before giving up — tab names from the
        # picker should be exact, but better to be forgiving.
        for k, v in parsed.items():
            if str(k).strip().lower() == str(tab_name).strip().lower():
                payload = v
                break
    if payload is None:
        raise ValueError(f"Tab '{tab_name}' not found in Excel file")
    return _excel_tab_payload_to_df(payload)


def load_excel_context_tabs(excel_file_id: int, tab_names: list, user_id: int) -> list:
    """Load context tabs from an uploaded Excel file as [{tab_name, df}, ...].
    Mirrors load_context_sheets() output so the rest of the pipeline is uniform."""
    out = []
    if not tab_names:
        return out
    row = _get_excel_file(excel_file_id, user_id)
    if not row:
        return out
    try:
        parsed = json.loads(row["parsed_json"] or "{}")
    except Exception:
        return out
    # Case-insensitive lookup map.
    lower_map = {str(k).strip().lower(): k for k in parsed.keys()}
    for tab in tab_names:
        key = lower_map.get(str(tab).strip().lower())
        if not key:
            log.warning("Excel context tab '%s' not found in file %s", tab, row["filename"])
            continue
        try:
            df = _excel_tab_payload_to_df(parsed[key])
            out.append({"tab_name": tab, "df": df})
        except Exception as e:
            log.warning("Failed to load excel context tab '%s': %s", tab, e)
    return out


@app.route("/api/workspace/nodes/<int:node_id>/upload_excel_source", methods=["POST"])
@login_required
def workspace_upload_excel_source(node_id):
    """Upload an Excel/CSV file as the transaction-statement source for a node.
    Parses all tabs and returns the tab list so the caller can show a picker."""
    uid = current_user_id()

    # Verify the node belongs to this user and is a sheet-type node.
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT id, node_type FROM workspace_nodes WHERE id=%s AND user_id=%s",
              (node_id, uid))
    nrow = c.fetchone()
    if not nrow:
        conn.close()
        return jsonify({"error": "not found"}), 404
    if nrow["node_type"] != "sheet":
        conn.close()
        return jsonify({"error": "Can only attach Excel to a sheet-type node"}), 400

    f = request.files.get("file")
    if not f or not f.filename:
        conn.close()
        return jsonify({"error": "No file provided"}), 400

    raw = f.read()
    try:
        # Reuse the existing context-file parser — same xlsx/xls/csv support.
        tabs_dict, total_rows = _parse_context_file_to_tabs(f.filename, raw)
    except Exception as e:
        conn.close()
        return jsonify({"error": f"Could not parse file: {e}"}), 400

    if not tabs_dict:
        conn.close()
        return jsonify({"error": "No readable tabs found in this file"}), 400

    # Save (or replace any previous upload for this node so we don't leak rows).
    # Replace = delete the old excel_file_id row if there is one, then insert.
    c.execute("""SELECT excel_file_id FROM workspace_nodes
                 WHERE id=%s AND user_id=%s""", (node_id, uid))
    prev = c.fetchone()
    prev_id = (prev["excel_file_id"] if prev and "excel_file_id" in prev.keys() else None)
    if prev_id:
        c.execute("DELETE FROM workspace_excel_files WHERE id=%s AND user_id=%s",
                  (prev_id, uid))

    c.execute("""
        INSERT INTO workspace_excel_files (user_id, filename, file_data, parsed_json)
        VALUES (%s, %s, %s, %s)
        RETURNING id
    """, (uid, f.filename, psycopg2.Binary(raw), json.dumps(tabs_dict)))
    new_id = c.fetchone()["id"]
    conn.commit()
    conn.close()

    # Per-tab summary for the picker UI.
    tabs = []
    for name, payload in tabs_dict.items():
        tabs.append({
            "name":      name,
            "row_count": payload.get("total_rows", len(payload.get("rows", []))),
            "col_count": len(payload.get("headers", [])),
            "truncated": bool(payload.get("truncated", False)),
        })
    return jsonify({
        "ok":            True,
        "excel_file_id": new_id,
        "filename":      f.filename,
        "tabs":          tabs,
        "total_rows":    total_rows,
    })


@app.route("/api/workspace/nodes/<int:node_id>/excel_tabs", methods=["GET"])
@login_required
def workspace_excel_tabs(node_id):
    """List the tabs available in the Excel file attached to a node.
    Used by the context-tabs editor when source_type='excel'."""
    uid = current_user_id()
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM workspace_nodes WHERE id=%s AND user_id=%s",
              (node_id, uid))
    node = c.fetchone()
    conn.close()
    if not node:
        return jsonify({"error": "not found"}), 404
    excel_id = node["excel_file_id"] if "excel_file_id" in node.keys() else None
    if not excel_id:
        return jsonify({"error": "No excel file attached"}), 400
    row = _get_excel_file(int(excel_id), uid)
    if not row:
        return jsonify({"error": "Excel file missing"}), 404
    try:
        parsed = json.loads(row["parsed_json"] or "{}")
    except Exception:
        parsed = {}
    tabs = []
    for name, payload in parsed.items():
        tabs.append({
            "name":      name,
            "row_count": payload.get("total_rows", len(payload.get("rows", []))),
            "col_count": len(payload.get("headers", [])),
        })
    try:
        ctx_tabs = json.loads(node["context_tabs"] or "[]")
    except Exception:
        ctx_tabs = []
    return jsonify({
        "tabs":         [t["name"] for t in tabs],
        "tab_details":  tabs,
        "filename":     row["filename"],
        "primary_tab":  node["tab_name"] or "",
        "context_tabs": ctx_tabs,
    })


@app.route("/api/workspace/nodes/<int:node_id>/excel_tab_data", methods=["GET"])
@login_required
def workspace_excel_tab_data(node_id):
    """Return rows of one tab from the uploaded Excel for viewing in the UI."""
    uid = current_user_id()
    tab = request.args.get("tab", "").strip()
    limit = int(request.args.get("limit", "500"))
    limit = max(1, min(limit, 5000))
    if not tab:
        return jsonify({"error": "tab required"}), 400

    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM workspace_nodes WHERE id=%s AND user_id=%s",
              (node_id, uid))
    node = c.fetchone()
    conn.close()
    if not node:
        return jsonify({"error": "not found"}), 404
    excel_id = node["excel_file_id"] if "excel_file_id" in node.keys() else None
    if not excel_id:
        return jsonify({"error": "No excel file attached"}), 400
    row = _get_excel_file(int(excel_id), uid)
    if not row:
        return jsonify({"error": "Excel file missing"}), 404
    try:
        parsed = json.loads(row["parsed_json"] or "{}")
    except Exception:
        parsed = {}
    payload = parsed.get(tab)
    if payload is None:
        for k, v in parsed.items():
            if str(k).strip().lower() == tab.strip().lower():
                payload = v; tab = k; break
    if payload is None:
        return jsonify({"error": "tab not found"}), 404
    headers = payload.get("headers", []) or []
    rows = payload.get("rows", []) or []
    total = payload.get("total_rows", len(rows))
    truncated = len(rows) > limit
    if truncated:
        rows = rows[:limit]
    return jsonify({
        "tab":        tab,
        "headers":    headers,
        "rows":       rows,
        "total_rows": total,
        "truncated":  truncated or bool(payload.get("truncated", False)),
        "limit":      limit,
    })


@app.route("/api/workspace/nodes/<int:node_id>/load_transactions", methods=["GET"])
@login_required
def workspace_load_node_transactions(node_id):
    """Load transactions for a specific workspace sheet node.
    Dispatches to either Google Sheets or the uploaded Excel file based on
    the node's source_type."""
    uid  = current_user_id()
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM workspace_nodes WHERE id=%s AND user_id=%s", (node_id, uid))
    node = c.fetchone()
    conn.close()
    if not node:
        return jsonify({"error": "not found"}), 404

    source_type = (node["source_type"] if "source_type" in node.keys() else None) or "gsheet"

    if node["node_type"] != "sheet" or not node["tab_name"]:
        return jsonify({"error": "Node has no sheet attached"}), 400

    try:
        source_label = ""  # human-readable bank/source name shown in the Bank column
        if source_type == "excel":
            excel_id = node["excel_file_id"] if "excel_file_id" in node.keys() else None
            if not excel_id:
                return jsonify({"error": "No Excel file attached to this node"}), 400
            df = load_excel_source_tab(int(excel_id), node["tab_name"], uid)
            # Try to derive bank name: check filename for known bank keywords first,
            # then fall back to the workspace node name.
            erow = _get_excel_file(int(excel_id), uid)
            if erow:
                fname = (erow["filename"] or "").upper()
                _KNOWN_BANKS = [
                    ("HDFC", "HDFC"), ("ICICI", "ICICI"), ("SBI", "SBI"),
                    ("AXIS", "Axis"), ("KOTAK", "Kotak"), ("YES", "Yes Bank"),
                    ("INDUSIND", "IndusInd"), ("PNB", "PNB"), ("BOB", "BoB"),
                    ("CANARA", "Canara"), ("UNION", "Union"), ("IDFC", "IDFC"),
                    ("FEDERAL", "Federal"), ("RBL", "RBL"), ("CITI", "Citi"),
                    ("STANDARD", "StanChart"), ("HSBC", "HSBC"),
                ]
                matched = next((label for kw, label in _KNOWN_BANKS if kw in fname), None)
                source_label = matched or node["name"]
        else:
            if not node["sheet_id"]:
                return jsonify({"error": "Node has no sheet attached"}), 400
            tokens_str = get_setting("oauth_tokens")
            if not tokens_str:
                return jsonify({"error": "not_connected"}), 401
            df = load_from_gsheets_oauth(node["sheet_id"], node["tab_name"], json.loads(tokens_str))
            source_label = node["sheet_name"] or ""

        txns = [{"date": str(row.get("Date","")).strip(),
                 "particulars": str(row.get("Particulars","")).strip(),
                 "voucher_type": str(row.get("Voucher Type","")).strip(),
                 "voucher_no": str(row.get("Voucher No.","")).strip(),
                 "narration": str(row.get("Narration","")).strip(),
                 "gross_total": str(row.get("Gross Total","")).strip(),
                 "source": source_label}
                for _, row in df.iterrows()]
        duplicates = detect_duplicates(df)
        return jsonify({"transactions": txns, "total": len(txns), "duplicates": duplicates})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/workspace/nodes/<int:node_id>/classify_stream", methods=["POST"])
@login_required
def workspace_classify_stream(node_id):
    """Streaming classification scoped to a workspace node."""
    if not _classify_lock.acquire(blocking=False):
        def _busy():
            yield "data: " + json.dumps({"error": "A classification is already running.", "done": True}) + "\n\n"
        return app.response_class(_busy(), mimetype="text/event-stream")

    uid  = current_user_id()
    data = request.json or {}
    indices = data.get("indices", None)

    # Load node details
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM workspace_nodes WHERE id=%s AND user_id=%s", (node_id, uid))
    node = c.fetchone()
    conn.close()

    if not node:
        _classify_lock.release()
        def _err():
            yield "data: " + json.dumps({"error": "Node not found", "done": True}) + "\n\n"
        return app.response_class(_err(), mimetype="text/event-stream")

    snap_sheet_id   = node["sheet_id"] or ""
    snap_tab_name   = node["tab_name"] or "query_sk"
    snap_tokens_str = get_setting("oauth_tokens", user_id=uid)
    snap_ctx_tabs   = node["context_tabs"] or "[]"
    snap_source     = (node["source_type"] if "source_type" in node.keys() else None) or "gsheet"
    snap_excel_id   = node["excel_file_id"] if "excel_file_id" in node.keys() else None

    def _generate():
        try:
            # Load transactions from whichever source the node is bound to.
            if snap_source == "excel":
                if not snap_excel_id:
                    raise ValueError("No Excel file attached to this node")
                transactions = load_excel_source_tab(int(snap_excel_id), snap_tab_name, uid)
            else:
                if not snap_sheet_id:
                    raise ValueError("No Google Sheet attached to this node")
                if not snap_tokens_str:
                    raise ValueError("Google Drive not connected")
                transactions = load_from_gsheets_oauth(
                    snap_sheet_id, snap_tab_name, json.loads(snap_tokens_str))

            context_sheets = []
            try:
                ctx_tabs = json.loads(snap_ctx_tabs) if isinstance(snap_ctx_tabs, str) else snap_ctx_tabs
                if snap_source == "excel" and snap_excel_id:
                    # Context tabs live inside the same uploaded Excel file.
                    context_sheets.extend(
                        load_excel_context_tabs(int(snap_excel_id), ctx_tabs, uid))
                elif snap_tokens_str and snap_sheet_id:
                    tokens = json.loads(snap_tokens_str)
                    for tab in ctx_tabs:
                        try:
                            df = load_sheet_as_context_oauth(snap_sheet_id, tab, tokens)
                            context_sheets.append({"tab_name": tab, "df": df})
                        except Exception as e:
                            log.warning("Context tab '%s' failed: %s", tab, e)
            except Exception as e:
                log.warning("Context load failed: %s", e)

            # Also include any user-uploaded context files attached to this node.
            try:
                context_sheets.extend(load_node_uploaded_context(node_id, uid))
            except Exception as e:
                log.warning("Uploaded context-file load failed for node %s: %s", node_id, e)

            if indices is not None:
                req_indices = indices
                subset      = transactions.iloc[req_indices].copy()
            else:
                subset      = transactions.copy()
                req_indices = list(range(len(transactions)))

            total     = len(req_indices)
            completed = 0
            all_output = []  # accumulate for server-side auto-persist

            for batch_results in _classify_transactions_stream(subset, context_sheets, uid):
                output = []
                for res in batch_results:
                    if res and "idx" in res:
                        original_idx = req_indices[res["idx"]]
                        output.append({"original_index": original_idx, **res})
                completed += len(output)
                all_output.extend(output)
                yield f"data: {json.dumps({'results': output, 'done': False, 'completed': completed, 'total': total})}\n\n"

            # ── Auto-persist classifications to DB (server-side, belt-and-suspenders) ──
            # The frontend also calls wsSaveNodeClassifications() after the stream,
            # but saving server-side too means classifications survive even if the
            # browser tab closes before that JS call fires.
            if all_output:
                try:
                    # Build a txn_key->row lookup so we can key by the same formula
                    # the frontend uses: date[:10]|narration[:80]|gross_total
                    txn_rows = list(transactions.iterrows())
                    conn2 = get_db()
                    c2 = conn2.cursor()
                    for item in all_output:
                        oi = item.get("original_index", 0)
                        if oi >= len(txn_rows):
                            continue
                        _, row = txn_rows[oi]
                        txn_key = (f"{str(row.get('Date',''))[:10]}"
                                   f"|{str(row.get('Narration',''))[:80]}"
                                   f"|{str(row.get('Gross Total',''))}")
                        if not txn_key.replace("|", "").strip():
                            continue
                        cls  = item.get("classification", "")
                        ref  = item.get("reference_id", "")
                        det  = item.get("matched_detail", "")
                        conf = item.get("confidence", "high")
                        rsn  = item.get("reasoning", "")
                        # node_classifications (per-node authoritative)
                        c2.execute("""
                            INSERT INTO node_classifications
                              (user_id, node_id, txn_key, classification, reference_id,
                               matched_detail, confidence, reasoning, review_decision, updated_at)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'',NOW())
                            ON CONFLICT (user_id, node_id, txn_key) DO UPDATE SET
                              classification  = EXCLUDED.classification,
                              reference_id    = EXCLUDED.reference_id,
                              matched_detail  = EXCLUDED.matched_detail,
                              confidence      = EXCLUDED.confidence,
                              reasoning       = EXCLUDED.reasoning,
                              updated_at      = NOW()
                        """, (uid, node_id, txn_key, cls, ref, det, conf, rsn))
                        # account_classifications (cross-node fallback cache)
                        c2.execute("""
                            INSERT INTO account_classifications
                              (user_id, txn_key, classification, reference_id,
                               matched_detail, confidence, reasoning, review_decision, updated_at)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,'',NOW())
                            ON CONFLICT (user_id, txn_key) DO UPDATE SET
                              classification  = EXCLUDED.classification,
                              reference_id    = EXCLUDED.reference_id,
                              matched_detail  = EXCLUDED.matched_detail,
                              confidence      = EXCLUDED.confidence,
                              reasoning       = EXCLUDED.reasoning,
                              updated_at      = NOW()
                        """, (uid, txn_key, cls, ref, det, conf, rsn))
                    conn2.commit()
                    conn2.close()
                    log.info("Auto-persisted %d classifications for node %s", len(all_output), node_id)
                except Exception as persist_err:
                    log.warning("Auto-persist classifications failed (non-fatal): %s", persist_err)

            # ── Auto-run ITC matching after classification completes ──────────
            itc_matches = {}
            itc_reflected = 0
            try:
                itc_matches = match_gst_itc(transactions, context_sheets)
                conn = get_db()
                c = conn.cursor()
                for txn_key, m in itc_matches.items():
                    c.execute("""
                        INSERT INTO gst_itc_matches
                          (user_id, node_id, txn_key, itc_status, confidence, signals,
                           invoice_no, vendor_name, invoice_date, invoice_value,
                           taxable_value, igst, cgst, sgst, source_sheet, updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                        ON CONFLICT (user_id, node_id, txn_key) DO UPDATE SET
                          itc_status    = EXCLUDED.itc_status,
                          confidence    = EXCLUDED.confidence,
                          signals       = EXCLUDED.signals,
                          invoice_no    = EXCLUDED.invoice_no,
                          vendor_name   = EXCLUDED.vendor_name,
                          invoice_date  = EXCLUDED.invoice_date,
                          invoice_value = EXCLUDED.invoice_value,
                          taxable_value = EXCLUDED.taxable_value,
                          igst          = EXCLUDED.igst,
                          cgst          = EXCLUDED.cgst,
                          sgst          = EXCLUDED.sgst,
                          source_sheet  = EXCLUDED.source_sheet,
                          updated_at    = NOW()
                    """, (uid, node_id, txn_key,
                          m['itc_status'], m['confidence'],
                          json.dumps(m['signals']),
                          m['invoice_no'], m['vendor_name'], m['invoice_date'],
                          m['invoice_value'], m['taxable_value'],
                          m['igst'], m['cgst'], m['sgst'], m['source_sheet']))
                conn.commit()
                conn.close()
                itc_reflected = sum(1 for m in itc_matches.values() if m['itc_status'] == 'reflected')
                log.info("Auto ITC match: %d reflected out of %d for node %s",
                         itc_reflected, len(itc_matches), node_id)
            except Exception as itc_err:
                log.warning("Auto ITC match failed (non-fatal): %s", itc_err)

            yield f"data: {json.dumps({'results': [], 'done': True, 'completed': completed, 'total': total, 'itc_matches': itc_matches, 'itc_reflected': itc_reflected})}\n\n"

        except Exception as e:
            log.exception("workspace_classify_stream error")
            yield f"data: {json.dumps({'error': str(e), 'done': True})}\n\n"
        finally:
            _classify_lock.release()

    return app.response_class(
        _generate(), mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ============================================================
# Excel Upload — multi-sheet combine
# ============================================================

@app.route("/api/upload/expense_sheet", methods=["POST"])
@login_required
def upload_expense_sheet():
    """Upload one or more Excel files to be combined into the user's transaction pool."""
    uid   = current_user_id()
    files = request.files.getlist("files")
    if not files:
        return jsonify({"error": "No files provided"}), 400

    saved = []
    for f in files:
        if not f.filename:
            continue
        raw = f.read()
        try:
            import io as _io
            _is_xls = (f.filename or "").lower().endswith(".xls")
            _engine  = "xlrd" if _is_xls else "openpyxl"
            _raw_df  = pd.read_excel(_io.BytesIO(raw), engine=_engine, header=None)
            _hrow    = _find_header_row(_raw_df)
            if _hrow > 0:
                _hdrs    = [str(v).strip() for v in _raw_df.iloc[_hrow].values]
                _data    = _raw_df.iloc[_hrow + 1:].values.tolist()
                _fixed   = pd.DataFrame(_data, columns=_hdrs)
            else:
                _fixed   = _raw_df.copy()
            # count only non-blank data rows (skip divider/padding rows)
            _valid   = _fixed[_fixed.apply(
                lambda r: any(str(v).strip() not in ("", "nan") for v in r), axis=1)]
            row_count = max(0, len(_valid))
        except Exception:
            row_count = 0
        label = request.form.get("label_" + f.filename, f.filename)
        conn = get_db()
        c = conn.cursor()
        c.execute("""
            INSERT INTO uploaded_sheets (user_id, filename, label, file_data, row_count)
            VALUES (%s, %s, %s, %s, %s) RETURNING id, filename, label, row_count, created_at
        """, (uid, f.filename, label, psycopg2.Binary(raw), row_count))
        row = c.fetchone()
        conn.commit()
        conn.close()
        saved.append({"id": row["id"], "filename": row["filename"],
                      "label": row["label"], "row_count": row["row_count"]})
    return jsonify({"ok": True, "uploaded": saved})


@app.route("/api/upload/expense_sheets", methods=["GET"])
@login_required
def list_expense_sheets():
    uid  = current_user_id()
    conn = get_db()
    c = conn.cursor()
    c.execute("""SELECT id, filename, label, row_count, created_at
                 FROM uploaded_sheets WHERE user_id=%s ORDER BY created_at DESC""", (uid,))
    rows = c.fetchall()
    conn.close()
    return jsonify({"sheets": [{"id": r["id"], "filename": r["filename"],
                                "label": r["label"], "row_count": r["row_count"],
                                "created_at": str(r["created_at"])} for r in rows]})


@app.route("/api/upload/expense_sheets/<int:sheet_id>", methods=["DELETE"])
@login_required
def delete_expense_sheet(sheet_id):
    uid  = current_user_id()
    conn = get_db()
    c = conn.cursor()
    c.execute("DELETE FROM uploaded_sheets WHERE id=%s AND user_id=%s", (sheet_id, uid))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/upload/expense_sheets/combined", methods=["GET"])
@login_required
def get_combined_transactions():
    """Merge all uploaded sheets into one transaction list."""
    uid  = current_user_id()
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT filename, label, file_data FROM uploaded_sheets WHERE user_id=%s ORDER BY created_at", (uid,))
    rows = c.fetchall()
    conn.close()
    if not rows:
        return jsonify({"transactions": [], "total": 0, "sources": []})

    frames = []
    sources = []
    for r in rows:
        try:
            import io
            raw      = bytes(r["file_data"])
            fname    = r["filename"] or ""
            is_xls   = fname.lower().endswith(".xls")
            engine   = "xlrd" if is_xls else "openpyxl"
            raw_df   = pd.read_excel(io.BytesIO(raw), engine=engine, header=None)
            # Auto-detect real header row (skips bank metadata rows) and
            # normalise column names (Withdrawal Amt. → _Debit, etc.)
            df = _load_and_fix(raw_df)
            # _load_and_fix already calls _normalise_bank_statement which
            # produces Date, Narration, Voucher Type, Gross Total, Voucher No.
            # Ensure all mandatory columns are present.
            for req in ("Date", "Narration", "Gross Total"):
                if req not in df.columns:
                    df[req] = ""
            if "Voucher Type" not in df.columns:
                df["Voucher Type"] = ""
            if "Voucher No." not in df.columns:
                df["Voucher No."] = ""
            # Drop remaining blank / placeholder date rows.
            df = df[df["Date"].astype(str).str.strip().ne("")]
            df = df[df["Date"].astype(str).str.lower().ne("nan")]
            df["_source"] = r["label"] or r["filename"]
            frames.append(df)
            sources.append(r["label"] or r["filename"])
        except Exception as e:
            log.warning("Failed to parse sheet %s: %s", r["filename"], e)

    if not frames:
        return jsonify({"transactions": [], "total": 0, "sources": sources})

    combined = pd.concat(frames, ignore_index=True)
    txns = []
    for _, row in combined.iterrows():
        txns.append({
            "date":         str(row.get("Date", "")).strip(),
            "narration":    str(row.get("Narration", "")).strip(),
            "voucher_type": str(row.get("Voucher Type", "")).strip(),
            "voucher_no":   str(row.get("Voucher No.", "")).strip(),
            "gross_total":  str(row.get("Gross Total", "")).strip(),
            "source":       str(row.get("_source", "")).strip(),
        })
    return jsonify({"transactions": txns, "total": len(txns), "sources": list(set(sources))})


# ============================================================
# GST / TDS Input Sheet Upload — attached PER expense sheet
# ============================================================
#
# Each uploaded expense sheet (uploaded_sheets row) can have at most one GST
# attachment and one TDS attachment. The frontend renders the attachments
# under each sheet chip on the Upload tab.

def _require_owned_sheet(uid: int, sheet_id):
    """Return the uploaded_sheets row if owned by uid, else None."""
    if sheet_id in (None, "", "null"):
        return None
    try:
        sheet_id = int(sheet_id)
    except (TypeError, ValueError):
        return None
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT id, filename, label FROM uploaded_sheets WHERE id=%s AND user_id=%s",
              (sheet_id, uid))
    row = c.fetchone()
    conn.close()
    return row


# ── GST -----------------------------------------------------------------

@app.route("/api/upload/gst_sheet", methods=["POST"])
@login_required
def upload_gst_sheet():
    """
    Upload a GSTR-2B / GST input credit sheet attached to a specific
    expense sheet (uploaded_sheets.id, passed as form field `sheet_id`).

    Multiple GST files may be attached to the same expense sheet — each
    upload appends a new row. Use DELETE /api/upload/gst_sheet/file/<gst_id>
    to remove a specific one. Files are stored permanently as BYTEA in
    Postgres.
    """
    uid       = current_user_id()
    f         = request.files.get("file")
    sheet_id  = request.form.get("sheet_id")
    if not f:
        return jsonify({"error": "No file provided"}), 400
    if not sheet_id or not _require_owned_sheet(uid, sheet_id):
        return jsonify({"error": "Missing or invalid sheet_id — attach GST to a specific expense sheet"}), 400

    raw  = f.read()
    conn = get_db()
    c    = conn.cursor()
    # Append (do NOT replace) — multiple GST files per expense sheet supported.
    c.execute("""
        INSERT INTO gst_input_sheets (user_id, sheet_id, filename, file_data, row_count)
        VALUES (%s, %s, %s, %s, %s) RETURNING id, filename
    """, (uid, int(sheet_id), f.filename, psycopg2.Binary(raw), 0))
    row = c.fetchone()
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "id": row["id"], "filename": row["filename"],
                    "sheet_id": int(sheet_id)})


@app.route("/api/upload/gst_sheet", methods=["GET"])
@login_required
def get_gst_sheet_data():
    """
    Return parsed GST input sheet rows.

    Query params:
      - gst_id   (optional): return that specific GST file's rows.
      - sheet_id (optional): if given AND gst_id omitted, return the
        UNION of all GST files attached to that expense sheet
        (since a sheet may have multiple GST files).
      - If neither is given: return rows from the most-recently-uploaded
        GST file for the user (legacy fallback).

    Response:
      { "rows": [...], "filename": "<single name or 'N files'>",
        "files": [{"id": int, "filename": str}, ...] }
    """
    uid      = current_user_id()
    gst_id   = request.args.get("gst_id")
    sheet_id = request.args.get("sheet_id")
    conn = get_db()
    c    = conn.cursor()
    if gst_id:
        c.execute("""SELECT id, filename, file_data FROM gst_input_sheets
                     WHERE user_id=%s AND id=%s""", (uid, gst_id))
    elif sheet_id:
        c.execute("""SELECT id, filename, file_data FROM gst_input_sheets
                     WHERE user_id=%s AND sheet_id=%s
                     ORDER BY created_at ASC""", (uid, sheet_id))
    else:
        c.execute("""SELECT id, filename, file_data FROM gst_input_sheets
                     WHERE user_id=%s ORDER BY created_at DESC LIMIT 1""", (uid,))
    rows = c.fetchall()
    conn.close()
    if not rows:
        return jsonify({"rows": [], "filename": None, "files": []})

    import io
    all_records = []
    files_meta  = []
    for r in rows:
        files_meta.append({"id": r["id"], "filename": r["filename"]})
        try:
            raw = bytes(r["file_data"])
            df  = pd.read_excel(io.BytesIO(raw), engine="openpyxl")
            df.columns = [str(c).strip() for c in df.columns]
            recs = df.fillna("").astype(str).to_dict("records")
            # Tag each record with its source filename so the UI / consumers
            # can tell rows from different GST files apart.
            for rec in recs:
                rec.setdefault("_source_file", r["filename"])
            all_records.extend(recs)
        except Exception as e:
            log.warning("Failed to parse GST attachment %s: %s", r["filename"], e)
            continue

    label = files_meta[0]["filename"] if len(files_meta) == 1 \
            else f"{len(files_meta)} files"
    return jsonify({
        "rows":     all_records[:2000],
        "filename": label,
        "files":    files_meta,
    })


@app.route("/api/upload/gst_sheet/<int:sheet_id>", methods=["DELETE"])
@login_required
def delete_gst_sheet(sheet_id):
    """Remove ALL GST attachments from a specific expense sheet (bulk)."""
    uid = current_user_id()
    conn = get_db()
    c    = conn.cursor()
    c.execute("DELETE FROM gst_input_sheets WHERE user_id=%s AND sheet_id=%s",
              (uid, sheet_id))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.route("/api/upload/gst_sheet/file/<int:gst_id>", methods=["DELETE"])
@login_required
def delete_gst_sheet_file(gst_id):
    """Remove a single GST attachment by its own id (gst_input_sheets.id)."""
    uid = current_user_id()
    conn = get_db()
    c    = conn.cursor()
    c.execute("DELETE FROM gst_input_sheets WHERE user_id=%s AND id=%s",
              (uid, gst_id))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


# ── TDS -----------------------------------------------------------------

@app.route("/api/upload/tds_sheet", methods=["POST"])
@login_required
def upload_tds_sheet():
    """
    Upload a TDS reconciliation sheet (e.g. Form 26AS) attached to a
    specific expense sheet. Replaces any prior TDS attachment on that sheet.
    """
    uid      = current_user_id()
    f        = request.files.get("file")
    sheet_id = request.form.get("sheet_id")
    if not f:
        return jsonify({"error": "No file provided"}), 400
    if not sheet_id or not _require_owned_sheet(uid, sheet_id):
        return jsonify({"error": "Missing or invalid sheet_id — attach TDS to a specific expense sheet"}), 400

    raw  = f.read()
    conn = get_db()
    c    = conn.cursor()
    c.execute("DELETE FROM tds_input_sheets WHERE user_id=%s AND sheet_id=%s",
              (uid, int(sheet_id)))
    c.execute("""
        INSERT INTO tds_input_sheets (user_id, sheet_id, filename, file_data, row_count)
        VALUES (%s, %s, %s, %s, %s) RETURNING id, filename
    """, (uid, int(sheet_id), f.filename, psycopg2.Binary(raw), 0))
    row = c.fetchone()
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "id": row["id"], "filename": row["filename"],
                    "sheet_id": int(sheet_id)})


@app.route("/api/upload/tds_sheet", methods=["GET"])
@login_required
def get_tds_sheet_data():
    """Return parsed TDS sheet rows for a specific expense sheet (or latest)."""
    uid      = current_user_id()
    sheet_id = request.args.get("sheet_id")
    conn = get_db()
    c    = conn.cursor()
    if sheet_id:
        c.execute("""SELECT filename, file_data FROM tds_input_sheets
                     WHERE user_id=%s AND sheet_id=%s
                     ORDER BY created_at DESC LIMIT 1""", (uid, sheet_id))
    else:
        c.execute("""SELECT filename, file_data FROM tds_input_sheets
                     WHERE user_id=%s ORDER BY created_at DESC LIMIT 1""", (uid,))
    row = c.fetchone()
    conn.close()
    if not row:
        return jsonify({"rows": [], "filename": None})
    try:
        import io
        raw = bytes(row["file_data"])
        df = pd.read_excel(io.BytesIO(raw), engine="openpyxl")
        df.columns = [str(c).strip() for c in df.columns]
        records = df.fillna("").astype(str).to_dict("records")
        return jsonify({"rows": records[:2000], "filename": row["filename"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/upload/tds_sheet/<int:sheet_id>", methods=["DELETE"])
@login_required
def delete_tds_sheet(sheet_id):
    """Remove the TDS attachment from a specific expense sheet."""
    uid = current_user_id()
    conn = get_db()
    c    = conn.cursor()
    c.execute("DELETE FROM tds_input_sheets WHERE user_id=%s AND sheet_id=%s",
              (uid, sheet_id))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


# ── Attachments summary (used by the Upload tab to render the per-sheet UI)

@app.route("/api/upload/sheet_attachments", methods=["GET"])
@login_required
def list_sheet_attachments():
    """
    For each expense sheet the user has uploaded, report all GST attachments
    (multiple allowed) and the TDS attachment (single) for that sheet.
    Used by the frontend to render attach-buttons-per-sheet.
    """
    uid  = current_user_id()
    conn = get_db()
    c    = conn.cursor()
    # Aggregate all GST files per sheet into a JSON array via a correlated
    # subquery — one row per uploaded_sheet, no fan-out duplication.
    c.execute("""
        SELECT u.id AS sheet_id,
               u.filename AS sheet_filename,
               u.label AS sheet_label,
               COALESCE(
                 (SELECT json_agg(json_build_object('id', g.id, 'filename', g.filename)
                                  ORDER BY g.created_at)
                  FROM gst_input_sheets g
                  WHERE g.user_id = u.user_id AND g.sheet_id = u.id),
                 '[]'::json
               ) AS gst_files,
               t.id AS tds_id,
               t.filename AS tds_filename
        FROM uploaded_sheets u
        LEFT JOIN tds_input_sheets t
               ON t.user_id = u.user_id AND t.sheet_id = u.id
        WHERE u.user_id = %s
        ORDER BY u.created_at DESC
    """, (uid,))
    rows = c.fetchall()
    conn.close()
    return jsonify({
        "attachments": [{
            "sheet_id":     r["sheet_id"],
            "sheet_label":  r["sheet_label"] or r["sheet_filename"],
            # List of {id, filename} dicts — possibly empty. The frontend
            # treats an empty list as "no GST attached yet".
            "gst_files":    r["gst_files"] or [],
            "tds": ({"id": r["tds_id"], "filename": r["tds_filename"]}
                    if r["tds_id"] else None),
        } for r in rows]
    })


# ============================================================
# GST ITC Matching Engine  (deterministic, 2-of-3 signal logic)
# ============================================================

import re as _re
from datetime import datetime as _dt

# Bank/routing codes that appear in UPI narrations but are NOT vendor names
_BANK_ROUTING_TOKENS = {
    'HDFC','ICIC','ICICI','SBIN','AXIS','KOTAK','YESB','IDIB','CNRB','BARB',
    'IDFB','PUNB','BKID','UBIN','CIUB','FDRL','IOBA','KARB','LAVB','MAHB',
    'NKGSB','PYTM','PAYT','PAYU','GPAY','RAZR','CASH','NEFT','RTGS','IMPS',
    'NACH','UPIRET','PHONEPE','BHIM','AIRP','AIRTEL','JIOP','JIOS',
}

def _parse_float(s):
    try:
        v = float(str(s).replace(',', '').strip())
        return v if v > 0 else None
    except Exception:
        return None

def _parse_dt(s):
    s = str(s).strip().split(' ')[0]
    for fmt in ('%d/%m/%y', '%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y'):
        try:
            return _dt.strptime(s, fmt)
        except Exception:
            pass
    return None

def _vendor_tokens(name):
    """Extract meaningful uppercase tokens from a vendor name,
    excluding generic words and bank routing codes."""
    _generic = {
        'AND','PVT','LTD','PRIVATE','LIMITED','INDIA','THE','FOR','OF','CO',
        'CORP','BANK','ENTERPRISE','ENTERPRISES','SERVICES','SERVICE','RETAIL',
        'TRADING','SOLUTIONS','SOLUTION','TECHNOLOGY','TECHNOLOGIES','ONLINE',
        'PAYMENT','PAYMENTS','GROUP','INFOTECH','SYSTEMS','SYSTEM','INFRA',
    }
    tokens = set(_re.findall(r'[A-Z]{4,}', str(name).upper()))
    return tokens - _generic - _BANK_ROUTING_TOKENS


def match_gst_itc(transactions_df, context_sheets):
    """
    Deterministically match bank transactions against GSTR-2B / purchase
    register context sheets using 2-of-3 fuzzy signals:
      - AMOUNT  : invoice value within ±50 rupees OR 2%
      - DATE    : invoice date within 45 days of payment date
      - VENDOR  : meaningful token from vendor name found in bank narration

    Rules:
      - amt+vendor        → high confidence always
      - all three         → high confidence
      - vendor+date only  → medium (amount mildly different, ≤ ₹50 diff)
      - amt+date only     → medium ONLY when amount ≥ ₹500 AND unique in
                            GST sheet (not a repeated common amount like ₹674)
      - Bank routing tokens (HDFC, ICIC etc.) excluded from vendor matching
        to avoid UPI narrations falsely matching bank-fee GST invoices

    Returns a dict keyed by txn_key (same format as classification store):
      { txn_key: { itc_status, confidence, signals, invoice_no, vendor_name,
                   invoice_date, invoice_value, taxable_value,
                   igst, cgst, sgst, source_sheet } }
    """
    from collections import Counter

    # Build GST entry list from context sheets that look like GSTR-2B / purchase registers
    gst_entries = []
    for ctx in context_sheets:
        df  = ctx['df']
        tab = ctx['tab_name']
        if df.empty:
            continue
        cols_lower = [str(c).strip().lower() for c in df.columns]
        # Only process sheets that have GST-register columns
        is_gst = any(k in cols_lower for k in (
            'gstin of supplier', 'gstin', 'trade/legal name',
            'invoice value(₹)', 'invoice value', 'taxable value (₹)',
        ))
        if not is_gst:
            continue

        # Map column names case-insensitively
        col = {c.strip().lower(): c for c in df.columns}
        def _gcol(*keys):
            for k in keys:
                if k in col: return col[k]
            return None

        vendor_col   = _gcol('trade/legal name', 'trade name', 'legal name', 'supplier name')
        inv_no_col   = _gcol('invoice details invoice number', 'invoice number', 'invoice no',
                             'inv no', 'invoice details invoice number')
        inv_date_col = _gcol('invoice date', 'inv date', 'invoice details invoice date')
        inv_val_col  = _gcol('invoice value(₹)', 'invoice value', 'invoice value(rs)',
                             'invoice details invoice value(₹)')
        taxable_col  = _gcol('taxable value (₹)', 'taxable value', 'taxable amount')
        igst_col     = _gcol('tax amount integrated tax(₹)', 'integrated tax(₹)', 'igst')
        cgst_col     = _gcol('central tax(₹)', 'central tax', 'cgst')
        sgst_col     = _gcol('state/ut tax(₹)', 'state tax', 'sgst')

        if not vendor_col or not inv_val_col:
            continue

        for _, row in df.iterrows():
            amt  = _parse_float(row.get(inv_val_col, ''))
            if amt is None:
                continue
            inv_date = _parse_dt(row.get(inv_date_col, '')) if inv_date_col else None
            gst_entries.append({
                'tab':         tab,
                'vendor':      str(row.get(vendor_col, '')).strip(),
                'invoice_no':  str(row.get(inv_no_col, '')).strip() if inv_no_col else '',
                'inv_date':    inv_date,
                'value':       amt,
                'taxable':     _parse_float(row.get(taxable_col, '') if taxable_col else '') or 0,
                'igst':        _parse_float(row.get(igst_col, '') if igst_col else '') or 0,
                'cgst':        _parse_float(row.get(cgst_col, '') if cgst_col else '') or 0,
                'sgst':        _parse_float(row.get(sgst_col, '') if sgst_col else '') or 0,
                'tokens':      _vendor_tokens(row.get(vendor_col, '')),
            })

    if not gst_entries:
        return {}

    # Amounts that appear more than once in the GST sheet are ambiguous for amt-only matching
    from collections import Counter
    amt_counts = Counter(round(g['value'], 2) for g in gst_entries)

    results = {}
    for _, txn in transactions_df.iterrows():
        narr      = str(txn.get('Narration', '')).upper()
        narr_toks = set(_re.findall(r'[A-Z]{4,}', narr)) - _BANK_ROUTING_TOKENS
        bank_amt  = _parse_float(txn.get('Gross Total', ''))
        bank_date = _parse_dt(str(txn.get('Date', '')))
        txn_key   = f"{str(txn.get('Date',''))[:10]}|{str(txn.get('Narration',''))[:80]}|{txn.get('Gross Total','')}"

        if bank_amt is None or bank_date is None:
            results[txn_key] = {'itc_status': 'unverifiable', 'confidence': 'low',
                                'signals': {}, 'invoice_no': '', 'vendor_name': '',
                                'invoice_date': '', 'invoice_value': '', 'taxable_value': '',
                                'igst': '', 'cgst': '', 'sgst': '', 'source_sheet': ''}
            continue

        # Skip transactions that cannot have ITC — salary, reimbursements,
        # personal UPI transfers, tax payments, bank wallet top-ups.
        # These should never match a purchase register invoice.
        _NO_ITC_SIGNALS = (
            'SALARY', 'SAL ', ' SAL ', 'PAYROLL', 'REIMBURSE', 'REIMBURSEMENT',
            'CBDT', 'TDS ', ' TDS', 'ADVANCE TAX', 'GST PAYMENT',
            'UPI LITE', 'ADD MONEY', 'UPIRET', 'PHONEPE REVERSE',
            'FD REDEEM', 'ESI ', ' ESI', 'PROVIDENT', ' PF ',
        )
        if any(sig in narr for sig in _NO_ITC_SIGNALS):
            results[txn_key] = {'itc_status': 'not_reflected', 'confidence': 'high',
                                'signals': {}, 'invoice_no': '', 'vendor_name': '',
                                'invoice_date': '', 'invoice_value': '', 'taxable_value': '',
                                'igst': '', 'cgst': '', 'sgst': '', 'source_sheet': ''}
            continue

        # Also skip individual person payments (IMPS/UPI to a named person
        # with keywords like SALARY, ADVANCE, REIMBURSE already caught above).
        # Detect: voucher_type is Payment AND narration has common individual-
        # payment patterns (single person name after IMPS/UPI with no business name).
        voucher_type = str(txn.get('Voucher Type', '')).strip().upper()
        _INDIVIDUAL_PATTERNS = _re.compile(
            r'(?:IMPS|UPI)-\d+-([A-Z\s]+)-(?:IDIB|CNRB|SBIN|BARB|PUNB|BKID|IOBA)',
            _re.IGNORECASE
        )
        if voucher_type == 'PAYMENT' and _INDIVIDUAL_PATTERNS.search(narr):
            # Only skip if it also has a salary/personal keyword
            _PERSONAL_KEYWORDS = ('SALARY', 'SAL', 'ADVANCE', 'REIMB', 'WAGES')
            if any(k in narr for k in _PERSONAL_KEYWORDS):
                results[txn_key] = {'itc_status': 'not_reflected', 'confidence': 'high',
                                    'signals': {}, 'invoice_no': '', 'vendor_name': '',
                                    'invoice_date': '', 'invoice_value': '', 'taxable_value': '',
                                    'igst': '', 'cgst': '', 'sgst': '', 'source_sheet': ''}
                continue

        best = None
        best_score = 0
        best_conf  = ''

        for g in gst_entries:
            amt_diff  = abs(g['value'] - bank_amt)
            amt_ok    = amt_diff <= 50 or (g['value'] > 0 and amt_diff / g['value'] <= 0.02)
            amt_exact = amt_diff <= 2

            date_ok   = (g['inv_date'] is not None and
                         abs((bank_date - g['inv_date']).days) <= 45)

            vendor_ok = bool(g['tokens'] & narr_toks)

            score = sum([amt_ok, date_ok, vendor_ok])
            if score < 2:
                continue

            # Determine confidence and filter weak combinations
            if vendor_ok and amt_ok:
                conf = 'high'
            elif vendor_ok and date_ok and not amt_ok:
                # vendor + date but amount differs → only if diff ≤ 50
                if amt_diff > 50:
                    continue
                conf = 'medium'
            elif amt_ok and date_ok and not vendor_ok:
                # amount + date but no vendor token → only unique amounts ≥ 500
                if not (amt_exact and g['value'] >= 500 and amt_counts[round(g['value'], 2)] == 1):
                    continue
                conf = 'medium'
            else:
                conf = 'medium'

            # Prefer higher score, then higher confidence
            conf_rank = {'high': 2, 'medium': 1, 'low': 0}
            if (score > best_score or
                    (score == best_score and conf_rank[conf] > conf_rank.get(best_conf, 0))):
                best_score = score
                best_conf  = conf
                best = (g, {'amt': amt_ok, 'date': date_ok, 'vendor': vendor_ok}, conf)

        if best:
            g, sigs, conf = best
            results[txn_key] = {
                'itc_status':    'reflected',
                'confidence':    conf,
                'signals':       sigs,
                'invoice_no':    g['invoice_no'],
                'vendor_name':   g['vendor'],
                'invoice_date':  str(g['inv_date'].strftime('%d/%m/%Y')) if g['inv_date'] else '',
                'invoice_value': str(g['value']),
                'taxable_value': str(g['taxable']),
                'igst':          str(g['igst']),
                'cgst':          str(g['cgst']),
                'sgst':          str(g['sgst']),
                'source_sheet':  g['tab'],
            }
        else:
            results[txn_key] = {
                'itc_status':    'not_reflected',
                'confidence':    'high',   # confidently not found
                'signals':       {},
                'invoice_no':    '', 'vendor_name':   '', 'invoice_date':  '',
                'invoice_value': '', 'taxable_value': '',
                'igst':          '', 'cgst':          '', 'sgst':          '',
                'source_sheet':  '',
            }

    return results


@app.route('/api/workspace/nodes/<int:node_id>/gst_itc_match', methods=['POST'])
@login_required
def workspace_gst_itc_match(node_id):
    """Run deterministic GST ITC matching for all transactions in a node
    against its uploaded GSTR-2B context files. Saves results to
    gst_itc_matches table and returns the full results dict."""
    uid = current_user_id()

    # Load node
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT * FROM workspace_nodes WHERE id=%s AND user_id=%s', (node_id, uid))
    node = c.fetchone()
    conn.close()
    if not node:
        return jsonify({'error': 'not found'}), 404

    try:
        # Load transactions
        source_type = (node['source_type'] if 'source_type' in node.keys() else None) or 'gsheet'
        if source_type == 'excel':
            excel_id = node['excel_file_id'] if 'excel_file_id' in node.keys() else None
            if not excel_id:
                return jsonify({'error': 'No Excel file attached'}), 400
            transactions = load_excel_source_tab(int(excel_id), node['tab_name'], uid)
        else:
            tokens_str = get_setting('oauth_tokens', user_id=uid)
            if not tokens_str:
                return jsonify({'error': 'not_connected'}), 401
            transactions = load_from_gsheets_oauth(
                node['sheet_id'], node['tab_name'], json.loads(tokens_str))

        # Load context sheets (uploaded files only — that's where GSTR-2B lives)
        context_sheets = load_node_uploaded_context(node_id, uid)

        if not context_sheets:
            return jsonify({'error': 'No context files uploaded for this node. '
                            'Upload your GSTR-2B / purchase register files first.'}), 400

        matches = match_gst_itc(transactions, context_sheets)

        # Persist to DB
        conn = get_db()
        c = conn.cursor()
        saved = 0
        for txn_key, m in matches.items():
            c.execute("""
                INSERT INTO gst_itc_matches
                  (user_id, node_id, txn_key, itc_status, confidence, signals,
                   invoice_no, vendor_name, invoice_date, invoice_value,
                   taxable_value, igst, cgst, sgst, source_sheet, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (user_id, node_id, txn_key) DO UPDATE SET
                  itc_status    = EXCLUDED.itc_status,
                  confidence    = EXCLUDED.confidence,
                  signals       = EXCLUDED.signals,
                  invoice_no    = EXCLUDED.invoice_no,
                  vendor_name   = EXCLUDED.vendor_name,
                  invoice_date  = EXCLUDED.invoice_date,
                  invoice_value = EXCLUDED.invoice_value,
                  taxable_value = EXCLUDED.taxable_value,
                  igst          = EXCLUDED.igst,
                  cgst          = EXCLUDED.cgst,
                  sgst          = EXCLUDED.sgst,
                  source_sheet  = EXCLUDED.source_sheet,
                  updated_at    = NOW()
            """, (uid, node_id, txn_key,
                  m['itc_status'], m['confidence'],
                  json.dumps(m['signals']),
                  m['invoice_no'], m['vendor_name'], m['invoice_date'],
                  m['invoice_value'], m['taxable_value'],
                  m['igst'], m['cgst'], m['sgst'], m['source_sheet']))
            saved += 1
        conn.commit()
        conn.close()

        # Return summary counts + full results
        reflected     = sum(1 for m in matches.values() if m['itc_status'] == 'reflected')
        not_reflected = sum(1 for m in matches.values() if m['itc_status'] == 'not_reflected')
        unverifiable  = sum(1 for m in matches.values() if m['itc_status'] == 'unverifiable')

        return jsonify({
            'ok':            True,
            'saved':         saved,
            'reflected':     reflected,
            'not_reflected': not_reflected,
            'unverifiable':  unverifiable,
            'matches':       matches,
        })

    except Exception as e:
        log.exception('gst_itc_match error')
        return jsonify({'error': str(e)}), 500


@app.route('/api/workspace/nodes/<int:node_id>/gst_itc_match', methods=['GET'])
@login_required
def workspace_get_gst_itc_match(node_id):
    """Return saved ITC match results for a node."""
    uid = current_user_id()
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT id FROM workspace_nodes WHERE id=%s AND user_id=%s', (node_id, uid))
    if not c.fetchone():
        conn.close()
        return jsonify({'error': 'not found'}), 404
    c.execute("""
        SELECT txn_key, itc_status, confidence, signals,
               invoice_no, vendor_name, invoice_date, invoice_value,
               taxable_value, igst, cgst, sgst, source_sheet
        FROM gst_itc_matches WHERE user_id=%s AND node_id=%s
    """, (uid, node_id))
    rows = c.fetchall()
    conn.close()
    matches = {}
    for r in rows:
        try:
            signals = json.loads(r['signals'] or '{}')
        except Exception:
            signals = {}
        matches[r['txn_key']] = {
            'itc_status':    r['itc_status'],
            'confidence':    r['confidence'],
            'signals':       signals,
            'invoice_no':    r['invoice_no'] or '',
            'vendor_name':   r['vendor_name'] or '',
            'invoice_date':  r['invoice_date'] or '',
            'invoice_value': r['invoice_value'] or '',
            'taxable_value': r['taxable_value'] or '',
            'igst':          r['igst'] or '',
            'cgst':          r['cgst'] or '',
            'sgst':          r['sgst'] or '',
            'source_sheet':  r['source_sheet'] or '',
        }
    reflected     = sum(1 for m in matches.values() if m['itc_status'] == 'reflected')
    not_reflected = sum(1 for m in matches.values() if m['itc_status'] == 'not_reflected')
    return jsonify({
        'matches':       matches,
        'reflected':     reflected,
        'not_reflected': not_reflected,
        'total':         len(matches),
    })


# ============================================================
# GST Non-filer Detection
# ============================================================

@app.route("/api/tax/gst_nonfilers", methods=["POST"])
@login_required
def detect_gst_nonfilers():
    """
    Cross-reference expense transactions against the uploaded GST input sheet(s).
    Flags vendors present in the balance sheet but absent from the GST filing.

    Body params:
      - transactions: list of txn dicts
      - sheet_id (optional): if provided, restrict matching to the GST
        attachment of that specific expense sheet. Otherwise the union of
        ALL the user's GST attachments is used.
    """
    uid = current_user_id()
    data = request.json or {}
    transactions = data.get("transactions", [])
    sheet_id     = data.get("sheet_id")

    # Load GST sheet(s) — per-sheet attachment or union across all user's sheets
    conn = get_db()
    c = conn.cursor()
    if sheet_id:
        c.execute("""SELECT file_data FROM gst_input_sheets
                     WHERE user_id=%s AND sheet_id=%s
                     ORDER BY created_at DESC""", (uid, sheet_id))
    else:
        c.execute("""SELECT file_data FROM gst_input_sheets
                     WHERE user_id=%s ORDER BY created_at DESC""", (uid,))
    rows = c.fetchall()
    conn.close()
    if not rows:
        return jsonify({"error": "No GST input sheet attached to any expense sheet"}), 400

    import io
    gst_vendor_tokens = set()
    gst_amounts       = set()

    for db_row in rows:
        try:
            raw    = bytes(db_row["file_data"])
            gst_df = pd.read_excel(io.BytesIO(raw), engine="openpyxl")
            gst_df.columns = [str(c).strip() for c in gst_df.columns]
            gst_df = gst_df.fillna("").astype(str)
        except Exception as e:
            log.warning("Failed to parse a GST attachment: %s", e)
            continue

        # Build vendor tokens (GSTIN, trade/legal name columns — heuristic)
        for _, r in gst_df.iterrows():
            row_vals = " ".join(r.values).upper()
            tokens = set(w for w in row_vals.split() if len(w) > 3)
            gst_vendor_tokens.update(tokens)

        # Fuzzy amount matching
        amount_col = next((c for c in gst_df.columns
                           if any(k in c.lower() for k in
                                  ("amount", "taxable", "igst", "cgst", "sgst", "value"))), None)
        if amount_col:
            for v in gst_df[amount_col]:
                try:
                    gst_amounts.add(round(float(str(v).replace(",", "")), 0))
                except Exception:
                    pass

    nonfilers = []
    for txn in transactions:
        narr = str(txn.get("narration", "")).upper()
        narr_tokens = set(w for w in narr.split() if len(w) > 3)
        # Skip salary/non-GST categories
        if any(kw in narr for kw in ("SALARY", "PAYROLL", "ESI", "PF ", "PROVIDENT")):
            continue

        # Check if vendor tokens appear in any GST sheet
        overlap = narr_tokens & gst_vendor_tokens
        if overlap:
            continue  # vendor is in the GST sheet → filed → OK

        # Try amount matching (round-off tolerance ±10)
        try:
            amt = round(float(str(txn.get("gross_total", "0")).replace(",", "")), 0)
        except Exception:
            amt = 0
        amt_match = any(abs(amt - ga) <= 10 for ga in gst_amounts) if amt > 0 else False

        if not amt_match:
            nonfilers.append({
                "narration":   txn.get("narration", ""),
                "date":        txn.get("date", ""),
                "gross_total": txn.get("gross_total", ""),
                "reason":      "Vendor not found in GST input sheet",
            })

    return jsonify({"nonfilers": nonfilers, "total": len(nonfilers)})


# ============================================================
# GST + TDS Classification (AI-powered)
# ============================================================

_TAX_CLASSIFY_PROMPT = """You are an expert Indian CA (Chartered Accountant) helping a Private Limited Company (Alfaleus Technology Pvt Ltd, a medical device company) classify transactions for GST and TDS compliance.

ENTITY TYPE: Private Limited Company

For each transaction, determine:

1. GST:
   - gst_applicable: "yes" | "no" | "reverse_charge" | "exempt"
   - gst_direction: "out" (GST collected from customer) | "in" (GST paid to vendor / ITC) | "" (N/A)
   - gst_rate: "0" | "5" | "12" | "18" | "28" | "" (leave blank ONLY when gst_applicable is "no" or "exempt")
   
   GST RULES (India):
   - Salaries, wages, ESI, PF → gst_applicable: "no"
   - Bank charges, interest → gst_applicable: "exempt"
   - Import of services (foreign vendor) → gst_applicable: "reverse_charge"
   - Freight / GTA services → gst_applicable: "reverse_charge" (if GTA)
   - Professional services, consulting, software → gst_applicable: "yes", rate: 18
   - Hotels, travel agents → gst_applicable: "yes", rate varies (5 or 12 or 18)
   - Medical devices sales (outgoing) → gst_applicable: "yes", rate: 12 or 18
   - Courier / logistics → gst_applicable: "yes", rate: 18
   - Rent on immovable property → gst_applicable: "yes", rate: 18
   - Office supplies / stationery → gst_applicable: "yes", rate: 12 or 18
   - UPI transfers / bank transfers (no goods/services) → gst_applicable: "no"

   STRICT GST RULES:
   - RATE REQUIRED: If gst_applicable is "yes" or "reverse_charge", gst_rate MUST be non-empty. Choose the most appropriate rate; never leave it blank when GST applies.
   - REIMBURSEMENTS: Narration containing "REIMBURSE" or payments to individuals for travel/expense recovery → gst_applicable="no". Reimbursements are not supply; no ITC claim.
   - UPI TO INDIVIDUAL: Payment to a named individual (not a registered business) via UPI/IMPS → gst_applicable="no". Individuals cannot charge GST.
   - LARGE UNMATCHED RECEIPTS: Incoming amounts >₹10,000 from unidentified source → flag with low confidence and note possible GST output liability in reasoning.

2. TDS (Pvt Ltd company deducts TDS as payer):
   - tds_applicable: "yes" | "no"
   - tds_direction: "out" (company deducts TDS when paying vendor) | "in" (TDS deducted by customer on payment to company) | "" (N/A)
   - tds_section: e.g. "192" | "194C" | "194J" | "194I" | "194Q" | "194H" | "194D" | ""
   - tds_rate: percentage as string e.g. "1" | "2" | "10" | ""
   
   TDS RULES (for Pvt Ltd):
   - Salary payments → 192, rate depends on slab (put "slab")
   - Contractor payments (fabrication, courier, transport) → 194C, rate: 1% (individual) or 2% (company)
   - Professional/technical services (consulting, IT, legal, CA) → 194J, rate: 10%
   - Rent of machinery/equipment → 194I(a), rate: 2%
   - Rent of land/building → 194I(b), rate: 10%
   - Commission/brokerage → 194H, rate: 5%
   - Purchase of goods > 50L/year from single vendor → 194Q, rate: 0.1%
   - Bank interest → 194A, rate: 10%
   - No TDS: GST payments, UPI personal transfers, bank charges, reimbursements < threshold

   STRICT TDS THRESHOLD RULES (do NOT apply TDS below these limits):
   - 194C: Single transaction below ₹30,000 → tds_applicable="no" (unless clearly a recurring vendor likely to cross ₹1,00,000 annually).
   - 194J: Aggregate payments to the professional below ₹30,000/year. Single payment below ₹3,000 → tds_applicable="no".
   - 194I: Monthly rent below ₹20,000 (annual below ₹2,40,000) → tds_applicable="no".
   - 192: Salary TDS only where annual salary exceeds ₹3,00,000. Single ad-hoc payment to individual below ₹5,000 → tds_applicable="no" unless it is a confirmed salary row.
   - 194Q: Only when annual purchase from that single vendor exceeds ₹50,00,000. Do NOT apply 194Q to routine small purchases.

   STRICT TDS CONSISTENCY RULES:
   - SALARY CATEGORY: If the transaction is clearly salary/payroll, tds_section must be "192", NOT "194J". 194J is for non-employee professionals only.
   - REIMBURSEMENTS: Reimbursements to employees for travel/expenses → tds_applicable="no".
   - TAX PAYMENTS (CBDT/GST/TDS challan) → tds_applicable="no", gst_applicable="no".

{prior_decisions_block}
TRANSACTIONS:
{batch_json}

Return ONLY a JSON array (same order):
[
  {{
    "idx": <same idx>,
    "gst_applicable": "yes|no|reverse_charge|exempt",
    "gst_direction": "in|out|",
    "gst_rate": "5|12|18|28|0|",
    "tds_applicable": "yes|no",
    "tds_direction": "in|out|",
    "tds_section": "192|194C|194J|...|",
    "tds_rate": "1|2|10|slab|",
    "reasoning": "<1-2 sentence explanation>"
  }}
]

No markdown. No extra text."""


@app.route("/api/tax/classify", methods=["POST"])
@login_required
def tax_classify():
    """Classify transactions for GST and TDS using Gemini AI."""
    uid  = current_user_id()
    data = request.json or {}
    transactions = data.get("transactions", [])
    if not transactions:
        return jsonify({"error": "No transactions provided"}), 400
    if not GEMINI_API_KEY:
        return jsonify({"error": "GEMINI_API_KEY not set"}), 500

    _generate = _make_model_runner()
    results = []
    prior_decisions: dict = {}
    batch_size = 15

    for start in range(0, len(transactions), batch_size):
        batch = [{"idx": i + start, **t} for i, t in enumerate(transactions[start:start+batch_size])]
        prompt = _TAX_CLASSIFY_PROMPT.format(
            prior_decisions_block=_build_prior_decisions_block(prior_decisions),
            batch_json=json.dumps(batch, indent=2))
        try:
            response = _generate(prompt)
            parsed   = _parse_gemini_json(response.text)
            if parsed:
                _update_prior_decisions(prior_decisions, parsed,
                                        transactions[start:start+batch_size])
                results.extend(parsed)
            else:
                for item in batch:
                    results.append({"idx": item["idx"], "gst_applicable": "no",
                                    "tds_applicable": "no", "reasoning": "Parse error"})
        except Exception as e:
            for item in batch:
                results.append({"idx": item["idx"], "gst_applicable": "no",
                                "tds_applicable": "no", "reasoning": str(e)[:80]})

    # Persist to DB
    conn = get_db()
    c = conn.cursor()
    for res in results:
        txn = transactions[res["idx"]] if res["idx"] < len(transactions) else {}
        key = f"{txn.get('date','')[:10]}|{txn.get('narration','')[:80]}|{txn.get('gross_total','')}"
        c.execute("""
            INSERT INTO tax_classifications
              (user_id, txn_key, gst_applicable, gst_direction, gst_rate,
               tds_applicable, tds_direction, tds_section, tds_rate, ai_reasoning, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (user_id, txn_key) DO UPDATE SET
              gst_applicable=EXCLUDED.gst_applicable,
              gst_direction=EXCLUDED.gst_direction,
              gst_rate=EXCLUDED.gst_rate,
              tds_applicable=EXCLUDED.tds_applicable,
              tds_direction=EXCLUDED.tds_direction,
              tds_section=EXCLUDED.tds_section,
              tds_rate=EXCLUDED.tds_rate,
              ai_reasoning=EXCLUDED.ai_reasoning,
              updated_at=NOW()
        """, (uid, key,
              res.get("gst_applicable","no"), res.get("gst_direction",""),
              res.get("gst_rate",""),
              res.get("tds_applicable","no"), res.get("tds_direction",""),
              res.get("tds_section",""), res.get("tds_rate",""),
              res.get("reasoning","")))
    conn.commit()
    conn.close()
    return jsonify({"results": results, "total": len(results)})


@app.route("/api/tax/classifications", methods=["GET"])
@login_required
def get_tax_classifications():
    uid  = current_user_id()
    conn = get_db()
    c = conn.cursor()
    c.execute("""SELECT txn_key, gst_applicable, gst_direction, gst_rate,
                        tds_applicable, tds_direction, tds_section, tds_rate,
                        ai_reasoning, confirmed
                 FROM tax_classifications WHERE user_id=%s""", (uid,))
    rows = c.fetchall()
    conn.close()
    return jsonify({"classifications": [dict(r) for r in rows]})


@app.route("/api/tax/classifications", methods=["POST"])
@login_required
def save_tax_classification():
    """Manually save/confirm a tax classification for a single transaction."""
    uid  = current_user_id()
    data = request.json or {}
    key  = data.get("txn_key","")
    if not key:
        return jsonify({"error": "txn_key required"}), 400
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        INSERT INTO tax_classifications
          (user_id, txn_key, gst_applicable, gst_direction, gst_rate,
           tds_applicable, tds_direction, tds_section, tds_rate, ai_reasoning, confirmed, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON CONFLICT (user_id, txn_key) DO UPDATE SET
          gst_applicable=EXCLUDED.gst_applicable,
          gst_direction=EXCLUDED.gst_direction,
          gst_rate=EXCLUDED.gst_rate,
          tds_applicable=EXCLUDED.tds_applicable,
          tds_direction=EXCLUDED.tds_direction,
          tds_section=EXCLUDED.tds_section,
          tds_rate=EXCLUDED.tds_rate,
          ai_reasoning=EXCLUDED.ai_reasoning,
          confirmed=EXCLUDED.confirmed,
          updated_at=NOW()
    """, (uid, key,
          data.get("gst_applicable","no"), data.get("gst_direction",""),
          data.get("gst_rate",""),
          data.get("tds_applicable","no"), data.get("tds_direction",""),
          data.get("tds_section",""), data.get("tds_rate",""),
          data.get("ai_reasoning",""), int(data.get("confirmed", 1))))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)