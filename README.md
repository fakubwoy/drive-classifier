# Alfaleus Transaction Classifier

AI-powered Flask app to classify bank transactions from the Suspense Account ledger using **Gemini 2.5 Flash**.

## What's New

- 🔐 **User Profiles & Login** — Google Sign-In gates the app; every user sees only their own data
- 🐘 **PostgreSQL backend** — replaces SQLite; required for multi-user isolation on Railway
- 📊 **Google Sheets integration** — connect directly to your live Drive sheet, no more download/upload cycle
- 🧠 **Feedback & Learning** — correct wrong classifications; the app learns and auto-applies your rules to similar transactions
- ⚠️ **Duplicate Detection** — flags payments from the same vendor with the same amount, so double-payments don't slip through
- ⚡ **Quick Review Mode** — keyboard-driven accept/reject flow (Enter = Accept, R = Reject, S = Skip)

---

## Data Isolation — How It Works

Each Google account is a separate user in the `users` table. All settings (active sheet, OAuth tokens), learned rules, and feedback are **scoped to `user_id`** in Postgres. No user can see another user's transactions, sheet links, or rules.

---

## Setup

### 1. Get a Gemini API Key
https://aistudio.google.com/apikey

### 2. Set up Google OAuth2 (one client, two redirect URIs)

Go to https://console.cloud.google.com → APIs & Services → Credentials → **Create OAuth 2.0 Client ID** (Web Application).

Add **both** of these as Authorised redirect URIs:

```
https://YOUR_RAILWAY_DOMAIN/auth/google/callback   ← user login
https://YOUR_RAILWAY_DOMAIN/api/gdrive/callback    ← Drive sheet picker
```

For local dev also add the `http://localhost:8080` variants.

Enable these APIs on the same project:
- Google Sheets API
- Google Drive API
- (Already implied) Google People/OAuth API

### 3. Add Postgres on Railway

In your Railway project → **+ New** → **Database** → **PostgreSQL**.  
Railway auto-injects `DATABASE_URL` into your service. No manual config needed.

### 4. Environment Variables (Railway → Variables)

| Variable | Value |
|---|---|
| `GEMINI_API_KEY` | Your Gemini key |
| `DATABASE_URL` | Auto-set by Railway Postgres plugin |
| `FLASK_SECRET_KEY` | Any long random string |
| `GOOGLE_OAUTH_CLIENT_ID` | From GCP |
| `GOOGLE_OAUTH_CLIENT_SECRET` | From GCP |
| `GOOGLE_LOGIN_REDIRECT_URI` | `https://YOUR_DOMAIN/auth/google/callback` |
| `GOOGLE_OAUTH_REDIRECT_URI` | `https://YOUR_DOMAIN/api/gdrive/callback` |

### 5. Run Locally

```bash
# Copy .env.example and fill values
cp .env.example .env

docker compose up --build
```

Open http://localhost:8080 — you'll be redirected to Google Sign-In.

---

## Features

### 🔐 Login & User Isolation
Every visitor must sign in with Google. Each account has a completely separate:
- Google Sheet connection
- Learned classification rules
- Feedback history

### 📊 Live Google Sheets
After logging in, click **Connect Drive** to link your personal Google Sheet. Data loads live on every page refresh.

### ⚡ Quick Review Mode
Click **⚡ Quick Review** after classifying to enter a focused keyboard-driven review:
- **Enter** — accept the AI's suggestion
- **R** — reject and pick a correction
- **S** — skip for now
- **Esc** — exit review

### 🧠 Feedback & Learning
When you correct a classification:
- Toggle **"Learn from this"** to create a rule
- The rule is stored against your user ID — other users are unaffected
- Manage rules in the **🧠 Learned Rules** tab

### ⚠️ Duplicate Detection
Payments to the same vendor for the same amount (within 1%) within 90 days are flagged with a yellow **⚠️ DUP** badge.

### ⬇️ Export CSV
Includes `Review Decision` and `Duplicate Flag` columns.

---

## File Structure

```
alfaleus_app/
├── app.py                        # Flask backend (auth + per-user Postgres)
├── templates/
│   └── index.html               # Frontend UI (login gate + user avatar)
├── Query_sheet_alfaleus.xlsx    # Excel fallback
├── requirements.txt
├── Dockerfile
├── docker-compose.yml           # Includes local Postgres service
├── railway.toml
└── README.md
```