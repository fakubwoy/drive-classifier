# Alfaleus Transaction Classifier

AI-powered Flask app to classify bank transactions from the Suspense Account ledger using **Gemini 2.5 Flash**.

## What's New

- 📊 **Google Sheets integration** — connect directly to your live Drive sheet, no more download/upload cycle
- 🧠 **Feedback & Learning** — correct wrong classifications; the app learns and auto-applies your rules to similar transactions
- ⚠️ **Duplicate Detection** — flags payments from the same vendor with the same amount, so double-payments don't slip through
- ⚡ **Quick Review Mode** — keyboard-driven accept/reject flow (Enter = Accept, R = Reject, S = Skip) with narration and date front and centre

---

## Setup

### 1. Get a Gemini API Key
https://aistudio.google.com/apikey

### 2a. Google Sheets Integration (recommended)

You need a **Google Service Account** to let the app read your sheet.

1. Go to https://console.cloud.google.com → create a project (or use existing)
2. Enable **Google Sheets API** and **Google Drive API**
3. Create a **Service Account** → download the JSON key → save as `credentials.json` next to `docker-compose.yml`
4. Open your Google Sheet → Share it with the service account email (e.g. `myapp@myproject.iam.gserviceaccount.com`) — **Viewer** access is enough
5. Copy the Sheet ID from the URL: `docs.google.com/spreadsheets/d/SHEET_ID_HERE/edit`

```bash
export GEMINI_API_KEY=your_key_here
export GOOGLE_SHEETS_ID=your_sheet_id_here
```

Then in `docker-compose.yml`, uncomment the `credentials.json` volume line:
```yaml
- ./credentials.json:/app/credentials.json:ro
```

### 2b. Excel fallback (no Sheets setup needed)

Just leave `GOOGLE_SHEETS_ID` unset — the app uses `Query_sheet_alfaleus.xlsx` as before.

### 3. Run

```bash
docker compose up --build
```

Open http://localhost:8080

---

## Features

### 📊 Live Google Sheets
Data is read directly from your Drive sheet every time the page loads. No exporting, no pasting — edit your sheet, refresh the app.

### ⚡ Quick Review Mode
Click **⚡ Quick Review** after classifying to enter a focused keyboard-driven review:
- **Enter** — accept the AI's suggestion
- **R** — reject and pick a correction
- **S** — skip for now
- **Esc** — exit review

Narration and date are shown prominently so you can decide in seconds.

### 🧠 Feedback & Learning
When you correct a classification:
- Toggle **"Learn from this"** to create a rule
- The app immediately re-applies the rule to all matching transactions in the current session
- Future AI classification batches also respect learned rules
- Manage rules in the **🧠 Learned Rules** tab

### ⚠️ Duplicate Detection
Payments to the same vendor for the same amount (within 1%) within 90 days are flagged with a yellow **⚠️ DUP** badge. The detail panel lists the other suspected duplicate so you can compare narration and date side-by-side.

### ⬇️ Export CSV
Now includes `Review Decision` (accepted/rejected) and `Duplicate Flag` columns.

---

## File Structure

```
alfaleus_app/
├── app.py                        # Flask backend
├── templates/
│   └── index.html               # Frontend UI
├── Query_sheet_alfaleus.xlsx    # Excel fallback
├── credentials.json             # Google service account key (optional)
├── feedback.db                  # SQLite: learned rules (auto-created)
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── README.md
```