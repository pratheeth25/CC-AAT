# DataQA — Data Quality Analyzer

A production-quality SaaS-style dashboard for analyzing, profiling, and cleaning datasets.

---

## Features

| Category | Capabilities |
|---|---|
| **Upload** | Drag-and-drop CSV / JSON, progress bar, instant metadata |
| **Data Profiling** | Column stats, dtype, missing %, unique count, top values, numeric summaries |
| **Quality Scoring** | 0–100 score across 5 dimensions, letter grade A–F |
| **Anomaly Detection** | IQR, Z-score, Isolation Forest, date validation, categorical rarity |
| **Smart Cleaning** | Toggle missing-value fixes, de-duplication, date standardization, outlier removal — saves as new version |
| **Data Drift** | KS-test (numeric) & chi-squared (categorical) drift detection between any two versions |
| **Alerts** | Severity-filtered alert feed with mark-as-read; per-dataset and global views |
| **Versioning** | Every cleaning run stores a new version; compare any two |

---

## Tech Stack

### Frontend
- React 18 · Vite 5 · React Router 6
- Tailwind CSS 3 · Recharts 2 · Lucide React
- Axios · React Hot Toast

### Backend
- Python 3.13 · FastAPI · Motor (async MongoDB)
- Pandas 2 · NumPy 2 · Scikit-learn 1.7 · SciPy 1

---

## Project Structure

```
cctaat/
├── README.md
├── RUN.md
├── backend/
│   ├── .env
│   ├── requirements.txt
│   ├── data/uploads/               # Uploaded files stored here
│   └── app/
│       ├── main.py                 # FastAPI app + CORS + lifespan
│       ├── config.py               # pydantic-settings
│       ├── database.py             # Motor async MongoDB client
│       ├── models/
│       │   ├── dataset.py          # All dataset Pydantic models
│       │   └── alert.py            # Alert models
│       ├── services/
│       │   ├── dataset_service.py  # Upload, versioning, CRUD
│       │   ├── profiling_service.py# Column stats, missing detection
│       │   ├── quality_service.py  # 0-100 score, A-F grade
│       │   ├── anomaly_service.py  # IQR, Z-score, Isolation Forest
│       │   ├── repair_service.py   # Smart fix suggestions
│       │   ├── cleaning_service.py # Apply fixes, save new version
│       │   ├── drift_service.py    # KS-test, chi-squared drift
│       │   └── alert_service.py    # Create/retrieve alerts
│       └── api/routes/
│           ├── datasets.py         # /upload, /datasets, /dataset/:id
│           ├── analysis.py         # /profile, /quality, /anomalies, /repairs, /drift
│           ├── cleaning.py         # POST /dataset/:id/clean
│           └── alerts.py           # GET /alerts, PATCH /alerts/:id/read
└── frontend/
    ├── index.html
    ├── vite.config.js
    ├── tailwind.config.js
    ├── package.json
    └── src/
        ├── main.jsx                # React root + router + toast provider
        ├── App.jsx                 # Route definitions
        ├── services/api.js         # Axios instance + all API calls
        ├── hooks/useAsync.js       # Generic async state hook
        ├── components/             # Reusable UI components
        └── pages/
            ├── Dashboard.jsx
            ├── DatasetList.jsx
            ├── Upload.jsx
            ├── AlertsPage.jsx
            └── DatasetDetail/      # 7-tab dataset detail view
```

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/upload` | Upload CSV/JSON file |
| `GET` | `/datasets` | List all datasets |
| `GET` | `/dataset/:id` | Get dataset metadata + versions |
| `GET` | `/dataset/:id/profile` | Column profiling stats |
| `GET` | `/dataset/:id/quality` | Quality score + breakdown |
| `GET` | `/dataset/:id/anomalies` | Anomaly detection results |
| `GET` | `/dataset/:id/repairs` | Smart repair suggestions |
| `POST` | `/dataset/:id/clean` | Apply cleaning pipeline |
| `GET` | `/dataset/:id/drift` | Data drift between versions |
| `GET` | `/alerts` | List all alerts |
| `PATCH` | `/alerts/:id/read` | Mark alert as read |

---

## Deployment (EC2 + Docker)

```bash
# On EC2
cp backend/.env.example backend/.env
nano backend/.env                          # set MONGO_URI and ALLOWED_ORIGINS
docker compose up -d --build
```

| Service | URL |
|---|---|
| Frontend | `http://<EC2_PUBLIC_IP>` |
| API Docs | `http://<EC2_PUBLIC_IP>/api/docs` |
| Health | `http://<EC2_PUBLIC_IP>/api/health` |

**EC2 Security Group:** open inbound TCP **port 80** only. Port 8000 stays internal.  
**Atlas Network Access:** add the EC2 public IP to allow the backend to connect.

---

## Local Development URLs

| Service | URL |
|---|---|
| Frontend (Vite dev) | http://localhost:5173 |
| Backend (FastAPI) | http://localhost:8000 |
| API Docs (Swagger) | http://localhost:8000/docs |

---

> See **RUN.md** for full setup and deployment instructions.
