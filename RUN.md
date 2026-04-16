# Running DataQA — Simple Frontend & Backend Steps

This file provides a minimal local execution guide for the backend and frontend.

---

## Prerequisites

- Python 3.11+
- Node.js 18+
- npm 9+
- MongoDB 6+

---

## Backend

1. Open a terminal.
2. Install dependencies:

```bash
cd cctaat/backend
pip install -r requirements.txt
```

3. Ensure MongoDB is running locally.

4. Create or update `backend/.env` with these values:

```env
MONGO_URI=mongodb://localhost:27017
MONGO_DB=data_quality
UPLOAD_DIR=./data/uploads
ALLOWED_ORIGINS=http://localhost:5173
MAX_UPLOAD_SIZE_MB=100

REDIS_URL=
REDIS_CACHE_TTL=3600

ANALYSIS_CACHE_SIZE=128
MAX_PROFILE_ROWS=50000

RATE_LIMIT_ENABLED=true
RATE_LIMIT=100/minute

ASYNC_ANALYSIS_ENABLED=true
SIGNED_URLS_ENABLED=false
SIGNED_URL_SECRET=change-me-before-deploying
SIGNED_URL_TTL_SECONDS=3600
```

5. Start the backend:

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

6. Backend URLs:

- http://localhost:8000
- API docs: http://localhost:8000/docs
- Health: http://localhost:8000/health

---

## Frontend

1. Open a second terminal.
2. Install dependencies:

```bash
cd cctaat/frontend
npm install
```

3. Optionally create `frontend/.env` if the backend is not at `http://localhost:8000`:

```env
VITE_API_URL=http://localhost:8000
```

4. Start the frontend:

```bash
npm run dev
```

5. Open the app at:

- http://localhost:5173

---

## Run both together

**Terminal 1 — Backend**
```bash
cd cctaat/backend
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

**Terminal 2 — Frontend**
```bash
cd cctaat/frontend
npm run dev
```

---

## Notes

- The backend uses an in-memory cache by default when `REDIS_URL` is empty.
- To enable Redis later, set `REDIS_URL=redis://localhost:6379/0`.
- The README and backend configuration support additional features, but this guide focuses on local run steps only.
