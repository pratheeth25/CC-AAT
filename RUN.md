# Running DataQA

---

## EC2 Deployment (Docker — recommended)

### Prerequisites on the EC2 instance

- Docker + Docker Compose installed
- Port **80** open in EC2 Security Group (inbound TCP from `0.0.0.0/0`)
- MongoDB Atlas cluster with the EC2 public IP added to **Network Access**

### Steps

1. **SSH into EC2 and clone / copy the project:**

```bash
ssh -i your-key.pem ec2-user@<EC2_PUBLIC_IP>
git clone <repo-url> cctaat
cd cctaat
```

2. **Create `backend/.env`** (copy from example and fill in real values):

```bash
cp backend/.env.example backend/.env
nano backend/.env
```

Required values to set:

```env
MONGO_URI=mongodb+srv://<user>:<password>@<cluster>.mongodb.net/?appName=Cluster0
MONGO_DB=data_quality

UPLOAD_DIR=/app/data/uploads
MAX_UPLOAD_SIZE_MB=100

ALLOWED_ORIGINS=http://<EC2_PUBLIC_IP>

ANALYSIS_CACHE_SIZE=128
MAX_PROFILE_ROWS=50000

RATE_LIMIT_ENABLED=true
RATE_LIMIT=100/minute

ASYNC_ANALYSIS_ENABLED=true
SIGNED_URLS_ENABLED=true
SIGNED_URL_SECRET=<run: python3 -c "import secrets; print(secrets.token_hex(32))">
SIGNED_URL_TTL_SECONDS=3600
```

> `UPLOAD_DIR` must be `/app/data/uploads` (the Docker container path — do not change).  
> Root `.env` is **not used** — only `backend/.env` is loaded by docker-compose.

3. **Build and start containers:**

```bash
docker compose up -d --build
```

4. **Access the app:**

| Service | URL |
|---|---|
| Frontend | `http://<EC2_PUBLIC_IP>` |
| API docs | `http://<EC2_PUBLIC_IP>/api/docs` |
| Health | `http://<EC2_PUBLIC_IP>/api/health` |

5. **View logs:**

```bash
docker compose logs -f backend
docker compose logs -f frontend
```

6. **Stop / restart:**

```bash
docker compose down          # stop
docker compose up -d         # start (no rebuild)
docker compose up -d --build # start with rebuild after code changes
```

---

## Atlas — Allow EC2 IP

1. Go to **MongoDB Atlas → Security → Network Access**
2. Click **Add IP Address**
3. Enter your EC2 public IP as `<IP>/32`
4. Save — takes ~30 seconds to apply

---

## Local Development (without Docker)

### Prerequisites

- Python 3.11+
- Node.js 18+
- MongoDB Atlas URI or local MongoDB 6+

### Backend

```bash
cd backend
pip install -r requirements.txt
cp .env.example .env          # edit MONGO_URI and ALLOWED_ORIGINS
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

- API: http://localhost:8000
- Swagger docs: http://localhost:8000/docs

### Frontend

```bash
cd frontend
npm install
npm run dev
```

- App: http://localhost:5173

> For local dev, set `ALLOWED_ORIGINS=http://localhost:5173` in `backend/.env`.

---

## Notes

- The frontend image bakes `VITE_API_URL=/api` at build time — no frontend `.env` needed for Docker deployment.
- nginx (port 80) proxies all `/api/` requests to the backend container on port 8000 — port 8000 does not need to be open in EC2.
- Uploaded files persist in the `backend/data/uploads/` volume mount defined in `docker-compose.yml`.
