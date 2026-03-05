# Geofence Web (Next.js)

Hello-world UI that lists all rows from `tbl_vworkjobs` in the geodata PostgreSQL DB. Connects directly via **pg**; credentials from env/secrets (e.g. **DATABASE_URL** or **PGHOST** + **PGPASSWORD**), no hardcoded credentials.

---

## Local dev – two ways to reach Cloud SQL

The app reads **DATABASE_URL** from `process.env` (e.g. `.env.local`). You can connect either **directly** to the Cloud SQL public IP or via the **Cloud SQL Auth Proxy**.

### Option A: Direct connection (no proxy)

Use this if your machine’s IP is in Cloud SQL **Authorized networks** (same DB as production at **35.197.176.76**).

1. **Set `.env.local`** (same password as Secret Manager `PG_PASSWORD`):
   ```bash
   cp .env.local.example .env.local
   # Edit .env.local: DATABASE_URL=postgresql://geofence:YOUR_PASSWORD@35.197.176.76:5432/geodata?schema=public
   ```
2. **Run the dev server:** `npm run dev`
3. **Check:** http://localhost:3000/api/health/db → `{"ok":true}`

No proxy process to run.

### Option B: Via Cloud SQL Auth Proxy

Use this if you don’t want to add your IP to authorized networks. The proxy creates a secure tunnel; the app connects to `127.0.0.1:5433` and the proxy talks to Cloud SQL.

1. **Start the proxy** (Terminal 1):
   ```bash
   C:\tools\cloud-sql-proxy.exe "cel-geosystem:australia-southeast1:geofence" --port 5433
   ```
2. **Set `.env.local`:**  
   `DATABASE_URL=postgresql://geofence:YOUR_PASSWORD@127.0.0.1:5433/geodata?schema=public`
3. **Run the dev server** (Terminal 2): `npm run dev`
4. **Check:** http://localhost:3000/api/health/db

### After either option

- **http://localhost:3000** – main page  
- **http://localhost:3000/api/vworkjobs** – raw data  
- **http://localhost:3000/api/debug** – env check  

Change code → save → refresh; no deploy needed.

---

## Local (quick reference)

**Direct (no proxy):**  
`.env.local`: `DATABASE_URL=postgresql://geofence:PASSWORD@35.197.176.76:5432/geodata?schema=public`  
Then: `npm run dev` → open /api/health/db.

**Via proxy:**  
Start proxy on 5433 → `.env.local` with `127.0.0.1:5433` → `npm run dev` → /api/health/db.

## Firebase App Hosting – DB access (step by step)

Do this in **Google Cloud / Firebase Console**, not in code. The app already uses `PGHOST=/cloudsql/cel-geosystem:australia-southeast1:geofence` in `apphosting.yaml`.

### Step 1: Create the App Hosting backend and connect GitHub

1. Open [Firebase Console](https://console.firebase.google.com) → your project (**cel-geosystem**).
2. Go to **Build** → **App Hosting** → **Create backend** (or **Get started** if first time).
3. Connect **GitHub** and choose repo [hslabs99/geofence](https://github.com/hslabs99/geofence).
4. Set **App root directory** to `web` (because the Next app lives in the `web` folder).
5. Choose **region** (e.g. same as Cloud SQL: australia-southeast1 if available).
6. Finish backend creation. Note the **backend name** (e.g. `geofence-web`).

### Step 2: Attach Cloud SQL to the backend

App Hosting runs on Cloud Run. You must attach the Cloud SQL instance so the app can use the Unix socket.

1. Open [Google Cloud Console](https://console.cloud.google.com) → same project **cel-geosystem**.
2. Go to **Cloud Run** → select the **service** that App Hosting created (name often matches your backend).
3. Click **Edit & deploy new revision**.
4. Open the **Connections** tab (or **Cloud SQL** / **Connections** depending on UI).
5. Click **Add connection** → choose **Cloud SQL**.
6. Select instance: **cel-geosystem:australia-southeast1:geofence**.
7. Deploy the new revision (or **Save** if it only updates config).

Alternative (if your Firebase/App Hosting UI has it): In **App Hosting** → your backend → **Settings** or **Resources**, look for **Cloud SQL** or **Connections** and add the same instance there.

### Step 3: Grant the backend’s service account access to Cloud SQL

1. In **Cloud Run** → your App Hosting service → **Security** (or **Permissions**) tab.
2. Note the **Service account** (e.g. `...@apphosting-....iam.gserviceaccount.com` or a default Compute one).
3. Go to **IAM & Admin** → **IAM** in Cloud Console.
4. Find that service account (or add it): click **Grant access**.
5. **New principals:** paste the service account email.
6. **Role:** **Cloud SQL Client**.
7. Save.

### Step 4: Ensure the DB password is available to App Hosting

1. In **Secret Manager**, ensure secret **PG_PASSWORD** exists (same as for the import service).
2. In Firebase: **App Hosting** → your backend → **Settings** (or **Secrets / Environment**).
3. Add or confirm env:
   - **PGPASSWORD** → from secret **PG_PASSWORD** (or reference it in `apphosting.yaml`; you already have `secret: PG_PASSWORD` there).

If App Hosting uses `apphosting.yaml` for secrets, ensure the backend’s service account has **Secret Manager Secret Accessor** on `PG_PASSWORD` (or use the Firebase CLI `firebase apphosting:secrets:set` flow so it grants access automatically).

### Step 5: Deploy

Push to the **live branch** you chose for the backend, or trigger a **Rollout** from App Hosting. The app will connect with `PGHOST=/cloudsql/cel-geosystem:australia-southeast1:geofence` and the Unix socket; no IP allowlisting needed.

---

## Debugging the live app

When the live URL shows an error, use this every time:

### 1. See the real error on the page

After the next deploy, the home page will show the **API error message** (e.g. `connection refused`, `password authentication failed`) instead of generic "Internal Server Error".

### 2. Hit the API directly (any deploy)

Open in the browser:

- **DB data:**  
  https://geofence--cel-geosystem.us-east4.hosted.app/api/vworkjobs  
  You get JSON: either `{ rows: [...] }` or `{ error: "actual error message" }`.

- **Env check (no secrets):**  
  https://geofence--cel-geosystem.us-east4.hosted.app/api/debug  
  You get: `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD_set`, `NODE_ENV`. Use this to confirm env is loaded (e.g. `PGHOST` should start with `/cloudsql/`, `PGPASSWORD_set` should be `true`).

### 3. Cloud Run logs

1. [Cloud Run](https://console.cloud.google.com/run) → select the App Hosting service (e.g. `geofence`).
2. Open **Logs** (or **Logging**).
3. Filter by severity **Error** or search for `vworkjobs` / `ECONNREFUSED` / `password`.

### 4. Checklist when DB errors persist

- [ ] Cloud SQL instance **cel-geosystem:australia-southeast1:geofence** is attached to this Cloud Run service (Connections tab).
- [ ] Service account `firebase-app-hosting-compute@cel-geosystem.iam.gserviceaccount.com` has **Cloud SQL Client**.
- [ ] Same account has **Secret Manager Secret Accessor** on **PG_PASSWORD**.
- [ ] `/api/debug` shows `PGPASSWORD_set: true` and `PGHOST` like `/cloudsql/...`.
- [ ] If you use `apphosting.yaml` only, ensure the backend was rolled out after the last change so env/secrets are applied.
