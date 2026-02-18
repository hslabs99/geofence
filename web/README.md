# Geofence Web (Next.js)

Hello-world UI that lists all rows from `tbl_vworkjobs` in the geodata PostgreSQL DB.

## Local

1. Copy env and set DB credentials (use Cloud SQL Proxy for Cloud DB):
   ```bash
   cp .env.local.example .env.local
   # Edit .env.local: PGHOST=127.0.0.1 if using Cloud SQL Proxy
   ```

2. Run dev server:
   ```bash
   npm run dev
   ```
   Open [http://localhost:3000](http://localhost:3000).

## Firebase App Hosting

1. Connect the GitHub repo ([hslabs99/geofence](https://github.com/hslabs99/geofence)) in [Firebase Console](https://console.firebase.google.com) → Build → App Hosting → Create backend.

2. Set **App root directory** to `web` (if the repo root is the parent of this folder).

3. Add Cloud SQL to the backend (same instance as import service):  
   In Google Cloud Console, ensure the App Hosting Cloud Run service has the Cloud SQL instance attached and has the same `PG_PASSWORD` secret. Configure VPC/Cloud SQL as needed for private DB access.

4. `apphosting.yaml` in this folder configures env and `PGPASSWORD` from Secret Manager (`PG_PASSWORD`). Adjust `PGHOST` if using public IP instead of Unix socket.

5. Deploy by pushing to the live branch; or trigger a rollout from the App Hosting dashboard.
