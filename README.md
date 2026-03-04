# Geodata Import Service

Node.js TypeScript service for Google Cloud Run that imports `.xlsx` and `.csv` files from a Google Drive folder into Cloud SQL PostgreSQL.

## Endpoints

- **GET /health** — Returns `{ ok: true }`
- **POST /run-import** — Imports new files from the configured Drive folder

## Environment Variables

The app reads all config from `process.env`. **Do not hardcode any credentials in code.**

| Variable | Source | Description |
|----------|--------|-------------|
| `PORT` | Auto (8080) | HTTP port |
| `PGHOST` | Env | `/cloudsql/cel-geosystem:australia-southeast1:geofence` (Cloud SQL Unix socket) |
| `PGPORT` | Env | 5432 |
| `PGDATABASE` | Env | geodata |
| `PGUSER` | Env | geofence |
| `PGPASSWORD` | Secret Manager (`PG_PASSWORD`) | Database password |
| `DRIVE_FOLDER_ID` | Secret Manager (`DRIVE_FOLDER_ID`) | Google Drive folder ID (aaGeoData folder) |

## Build & Run

```bash
npm install
npm run build
npm start
```

## Deploy to Cloud Run

Secrets `PG_PASSWORD` and `DRIVE_FOLDER_ID` are already in Google Secret Manager.

```bash
gcloud run deploy geodata-import \
  --source . \
  --region australia-southeast1 \
  --allow-unauthenticated \
  --service-account ce-drive-importer@cel-geosystem.iam.gserviceaccount.com \
  --set-env-vars "PGHOST=/cloudsql/cel-geosystem:australia-southeast1:geofence,PGPORT=5432,PGDATABASE=geodata,PGUSER=geofence" \
  --set-secrets "PGPASSWORD=PG_PASSWORD:latest,DRIVE_FOLDER_ID=DRIVE_FOLDER_ID:latest" \
  --add-cloudsql-instances cel-geosystem:australia-southeast1:geofence
```

- **Service account** `ce-drive-importer@cel-geosystem.iam.gserviceaccount.com` has Editor access to the Drive folder (ADC used in Cloud Run).
- **Cloud SQL** connects via Unix socket inside Cloud Run.

## Database Setup

Ensure these objects exist:

- Sequence: `seq_vwork_batch`
- Sequence: `seq_gpsdata_batch` (for GPS import batchnumber)
- Table: `tbl_vworkjobs` with columns including `job_id` (unique), `raw_row` (jsonb), and mapped columns
- Table: `import_file` with `drive_file_id` (unique), `filename`, `status`, `row_count`, `batchnumber`, `error_message`
