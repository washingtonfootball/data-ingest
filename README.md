# data-ingest

Source-system ingest scripts that land raw data into ADLS bronze for downstream Databricks merge/transform jobs.

## Layout

| Directory | Source | Destination |
|-----------|--------|-------------|
| `dynamics/`  | Microsoft Dynamics 365 (CRM) | `commandersdatabricks/dynamics` |
| `e15/`       | Cheq POS + Mashgin POS       | `commandersdatabricks/e15` |
| `eloqua/`    | Oracle Eloqua (marketing)    | `commandersdatabricks/eloqua` |
| `emplifi/`   | Emplifi social analytics     | `commandersdatabricks/emplifi` |
| `fortress/`  | Fortress GB ticketing        | `commandersdatabricks/fortress` |
| `qualtrics/` | Qualtrics surveys            | `commandersdatabricks/qualtrics` |
| `seatgeek/`  | SeatGeek / Ringside          | `commandersdatabricks/seatgeek` |
| `vof/`       | Forsta (Voice of the Fan)    | `commandersdatabricks/vof` |

## Pattern

Each directory contains a self-contained `ingest.py` and `.env` (gitignored).

```
ingest.py  →  ADLS bronze/{table}/pending/{timestamp}.json (NDJSON)
                        └─ Databricks job picks up, MERGEs to Delta, archives
```

Cursors / processed-file trackers are persisted in the same blob container so
runs are resume-safe.

## Running

```bash
cd <source>
python3 ingest.py
```

Most scripts trigger a downstream Databricks job at the end via
`DATABRICKS_HOST` / `DATABRICKS_TOKEN` / `DATABRICKS_JOB_ID` env vars.

## Scheduling

- Airflow DAGs (Dynamics, Qualtrics, Emplifi, SeatGeek, VOF, Fortress) live in
  `~/airflow/dags/` and exec these scripts directly.
- Emplifi runs weekly via cron.

## Secrets

`.env` files are **not** committed. Required keys per source are referenced at
the top of each `ingest.py`.
