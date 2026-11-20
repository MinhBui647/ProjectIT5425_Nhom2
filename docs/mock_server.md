# Mock API đa nguồn

Mock phục vụ raw handoff S1–S5, không giả lập toàn bộ upstream protocol. Không có
SCADA/MES production connector. Source names là tên bảng; USDA Bronze-only.

## Chạy riêng

```bash
PYTHONPATH=src STORAGE_DEMO_ROOT=/tmp/storage_demo \
  .venv/bin/python -m lakehouse_storage.mock_server
```

Mặc định `127.0.0.1:8010`; `MOCK_HOST`, `MOCK_PORT`, `STORAGE_DEMO_ROOT` điều khiển
bind/payload root. Trong Compose mới: host `127.0.0.1:28010`, nội bộ `mock:8010`.
Swagger `/docs`, OpenAPI `/openapi.json`, readiness `/health`.

## Tạo và replay

```bash
curl -fsS http://127.0.0.1:28010/runs \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: demo-full-001' \
  -d '{"sources":["mes_lims","sensor_telemetry","market_prices","market_indices","weather","food_recalls","market_reports"],"samples_per_batch":80,"batches":2,"fao_variant":"both","start_time":"2026-01-15T00:00:00Z","end_time":"2026-01-16T00:00:00Z"}'
# Use run_id returned by POST:
curl -fsS http://127.0.0.1:28010/runs/RUN_ID
curl -fsS http://127.0.0.1:28010/runs/RUN_ID/batches/0
```

POST returns 201, manifest_version=2, run_id, request, batch_count, batches.
Each entry has sequence, table_name, source, schema_variant, window,
payload_sha256, bronze_only. GET batch adds data and run_id. Digest is SHA-256
of canonical raw rows JSON, not HTTP bytes or Parquet bytes. Bronze checksum is
computed separately after serialization. Default request still returns MES at
sequence 0 and sensor at sequences 1..batches. Full mode must consult manifest.

Same key/config returns identical run after server restart or response loss;
same key/different config returns 409. No key creates a new UUID. Create is
serialized by a local filesystem lock. Interrupted staging needs review;
GET cannot see a staging directory. Payloads persist on payload volume.

| Table | Raw shape | Target |
|---|---|---|
| mes_lims | production timestamps string, chemistry float64, flag integer | Silver |
| sensor_telemetry | 1 Hz, timestamps string, measures float64, linked MES batch | Silver |
| market_prices | GDT EventNumber/EventDate/ProductGroupCode/string price | Silver |
| market_indices | FAO long; wide six index columns as strings | Silver |
| weather | five farms, temperature float64, humidity integer | Silver; THI from P3 |
| food_recalls | FDA recall_number/report_date YYYYMMDD/classification | Silver |
| market_reports | USDA prices/commentary sections with different shape | Bronze only |

FAO period_start represents its containing calendar month; an event source and
monthly index have different cadence. Mock emits finite representative handoffs,
not a complete history of every upstream event in the requested window.

## Controls and failures

Seed 0..2^32-1, samples_per_batch 1..50,000, batches 1..200, total sensor samples
<=1,000,000. Response <=32 MiB. Sources unique/nonempty/allowlisted. Start/end
normalize to UTC; sensor samples must fit the window. `fao_variant` is long,
wide or both; both emits separate entries. USDA emits two section entries.

`scenario`: normal, invalid-value, invalid-time, missing-field, transient-500,
delay. `fault_table` defaults sensor_telemetry and can select another table.
Value/time/missing faults change raw fields; P3 or Bronze rejects them rather
than the API silently repairing them. Delivery faults affect sequences after 0:
`failure_attempts` 0..5, `delay_seconds` 0..5. Counters persist after restart.
Normal/replay are controls for fault experiments.

422 = invalid request controls; 404 = unknown run/sequence; 409 = key/config or
staging conflict; 503 = create busy/failure; scripted 500 = bounded transient.
Client retries timeout/network/429/5xx only; 404/payload/schema failures stop.
Keep payload volumes when stopping services; never use `down -v` to restart.
