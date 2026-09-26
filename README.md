# DB Cloud Relay

Server sinkronisasi Cloud Relay (Rust: axum + yrs + SQLite). Single binary / Docker.

> **Melanjutkan pengembangan?** Baca [`DEVELOPMENT.md`](DEVELOPMENT.md) — arsitektur, protokol, skema DB, invariants, riwayat insiden, dan roadmap.

## Install di server

```bash
git clone https://github.com/enlnlnl79/db-cloud-relay.git
cd db-cloud-relay
docker compose up -d        # jalan di port 1111, data di ./data
```

## Update

```bash
git pull
docker compose up -d --build
```

Expose via Cloudflare Tunnel → `dbcloudrelay.enlnlnl79.my.id → localhost:1111`.
Backup cukup folder `./data`.

## Dev lokal

```bash
RELAY_DATA_DIR=./data PORT=18080 cargo run
curl localhost:18080/healthz
```

Log mencetak admin token saat pertama jalan — simpan untuk membuat vault pertama.

## E2E test

```bash
cd scripts/e2e && npm install   # sekali
# server jalan di :18080
VID=...; VTOK=...               # dari curl -X POST /v1/vaults -H "x-admin-token: …"
node scripts/e2e/push.mjs $VID $VTOK note-a "hello world" halo.md
node scripts/e2e/pull-verify.mjs $VID $VTOK   # harus PASS
```
