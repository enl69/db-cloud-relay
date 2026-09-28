# DB Cloud Relay — Panduan Lengkap Pengembangan

> Dokumen ini untuk siapa pun yang melanjutkan pengembangan. Ditulis dari
> pengalaman membangun + debugging nyata, termasuk insiden yang sudah
> terjadi dan cara mencegahnya terulang.

## 1. Gambaran Besar

DB Cloud Relay adalah server sinkronisasi untuk plugin Obsidian "Cloud Relay"
(repo terpisah: `enl69/cloud-relay`). Keduanya menyinkronkan vault
Obsidian antar device secara **realtime per kata, offline-first, tanpa
konflik**.

```
┌─────────────┐  WebSocket (y-protocols style)  ┌──────────────┐  WebSocket  ┌─────────────┐
│  Obsidian    │ ◄────────────────────────────► │ DB Cloud Relay │ ◄─────────► │  Obsidian    │
│  Device A    │   binary frames                │   (server)    │            │  Device B    │
│  (plugin)    │ ◄────────────────────────────► │  Rust + yrs   │            │  (plugin)    │
└─────────────┘  HTTP (blob lampiran/pengaturan)└──────────────┘            └─────────────┘
```

**Prinsip desain yang HARUS dipertahankan:**

1. **Server menyimpan hasil akhir, bukan antrian.** Setiap note = dokumen
   CRDT (yrs) yang sudah menyatu. Update log di-squash jadi snapshot otomatis
   (tiap 200 update). Device join tidak memutar ulang riwayat — dia menerima
   state yang sudah jadi.
2. **Server sengaja "bego" soal isi.** Dia hanya: menerima update biner,
   meneruskan (broadcast) ke device lain yang online, menyimpan. Tidak ada
   logika bisnis isi catatan di server.
3. **CRDT = tidak ada konflik.** Semua update bisa datang dalam urutan apa
   pun dan hasilnya identik di semua device (merge per karakter, YATA —
   implementasi yrs kompatibel wire dengan Yjs di plugin).
4. **Server disposable.** Semua device menyimpan copy penuh. Server hilang →
   deploy ulang binary baru, device pertama yang connect otomatis mengisi
   ulang.

## 2. Stack & Struktur

- **Rust** (edition 2021) — `axum 0.8` (HTTP+WS), `yrs 0.21` (CRDT),
  `rusqlite 0.32` (SQLite bundled), `tokio`, `rand`, `sha2`,
  `futures-util`, `tracing`.
- Tiga file inti (`src/`):
  - `main.rs` — routing, auth, init, dashboard teks
  - `store.rs` — semua akses SQLite (satu struct `Store`, method jelas)
  - `sync.rs` — protokol WS, room broadcast, squash

## 3. Skema Database (SQLite, WAL)

```sql
meta     (key, value)                    -- admin_token
vaults   (id, token_hash, created_at, last_update)
notes    (vault_id, note_id, snapshot)   -- hasil squash, PK (vault_id, note_id)
updates  (vault_id, note_id, seq AI, data) -- log update sebelum disquash
blobs    (sha, data, size)               -- lampiran & file .obsidian (content-addressed)
```

- `token_hash` = SHA-256 hex dari vault token (token asli tidak disimpan).
- `last_update` = unix detik, di-update tiap append/squash — dipakai plugin
  untuk menampilkan "update terakhir X menit lalu" sebelum join.
- Migrasi aman: `init_db` pakai `CREATE IF NOT EXISTS` + `ALTER TABLE ...
  ADD COLUMN` yang errornya diabaikan (idempotent).

## 4. Protokol

### 4.1 Frame WS (binary)

```
[1 byte type][2 byte id_len BE][id bytes][payload bytes]
```

| type | Nama | Arah | Payload |
|---|---|---|---|
| 0 | DOC_LIST | server→client (saat connect) | daftar note_id (u16-len prefixed) |
| 1 | SYNC_STEP1 | dua arah | state vector (yrs/Yjs) |
| 2 | SYNC_STEP2 | dua arah | update diff (encode_state_as_update(sv)) |
| 3 | UPDATE | dua arah | update increment (broadcast) |
| 254 | PING | client→server | kosong |
| 255 | PONG | server→client | echo payload |

Catatan penting:
- ID kosong (`""`) dipakai untuk DOC_LIST; ID khusus `__attachments__` dan
  `__hiddens__` (dibuat plugin, bukan server) untuk metadata lampiran/
  pengaturan — server tidak perlu tahu artinya, dia perlakukan sama saja.
- **Jangan kirim state vector kosong (0 byte)** dari client. Selalu pakai
  `Y.encodeStateVector(new Y.Doc())` (≡ SV identitas) kalau mau minta semua.
  Decode SV 0-byte gagal diam-diam di server.

### 4.2 HTTP API

| Endpoint | Fungsi | Auth |
|---|---|---|
| `GET /` | dashboard teks (vault count, notes) | — |
| `GET /healthz` | health check + cek DB | — |
| `POST /v1/vaults` | buat vault → `{vault_id, token}` | header `x-admin-token` |
| `POST /v1/vaults/{id}/reset?token=` | hapus SEMUA catatan vault + reset room | token vault |
| `GET /v1/vaults/{id}/info?token=` | `{last_update, notes}` | token vault |
| `GET /v1/vaults/{id}/ids?token=` | `{count, note_ids[]}` | token vault |
| `PUT /v1/blobs/{sha}?token=` | upload blob (dedup via `ON CONFLICT DO NOTHING`) | token vault mana pun |
| `GET /v1/blobs/{sha}?token=` | download blob | token vault mana pun |
| `WS /sync/{vault_id}?token=` | kanal sync utama | token vault |

Admin token dicetak di log saat pertama jalan (`ADMIN_TOKEN=...`, satu
baris agar mudah di-grep) dan disimpan di tabel `meta` — tidak berubah.

### 4.3 Alur koneksi

```
client connect → auth token → server kirim DOC_LIST (semua note_id vault)
client (untuk tiap id) → SYNC_STEP1(state vector lokal)
server → SYNC_STEP2(diff yang client tidak punya)
server juga → SYNC_STEP1(SV server) → client balas STEP2 (server persist)
tiap update → server: apply ke Doc in-memory, persist (updates), broadcast
              ke semua koneksi lain (conn_id di frame broadcast = pengirim,
              dilewati oleh writer task)
```

- **Room**: `VaultRoom` per vault = `HashMap<note_id, Arc<Mutex<Doc>>>` +
  `broadcast::channel(1024)`. Koneksi subscribe channel; lag → log warning
  (update hilang untuk client itu, tapi dia akan catch-up saat reconnect
  via state vector — aman secara desain).
- **Squash**: setelah `append_update`, kalau update pending note itu ≥ 200 →
  encode state penuh → simpan ke `notes.snapshot` → hapus `updates` note itu.
- **Reset vault** menghapus notes+updates (bukan blobs, tidak apa — blob
  content-addressed aman) + mengosongkan room in-memory.

## 5. Menjalankan & Deployment

```bash
# dev
RELAY_DATA_DIR=./data PORT=18080 cargo run
curl localhost:18080/healthz        # → "ok"
docker logs | grep ADMIN_TOKEN      # (produksi)

# produksi (server user)
git clone https://github.com/enl69/db-cloud-relay.git
cd db-cloud-relay && docker compose up -d   # port 1111, data di ./data

# update (cara resmi saat ini)
git pull && docker compose up -d --build
```

- Environment: `RELAY_DATA_DIR` (default `/data`), `PORT` (default `8080`).
  Container: port `1111:8080`, volume `./data:/data`, user non-root.
- **Backup = folder `./data` saja** (satu file SQLite WAL).
- Eksposur publik via Cloudflare Tunnel (WS didukung native). CATATAN dari
  insiden: **Cloudflare Tunnel membuat koneksi mati tetap tampak OPEN dari
  sisi client** → plugin lama diam selamanya. Karena itu ada heartbeat
  PING/PONG 15 detik + watchdog 35 detik di plugin. Kalau mengubah
  interval, jaga watchdog > 2× ping.

## 6. Test E2E

`scripts/e2e/` (Node ≥22, `npm i` sekali, butuh Yjs):

```bash
# server jalan di :18080
VID=$(curl -s -X POST localhost:18080/v1/vaults -H "x-admin-token: $ADMIN" | jq -r .vault_id)
VTOK=$(... token ...)
RELAY_PORT=18080 node push.mjs        $VID $VTOK note-a "isi" path/file.md
RELAY_PORT=18080 node pull-verify.mjs $VID $VTOK note-a "isi" path/file.md
# harus keluar: PASS: sync bekerja — client B menerima data client A via server
```

Kedua script adalah "client Yjs minimal" — berguna juga sebagai contoh
protokol jika menulis client baru.

## 7. Keputusan Desain Penting (jangan dilanggar tanpa alasan kuat)

| Keputusan | Alasan |
|---|---|
| Tidak ada akun user | personal/self-host; vault ID unguessable + token = capability |
| Blob content-addressed (SHA-256) | dedup otomatis; identik lintas vault tidak menumpuk |
| `DO NOTHING` on conflict blob | idempotent; retry aman |
| Token hash disimpan, bukan token | DB bocor ≠ akses bocor |
| Squash otomatis di server | mencegah bloat (lihat §9 insiden) |
| CORS `*` di `/v1/*` | plugin Obsidian fetch via `app://obsidian.md` origin; preflight OPTIONS 204 |

### ATURAN KIBLAT SERVER (v0.3.5 — invariant tertinggi, keputusan user)

> **Server adalah kiblat. 1 path = 1 note. Dilarang dobel. Dilarang data lama.**

Semua fix dobel sebelum v0.3.5 ada di CLIENT — device lama / index basi
tetap bisa mengotori server (5 insiden). Mulai v0.3.5 server MENEGAKKAN
sendiri, dari device mana pun asalnya:

1. **Tolak klaim path yang sudah dimiliki** (`sync.rs`, arm STEP2/UPDATE):
   update dengan `meta.path` milik note-id lain DITOLAK, tidak pernah
   di-apply / disimpan / disiarkan. Server membalas **state penuh pemilik
   sah** (`MSG_UPDATE` dengan note-id pemilik) — plugin 0.13.8+ mengadopsi
   ID kanonik; plugin lama diabaikan tapi server tetap bersih.
2. **Atomik via `write_lock`** (per room): urutan cek-pemilik → terap →
   simpan → klaim path dijalankan eksklusif antar semua koneksi — dua
   device yang klaim path baru yang sama hampir bersamaan tidak bisa
   lolos berdua (race tertutup).
3. **Cache `path_owner`** (path → note-id) di `VaultRoom`: sumber kebenaran
   penegakan in-memory; dibangun dari store (`build_path_index`, urutan:
   `notes` rowid dulu lalu `updates` MIN(seq) = "pertama dilihat server"
   menang — semantik server-first). Pemeliharaan rename/delete via
   `claim_path`.
4. **Sweep self-healing**: `dedup_paths` (a) hapus note-id kalah untuk path
   yang punya >1 pemilik, (b) hapus hantu (note tanpa path DAN tanpa isi,
   bukan `__metadata__`). Dijalankan: **saat boot** (semua vault), **tiap
   60 detik** (sweeper; kalau ada perubahan → rebuild room + generation
   bump supaya koneksi basi putus dan klien menarik DOC_LIST bersih), dan
   **endpoint `POST /v1/vaults/{id}/dedup`** (manual).
5. Baris kalah DIHAPUS (bukan tombstone): penegakan runtime menolak
   pengiriman ulang ID kalah, jadi tidak bisa muncul kembali. Device
   pemilik ID kalah membersihkan dirinya via rejoin "ganti total".

Pitfall yang pernah ada di draft (jangan ulangi): `find_path_owner` lama
memegang lock koneksi sambil memanggil `load_doc_blobs` (lock lagi) =
**deadlock nested-lock**. Aturan: KALAU scan butuh decode per-note,
kumpulkan ID dulu, LEPAS lock, baru `load_doc_blobs`.

Uji: `npm run e2e-kiblat` di repo plugin (7 test: tolak dobel + balas
pemilik + idempoten + race + sweep warisan + device sah selamat).

## 8. Roadmap / Pekerjaan Lanjutan

1. **Rencana ops (disetujui, ditunda sampai stabil)**: GitHub Actions
   build+push ke `ghcr.io/enl69/db-cloud-relay` + Watchtower di server
   → auto-update image, tanpa build di server.
2. Multi-vault UI (endpoint sudah siap; plugin saat ini 1 vault aktif).
3. GC blob (hapus blob tak tersisa direferensikan catatan mana pun) —
   sekarang blob reset-vault tidak dihapus (aman tapi tumbuh).
4. Quota/rate-limit per vault untuk rilis publik.
5. Metrik (jumlah koneksi, throughput) di dashboard `/`.
6. E2E untuk blob & reset (harness push/pull belum menutupi).

## 9. Riwayat Insiden (pelajaran)

| Insiden | Akar | Pencegahan |
|---|---|---|
| Bloat 272MB di device (plugin) | blob per-ketikan | debounce 3s + squash (plugin side persist) |
| Duplikasi isi masal (x520) | race plugin (lihat repo plugin) | self-write guard mtime |
| "Koneksi zombie" setelah restart server | CF Tunnel keepalive palsu | heartbeat PING/PONG + watchdog |
| 401 massal saat setup | user salah paste admin token | log `ADMIN_TOKEN=` satu baris |

## 10. Konvensi

- Bahasa log/UI server: campuran EN/ID (produk personal); komentar kode
  minimal, nama variabel jelas.
- Commit message: satu baris ringkas, jelaskan "apa+kenapa" kalau nontrivial.
- Versioning: `0.MINOR.PATCH`, naik minor untuk fitur, patch untuk fix.
  Rekan plugin harus kompatibel (lihat plugin README).
