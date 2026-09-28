mod store;
mod sync;

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use axum::{
    extract::{
        ws::WebSocketUpgrade,
        Path, Query, State,
    },
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    middleware::{self, Next},
    body::Bytes,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Serialize;
use tracing::info;

use store::Store;
use sync::VaultRoom;

struct AppState {
    store: Arc<Store>,
    rooms: Mutex<HashMap<String, Arc<VaultRoom>>>,
    conn_counter: AtomicU64,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "dbcloudrelay=info".into()),
        )
        .init();

    let data_dir = std::env::var("RELAY_DATA_DIR").unwrap_or_else(|_| "/data".to_string());
    std::fs::create_dir_all(&data_dir).expect("failed to create data dir");
    let db_path = format!("{data_dir}/relay.db");

    let store = Arc::new(Store::open(&db_path));
    let admin_token = store.ensure_admin_token();
    info!(%db_path, "database ready");
    info!(token = %admin_token, "admin token — simpan baik-baik, dipakai untuk membuat vault pertama");
    println!("ADMIN_TOKEN={admin_token}");

    let state = Arc::new(AppState {
        store,
        rooms: Mutex::new(HashMap::new()),
        conn_counter: AtomicU64::new(1),
    });

    // ═══ KIBLAT: bersihkan warisan dobel/hantu saat boot (self-healing).
    // Server adalah kiblat — state yang tidak sah tidak boleh bertahan
    // melewati restart, dari device mana pun asalnya. ═══
    {
        let legacy: Vec<String> = {
            let conn = state.store.conn.lock().unwrap();
            let mut stmt = conn.prepare("SELECT id FROM vaults").unwrap();
            stmt.query_map([], |r| r.get::<_, String>(0))
                .unwrap()
                .flatten()
                .collect()
        };
        for vault_id in legacy {
            let removed = state.store.dedup_paths(&vault_id);
            if removed > 0 {
                info!(%vault_id, removed,
                      "kiblat boot-sweep: dobel/hantu path warisan dihapus");
            }
        }
    }

    // ═══ KIBLAT: sweeper periodik — jaring pengaman untuk data yang lolos
    // dari era sebelum aturan, atau regresi di masa depan. Setiap 60 detik
    // server menegaskan sendiri invariant 1 path = 1 note. ═══
    {
        let store = state.store.clone();
        let rooms_state = state.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                let vaults: Vec<String> = {
                    let conn = store.conn.lock().unwrap();
                    let Ok(mut stmt) = conn.prepare("SELECT id FROM vaults") else {
                        continue;
                    };
                    let rows: Vec<String> = match stmt.query_map([], |r| r.get::<_, String>(0)) {
                        Ok(rows) => rows.flatten().collect(),
                        Err(_) => continue,
                    };
                    rows
                };
                for vault_id in vaults {
                    if let Some(room) = rooms_state.rooms.lock().unwrap().get(&vault_id) {
                        let removed = room.sweep(&store, &vault_id);
                        if removed > 0 {
                            info!(%vault_id, removed,
                                  "kiblat sweep: dobel/hantu path dibersihkan");
                        }
                    } else {
                        // room belum ada → tidak ada klien aktif; bersihkan
                        // langsung di store, room berikutnya membangun indeks
                        // dari state yang sudah bersih
                        let removed = store.dedup_paths(&vault_id);
                        if removed > 0 {
                            info!(%vault_id, removed,
                                  "kiblat sweep (idle): dobel/hantu path dibersihkan");
                        }
                    }
                }
            }
        });
    }

    let app = Router::new()
        .route("/", get(dashboard))
        .route("/healthz", get(healthz))
        .route("/v1/vaults", post(create_vault))
        .route("/v1/vaults/{vault_id}/reset", post(reset_vault))
        .route("/v1/vaults/{vault_id}/info", get(vault_info))
        .route("/v1/vaults/{vault_id}/ids", get(vault_ids))
        .route("/v1/vaults/{vault_id}/counts", get(vault_counts))
        .route("/v1/vaults/{vault_id}/dedup", post(vault_dedup))
        .route("/v1/blobs/{sha}", get(get_blob).put(put_blob))
        .route("/sync/{vault_id}", get(sync_ws))
        .layer(middleware::from_fn(cors_mw))
        .with_state(state);

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("failed to bind port");
    info!(%port, "DB Cloud Relay listening");
    axum::serve(listener, app).await.expect("server error");
}

async fn dashboard(State(state): State<Arc<AppState>>) -> String {
    // kumpulkan ringkasan vault dalam scope lock sendiri (JANGAN pegang lock
    // saat memanggil count_real_notes — deadlock)
    let vault_rows: Vec<(String, i64)> = {
        let conn = state.store.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT id, last_update FROM vaults ORDER BY created_at")
            .expect("failed to read vault summary");
        stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .expect("failed to query vault summary")
        .filter_map(|r| r.ok())
        .collect()
    };

    let mut output = String::from("Database Cloud Relay berhasil berjalan.\n\n");
    output.push_str("Status: OK\n");
    let mut total_notes = 0_i64;
    let mut total_vaults = 0_i64;
    output.push_str("Vault detail:\n");
    for (id, last_update) in vault_rows {
        let notes: i64 = state.store.count_real_notes(&id);
        total_vaults += 1;
        total_notes += notes;
        let short_id = &id[..id.len().min(8)];
        let updated = if last_update == 0 {
            "belum ada update".to_string()
        } else {
            last_update.to_string()
        };
        output.push_str(&format!(
            "- vault {} | notes: {} | last_update_unix: {}\n",
            short_id, notes, updated
        ));
    }
    output.push_str(&format!(
        "\nTotal vault: {}\nTotal notes semua vault: {}\n",
        total_vaults, total_notes
    ));
    output.push_str("Catatan: angka di atas adalah gabungan semua vault; gunakan Cloud Relay > Cek sinkronisasi untuk detail vault aktif.\n");
    output
}

async fn healthz(State(state): State<Arc<AppState>>) -> &'static str {
    let conn = state.store.conn.lock().unwrap();
    match conn.query_row("SELECT 1", [], |_| Ok(())) {
        Ok(_) => "ok",
        Err(_) => "db error",
    }
}

#[derive(Serialize)]
struct CreateVaultResponse {
    vault_id: String,
    token: String,
}

async fn create_vault(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<CreateVaultResponse>, StatusCode> {
    let admin = headers
        .get("x-admin-token")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !state.store.admin_token_matches(admin) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let (vault_id, token) = state.store.create_vault();
    info!(%vault_id, "vault created");
    Ok(Json(CreateVaultResponse { vault_id, token }))
}

async fn reset_vault(
    Path(vault_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Result<StatusCode, StatusCode> {
    let token = params.get("token").cloned().unwrap_or_default();
    if !state.store.vault_token_valid(&vault_id, &token) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    state.store.reset_vault(&vault_id);
    if let Some(room) = state.rooms.lock().unwrap().get(&vault_id) {
        room.reset();
    }
    info!(%vault_id, "vault reset (semua catatan di server dihapus)");
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Serialize)]
struct VaultInfoResponse {
    last_update: i64,
    notes: i64,
}

async fn vault_info(
    Path(vault_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<VaultInfoResponse>, StatusCode> {
    let token = params.get("token").cloned().unwrap_or_default();
    if !state.store.vault_token_valid(&vault_id, &token) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let (last_update, notes) = state.store.vault_info(&vault_id);
    Ok(Json(VaultInfoResponse { last_update, notes }))
}

#[derive(Serialize)]
struct VaultIdsResponse {
    count: usize,
    note_ids: Vec<String>,
}

async fn vault_ids(
    Path(vault_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<VaultIdsResponse>, StatusCode> {
    let token = params.get("token").cloned().unwrap_or_default();
    if !state.store.vault_token_valid(&vault_id, &token) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let note_ids = state.store.list_notes(&vault_id);
    Ok(Json(VaultIdsResponse {
        count: note_ids.len(),
        note_ids,
    }))
}

async fn cors_mw(req: axum::extract::Request, next: Next) -> Response {
    let is_options = req.method() == Method::OPTIONS;
    let mut res = if is_options {
        StatusCode::NO_CONTENT.into_response()
    } else {
        next.run(req).await
    };
    let h = res.headers_mut();
    h.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    h.insert(header::ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("GET,PUT,POST,OPTIONS"));
    h.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("content-type"));
    h.insert(header::ACCESS_CONTROL_MAX_AGE, HeaderValue::from_static("86400"));
    res
}

async fn put_blob(
    Path(sha): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
    body: Bytes,
) -> StatusCode {
    let token = params.get("token").cloned().unwrap_or_default();
    if !state.store.any_vault_token_valid(&token) {
        return StatusCode::UNAUTHORIZED;
    }
    state.store.put_blob(&sha, &body);
    StatusCode::CREATED
}

async fn get_blob(
    Path(sha): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Result<Response, StatusCode> {
    let token = params.get("token").cloned().unwrap_or_default();
    if !state.store.any_vault_token_valid(&token) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    match state.store.get_blob(&sha) {
        Some(data) => Ok((
            [(header::CONTENT_TYPE, "application/octet-stream")],
            data,
        )
            .into_response()),
        None => Err(StatusCode::NOT_FOUND),
    }
}


#[derive(Serialize)]
struct VaultCountsResponse {
    notes: i64,
    attachments: i64,
    folders: i64,
    devices: i64,
}

async fn vault_counts(
    Path(vault_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<VaultCountsResponse>, StatusCode> {
    let token = params.get("token").cloned().unwrap_or_default();
    if !state.store.vault_token_valid(&vault_id, &token) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let store = &state.store;
    let notes = store.count_real_notes(&vault_id);
    let attachments = store.count_map_entries(&vault_id, "__attachments__", "files");
    let folders = store.count_map_entries(&vault_id, "__folders__", "meta");
    let devices = store.count_map_entries(&vault_id, "__devices__", "meta");
    Ok(Json(VaultCountsResponse {
        notes,
        attachments,
        folders,
        devices,
    }))
}


/// Bersihkan pelanggaran kiblat yang SUDAH tersimpan (warisan era lama):
/// untuk tiap path dengan >1 note-id, pertahankan PEMILIK PERTAMA
/// (kanonik), hapus baris sisanya; hantu tanpa path+isi juga dihapus.
/// Sweep berjalan atomik terhadap apply klien via write_lock room.
async fn vault_dedup(
    Path(vault_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let token = params.get("token").cloned().unwrap_or_default();
    if !state.store.vault_token_valid(&vault_id, &token) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let removed = if let Some(room) = state.rooms.lock().unwrap().get(&vault_id) {
        room.sweep(&state.store, &vault_id)
    } else {
        state.store.dedup_paths(&vault_id)
    };
    info!(%vault_id, removed, "dedup selesai — server kini 1 path 1 note");
    Ok(Json(serde_json::json!({ "removed": removed })))
}

async fn sync_ws(
    ws: WebSocketUpgrade,
    Path(vault_id): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, StatusCode> {
    let token = params.get("token").cloned().unwrap_or_default();
    if !state.store.vault_token_valid(&vault_id, &token) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let room = {
        let mut rooms = state.rooms.lock().unwrap();
        rooms
            .entry(vault_id.clone())
            .or_insert_with(|| Arc::new(VaultRoom::new()))
            .clone()
    };
    // KIBLAT: pastikan cache path→owner terisi dari store sebelum koneksi
    // mulai bertukar frame (rebuild menaikkan generation → koneksi ini
    // merekam generation baru, bukan yang basi).
    room.ensure_indexed(&state.store, &vault_id);
    let store = state.store.clone();
    let conn_id = state
        .conn_counter
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(ws.on_upgrade(move |socket| {
        sync::handle_socket(socket, vault_id, room, store, conn_id)
    }))
}
