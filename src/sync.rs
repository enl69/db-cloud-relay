use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{broadcast, mpsc};
use tracing::{info, warn};
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::types::Map;
use yrs::{Doc, ReadTxn, StateVector, Transact, Update};

use crate::store::Store;

pub const MSG_DOC_LIST: u8 = 0;
pub const MSG_SYNC_STEP1: u8 = 1;
pub const MSG_SYNC_STEP2: u8 = 2;
pub const MSG_UPDATE: u8 = 3;
pub const MSG_PING: u8 = 254;
pub const MSG_PONG: u8 = 255;

const SQUASH_THRESHOLD: i64 = 200;

#[derive(Clone)]
pub struct BroadcastMsg {
    pub conn_id: u64,
    pub frame: Vec<u8>,
}

pub struct VaultRoom {
    docs: Mutex<HashMap<String, Arc<Mutex<Doc>>>>,
    tx: broadcast::Sender<BroadcastMsg>,
    /// generation bertambah setiap reset; koneksi lama (generation beda)
    /// berhenti memproses/menyiarkan frame — mencegah klien state-basi
    /// mengotori vault yang baru direset.
    generation: std::sync::atomic::AtomicU64,
    /// KIBLAT: cache path → note-id pemilik sah (1 path = 1 note).
    /// Sumber kebenaran tunggal penegakan aturan anti-dobel.
    path_owner: Mutex<HashMap<String, String>>,
    /// KIBLAT: membuat urutan cek-pemilik → terap → simpan → klaim jadi
    /// ATOMIK antar semua koneksi — menutup race dua device mengklaim
    /// path yang sama hampir bersamaan (keduanya lolos cek terpisah).
    write_lock: std::sync::Mutex<()>,
    /// true bila path_owner sudah diisi dari store (lazy init per room).
    indexed: std::sync::atomic::AtomicBool,
}

impl VaultRoom {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(1024);
        VaultRoom {
            docs: Mutex::new(HashMap::new()),
            tx,
            generation: std::sync::atomic::AtomicU64::new(1),
            path_owner: Mutex::new(HashMap::new()),
            write_lock: std::sync::Mutex::new(()),
            indexed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn reset(&self) {
        self.docs.lock().unwrap().clear();
        self.path_owner.lock().unwrap().clear();
        self.indexed
            .store(false, std::sync::atomic::Ordering::SeqCst);
        self.generation
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Isi ulang cache path→owner dari store (dipakai room baru / pasca
    /// dedup). Naikkan generation agar koneksi basi diputus dan klien
    /// menarik ulang DOC_LIST yang sudah bersih.
    pub fn rebuild(&self, store: &Store, vault_id: &str) {
        let map = store.build_path_index(vault_id);
        *self.path_owner.lock().unwrap() = map;
        self.docs.lock().unwrap().clear();
        self.indexed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.generation
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    /// Lazy init indeks kiblat saat koneksi pertama masuk.
    pub fn ensure_indexed(&self, store: &Store, vault_id: &str) {
        if !self.indexed.load(std::sync::atomic::Ordering::SeqCst) {
            self.rebuild(store, vault_id);
        }
    }

    /// KIBLAT sweep: bersihkan pelanggaran yang sudah tersimpan lalu
    /// rebuild cache — atomik terhadap apply klien (write_lock).
    /// Kembalikan jumlah note yang dihapus.
    pub fn sweep(&self, store: &Store, vault_id: &str) -> i64 {
        let _wl = self.write_lock.lock().unwrap();
        let removed = store.dedup_paths(vault_id);
        if removed > 0 {
            let map = store.build_path_index(vault_id);
            *self.path_owner.lock().unwrap() = map;
            self.docs.lock().unwrap().clear();
            self.generation
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        removed
    }

    /// Perbarui klaim path note setelah update diterapkan:
    /// - deleted → lepas semua klaim note
    /// - pindah path → lepas klaim lama, klaim path baru
    fn claim_path(&self, note_id: &str, path: Option<&str>, deleted: bool) {
        let mut idx = self.path_owner.lock().unwrap();
        if deleted {
            idx.retain(|_p, id| id != note_id);
            return;
        }
        let Some(path) = path else { return };
        if path.is_empty() {
            return;
        }
        idx.retain(|p, id| id != note_id || p == path);
        idx.insert(path.to_string(), note_id.to_string());
    }

    fn doc(&self, store: &Store, vault_id: &str, note_id: &str) -> Arc<Mutex<Doc>> {
        let mut docs = self.docs.lock().unwrap();
        if let Some(doc) = docs.get(note_id) {
            return doc.clone();
        }
        let doc = Doc::new();
        for blob in store.load_doc_blobs(vault_id, note_id) {
            if let Ok(update) = Update::decode_v1(&blob) {
                let mut txn = doc.transact_mut();
                if let Err(e) = txn.apply_update(update) {
                    warn!(%note_id, error = %e, "failed to apply stored update");
                }
            }
        }
        let doc = Arc::new(Mutex::new(doc));
        docs.insert(note_id.to_string(), doc.clone());
        doc
    }
}

pub fn encode_frame(msg_type: u8, note_id: &str, payload: &[u8]) -> Vec<u8> {
    let id = note_id.as_bytes();
    let mut frame = Vec::with_capacity(3 + id.len() + payload.len());
    frame.push(msg_type);
    frame.extend_from_slice(&(id.len() as u16).to_be_bytes());
    frame.extend_from_slice(id);
    frame.extend_from_slice(payload);
    frame
}

pub fn encode_doc_list(note_ids: &[String]) -> Vec<u8> {
    let mut payload = Vec::new();
    for id in note_ids {
        let b = id.as_bytes();
        payload.extend_from_slice(&(b.len() as u16).to_be_bytes());
        payload.extend_from_slice(b);
    }
    encode_frame(MSG_DOC_LIST, "", &payload)
}

fn parse_frame(data: &[u8]) -> Option<(u8, String, &[u8])> {
    if data.len() < 3 {
        return None;
    }
    let msg_type = data[0];
    let id_len = u16::from_be_bytes([data[1], data[2]]) as usize;
    if data.len() < 3 + id_len {
        return None;
    }
    let note_id = String::from_utf8(data[3..3 + id_len].to_vec()).ok()?;
    Some((msg_type, note_id, &data[3 + id_len..]))
}

pub async fn handle_socket(
    socket: WebSocket,
    vault_id: String,
    room: Arc<VaultRoom>,
    store: Arc<Store>,
    conn_id: u64,
) {
    info!(%vault_id, conn_id, "device connected");

    let conn_generation = room.generation();
    let generation_room = room.clone();

    let (mut sink, stream) = socket.split();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let mut bcast = room.tx.subscribe();

    let notes = store.list_notes(&vault_id);
    let _ = out_tx.send(encode_doc_list(&notes));

    let vault_id_writer = vault_id.clone();
    let writer = tokio::spawn(async move {
        loop {
            tokio::select! {
                msg = out_rx.recv() => {
                    match msg {
                        Some(frame) => {
                            if sink.send(Message::Binary(frame.into())).await.is_err() {
                                break;
                            }
                        }
                        None => break,
                    }
                }
                msg = bcast.recv() => {
                    match msg {
                        Ok(b) if b.conn_id != conn_id => {
                            if sink.send(Message::Binary(b.frame.into())).await.is_err() {
                                break;
                            }
                        }
                        Ok(_) => {}
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!(%vault_id_writer, conn_id, lagged = n, "client lagging, updates dropped");
                        }
                        Err(_) => break,
                    }
                }
            }
        }
    });

    let mut stream_pin = stream;
    let reset_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let rf = reset_flag.clone();
    let conn_generation = conn_generation;
    let generation_room = generation_room.clone();
    let rf_watcher = tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            if generation_room.generation() != conn_generation {
                info!("room reset — koneksi basi dihentikan");
                reset_flag.store(true, std::sync::atomic::Ordering::SeqCst);
                break;
            }
        }
    });

    loop {
        if rf.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }
        let next = tokio::select! {
            m = stream_pin.next() => m,
            _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {
                if rf.load(std::sync::atomic::Ordering::SeqCst) { None } else { continue }
            }
        };
        let Some(Ok(Message::Binary(data))) = next else { break };
        let Some((msg_type, note_id, payload)) = parse_frame(&data) else {
            continue;
        };
        let doc = room.doc(&store, &vault_id, &note_id);

        match msg_type {
            MSG_SYNC_STEP1 => {
                let client_sv_empty = StateVector::decode_v1(payload)
                    .map(|sv| sv.is_empty())
                    .unwrap_or(true);
                let doc = doc.lock().unwrap();
                let txn = doc.transact();
                // apakah server benar-benar punya isi note ini?
                let server_sv = txn.state_vector();
                let server_empty = server_sv.is_empty();
                let reply = StateVector::decode_v1(payload)
                    .ok()
                    .map(|sv| txn.encode_state_as_update_v1(&sv));
                if let Some(diff) = reply {
                    // jangan kirim STEP2 kosong-boros: kalau server kosong, diff pasti kosong
                    if !server_empty || !diff.is_empty() {
                        let _ = out_tx.send(encode_frame(MSG_SYNC_STEP2, &note_id, &diff));
                    }
                }
                let sv = txn.state_vector().encode_v1();
                drop(txn);
                drop(doc);
                // ANTI-HANTU: kalau client tidak punya apa pun (SV kosong) DAN
                // server juga tidak punya note ini — jangan balas STEP1.
                // Balasan STEP1 memicu client mengirim STEP2 kosong yang lalu
                // tersimpan sebagai dokumen hantu di server.
                if !client_sv_empty || !server_empty {
                    let _ = out_tx.send(encode_frame(MSG_SYNC_STEP1, &note_id, &sv));
                }
            }
            MSG_PING => {
                let frame = encode_frame(MSG_PONG, &note_id, payload);
                let _ = out_tx.send(frame);
            }
            MSG_SYNC_STEP2 | MSG_UPDATE => {
                // ═══════════════════════════════════════════════════════
                // ATURAN KIBLAT SERVER (path uniqueness):
                // satu path = satu note-id. Device (terutama yang lama /
                // ber-index basi) yang mengirim note-id berbeda untuk path
                // yang sudah dimiliki note lain DITOLAK — server tidak
                // pernah menyimpan dobel. Balas state pemilik sah agar
                // plugin baru (0.13.8+) mengadopsi ID kanonik.
                //
                // write_lock membuat cek → terap → simpan → klaim path
                // ATOMIK antar koneksi: dua device yang mengklaim path
                // baru yang sama hampir bersamaan tak bisa lolos berdua.
                // ═══════════════════════════════════════════════════════
                let incoming_path: Option<String> = {
                    let probe = yrs::Doc::new();
                    if let Ok(u) = Update::decode_v1(payload) {
                        let mut txn = probe.transact_mut();
                        let _ = txn.apply_update(u);
                        drop(txn);
                        let txn = probe.transact();
                        txn.get_map("meta").and_then(|m| {
                            m.get(&txn, "path").and_then(|v| match v {
                                yrs::Out::Any(yrs::Any::String(s)) => {
                                    Some(s.to_string())
                                }
                                _ => None,
                            })
                        })
                    } else {
                        None
                    }
                };

                let _kiblat_guard = room.write_lock.lock().unwrap();

                if let Some(path) = incoming_path.as_ref() {
                    if !path.is_empty() {
                        let owner = room.path_owner.lock().unwrap().get(path).cloned();
                        if let Some(owner_id) = owner {
                            if owner_id != note_id {
                                warn!(%vault_id, %note_id, path = %path,
                                      owner = %owner_id,
                                      "UPDATE DITOLAK: path sudah dimiliki note lain (kiblat)");
                                let owner_doc = room.doc(&store, &vault_id, &owner_id);
                                let od = owner_doc.lock().unwrap();
                                let txn = od.transact();
                                let full = txn.encode_state_as_update_v1(&StateVector::default());
                                drop(txn);
                                drop(od);
                                let _ = out_tx.send(encode_frame(MSG_UPDATE, &owner_id, &full));
                                continue;
                            }
                        }
                    }
                }

                let applied = {
                    let doc = doc.lock().unwrap();
                    match Update::decode_v1(payload) {
                        Ok(update) => {
                            let mut txn = doc.transact_mut();
                            txn.apply_update(update).is_ok()
                        }
                        Err(_) => false,
                    }
                };
                if !applied {
                    warn!(%vault_id, %note_id, "failed to apply update");
                    continue;
                }
                // baca meta final (path/deleted) SETELAH terap untuk
                // pemeliharaan indeks kiblat (rename / delete)
                let (final_path, final_deleted) = {
                    let d = doc.lock().unwrap();
                    let txn = d.transact();
                    let meta = txn.get_map("meta");
                    let p = meta.as_ref().and_then(|m| {
                        m.get(&txn, "path").and_then(|v| match v {
                            yrs::Out::Any(yrs::Any::String(s)) => Some(s.to_string()),
                            _ => None,
                        })
                    });
                    let del = meta
                        .as_ref()
                        .and_then(|m| m.get(&txn, "deleted"))
                        .map(|v| matches!(v, yrs::Out::Any(yrs::Any::Bool(true))))
                        .unwrap_or(false);
                    (p, del)
                };
                let pending = store.append_update(&vault_id, &note_id, payload);
                if pending >= SQUASH_THRESHOLD {
                    let doc = doc.lock().unwrap();
                    let txn = doc.transact();
                    let full = txn.encode_state_as_update_v1(&StateVector::default());
                    drop(txn);
                    drop(doc);
                    store.squash(&vault_id, &note_id, &full);
                }
                room.claim_path(&note_id, final_path.as_deref(), final_deleted);
                let frame = encode_frame(MSG_UPDATE, &note_id, payload);
                let _ = room.tx.send(BroadcastMsg { conn_id, frame });
            }
            _ => {}
        }
    }

    rf_watcher.abort();
    drop(writer);
    info!(%vault_id, conn_id, "device disconnected");
}
