use rand::Rng;
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use std::sync::Mutex;
use yrs::updates::decoder::Decode;
use yrs::types::Map;
use yrs::{Doc, Out, ReadTxn, Text, Transact, Update};

pub struct Store {
    pub conn: Mutex<Connection>,
}

pub fn sha256_hex(data: &str) -> String {
    let digest = Sha256::digest(data.as_bytes());
    digest.iter().map(|b| format!("{:02x}", b)).collect()
}

pub fn random_hex(n: usize) -> String {
    let mut rng = rand::thread_rng();
    (0..n).map(|_| format!("{:02x}", rng.gen::<u8>())).collect()
}

impl Store {
    pub fn open(path: &str) -> Self {
        let conn = Connection::open(path).expect("failed to open sqlite");
        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS vaults (
                id         TEXT PRIMARY KEY,
                token_hash TEXT NOT NULL,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
                last_update INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS notes (
                vault_id TEXT NOT NULL,
                note_id  TEXT NOT NULL,
                snapshot BLOB NOT NULL,
                PRIMARY KEY (vault_id, note_id)
            );
            CREATE TABLE IF NOT EXISTS updates (
                vault_id TEXT NOT NULL,
                note_id  TEXT NOT NULL,
                seq      INTEGER PRIMARY KEY AUTOINCREMENT,
                data     BLOB NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_updates_doc ON updates (vault_id, note_id);
            CREATE TABLE IF NOT EXISTS blobs (
                sha  TEXT PRIMARY KEY,
                data BLOB NOT NULL,
                size INTEGER NOT NULL
            );
            ",
        )
        .expect("failed to init schema");
        let _ = conn.execute(
            "ALTER TABLE vaults ADD COLUMN last_update INTEGER NOT NULL DEFAULT 0",
            [],
        );
        Store {
            conn: Mutex::new(conn),
        }
    }

    pub fn vault_info(&self, vault_id: &str) -> (i64, i64) {
        let conn = self.conn.lock().unwrap();
        let last: i64 = conn
            .query_row(
                "SELECT COALESCE(last_update, 0) FROM vaults WHERE id = ?1",
                [vault_id],
                |r| r.get(0),
            )
            .unwrap_or(0);
        let notes: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM (SELECT note_id FROM notes WHERE vault_id = ?1
                 AND note_id NOT IN ('__attachments__', '__hiddens__', '__folders__', '__devices__')
                 UNION SELECT note_id FROM updates WHERE vault_id = ?1
                 AND note_id NOT IN ('__attachments__', '__hiddens__', '__folders__', '__devices__'))",
                [vault_id],
                |r| r.get(0),
            )
            .unwrap_or(0);
        (last, notes)
    }

    pub fn ensure_admin_token(&self) -> String {
        let conn = self.conn.lock().unwrap();
        let existing: Option<String> = conn
            .query_row("SELECT value FROM meta WHERE key = 'admin_token'", [], |r| {
                r.get(0)
            })
            .ok();
        if let Some(token) = existing {
            return token;
        }
        let token = random_hex(32);
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('admin_token', ?1)",
            [&token],
        )
        .expect("failed to store admin token");
        token
    }

    pub fn admin_token_matches(&self, token: &str) -> bool {
        let conn = self.conn.lock().unwrap();
        let stored: Option<String> = conn
            .query_row("SELECT value FROM meta WHERE key = 'admin_token'", [], |r| {
                r.get(0)
            })
            .ok();
        stored.map(|t| t == token).unwrap_or(false)
    }

    pub fn create_vault(&self) -> (String, String) {
        let vault_id = random_hex(8);
        let token = random_hex(32);
        let token_hash = sha256_hex(&token);
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO vaults (id, token_hash) VALUES (?1, ?2)",
            [&vault_id, &token_hash],
        )
        .expect("failed to create vault");
        (vault_id, token)
    }

    pub fn vault_token_valid(&self, vault_id: &str, token: &str) -> bool {
        let conn = self.conn.lock().unwrap();
        let stored: Option<String> = conn
            .query_row(
                "SELECT token_hash FROM vaults WHERE id = ?1",
                [vault_id],
                |r| r.get(0),
            )
            .ok();
        stored
            .map(|h| h == sha256_hex(token))
            .unwrap_or(false)
    }

    pub fn list_notes(&self, vault_id: &str) -> Vec<String> {
        // DOC_LIST hanya boleh berisi catatan aktif yang punya path valid.
        // Row tombstone/deleted tidak dikirim ke device join: mengirimnya
        // membuat client menghidupkan ID lama atau menghasilkan selisih -1.
        let mut ids: Vec<String> = self.build_path_index(vault_id).into_values().collect();
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT note_id FROM notes WHERE vault_id = ?1 AND note_id LIKE '__%'")
            .unwrap();
        ids.extend(
            stmt.query_map([vault_id], |r| r.get::<_, String>(0))
                .unwrap()
                .filter_map(|r| r.ok()),
        );
        let mut stmt = conn
            .prepare("SELECT DISTINCT note_id FROM updates WHERE vault_id = ?1 AND note_id LIKE '__%'")
            .unwrap();
        ids.extend(
            stmt.query_map([vault_id], |r| r.get::<_, String>(0))
                .unwrap()
                .filter_map(|r| r.ok()),
        );
        ids.sort();
        ids.dedup();
        ids
    }


    /// Hitung jumlah entry aktif (non-deleted) pada dokumen metadata CRDT.
    /// `map_name`: "files" untuk __attachments__, "meta" untuk lainnya.
    pub fn count_map_entries(&self, vault_id: &str, note_id: &str, map_name: &str) -> i64 {
        let blobs = self.load_doc_blobs(vault_id, note_id);
        if blobs.is_empty() {
            return 0;
        }
        let doc = Doc::new();
        for blob in &blobs {
            if let Ok(update) = Update::decode_v1(blob.as_slice()) {
                let mut txn = doc.transact_mut();
                let _ = txn.apply_update(update);
            }
        }
        let txn = doc.transact();
        let map = txn.get_map(map_name);
        let mut count = 0i64;
        if let Some(map) = map {
            for (_key, value) in map.iter(&txn) {
                let deleted = match value {
                    Out::YMap(entry) => matches!(
                        entry.get(&txn, "deleted"),
                        Some(Out::Any(yrs::Any::Bool(true)))
                    ),
                    _ => false,
                };
                if !deleted {
                    count += 1;
                }
            }
        }
        count
    }



    /// KIBLAT: scan semua note-id vault → (note_id, path, deleted, text_len).
    /// Urutan deterministik: snapshot `notes` (rowid = urutan simpan) dulu,
    /// lalu note yang hanya ada di `updates`. Pemilik kanonik = entri
    /// PERTAMA yang mengklaim path (semantik "server-first").
    ///
    /// PENTING: lock koneksi WAJIB dilepas sebelum `load_doc_blobs`
    /// (rusqlite Mutex tidak reentrant — nested lock = deadlock).
    fn scan_note_paths(&self, vault_id: &str) -> Vec<(String, Option<String>, bool, usize)> {
        let ids: Vec<String> = {
            let conn = self.conn.lock().unwrap();
            let mut ids = Vec::new();
            let mut stmt = conn
                .prepare("SELECT note_id FROM notes WHERE vault_id = ?1 ORDER BY rowid")
                .unwrap();
            for r in stmt
                .query_map([vault_id], |r| r.get::<_, String>(0))
                .unwrap()
                .flatten()
            {
                ids.push(r);
            }
            drop(stmt);
            let mut stmt = conn
                .prepare(
                    "SELECT note_id FROM updates WHERE vault_id = ?1
                     AND note_id NOT IN (SELECT note_id FROM notes WHERE vault_id = ?1)
                     GROUP BY note_id ORDER BY MIN(seq)",
                )
                .unwrap();
            for r in stmt
                .query_map([vault_id], |r| r.get::<_, String>(0))
                .unwrap()
                .flatten()
            {
                ids.push(r);
            }
            ids
        };
        let mut out = Vec::new();
        for nid in ids {
            let blobs = self.load_doc_blobs(vault_id, &nid);
            if blobs.is_empty() {
                continue;
            }
            let doc = Doc::new();
            for b in &blobs {
                if let Ok(u) = Update::decode_v1(b.as_slice()) {
                    let mut txn = doc.transact_mut();
                    let _ = txn.apply_update(u);
                }
            }
            let txn = doc.transact();
            let meta = txn.get_map("meta");
            let deleted = meta
                .as_ref()
                .and_then(|m| m.get(&txn, "deleted"))
                .map(|v| matches!(v, Out::Any(yrs::Any::Bool(true))))
                .unwrap_or(false);
            let path = meta.and_then(|m| {
                m.get(&txn, "path").and_then(|v| match v {
                    Out::Any(yrs::Any::String(s)) => Some(s.to_string()),
                    _ => None,
                })
            });
            let text_len = txn
                .get_text("content")
                .map(|t| t.len(&txn) as usize)
                .unwrap_or(0);
            out.push((nid, path, deleted, text_len));
        }
        out
    }

    /// KIBLAT: indeks path → note-id pemilik sah (1 path = 1 note).
    /// Untuk VaultRoom (cache in-memory penegakan aturan).
    pub fn build_path_index(&self, vault_id: &str) -> std::collections::HashMap<String, String> {
        let mut map = std::collections::HashMap::new();
        for (nid, path, deleted, _len) in self.scan_note_paths(vault_id) {
            if deleted {
                continue;
            }
            if let Some(p) = path {
                if !p.is_empty() {
                    map.entry(p).or_insert(nid);
                }
            }
        }
        map
    }

    /// KIBLAT: bersihkan pelanggaran yang SUDAH tersimpan (warisan era lama):
    /// (a) dobel path — pertahankan pemilik PERTAMA, hapus baris sisanya;
    /// (b) hantu — note tanpa path DAN tanpa isi (bukan dokumen metadata).
    /// Baris dihapus, bukan ditandai: penegakan runtime menolak pengiriman
    /// ulang ID kalah, sehingga tidak bisa muncul kembali.
    pub fn dedup_paths(&self, vault_id: &str) -> i64 {
        let entries = self.scan_note_paths(vault_id);
        let mut by_path: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        let mut ghosts: Vec<String> = Vec::new();
        for (nid, path, deleted, text_len) in &entries {
            if nid.starts_with("__") {
                continue; // dokumen metadata internal (__attachments__ dll)
            }
            if *deleted {
                continue;
            }
            match path {
                Some(p) if !p.is_empty() => {
                    by_path.entry(p.clone()).or_default().push(nid.clone())
                }
                _ => {
                    if *text_len == 0 {
                        ghosts.push(nid.clone());
                    }
                }
            }
        }
        let mut losers: Vec<String> = ghosts;
        for (_path, owners) in &by_path {
            if owners.len() > 1 {
                for loser in owners.iter().skip(1) {
                    losers.push(loser.clone());
                }
            }
        }
        let removed = losers.len() as i64;
        if removed > 0 {
            let conn = self.conn.lock().unwrap();
            for loser in &losers {
                let _ = conn.execute(
                    "DELETE FROM notes WHERE vault_id = ?1 AND note_id = ?2",
                    rusqlite::params![vault_id, loser],
                );
                let _ = conn.execute(
                    "DELETE FROM updates WHERE vault_id = ?1 AND note_id = ?2",
                    rusqlite::params![vault_id, loser],
                );
            }
        }
        removed
    }

    /// Jumlah catatan AKTIF yang benar-benar dapat dipulihkan: punya
    /// meta.path tidak kosong, tidak deleted, dan path unik. ID database
    /// saja tidak cukup karena tombstone/ghost tidak menjadi file join.
    pub fn count_real_notes(&self, vault_id: &str) -> i64 {
        self.build_path_index(vault_id).len() as i64
    }

    pub fn load_doc_blobs(&self, vault_id: &str, note_id: &str) -> Vec<Vec<u8>> {
        let conn = self.conn.lock().unwrap();
        let mut blobs = Vec::new();
        let snapshot: Option<Vec<u8>> = conn
            .query_row(
                "SELECT snapshot FROM notes WHERE vault_id = ?1 AND note_id = ?2",
                [vault_id, note_id],
                |r| r.get(0),
            )
            .ok();
        if let Some(s) = snapshot {
            blobs.push(s);
        }
        let mut stmt = conn
            .prepare(
                "SELECT data FROM updates WHERE vault_id = ?1 AND note_id = ?2 ORDER BY seq",
            )
            .unwrap();
        let rows = stmt
            .query_map([vault_id, note_id], |r| r.get::<_, Vec<u8>>(0))
            .unwrap();
        for row in rows.flatten() {
            blobs.push(row);
        }
        blobs
    }

    pub fn append_update(&self, vault_id: &str, note_id: &str, data: &[u8]) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO updates (vault_id, note_id, data) VALUES (?1, ?2, ?3)",
            rusqlite::params![vault_id, note_id, data],
        )
        .expect("failed to append update");
        let _ = conn.execute(
            "UPDATE vaults SET last_update = strftime('%s','now') WHERE id = ?1",
            [vault_id],
        );
        conn.query_row(
            "SELECT COUNT(*) FROM updates WHERE vault_id = ?1 AND note_id = ?2",
            [vault_id, note_id],
            |r| r.get(0),
        )
        .unwrap_or(0)
    }

    pub fn any_vault_token_valid(&self, token: &str) -> bool {
        let conn = self.conn.lock().unwrap();
        let token_hash = sha256_hex(token);
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM vaults WHERE token_hash = ?1",
                [&token_hash],
                |r| r.get(0),
            )
            .unwrap_or(0);
        n > 0
    }

    pub fn put_blob(&self, sha: &str, data: &[u8]) {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO blobs (sha, data, size) VALUES (?1, ?2, ?3)
             ON CONFLICT (sha) DO NOTHING",
            rusqlite::params![sha, data, data.len() as i64],
        )
        .expect("failed to store blob");
    }

    pub fn get_blob(&self, sha: &str) -> Option<Vec<u8>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row("SELECT data FROM blobs WHERE sha = ?1", [sha], |r| r.get(0))
            .ok()
    }

    pub fn reset_vault(&self, vault_id: &str) {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM notes WHERE vault_id = ?1", [vault_id])
            .expect("failed to delete notes");
        conn.execute("DELETE FROM updates WHERE vault_id = ?1", [vault_id])
            .expect("failed to delete updates");
        let _ = conn.execute(
            "UPDATE vaults SET last_update = 0 WHERE id = ?1",
            [vault_id],
        );
    }

    pub fn squash(&self, vault_id: &str, note_id: &str, snapshot: &[u8]) {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO notes (vault_id, note_id, snapshot) VALUES (?1, ?2, ?3)
             ON CONFLICT (vault_id, note_id) DO UPDATE SET snapshot = excluded.snapshot",
            rusqlite::params![vault_id, note_id, snapshot],
        )
        .expect("failed to store snapshot");
        conn.execute(
            "DELETE FROM updates WHERE vault_id = ?1 AND note_id = ?2",
            rusqlite::params![vault_id, note_id],
        )
        .expect("failed to prune updates");
        let _ = conn.execute(
            "UPDATE vaults SET last_update = strftime('%s','now') WHERE id = ?1",
            [vault_id],
        );
    }
}
