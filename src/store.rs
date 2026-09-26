use rand::Rng;
use rusqlite::Connection;
use sha2::{Digest, Sha256};
use std::sync::Mutex;

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
                 AND note_id NOT IN ('__attachments__', '__hiddens__', '__folders__')
                 UNION SELECT note_id FROM updates WHERE vault_id = ?1
                 AND note_id NOT IN ('__attachments__', '__hiddens__', '__folders__'))",
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
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare(
                "SELECT note_id FROM (SELECT note_id FROM notes WHERE vault_id = ?1
                 UNION SELECT note_id FROM updates WHERE vault_id = ?1)",
            )
            .unwrap();
        stmt.query_map([vault_id], |r| r.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect()
    }

    /// Jumlah catatan NYATA (tanpa dokumen metadata internal) — untuk dashboard & /info.
    pub fn count_real_notes(&self, vault_id: &str) -> i64 {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT COUNT(*) FROM (SELECT note_id FROM notes WHERE vault_id = ?1
             AND note_id NOT IN ('__attachments__', '__hiddens__', '__folders__')
             UNION SELECT note_id FROM updates WHERE vault_id = ?1
             AND note_id NOT IN ('__attachments__', '__hiddens__', '__folders__'))",
            [vault_id],
            |r| r.get(0),
        )
        .unwrap_or(0)
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
