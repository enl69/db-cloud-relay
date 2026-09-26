import { createRequire } from "module";
const require = createRequire(import.meta.url);
const Y = require("yjs");

const BASE = `http://localhost:${process.env.RELAY_PORT ?? "18080"}`;
const enc = new TextEncoder();
const dec = new TextDecoder();

const MSG_DOC_LIST = 0;
const MSG_SYNC_STEP1 = 1;
const MSG_SYNC_STEP2 = 2;
const MSG_UPDATE = 3;

const EMPTY_SV = new Uint8Array(Y.encodeStateVector(new Y.Doc()));

function encodeFrame(type, noteId, payload) {
  const id = enc.encode(noteId);
  const frame = new Uint8Array(3 + id.length + payload.length);
  frame[0] = type;
  frame[1] = (id.length >> 8) & 0xff;
  frame[2] = id.length & 0xff;
  frame.set(id, 3);
  frame.set(payload, 3 + id.length);
  return frame;
}

function decodeFrame(data) {
  const idLen = (data[1] << 8) | data[2];
  return {
    type: data[0],
    noteId: dec.decode(data.subarray(3, 3 + idLen)),
    payload: data.subarray(3 + idLen),
  };
}

function connect(vaultId, token) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://localhost:${process.env.RELAY_PORT ?? "18080"}/sync/${vaultId}?token=${token}`);
    ws.binaryType = "arraybuffer";
    const waiters = [];
    ws.onmessage = (e) => {
      const frame = decodeFrame(new Uint8Array(e.data));
      const idx = waiters.findIndex((w) => w.match(frame));
      if (idx >= 0) {
        const w = waiters.splice(idx, 1)[0];
        w.resolve(frame);
      }
    };
    ws.onerror = reject;
    ws.onopen = () =>
      resolve({
        ws,
        send: (frame) => ws.send(frame),
        waitFor: (match, ms = 5000) =>
          new Promise((resolveW, rejectW) => {
            const timer = setTimeout(() => rejectW(new Error(`timeout waiting type`)), ms);
            waiters.push({
              match,
              resolve: (f) => {
                clearTimeout(timer);
                resolveW(f);
              },
            });
          }),
      });
  });
}

const VAULT = process.argv[2];
const TOKEN = process.argv[3];
const NOTE_ID = process.argv[4] ?? "note-a";
const EXPECT_TEXT = process.argv[5] ?? "hello world";
const EXPECT_PATH = process.argv[6] ?? "halo.md";

// Client B: connect ke vault yang SUDAH berisi "hello world" dari Client A sebelumnya
{
  const conn = await connect(VAULT, TOKEN);
  const docListFrame = await conn.waitFor((f) => f.type === MSG_DOC_LIST);
  const ids = [];
  let i = 0;
  const p = docListFrame.payload;
  while (i + 2 <= p.length) {
    const len = (p[i] << 8) | p[i + 1];
    i += 2;
    ids.push(dec.decode(p.subarray(i, i + len)));
    i += len;
  }
  console.log(`doc list dari server: [${ids.join(", ")}]`);
  if (!ids.includes(NOTE_ID)) {
    console.error(`FAIL: server tidak punya note ${NOTE_ID}`);
    process.exit(1);
  }

  const doc = new Y.Doc();
  conn.send(encodeFrame(MSG_SYNC_STEP1, NOTE_ID, EMPTY_SV));
  const step2 = await conn.waitFor((f) => f.type === MSG_SYNC_STEP2 && f.noteId === NOTE_ID);
  Y.applyUpdate(doc, step2.payload);
  const text = doc.getText("content").toString();
  const path = doc.getMap("meta").get("path");
  console.log(`isi ${NOTE_ID}: '${text}', path: '${path}'`);
  if (text === EXPECT_TEXT && path === EXPECT_PATH) {
    console.log("PASS: sync bekerja — client B menerima data client A via server");
    process.exit(0);
  }
  console.error(`FAIL: expected '${EXPECT_TEXT}' + '${EXPECT_PATH}'`);
  process.exit(1);
}
