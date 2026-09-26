import { createRequire } from "module";
const require = createRequire(import.meta.url);
const Y = require("yjs");
const enc = new TextEncoder();
const MSG_SYNC_STEP1 = 1, MSG_SYNC_STEP2 = 2, MSG_UPDATE = 3;
const EMPTY_SV = new Uint8Array(Y.encodeStateVector(new Y.Doc()));
function encodeFrame(type, noteId, payload) {
  const id = enc.encode(noteId);
  const f = new Uint8Array(3 + id.length + payload.length);
  f[0] = type; f[1] = (id.length >> 8) & 0xff; f[2] = id.length & 0xff;
  f.set(id, 3); f.set(payload, 3 + id.length);
  return f;
}
const [vaultId, token, noteId, content, path] = process.argv.slice(2);
const ws = new WebSocket(`ws://localhost:${process.env.RELAY_PORT ?? "18080"}/sync/${vaultId}?token=${token}`);
ws.binaryType = "arraybuffer";
ws.onopen = async () => {
  ws.send(encodeFrame(MSG_SYNC_STEP1, noteId, EMPTY_SV));
  await new Promise((r) => setTimeout(r, 400));
  const doc = new Y.Doc();
  let delta = null;
  doc.on("update", (u) => { delta = new Uint8Array(u); });
  doc.transact(() => {
    doc.getText("content").insert(0, content);
    const meta = doc.getMap("meta");
    meta.set("path", path);
    meta.set("deleted", false);
  });
  ws.send(encodeFrame(MSG_UPDATE, noteId, delta));
  await new Promise((r) => setTimeout(r, 400));
  ws.close();
  console.log("pushed");
};
