import { ParquetAsAnnDataFrameZarr } from "parquet-as-virtual-zarr";

/**
 * AsyncReadable backed by a browser File object.
 * Uses File.slice() so only the requested byte ranges are read —
 * no full-file load is needed.
 */
class FileSource {
  constructor(file) {
    this.file = file;
    this._getRangeCalls = [];
  }

  async get(_key) {
    return new Uint8Array(await this.file.arrayBuffer());
  }

  async getRange(_key, range) {
    let offset, length;
    if ("suffixLength" in range) {
      offset = this.file.size - range.suffixLength;
      length = range.suffixLength;
    } else {
      offset = range.offset;
      length = range.length;
    }
    const blob = this.file.slice(offset, offset + length);
    const ab = await blob.arrayBuffer();
    this._getRangeCalls.push({ offset, length: ab.byteLength });
    return new Uint8Array(ab);
  }

  uniqueBytesFetched() {
    if (this._getRangeCalls.length === 0) return 0;
    const intervals = this._getRangeCalls
      .map(({ offset, length }) => [offset, offset + length])
      .sort((a, b) => a[0] - b[0]);
    let total = 0;
    let [curStart, curEnd] = intervals[0];
    for (let i = 1; i < intervals.length; i++) {
      const [start, end] = intervals[i];
      if (start <= curEnd) {
        curEnd = Math.max(curEnd, end);
      } else {
        total += curEnd - curStart;
        curStart = start;
        curEnd = end;
      }
    }
    return total + (curEnd - curStart);
  }
}

const dropZone = document.getElementById("drop-zone");
const fileInput = document.getElementById("file-input");
const dirInput = document.getElementById("dir-input");
const output = document.getElementById("output");
const status = document.getElementById("status");

/* ── drag & drop ─────────────────────────────────────────────────────────── */

dropZone.addEventListener("dragover", (e) => {
  e.preventDefault();
  dropZone.classList.add("drag-over");
});
dropZone.addEventListener("dragleave", () => dropZone.classList.remove("drag-over"));
dropZone.addEventListener("drop", async (e) => {
  e.preventDefault();
  dropZone.classList.remove("drag-over");

  const items = [...e.dataTransfer.items];
  // Prefer a .parquet file directly; otherwise search directory entries.
  for (const item of items) {
    const entry = item.webkitGetAsEntry?.();
    if (entry?.isFile && entry.name.endsWith(".parquet")) {
      processFile(await entryToFile(entry));
      return;
    }
  }
  // If a directory was dropped, scan it for the first .parquet file.
  for (const item of items) {
    const entry = item.webkitGetAsEntry?.();
    if (entry?.isDirectory) {
      const parquetEntry = await findParquetInDirectory(entry);
      if (parquetEntry) {
        processFile(await entryToFile(parquetEntry));
        return;
      }
    }
  }
  // Fallback: use dataTransfer.files
  const file = [...e.dataTransfer.files].find((f) => f.name.endsWith(".parquet"));
  if (file) processFile(file);
  else setStatus("No .parquet file found in the dropped item.");
});

/* ── file / directory inputs ─────────────────────────────────────────────── */

fileInput.addEventListener("change", () => {
  const file = fileInput.files[0];
  if (file) processFile(file);
});

dirInput.addEventListener("change", () => {
  const file = [...dirInput.files].find((f) => f.name.endsWith(".parquet"));
  if (file) processFile(file);
  else setStatus("No .parquet file found in the selected directory.");
});

/* ── process ─────────────────────────────────────────────────────────────── */

async function processFile(file) {
  output.style.display = "none";
  output.textContent = "";
  setStatus(`Reading ${file.name}…`);

  try {
    const source = new FileSource(file);
    const store = ParquetAsAnnDataFrameZarr.fromStore(source);
    const bytes = await store.get("/zarr.json");
    if (!bytes) throw new Error("Store returned nothing for /zarr.json");
    const zarrJson = JSON.parse(new TextDecoder().decode(bytes));
    output.textContent = JSON.stringify(zarrJson, null, 2);
    output.style.display = "block";

    const consolidated = await store.getConsolidatedMetadata();
    const consolidatedOutput = document.getElementById("consolidated-output");
    const consolidatedHeading = document.getElementById("consolidated-heading");
    consolidatedOutput.textContent = JSON.stringify(consolidated, null, 2);
    consolidatedOutput.style.display = "block";
    consolidatedHeading.style.display = "block";

    const bytesRead = source.uniqueBytesFetched();
    const pct = ((bytesRead / file.size) * 100).toFixed(1);
    const fmt = (n) => n >= 1024 * 1024
      ? (n / (1024 * 1024)).toFixed(2) + " MB"
      : (n / 1024).toFixed(1) + " KB";
    setStatus(`${file.name} — read ${fmt(bytesRead)} of ${fmt(file.size)} (${pct}%)`);
  } catch (err) {
    setStatus("Error: " + err.message);
    console.error(err);
  }
}

/* ── helpers ─────────────────────────────────────────────────────────────── */

function setStatus(msg) {
  document.getElementById("status").textContent = msg;
}

function entryToFile(entry) {
  return new Promise((resolve, reject) => entry.file(resolve, reject));
}

async function findParquetInDirectory(dirEntry) {
  const entries = await readDirectoryEntries(dirEntry);
  for (const entry of entries) {
    if (entry.isFile && entry.name.endsWith(".parquet")) return entry;
  }
  // One level deep only
  for (const entry of entries) {
    if (entry.isDirectory) {
      const found = await findParquetInDirectory(entry);
      if (found) return found;
    }
  }
  return null;
}

function readDirectoryEntries(dirEntry) {
  return new Promise((resolve, reject) => {
    const reader = dirEntry.createReader();
    const results = [];
    const read = () =>
      reader.readEntries((batch) => {
        if (batch.length === 0) resolve(results);
        else { results.push(...batch); read(); }
      }, reject);
    read();
  });
}
