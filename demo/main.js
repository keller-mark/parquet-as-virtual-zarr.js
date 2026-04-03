import { open, get, root, slice } from "zarrita";
import { ParquetAsAnnDataFrameStore, CsvAsAnnDataFrameStore } from "parquet-as-virtual-zarr";

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
  // Prefer a .parquet, .csv, or .tsv file dropped directly.
  for (const item of items) {
    const entry = item.webkitGetAsEntry?.();
    if (entry?.isFile && isSupportedFile(entry.name)) {
      processFile(await entryToFile(entry));
      return;
    }
  }
  // If a directory was dropped, scan it for the first .parquet file (CSV/TSV
  // directories are not supported).
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
  const file = [...e.dataTransfer.files].find((f) => isSupportedFile(f.name));
  if (file) processFile(file);
  else setStatus("No supported file (.parquet, .csv, .tsv) found in the dropped item.");
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
  document.getElementById("chunk-table-container").style.display = "none";
  document.getElementById("chunk-table-container").innerHTML = "";
  document.getElementById("chunk-table-heading").style.display = "none";
  setStatus(`Reading ${file.name}…`);

  const fmt = (n) => n >= 1024 * 1024
    ? (n / (1024 * 1024)).toFixed(2) + " MB"
    : (n / 1024).toFixed(1) + " KB";

  try {
    const source = new FileSource(file);
    const isCsv = file.name.endsWith(".csv") || file.name.endsWith(".tsv");
    let store;
    if (isCsv) {
      const sep = file.name.endsWith(".tsv") ? "\t" : ",";
      store = await CsvAsAnnDataFrameStore.fromStore(source, { separator: sep });
    } else {
      store = ParquetAsAnnDataFrameStore.fromStore(source);
    }

    const grp = await open(root(store), { kind: "group" });
    output.textContent = JSON.stringify(grp.attrs, null, 2);
    output.style.display = "block";

    const consolidated = await store.getConsolidatedMetadata();
    const consolidatedOutput = document.getElementById("consolidated-output");
    const consolidatedHeading = document.getElementById("consolidated-heading");
    consolidatedOutput.textContent = JSON.stringify(consolidated, null, 2);
    consolidatedOutput.style.display = "block";
    consolidatedHeading.style.display = "block";

    await renderFirstChunkTable(grp);

    if (isCsv) {
      setStatus(`${file.name} — loaded ${fmt(file.size)}`);
    } else {
      const bytesRead = source.uniqueBytesFetched();
      const pct = ((bytesRead / file.size) * 100).toFixed(1);
      setStatus(`${file.name} — read ${fmt(bytesRead)} of ${fmt(file.size)} (${pct}%)`);
    }
  } catch (err) {
    setStatus("Error: " + err.message);
    console.error(err);
  }
}

/* ── first-chunk table ───────────────────────────────────────────────────── */

async function renderFirstChunkTable(grp) {
  const tableContainer = document.getElementById("chunk-table-container");
  tableContainer.innerHTML = "";
  document.getElementById("chunk-table-heading").style.display = "block";

  const columns = grp.attrs["column-order"] ?? [];
  if (columns.length === 0) return;

  // For each column, read the first chunk.
  const columnData = [];
  for (const col of columns) {
    const colNode = await open(grp.resolve(col));
    const encodingType = colNode.attrs?.["encoding-type"];
    const isCategorical = encodingType === "categorical";
    const isNullable = encodingType === "nullable-integer" || encodingType === "nullable-string-array";

    let values;
    if (isCategorical) {
      const codesArr = await open(grp.resolve(`${col}/codes`), { kind: "array" });
      const catsArr = await open(grp.resolve(`${col}/categories`), { kind: "array" });
      const chunkSize = codesArr.chunks[0];
      const codesChunk = await get(codesArr, [slice(0, chunkSize)]);
      const catsChunk = await get(catsArr);
      const categories = catsChunk.data;
      values = Array.from(codesChunk.data, (code) => categories[code]);
    } else if (isNullable) {
      const valuesArr = await open(grp.resolve(`${col}/values`), { kind: "array" });
      const maskArr = await open(grp.resolve(`${col}/mask`), { kind: "array" });
      const chunkSize = valuesArr.chunks[0];
      const valuesChunk = await get(valuesArr, [slice(0, chunkSize)]);
      const maskChunk = await get(maskArr, [slice(0, chunkSize)]);
      const rawValues = Array.from(valuesChunk.data);
      const mask = Array.from(maskChunk.data);
      values = rawValues.map((v, i) => (mask[i] ? null : v));
    } else {
      const arr = await open(grp.resolve(col), { kind: "array" });
      const chunkSize = arr.chunks[0];
      const chunk = await get(arr, [slice(0, chunkSize)]);
      values = Array.from(chunk.data);
    }
    columnData.push({ col, values });
  }

  const nRows = columnData[0]?.values.length ?? 0;
  const table = document.createElement("table");
  const thead = table.createTHead();
  const headerRow = thead.insertRow();
  for (const { col } of columnData) {
    const th = document.createElement("th");
    th.textContent = col;
    headerRow.appendChild(th);
  }
  const tbody = table.createTBody();
  for (let i = 0; i < nRows; i++) {
    const row = tbody.insertRow();
    for (const { values } of columnData) {
      const td = row.insertCell();
      td.textContent = values[i] ?? "";
    }
  }
  tableContainer.appendChild(table);
  tableContainer.style.display = "block";
}

/* ── helpers ─────────────────────────────────────────────────────────────── */

function isSupportedFile(name) {
  return name.endsWith(".parquet") || name.endsWith(".csv") || name.endsWith(".tsv");
}

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
