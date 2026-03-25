/**
 * Zarr v3 compression codecs for parquet pass-through.
 *
 * Registers codecs with zarrita's codec registry so that zarr arrays
 * declaring these codecs in their metadata can be decoded transparently.
 *
 * Codecs registered here:
 * - "snappy"  — vendored from hyparquet
 * - "lz4_raw" — vendored LZ4 raw block decoder
 * - "brotli"  — Node.js zlib
 *
 * Codecs already registered by zarrita (no action needed):
 * - "gzip"  — via zarrita's built-in GzipCodec
 * - "zstd"  — via numcodecs/zstd (WASM)
 */

import { registry } from "zarrita";
import { snappyDecode } from "./vendored/snappy.js";
import { lz4RawDecompress } from "./vendored/lz4raw.js";

// ── Snappy ──────────────────────────────────────────────────────────────

class SnappyCodec {
  kind = "bytes_to_bytes" as const;

  static fromConfig(): SnappyCodec {
    return new SnappyCodec();
  }

  encode(_bytes: Uint8Array): never {
    throw new Error("Snappy encoding is not supported. This codec is decode-only.");
  }

  decode(bytes: Uint8Array): Uint8Array {
    return snappyDecode(bytes);
  }
}

// ── LZ4 Raw ─────────────────────────────────────────────────────────────

/**
 * LZ4 raw block codec.
 * Parquet's LZ4_RAW uses the raw LZ4 block format (no framing), which is
 * different from zarrita's "lz4" codec (which uses the framed LZ4 format
 * via numcodecs). We register this under a separate name.
 *
 * The uncompressed size is encoded as a 4-byte little-endian prefix
 * prepended by the parquet store's zero-copy path.
 */
class Lz4RawCodec {
  kind = "bytes_to_bytes" as const;

  static fromConfig(): Lz4RawCodec {
    return new Lz4RawCodec();
  }

  encode(_bytes: Uint8Array): never {
    throw new Error("LZ4_RAW encoding is not supported. This codec is decode-only.");
  }

  decode(bytes: Uint8Array): Uint8Array {
    // Read 4-byte LE uncompressed size prefix (added by the store)
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const uncompressedSize = view.getUint32(0, true);
    const compressed = bytes.subarray(4);
    const output = new Uint8Array(uncompressedSize);
    lz4RawDecompress(compressed, output);
    return output;
  }
}

// ── Brotli ──────────────────────────────────────────────────────────────

class BrotliCodec {
  kind = "bytes_to_bytes" as const;

  static fromConfig(): BrotliCodec {
    return new BrotliCodec();
  }

  encode(_bytes: Uint8Array): never {
    throw new Error("Brotli encoding is not supported. This codec is decode-only.");
  }

  async decode(bytes: Uint8Array): Promise<Uint8Array> {
    try {
      const zlib = await import("node:zlib");
      const result = zlib.brotliDecompressSync(bytes);
      return new Uint8Array(result.buffer, result.byteOffset, result.byteLength);
    } catch {
      throw new Error(
        "Brotli decompression requires Node.js. " +
          "Not supported in browser environments without a polyfill.",
      );
    }
  }
}

// ── Registration ────────────────────────────────────────────────────────

registry.set("snappy", () => Promise.resolve(SnappyCodec));
registry.set("lz4_raw", () => Promise.resolve(Lz4RawCodec));
registry.set("brotli", () => Promise.resolve(BrotliCodec));
// "gzip" and "zstd" are already registered by zarrita's default registry
