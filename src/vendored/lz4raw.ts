/**
 * LZ4 raw block decompressor.
 * Decodes a single LZ4 block (no framing) as used by Parquet's LZ4_RAW codec.
 *
 * Reference: https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md
 */

/**
 * Decompress an LZ4 raw block into the provided output buffer.
 */
export function lz4RawDecompress(
  input: Uint8Array,
  output: Uint8Array,
): void {
  const inputLength = input.byteLength;
  const outputLength = output.byteLength;
  let ip = 0; // input position
  let op = 0; // output position

  while (ip < inputLength) {
    // Read token
    const token = input[ip++];
    let literalLength = token >>> 4;
    let matchLength = (token & 0x0f) + 4;

    // Read literal length (if 15, read additional bytes)
    if (literalLength === 15) {
      let extra: number;
      do {
        extra = input[ip++];
        literalLength += extra;
      } while (extra === 255);
    }

    // Copy literals
    for (let i = 0; i < literalLength; i++) {
      output[op++] = input[ip++];
    }

    // End of block — the last sequence is always a literal-only sequence
    if (ip >= inputLength) break;

    // Read match offset (2 bytes, little-endian)
    const offset = input[ip] | (input[ip + 1] << 8);
    ip += 2;

    if (offset === 0) {
      throw new Error("lz4: invalid match offset 0");
    }

    // Read match length (if 15, read additional bytes)
    if (matchLength === 19) {
      // 15 + 4
      let extra: number;
      do {
        extra = input[ip++];
        matchLength += extra;
      } while (extra === 255);
    }

    // Copy match (may overlap — copy byte by byte)
    const matchStart = op - offset;
    for (let i = 0; i < matchLength; i++) {
      output[op++] = output[matchStart + i];
    }
  }

  if (op !== outputLength) {
    throw new Error(
      `lz4: decompressed size ${op} does not match expected ${outputLength}`,
    );
  }
}
