/**
 * Snappy decompression.
 * Vendored from hyparquet (MIT license, original by Zhipeng Jia).
 */

const WORD_MASK = [0, 0xff, 0xffff, 0xffffff, 0xffffffff];

function copyBytes(
  fromArray: Uint8Array,
  fromPos: number,
  toArray: Uint8Array,
  toPos: number,
  length: number,
): void {
  for (let i = 0; i < length; i++) {
    toArray[toPos + i] = fromArray[fromPos + i];
  }
}

/**
 * Read the uncompressed length from the snappy preamble (varint).
 */
export function snappyUncompressedLength(input: Uint8Array): number {
  let result = 0;
  let shift = 0;
  let pos = 0;
  while (pos < input.byteLength) {
    const byte = input[pos++];
    result |= (byte & 0x7f) << shift;
    if (!(byte & 0x80)) {
      return result;
    }
    shift += 7;
  }
  throw new Error("invalid snappy length header");
}

/**
 * Decompress snappy data.
 * Accepts an output buffer to avoid allocating a new buffer for each call.
 */
export function snappyUncompress(input: Uint8Array, output: Uint8Array): void {
  const inputLength = input.byteLength;
  const outputLength = output.byteLength;
  let pos = 0;
  let outPos = 0;

  // skip preamble (contains uncompressed length as varint)
  while (pos < inputLength) {
    const c = input[pos];
    pos++;
    if (c < 128) {
      break;
    }
  }
  if (outputLength && pos >= inputLength) {
    throw new Error("invalid snappy length header");
  }

  while (pos < inputLength) {
    const c = input[pos];
    let len = 0;
    pos++;

    if (pos >= inputLength) {
      throw new Error("missing eof marker");
    }

    if ((c & 0x3) === 0) {
      // Literal
      let len = (c >>> 2) + 1;
      if (len > 60) {
        if (pos + 3 >= inputLength) {
          throw new Error("snappy error literal pos + 3 >= inputLength");
        }
        const lengthSize = len - 60;
        len =
          input[pos] +
          (input[pos + 1] << 8) +
          (input[pos + 2] << 16) +
          (input[pos + 3] << 24);
        len = (len & WORD_MASK[lengthSize]) + 1;
        pos += lengthSize;
      }
      if (pos + len > inputLength) {
        throw new Error("snappy error literal exceeds input length");
      }
      copyBytes(input, pos, output, outPos, len);
      pos += len;
      outPos += len;
    } else {
      // Copy elements
      let offset = 0;
      switch (c & 0x3) {
        case 1:
          len = ((c >>> 2) & 0x7) + 4;
          offset = input[pos] + ((c >>> 5) << 8);
          pos++;
          break;
        case 2:
          if (inputLength <= pos + 1) {
            throw new Error("snappy error end of input");
          }
          len = (c >>> 2) + 1;
          offset = input[pos] + (input[pos + 1] << 8);
          pos += 2;
          break;
        case 3:
          if (inputLength <= pos + 3) {
            throw new Error("snappy error end of input");
          }
          len = (c >>> 2) + 1;
          offset =
            input[pos] +
            (input[pos + 1] << 8) +
            (input[pos + 2] << 16) +
            (input[pos + 3] << 24);
          pos += 4;
          break;
        default:
          break;
      }
      if (offset === 0 || isNaN(offset)) {
        throw new Error(
          `invalid offset ${offset} pos ${pos} inputLength ${inputLength}`,
        );
      }
      if (offset > outPos) {
        throw new Error("cannot copy from before start of buffer");
      }
      copyBytes(output, outPos - offset, output, outPos, len);
      outPos += len;
    }
  }

  if (outPos !== outputLength) throw new Error("premature end of input");
}

/**
 * Convenience: decompress snappy data, auto-detecting output size from preamble.
 */
export function snappyDecode(input: Uint8Array): Uint8Array {
  const uncompressedLength = snappyUncompressedLength(input);
  const output = new Uint8Array(uncompressedLength);
  snappyUncompress(input, output);
  return output;
}
