/**
 * Zarr v3 snappy codec.
 * Registered with zarrita's codec registry so that zarr arrays declaring
 * `{name: "snappy"}` in their codecs can be decoded transparently.
 *
 * Uses the snappy decompressor vendored from hyparquet.
 */

import { registry } from "zarrita";
import { snappyDecode } from "./vendored/snappy.js";

class SnappyCodec {
  kind = "bytes_to_bytes" as const;

  static fromConfig(): SnappyCodec {
    return new SnappyCodec();
  }

  encode(_bytes: Uint8Array): never {
    throw new Error(
      "Snappy encoding is not supported. This codec is decode-only.",
    );
  }

  decode(bytes: Uint8Array): Uint8Array {
    return snappyDecode(bytes);
  }
}

// Register the snappy codec so zarrita can resolve {name: "snappy"} in array metadata.
registry.set("snappy", () => Promise.resolve(SnappyCodec));
