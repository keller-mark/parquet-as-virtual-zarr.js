/**
 * Minimal Thrift Compact Protocol parser.
 * Vendored from hyparquet (MIT license).
 * Only includes what's needed for parsing parquet page headers.
 */

// TCompactProtocol types
const STOP = 0;
const TRUE = 1;
const FALSE = 2;
const BYTE = 3;
const I16 = 4;
const I32 = 5;
const I64 = 6;
const DOUBLE = 7;
const BINARY = 8;
const LIST = 9;
const STRUCT = 12;

export interface DataReader {
  view: DataView;
  offset: number;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type ThriftObject = { [key: `field_${number}`]: any };

export function createReader(bytes: Uint8Array): DataReader {
  return {
    view: new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength),
    offset: 0,
  };
}

export function deserializeTCompactProtocol(reader: DataReader): ThriftObject {
  const value: ThriftObject = {};
  let fid = 0;

  while (reader.offset < reader.view.byteLength) {
    const byte = reader.view.getUint8(reader.offset++);
    const type = byte & 0x0f;
    if (type === STOP) break;
    const delta = byte >> 4;
    fid = delta ? fid + delta : readZigZag(reader);
    value[`field_${fid}`] = readElement(reader, type);
  }

  return value;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function readElement(reader: DataReader, type: number): any {
  switch (type) {
    case TRUE:
      return true;
    case FALSE:
      return false;
    case BYTE:
      return reader.view.getInt8(reader.offset++);
    case I16:
    case I32:
      return readZigZag(reader);
    case I64:
      return readZigZagBigInt(reader);
    case DOUBLE: {
      const value = reader.view.getFloat64(reader.offset, true);
      reader.offset += 8;
      return value;
    }
    case BINARY: {
      const stringLength = readVarInt(reader);
      const strBytes = new Uint8Array(
        reader.view.buffer,
        reader.view.byteOffset + reader.offset,
        stringLength,
      );
      reader.offset += stringLength;
      return strBytes;
    }
    case LIST: {
      const byte = reader.view.getUint8(reader.offset++);
      const elemType = byte & 0x0f;
      let listSize = byte >> 4;
      if (listSize === 15) {
        listSize = readVarInt(reader);
      }
      const boolType = elemType === TRUE || elemType === FALSE;
      const values = new Array(listSize);
      for (let i = 0; i < listSize; i++) {
        values[i] = boolType
          ? readElement(reader, BYTE) === 1
          : readElement(reader, elemType);
      }
      return values;
    }
    case STRUCT:
      return deserializeTCompactProtocol(reader);
    default:
      throw new Error(`thrift unhandled type: ${type}`);
  }
}

export function readVarInt(reader: DataReader): number {
  let result = 0;
  let shift = 0;
  while (true) {
    const byte = reader.view.getUint8(reader.offset++);
    result |= (byte & 0x7f) << shift;
    if (!(byte & 0x80)) {
      return result;
    }
    shift += 7;
  }
}

function readVarBigInt(reader: DataReader): bigint {
  let result = 0n;
  let shift = 0n;
  while (true) {
    const byte = reader.view.getUint8(reader.offset++);
    result |= BigInt(byte & 0x7f) << shift;
    if (!(byte & 0x80)) {
      return result;
    }
    shift += 7n;
  }
}

export function readZigZag(reader: DataReader): number {
  const zigzag = readVarInt(reader);
  return (zigzag >>> 1) ^ -(zigzag & 1);
}

export function readZigZagBigInt(reader: DataReader): bigint {
  const zigzag = readVarBigInt(reader);
  return (zigzag >> 1n) ^ -(zigzag & 1n);
}
