export { ParquetAsAnnDataFrameStore } from "./parquet-store.js";
export { ArrowAsAnnDataFrameStore } from "./arrow-store.js";
// Side-effect: registers snappy codec with zarrita's codec registry
import "./snappy-codec.js";
