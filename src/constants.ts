import { sha256 } from "./utils";

export const MAGIC = Buffer.from([ 0xFA, 0xF2, 0xD6, 0x91 ]);

export const EMPTY_ENTRY = Buffer.allocUnsafe(EntryHeaderOffsets_V0.SIZE + EntryOffsets_V0.SIZE);
EMPTY_ENTRY[EntryHeaderOffsets_V0.VERSION] = 0;
EMPTY_ENTRY.fill(0, EntryHeaderOffsets_V0.SIZE + EntryOffsets_V0.LOCATION);
const entryHash = sha256(EMPTY_ENTRY.subarray(EntryHeaderOffsets_V0.SIZE));
entryHash.copy(EMPTY_ENTRY, EntryHeaderOffsets_V0.ENTRY_HASH);

export const enum Bytes {
    SHA_256 = 256 / 8,
    UINT_8 = 8 / 8,
    UINT_16 = 16 / 8,
    UINT_32 = 32 / 8,
    UINT_48 = 48 / 8,
    UINT_56 = 56 / 8
}
export const enum Values {
    UINT_32 = 0xFFFFFFFF,
    UINT_32_HALF = ((Values.UINT_32 / 2) | 0) + 1,
    UINT_48_ROLLOVER = 2 ** 48
}
export const enum EntryHeaderOffsets_V0 {
    VERSION = 0,
    ENTRY_HASH = VERSION + Bytes.UINT_8,
    SIZE = ENTRY_HASH + Bytes.SHA_256
}
export const enum EntryOffsets_V0 {
    LOCATION = 0,
    DATA_HASH = LOCATION + Bytes.UINT_56,
    TS = DATA_HASH + Bytes.SHA_256,
    KEY_SIZE = TS + Bytes.UINT_32,
    VALUE_SIZE = KEY_SIZE + Bytes.UINT_32,
    SIZE = VALUE_SIZE + Bytes.UINT_48
}