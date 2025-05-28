import { shake128 } from "./utils";

export const MAGIC = Buffer.from([ 0xFA, 0xF2, 0xD6, 0x91 ]);

export const DATA_VERSION = Buffer.from([ 0x00 ]);

export const EMPTY_ENTRY = Buffer.allocUnsafe(EntryHeaderOffsets_V0.SIZE + EntryOffsets_V0.SIZE);
EMPTY_ENTRY[EntryHeaderOffsets_V0.ENTRY_VERSION] = 0;
EMPTY_ENTRY.fill(0, EntryHeaderOffsets_V0.SIZE + EntryOffsets_V0.LOCATION);
const entryHash = shake128(EMPTY_ENTRY.subarray(EntryHeaderOffsets_V0.SIZE));
entryHash.copy(EMPTY_ENTRY, EntryHeaderOffsets_V0.HASH);

export const enum Bytes {
    SHAKE_128 = 128 / 8,
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
    ENTRY_VERSION = 0,
    HASH = ENTRY_VERSION + Bytes.UINT_8,
    SIZE = HASH + Bytes.SHAKE_128
}
export const enum EntryOffsets_V0 {
    LOCATION = 0,
    DATA_VERSION = LOCATION + Bytes.UINT_56,
    KEY_SIZE = DATA_VERSION + Bytes.UINT_32,
    VALUE_SIZE = KEY_SIZE + Bytes.UINT_32,
    SIZE = VALUE_SIZE + Bytes.UINT_32
}
export const enum DataOffsets_V0 {
    VERSION = 0,
    SIZE = VERSION + Bytes.UINT_8
}