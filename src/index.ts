import * as Path from "path";

import { openFiles, sha256 } from "./utils";
import { Bytes, EMPTY_ENTRY, EntryHeaderOffsets_V0, EntryOffsets_V0, MAGIC, Values } from "./constants";

export type PersistencyOptions = {
    folder:string;
    reclaimTimeout?:number;
};
type Entry = {
    location:number;
    dataLocation:number;
    valueLocation:number;
    valueSize:number;
    ts:number;
    purging:boolean;
};
type FreeLocation = {
    start:number;
} & ({
    end:number;
    next:FreeLocation;
} | {
    end:null;
    next:null;
});
export class Persistency {
    private _fd;
    private _closed = false;
    readonly entriesFile;
    readonly dataFile;
    readonly reclaimTimeout;
    private _data = new Map<string, Entry[]>();
    private _nextFreeEntry:FreeLocation = {
        start: MAGIC.length,
        end: null,
        next: null
    };
    private _nextFreeData:FreeLocation = {
        start: MAGIC.length,
        end: null,
        next: null
    };
    private _purgeEntries:{key:string; entry:Entry; ts:number}[] = [];
    constructor(options:PersistencyOptions) {
        if (!options.folder) {
            throw new Error("Invalid folder");
        }
        this.entriesFile = Path.join(options.folder, "entries.db");
        this.dataFile = Path.join(options.folder, "data.db");
        this.reclaimTimeout = options.reclaimTimeout ?? 10000;
        this._fd = this._loadDataSync();
    }
    private _loadDataSync() {
        const fd = openFiles({
            entriesFile: this.entriesFile,
            dataFile: this.dataFile
        });
        try {
            const entryHeaderBuffer = Buffer.allocUnsafe(EntryHeaderOffsets_V0.SIZE);
            const entryBuffer = Buffer.allocUnsafe(EntryOffsets_V0.SIZE);
            const magic = Buffer.allocUnsafe(4);
            const entriesReader = fd.entries.reader();
            if (fd.data.read(magic, 0, false)) {
                if (!magic.equals(MAGIC)) {
                    throw new Error("Data file is not a persistency one");
                }
            } else {
                fd.data.write(MAGIC, 0);
            }
            if (!entriesReader.read(magic, false)) {
                fd.entries.write(MAGIC, 0);
            } else {
                if (!magic.equals(MAGIC)) {
                    throw new Error("Entries file is not a persistency one");
                }
                while (true) {
                    const entryLocation = entriesReader.offset;
                    if (!entriesReader.read(entryHeaderBuffer, false)) {
                        break;
                    }
                    try {
                        if (entryHeaderBuffer[EntryHeaderOffsets_V0.VERSION] !== 0) {
                            throw new Error("Invalid entry version");
                        }
                        const storedEntryHash = entryHeaderBuffer.subarray(EntryHeaderOffsets_V0.ENTRY_HASH, EntryHeaderOffsets_V0.ENTRY_HASH + Bytes.SHA_256);
                        entriesReader.read(entryBuffer, true);
                        const entryHash = sha256(entryBuffer);
                        if (!entryHash.equals(storedEntryHash)) {
                            throw new Error("Invalid entry hash");
                        }
                        const dataLocation = entryBuffer.readUIntBE(EntryOffsets_V0.LOCATION, Bytes.UINT_48) + (entryBuffer[EntryOffsets_V0.LOCATION + 6] * Values.UINT_48_ROLLOVER); // Maximum read value in nodejs is 6 bytes, so we need a workaround for 7 bytes
                        if (dataLocation > 0) {
                            const storedDataHash = entryBuffer.subarray(EntryOffsets_V0.DATA_HASH, EntryOffsets_V0.DATA_HASH + Bytes.SHA_256);
                            const ts = entryBuffer.readUInt32BE(EntryOffsets_V0.TS);
                            const keySize = entryBuffer.readUInt32BE(EntryOffsets_V0.KEY_SIZE);
                            const valueSize = entryBuffer.readUIntBE(EntryOffsets_V0.VALUE_SIZE, Bytes.UINT_48);
                            const dataBuffer = Buffer.allocUnsafe(keySize + valueSize);
                            fd.data.read(dataBuffer, dataLocation, true);
                            const dataHash = sha256(dataBuffer);
                            if (!dataHash.equals(storedDataHash)) {
                                throw new Error("Invalid data hash");
                            }
                            const key = (dataBuffer as any).utf8Slice(0, keySize); // small optimization non-documented methods
                            const entry:Entry = {
                                location: entryLocation,
                                dataLocation: dataLocation,
                                valueLocation: dataLocation + keySize,
                                valueSize: valueSize,
                                ts: ts,
                                purging: false
                            };
                            const entries = this._data.get(key);
                            if (entries) {
                                const lastEntry = entries[0];
                                if (entry.ts > lastEntry.ts) {
                                    if ((entry.ts - lastEntry.ts) < Values.UINT_32_HALF) {
                                        entries.splice(0, 1, entry);
                                    }
                                } else if ((lastEntry.ts - entry.ts) >= Values.UINT_32_HALF) {
                                    entries.splice(0, 1, entry);
                                }
                            } else {
                                this._data.set(key, [ entry ]);
                            }
                        }
                    } catch (e) {
                        console.error(e);
                        // TODO: log invalid entry
                    }
                }
            }
        } catch (e) {
            this.close(); // free resources
            console.error(e);
            // TODO: log invalid persistency
            throw e;
        }
        fd.entries.truncate(this._updateFreeEntryList());
        fd.data.truncate(this._updateFreeDataList());
        // Reopen as reaching EOF may close the files
        fd.close();
        return openFiles({
            entriesFile: this.entriesFile,
            dataFile: this.dataFile
        });
    }
    private _updateFreeEntryList() {
        let nextFreeEntry = this._nextFreeEntry;
        const entrySize = EntryHeaderOffsets_V0.SIZE + EntryOffsets_V0.SIZE;
        for (const [entry] of this._data.values()) {
            if (nextFreeEntry.start === entry.location) {
                nextFreeEntry.start += entrySize;
            } else {
                nextFreeEntry.end = entry.location;
                nextFreeEntry = nextFreeEntry.next = {
                    start: entry.location + entrySize,
                    end: null,
                    next: null
                };
            }
        }
        return nextFreeEntry.start;
    }
    private _updateFreeDataList() {
        let nextFreeData = this._nextFreeData;
        for (const [entry] of Array.from(this._data.values()).sort(([a], [b]) => a.dataLocation - b.dataLocation)) {
            if (nextFreeData.start === entry.dataLocation) {
                nextFreeData.start = entry.valueLocation + entry.valueSize;
            } else {
                nextFreeData.end = entry.dataLocation;
                nextFreeData = nextFreeData.next = {
                    start: entry.valueLocation + entry.valueSize,
                    end: null,
                    next: null
                };
            }
        }
        return nextFreeData.start;
    }
    private _getFreeEntryLocation() {
        const location = this._nextFreeEntry.start;
        this._nextFreeEntry.start += EntryHeaderOffsets_V0.SIZE + EntryOffsets_V0.SIZE;
        if (this._nextFreeEntry.end != null && this._nextFreeEntry.start >= this._nextFreeEntry.end) {
            this._nextFreeEntry = this._nextFreeEntry.next;
        }
        return location;
    }
    private _getFreeDataLocation(size:number) {
        let dataLocation:FreeLocation|null = this._nextFreeData;
        do {
            if (dataLocation.end == null) {
                const location = dataLocation.start;
                dataLocation.start += size;
                return location;
            } else if (dataLocation.end - dataLocation.start > size) {
                const location = dataLocation.start;
                dataLocation.start += size;
                if (dataLocation.start >= dataLocation.end) {
                    dataLocation.start = dataLocation.next.start;
                    dataLocation.end = dataLocation.next.end!;
                    dataLocation.next = dataLocation.next.next!;
                }
                return location;
            }
            dataLocation = dataLocation.next;
        } while(dataLocation);
        throw new Error("Free data location not found");
    }
    private _freeLocation(root:FreeLocation, start:number, end:number) {
        // Returns true if the free happened at the end
        let next:FreeLocation|null = root;
        do {
            if (next.start <= end) {
                if (next.end === start) {
                    next.end = end;
                    if (next.end >= next.next.start) {
                        next.end = next.next.end!;
                        next.next = next.next.next!;
                        if (next.end == null) {
                            return true;
                        }
                    }
                } else if (next.start === end) {
                    next.start = start;
                    if (next.end == null) {
                        return true;
                    }
                } else {
                    next.next = {
                        start: start,
                        end: end,
                        next: next.next!
                    };
                }
                break;
            }
            next = next.next;
        } while (next);
        return false;
        next = {
            start: root.start,
            end: root.end!,
            next: root.next!
        };
        root.start = start;
        root.end = end;
        root.next = next;
    }
    private _freeEntry(entry:Entry) {
        if (this._freeLocation(this._nextFreeEntry, entry.location, entry.location + EntryHeaderOffsets_V0.SIZE + EntryOffsets_V0.SIZE)) {
            this._fd.entries.truncate(entry.location); // No need to fsync after this
        }
    }
    private _freeData(entry:Entry) {
        if (this._freeLocation(this._nextFreeData, entry.dataLocation, entry.valueLocation + entry.valueSize)) {
            this._fd.data.truncate(entry.location); // No need to fsync after this
        }
    }
    private _deleteEntry(entry:Entry) {
        this._fd.entries.write(EMPTY_ENTRY, entry.location); // No need to fsync after this. The worse that can happen is that data is not deleted if it crashes
        this._freeEntry(entry);
        this._freeData(entry);
    }
    private _purgeEntry(key:string, entry:Entry) {
        if (!entry.purging) {
            entry.purging = true;
            this._purgeEntries.push({
                key: key,
                entry: entry,
                ts: Date.now() + this.reclaimTimeout
            });
        }
    }
    private _checkPurge() {
        if (this._purgeEntries.length > 0) {
            const now = Date.now();
            let i = 0;
            while (i < this._purgeEntries.length) {
                const data = this._purgeEntries[i];
                if (data.ts <= now) {
                    this._deleteEntry(data.entry); // Delete instead of just freeing to avoid data never being overwritten and TS being the highest again
                    const entries = this._data.get(data.key)!;
                    entries.splice(entries.indexOf(data.entry), 1);
                    if (entries.length === 0) {
                        this._data.delete(data.key);
                    }
                } else {
                    break;
                }
                i++; // After the break so _purgeEntries.splice applies only over purged entries
            }
            if (i > 0) {
                this._purgeEntries.splice(0, i);
            }
        }
    }
    private _getEntry(entry:Entry) {
        const valueBuffer = Buffer.allocUnsafe(entry.valueSize);
        this._fd.data.read(valueBuffer, entry.valueLocation, true); // Always read from file so can have more data than available RAM. The OS will handle the caché
        return valueBuffer;
    }
    *cursor():Generator<[string, Buffer], null, void> {
        for (const [key, entry] of this._data) {
            yield [key, this._getEntry(entry[entry.length - 1])];
        }
        return null;
    }
    set(key:string, value:Buffer) {
        this._checkPurge();
        const entries = this._data.get(key);
        const keyBuffer = Buffer.from(key);
        const dataHash = sha256(keyBuffer, value);
        const dataLocation = this._getFreeDataLocation(keyBuffer.length + value.length);
        const entry:Entry = {
            location: this._getFreeEntryLocation(),
            dataLocation: dataLocation,
            valueLocation: dataLocation + keyBuffer.length,
            valueSize: value.length,
            ts: 0,
            purging: false
        };
        if (entries) {
            const lastEntry = entries[entries.length - 1];
            entry.ts = (lastEntry.ts + 1) & Values.UINT_32 >>> 0;
            if (this.reclaimTimeout > 0) {
                this._purgeEntry(key, lastEntry);
            }
            entries.push(entry);
        } else {
            this._data.set(key, [ entry ]);
        }
        const entryHeaderBuffer = Buffer.allocUnsafe(EntryHeaderOffsets_V0.SIZE);
        const entryBuffer = Buffer.allocUnsafe(EntryOffsets_V0.SIZE);
        entryHeaderBuffer[EntryHeaderOffsets_V0.VERSION] = 0;

        // Workaround to write 7 bytes in nodejs
        const dataLocationByte7 = (dataLocation / Values.UINT_48_ROLLOVER) | 0;
        entryBuffer.writeUIntBE(dataLocation - (dataLocationByte7 * Values.UINT_48_ROLLOVER), EntryOffsets_V0.LOCATION, Bytes.UINT_48);
        entryBuffer[EntryOffsets_V0.LOCATION + 6] = dataLocationByte7;

        dataHash.copy(entryBuffer, EntryOffsets_V0.DATA_HASH);
        entryBuffer.writeUInt32BE(entry.ts, EntryOffsets_V0.TS);
        entryBuffer.writeUInt32BE(keyBuffer.length, EntryOffsets_V0.KEY_SIZE);
        entryBuffer.writeUIntBE(value.length, EntryOffsets_V0.VALUE_SIZE, Bytes.UINT_48);
        const entryHash = sha256(entryBuffer);
        entryHash.copy(entryHeaderBuffer, EntryHeaderOffsets_V0.ENTRY_HASH);

        this._fd.data.write(keyBuffer, dataLocation);
        this._fd.data.write(value, dataLocation + keyBuffer.length);
        this._fd.data.fsync();
        
        this._fd.entries.write(entryHeaderBuffer, entry.location);
        this._fd.entries.write(entryBuffer, entry.location + entryHeaderBuffer.length);
        this._fd.entries.fsync();

        if (entries && this.reclaimTimeout <= 0) {
            for (const entry of entries.splice(0, entries.length - 1)) {
                this._deleteEntry(entry);
            }
            this._fd.entries.fsync(); // Another fsync to ensure that previous entry was fully written
        }
    }
    get(key:string) {
        const entries = this._data.get(key);
        if (entries) {
            const lastEntry = entries[entries.length - 1];
            return this._getEntry(lastEntry);
        } else {
            return null;
        }
    }
    delete(key:string) {
        const entries = this._data.get(key);
        if (entries) {
            let isPurging = false;
            for (const entry of entries) {
                if (entry.purging) {
                    isPurging = true;
                }
                this._deleteEntry(entry);
            }
            this._fd.entries.fsync();
            this._data.delete(key);
            if (isPurging) {
                for (let i = 0; i < this._purgeEntries.length; i++) {
                    if (this._purgeEntries[i].key === key) {
                        this._purgeEntries.splice(i--, 1);
                    }
                }
            }
            return true;
        } else {
            return false;
        }
    }
    close() {
        if (this._closed) {
            return;
        }
        this._closed = true;
        this._checkPurge();
        if (this._fd) {
            this._fd.entries.fsync();
            this._fd.data.fsync();
            this._fd.close();
        }
    }
}