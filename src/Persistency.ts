import * as Fs from "fs";
import * as Path from "path";

import { openFiles, OpenFilesContext, shake128 } from "./utils";
import { MemoryBlocks, Block } from "./MemoryBlocks";
import { EMPTY_ENTRY, Bytes, EntryHeaderOffsets_V0, EntryOffsets_V0, MAGIC, Values } from "./constants";

export type PersistencyOptions = {
    folder:string;
    reclaimTimeout?:number;
};
export type PersistencyContext = Partial<{
    now():number; // Inject "world access" dependency for easier testing
    fs:OpenFilesContext;
}>;
const enum Purging {
    None,
    Entry,
    EntryAndData
}
type Entry = {
    block:Block<Entry>;
    dataBlock:Block<Entry>;
    valueLocation:number;
    dataVersion:number;
    purging:Purging;
};
type PartialEntry = Omit<Entry, "block"|"dataBlock"> & {
    block:Block<Entry>|null;
    dataBlock:Block<Entry>|null;
};
type PartialDataEntry = Omit<Entry, "dataBlock"> & {
    block:Block<Entry>;
    dataBlock:Block<Entry>|null;
};
type LoadingEntry = {
    entry:PartialEntry;
    location:number;
    dataLocation:number;
    valueSize:number;
};
export class Persistency {
    private _fd;
    private _closed = false;
    readonly entriesFile;
    readonly dataFile;
    readonly reclaimTimeout;
    private _data = new Map<string, [Entry, ...Entry[]]>();
    private _purgeEntries:{key:string; entry:Entry; ts:number}[] = [];
    private _entriesMemory = new MemoryBlocks<Entry>(MAGIC.length);
    private _dataMemory = new MemoryBlocks<Entry>(MAGIC.length);
    private _context;
    private _fileSizes = {
        entries: Infinity,
        data: Infinity
    };
    constructor(options:PersistencyOptions, context?:PersistencyContext) {
        if (!options.folder) {
            throw new Error("Invalid folder");
        }
        this._context = {
            now: Date.now,
            fs: Fs,
            ...context
        };
        this.entriesFile = Path.join(options.folder, "entries.db");
        this.dataFile = Path.join(options.folder, "data.db");
        this.reclaimTimeout = options.reclaimTimeout ?? 10000;
        this._fd = this._loadDataSync();
        this._checkPurge();
        this._compact();
        this._checkTruncate();
    }
    private _loadDataSync() {
        const fd = openFiles({
            entriesFile: this.entriesFile,
            dataFile: this.dataFile
        }, this._context.fs);
        try {
            const entryHeaderBuffer = Buffer.allocUnsafe(EntryHeaderOffsets_V0.SIZE);
            const entryBuffer = Buffer.allocUnsafe(EntryOffsets_V0.SIZE);
            const data = new Map<string, [LoadingEntry, ...LoadingEntry[]]>();
            const magic = Buffer.allocUnsafe(4);
            const entriesReader = fd.entries.reader();
            if (!fd.data.read(magic, 0, false)) {
                if (!magic.equals(MAGIC)) {
                    throw new Error("Data file is not a persistency one");
                }
            } else {
                fd.data.write(MAGIC, 0);
            }
            if (entriesReader.read(magic, false)) {
                fd.entries.write(MAGIC, 0);
            } else {
                if (!magic.equals(MAGIC)) {
                    throw new Error("Entries file is not a persistency one");
                }
                while (true) {
                    const entryLocation = entriesReader.offset;
                    try {
                        if (entriesReader.read(entryHeaderBuffer, false)) {
                            break;
                        }
                        if (entryHeaderBuffer[EntryHeaderOffsets_V0.ENTRY_VERSION] !== 0) {
                            entriesReader.advance(entryBuffer.length);
                            throw new Error("Invalid entry version");
                        }
                        const storedEntryHash = entryHeaderBuffer.subarray(EntryHeaderOffsets_V0.ENTRY_HASH, EntryHeaderOffsets_V0.ENTRY_HASH + Bytes.SHAKE_128);
                        entriesReader.read(entryBuffer, true);
                        const entryHash = shake128(entryBuffer);
                        if (!entryHash.equals(storedEntryHash)) {
                            throw new Error("Invalid entry hash");
                        }
                        const dataLocation = entryBuffer.readUIntBE(EntryOffsets_V0.LOCATION, Bytes.UINT_48) + (entryBuffer[EntryOffsets_V0.LOCATION + 6] * Values.UINT_48_ROLLOVER); // Maximum read value in nodejs is 6 bytes, so we need a workaround for 7 bytes
                        if (dataLocation > 0) {
                            const storedDataHash = entryBuffer.subarray(EntryOffsets_V0.DATA_HASH, EntryOffsets_V0.DATA_HASH + Bytes.SHAKE_128);
                            const dataVersion = entryBuffer.readUInt32BE(EntryOffsets_V0.DATA_VERSION);
                            const keySize = entryBuffer.readUInt32BE(EntryOffsets_V0.KEY_SIZE);
                            const valueSize = entryBuffer.readUInt32BE(EntryOffsets_V0.VALUE_SIZE);
                            const dataBuffer = Buffer.allocUnsafe(keySize + valueSize);
                            fd.data.read(dataBuffer, dataLocation, true);
                            const dataHash = shake128(dataBuffer);
                            if (!dataHash.equals(storedDataHash)) {
                                // TODO: Test this
                                throw new Error("Invalid data hash");
                            }
                            const key:string = (dataBuffer as any).utf8Slice(0, keySize); // small optimization non-documented methods
                            const valueLocation = dataLocation + keySize;
                            const loadingEntry:LoadingEntry = {
                                entry: {
                                    block: null,
                                    dataBlock: null,
                                    dataVersion: dataVersion,
                                    valueLocation: valueLocation,
                                    purging: Purging.None
                                },
                                location: entryLocation,
                                dataLocation: dataLocation,
                                valueSize: valueSize
                            };
                            const loadingEntries = data.get(key);
                            if (loadingEntries) {
                                loadingEntries.push(loadingEntry);
                            } else {
                                data.set(key, [ loadingEntry ]);
                            }
                        }
                    } catch (e) {
                        console.error(e);
                        // TODO: log invalid entry
                    }
                }
            }
            const allLoadingEntries:LoadingEntry[] = [];
            for (const [ key, loadingEntries ] of data) {
                loadingEntries.sort((a, b) => {
                    if (a.entry.dataVersion > b.entry.dataVersion) {
                        if ((a.entry.dataVersion - b.entry.dataVersion) >= Values.UINT_32_HALF) {
                            return -1;
                        }
                    } else if (a.entry.dataVersion < b.entry.dataVersion) {
                        if ((b.entry.dataVersion - a.entry.dataVersion) < Values.UINT_32_HALF) {
                            return -1;
                        }
                    } else {
                        return b.location - a.location;
                    }
                    return 1;
                });
                const entries = [];
                for (let i = 0; i < loadingEntries.length - 1; i++) {
                    if (this.reclaimTimeout > 0) {
                        const nextEntry = loadingEntries[i + 1].entry;
                        if (nextEntry.dataVersion === loadingEntries[i].entry.dataVersion) {
                            this._purgeEntry(key, loadingEntries[i].entry as Required<Entry>);
                        } else {
                            this._purgeEntryAndData(key, loadingEntries[i].entry as Required<Entry>);
                        }
                        entries.push(loadingEntries[i].entry as Required<Entry>);
                        allLoadingEntries.push(loadingEntries[i]);
                    } else {
                        this._fd.entries.write(EMPTY_ENTRY, loadingEntries[i].location);
                    }
                }
                allLoadingEntries.push(loadingEntries[loadingEntries.length - 1]);
                entries.push(loadingEntries[loadingEntries.length - 1].entry as Required<Entry>);
                this._data.set(key, entries as [Entry, ...Entry[]]);
            }
            this._setAllocatedMemory(allLoadingEntries);
        } catch (e) {
            console.error(e);
            // TODO: log invalid persistency
            throw e;
        } finally {
            // Always close. Will reopen if needed
            fd.close();
        }
        // Reopen as reaching EOF may close the files
        return openFiles({
            entriesFile: this.entriesFile,
            dataFile: this.dataFile
        }, this._context.fs);
    }
    private _compact() {
        this._compactData();
        this._compactEntries();
        if (this._checkPurge()) {
            this._compact();
        }
    }
    private _compactData() {
        let lastDataBlock = this._dataMemory.getLastBlock();
        if (lastDataBlock) {
            let { maxSpace, blocks } = this._dataMemory.getFreeBlocks();
            if (blocks.length > 0) {
                let minLocation = blocks[0].location;
                do {
                    if (!lastDataBlock.data.purging) {
                        const dataSize = lastDataBlock.end - lastDataBlock.start;
                        if (dataSize <= maxSpace) {
                            for (let i = 0; i < blocks.length; i++) {
                                const { location, space, block } = blocks[i];
                                if (lastDataBlock.start < location) {
                                    blocks.splice(i);
                                    break;
                                }
                                if (dataSize <= space) {
                                    const dataBuffer = Buffer.allocUnsafe(dataSize);
                                    this._fd.data.read(dataBuffer, lastDataBlock.start, true);
                                    const keySize = lastDataBlock.data.valueLocation - lastDataBlock.data.dataBlock.start;
                                    const key:string = (dataBuffer as any).utf8Slice(0, lastDataBlock.data.valueLocation - lastDataBlock.data.dataBlock.start); // small optimization non-documented methods
                                    const entries = this._data.get(key)!;
    
                                    const lastEntry = entries[entries.length - 1];
                                    const partialEntry = this._getFreeEntryLocation({
                                        block: null,
                                        dataBlock: null,
                                        valueLocation: 0,
                                        dataVersion: (lastEntry.dataVersion + 1) & Values.UINT_32 >>> 0,
                                        purging: Purging.None
                                    });
                                    const newEntry = this._getFreeDataAfterBlock(block.prev, dataSize, partialEntry);
                                    newEntry.valueLocation = newEntry.dataBlock.start + keySize;
                                    entries.push(newEntry);
    
                                    const entryHeaderBuffer = Buffer.allocUnsafe(EntryHeaderOffsets_V0.SIZE);
                                    const entryBuffer = Buffer.allocUnsafe(EntryOffsets_V0.SIZE);
                                    entryHeaderBuffer[EntryHeaderOffsets_V0.ENTRY_VERSION] = 0;
    
                                    const dataLocationByte7 = (newEntry.dataBlock.start / Values.UINT_48_ROLLOVER) | 0;
                                    entryBuffer.writeUIntBE(newEntry.dataBlock.start - (dataLocationByte7 * Values.UINT_48_ROLLOVER), EntryOffsets_V0.LOCATION, Bytes.UINT_48);
                                    entryBuffer[EntryOffsets_V0.LOCATION + 6] = dataLocationByte7;
    
                                    const dataHash = shake128(dataBuffer);
                                    dataHash.copy(entryBuffer, EntryOffsets_V0.DATA_HASH);
                                    entryBuffer.writeUInt32BE(newEntry.dataVersion, EntryOffsets_V0.DATA_VERSION);
                                    entryBuffer.writeUInt32BE(keySize, EntryOffsets_V0.KEY_SIZE);
                                    entryBuffer.writeUInt32BE(dataSize - keySize, EntryOffsets_V0.VALUE_SIZE);
                                    const entryHash = shake128(entryBuffer);
                                    entryHash.copy(entryHeaderBuffer, EntryHeaderOffsets_V0.ENTRY_HASH);
    
                                    this._fd.data.write(dataBuffer, newEntry.dataBlock.start);
                                    this._fd.data.fsync();
    
                                    this._fd.entries.write(entryHeaderBuffer, newEntry.block.start);
                                    this._fd.entries.write(entryBuffer, newEntry.block.start + entryHeaderBuffer.length);
                                    this._fd.entries.fsync();
                                    
                                    this._purgeEntryAndData(key, lastEntry);
                                    if (dataSize < space) {
                                        blocks[i].space = space - dataSize;
                                    } else {
                                        blocks.splice(i, 1);
                                    }
                                    if (maxSpace === space) {
                                        let maxSpace = 0;
                                        for (let i = 0; i < blocks.length; i++) {
                                            if (blocks[i].space > maxSpace) {
                                                maxSpace = blocks[i].space;
                                            }
                                        }
                                    }
                                    if (blocks.length > 0) {
                                        minLocation = blocks[0].location;
                                    }
                                    break;
                                }
                            }
                            if (blocks.length === 0) {
                                break;
                            }
                        }
                    }
                    lastDataBlock = lastDataBlock.prev;
                } while (lastDataBlock && lastDataBlock.start > minLocation);
            }
        }
    }
    private _compactEntries() {
        let lastEntryBlock = this._entriesMemory.getLastBlock();
        if (lastEntryBlock) {
            const { blocks } = this._entriesMemory.getFreeBlocks();
            if (blocks.length > 0) {
                do {
                    if (!lastEntryBlock.data.purging) {
                        const { location, block } = blocks.shift()!;
                        if (lastEntryBlock.start < location) {
                            break;
                        }
                        const entryBuffer = Buffer.allocUnsafe(lastEntryBlock.end - lastEntryBlock.start);
                        this._fd.entries.read(entryBuffer, lastEntryBlock.start, true);
                        const keySize = lastEntryBlock.data.valueLocation - lastEntryBlock.data.dataBlock.start;
                        const keyBuffer = Buffer.allocUnsafe(keySize);
                        this._fd.data.read(keyBuffer, lastEntryBlock.data.dataBlock.start, true);
                        const key:string = (keyBuffer as any).utf8Slice(); // small optimization non-documented methods
                        const entries = this._data.get(key)!;

                        const newEntry = this._getFreeEntryAfterBlock(block.prev, {
                            block: null,
                            dataBlock: lastEntryBlock.data.dataBlock,
                            valueLocation: lastEntryBlock.data.valueLocation,
                            dataVersion: lastEntryBlock.data.dataVersion,
                            purging: Purging.None
                        }) as Entry;

                        entries.push(newEntry);
                        this._fd.entries.write(entryBuffer, newEntry.block.start);
                        this._fd.entries.fsync();
                        this._purgeEntry(key, lastEntryBlock.data);
                    }
                    lastEntryBlock = lastEntryBlock.prev;
                } while (lastEntryBlock && blocks.length > 0 && lastEntryBlock.start > blocks[0].location);
            }
        }
    }
    private _deleteEntryAndData(entry:Entry) {
        const memoryShrinked = this._deleteEntry(entry);
        return this._dataMemory.free(entry.dataBlock) || memoryShrinked;
    }
    private _deleteEntry(entry:Entry) {
        this._fd.entries.write(EMPTY_ENTRY, entry.block.start);
        return this._entriesMemory.free(entry.block);
    }
    private _setAllocatedMemory(entries:LoadingEntry[]) {
        this._setAllocatedEntries(entries);
        this._setFreeMemoryData(entries);
    }
    private _setAllocatedEntries(entries:LoadingEntry[]) {
        entries.sort((a, b) => a.location - b.location);
        const entrySize = EntryHeaderOffsets_V0.SIZE + EntryOffsets_V0.SIZE;
        const entryAllocation = this._entriesMemory.setAllocation();
        for (const loadingEntry of entries) {
            loadingEntry.entry.block = entryAllocation.add(loadingEntry.location, loadingEntry.location + entrySize, loadingEntry.entry as Required<Entry>);
        }
    }
    private _setFreeMemoryData(entries:LoadingEntry[]) {
        entries.sort((a, b) => a.dataLocation - b.dataLocation);
        const dataAllocation = this._dataMemory.setAllocation();
        for (const loadingEntry of entries) {
            loadingEntry.entry.dataBlock = dataAllocation.add(loadingEntry.dataLocation, loadingEntry.entry.valueLocation + loadingEntry.valueSize, loadingEntry.entry as Required<Entry>);
        }
    }
    private _getFreeEntryLocation(entry:PartialEntry) {
        entry.block = this._entriesMemory.alloc(EntryHeaderOffsets_V0.SIZE + EntryOffsets_V0.SIZE, entry as Required<Entry>);
        return entry as PartialDataEntry;
    }
    private _getFreeDataLocation(size:number, entry:PartialDataEntry) {
        entry.dataBlock = this._dataMemory.alloc(size, entry as Required<Entry>);
        return entry as Entry;
    }
    private _getFreeEntryAfterBlock(block:Block<Entry>|null, entry:PartialEntry) {
        if (block != null) {
            entry.block = this._entriesMemory.allocAfter(block, EntryHeaderOffsets_V0.SIZE + EntryOffsets_V0.SIZE, entry as Required<Entry>);
        } else {
            entry.block = this._entriesMemory.alloc(EntryHeaderOffsets_V0.SIZE + EntryOffsets_V0.SIZE, entry as Required<Entry>);
        }
        return entry as PartialDataEntry;
    }
    private _getFreeDataAfterBlock(block:Block<Entry>|null, size:number, entry:PartialDataEntry) {
        if (block != null) {
            entry.dataBlock = this._dataMemory.allocAfter(block, size, entry as Required<Entry>);
        } else {
            entry.dataBlock = this._dataMemory.alloc(size, entry as Required<Entry>);
        }
        return entry as Entry;
    }
    private _purgeEntryAndData(key:string, entry:Entry) {
        entry.purging = Purging.EntryAndData;
        this._purgeEntries.push({
            key: key,
            entry: entry,
            ts: this._context.now() + this.reclaimTimeout
        });
    }
    private _purgeEntry(key:string, entry:Entry) {
        entry.purging = Purging.Entry;
        this._purgeEntries.push({
            key: key,
            entry: entry,
            ts: this._context.now() + this.reclaimTimeout
        });
    }
    private _checkTruncate() {
        this._checkTruncateEntries();
        this._checkTruncateData();
    }
    private _checkTruncateEntries() {
        const lastEntryBlock = this._entriesMemory.getLastBlock();
        const end = lastEntryBlock ? lastEntryBlock.end : this._entriesMemory.offset;
        if (this._fileSizes.entries > end) {
            this._fd.entries.truncate(end);
        }
        this._fileSizes.entries = end;
    }
    private _checkTruncateData() {
        const lastDataBlock = this._dataMemory.getLastBlock();
        const end = lastDataBlock ? lastDataBlock.end : this._dataMemory.offset;
        if (this._fileSizes.data > end) {
            this._fd.data.truncate(end);
        }
        this._fileSizes.data = end;
    }
    private _checkPurge() {
        let needsCompact = false;
        if (this._purgeEntries.length > 0) {
            const now = this._context.now();
            let i = 0;
            while (i < this._purgeEntries.length) {
                const data = this._purgeEntries[i];
                if (data.ts <= now) {
                    if (data.entry.purging === Purging.EntryAndData) {
                        needsCompact = !this._deleteEntryAndData(data.entry) || needsCompact;
                    } else {
                        needsCompact = !this._deleteEntry(data.entry) || needsCompact;
                    }
                    const entries = this._data.get(data.key)!;
                    entries.splice(entries.indexOf(data.entry), 1);
                } else {
                    break;
                }
                i++; // After the break so _purgeEntries.splice applies only over purged entries
            }
            if (i > 0) {
                this._purgeEntries.splice(0, i);
            }
        }
        return needsCompact;
    }
    private _getEntryValue(entry:Entry) {
        const valueBuffer = Buffer.allocUnsafe(entry.dataBlock.end - entry.valueLocation);
        this._fd.data.read(valueBuffer, entry.valueLocation, true); // Always read from file so can have more data than available RAM. The OS will handle the caché
        return valueBuffer;
    }
    compact() {
        if (this._checkPurge()) {
            this._compact();
        }
        this._checkTruncate();
    }
    count() {
        return this._data.size;
    }
    *cursor():Generator<[string, Buffer], null, void> {
        for (const [key, entry] of this._data) {
            yield [key, this._getEntryValue(entry[entry.length - 1])];
        }
        return null;
    }
    set(key:string, value:Buffer) {
        let needsCompact = this._checkPurge();
        const entries = this._data.get(key);
        const keyBuffer = Buffer.from(key);
        const dataHash = shake128(keyBuffer, value);

        // TODO: Duplicated in compact logic. Fix somehow...
        const partialEntry = this._getFreeEntryLocation({
            block: null,
            dataBlock: null,
            valueLocation: 0,
            dataVersion: 0,
            purging: Purging.None
        });
        const entry = this._getFreeDataLocation(keyBuffer.length + value.length, partialEntry);
        entry.valueLocation = entry.dataBlock.start + keyBuffer.length;

        let lastEntry;
        if (entries) {
            lastEntry = entries[entries.length - 1];
            entry.dataVersion = (lastEntry.dataVersion + 1) & Values.UINT_32 >>> 0;
            entries.push(entry);
        } else {
            this._data.set(key, [ entry ]);
        }
        const entryHeaderBuffer = Buffer.allocUnsafe(EntryHeaderOffsets_V0.SIZE);
        const entryBuffer = Buffer.allocUnsafe(EntryOffsets_V0.SIZE);
        entryHeaderBuffer[EntryHeaderOffsets_V0.ENTRY_VERSION] = 0;

        // Workaround to write 7 bytes in nodejs
        const dataLocationByte7 = (entry.dataBlock.start / Values.UINT_48_ROLLOVER) | 0;
        entryBuffer.writeUIntBE(entry.dataBlock.start - (dataLocationByte7 * Values.UINT_48_ROLLOVER), EntryOffsets_V0.LOCATION, Bytes.UINT_48);
        entryBuffer[EntryOffsets_V0.LOCATION + 6] = dataLocationByte7;

        dataHash.copy(entryBuffer, EntryOffsets_V0.DATA_HASH);
        entryBuffer.writeUInt32BE(entry.dataVersion, EntryOffsets_V0.DATA_VERSION);
        entryBuffer.writeUInt32BE(keyBuffer.length, EntryOffsets_V0.KEY_SIZE);
        entryBuffer.writeUInt32BE(value.length, EntryOffsets_V0.VALUE_SIZE);
        const entryHash = shake128(entryBuffer);
        entryHash.copy(entryHeaderBuffer, EntryHeaderOffsets_V0.ENTRY_HASH);

        this._fd.data.write(keyBuffer, entry.dataBlock.start);
        this._fd.data.write(value, entry.dataBlock.start + keyBuffer.length);
        this._fd.data.fsync();

        this._fd.entries.write(entryHeaderBuffer, entry.block.start);
        this._fd.entries.write(entryBuffer, entry.block.start + entryHeaderBuffer.length);
        this._fd.entries.fsync();

        if (entries && this.reclaimTimeout <= 0) {
            // Delete old entries after data write
            for (const entry of entries.splice(0, entries.length - 1)) {
                needsCompact = !this._deleteEntryAndData(entry) || needsCompact;
            }
        } else if (lastEntry) {
            this._purgeEntryAndData(key, lastEntry);
        }
        if (needsCompact) {
            this._compact();
        }
        this._checkTruncate();
    }
    get(key:string) {
        const entries = this._data.get(key);
        if (entries) {
            const lastEntry = entries[entries.length - 1];
            return this._getEntryValue(lastEntry);
        } else {
            return null;
        }
    }
    delete(key:string) {
        const entries = this._data.get(key);
        if (entries) {
            let isPurging = false;
            let needsCompact = false;
            for (const entry of entries) {
                if (entry.purging !== Purging.None) {
                    isPurging = true;
                }
                needsCompact = !this._deleteEntryAndData(entry) || needsCompact;
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
            if (needsCompact) {
                this._compact();
            }
            this._checkTruncate();
            return true;
        } else {
            return false;
        }
    }
    getAllocatedBlocks() {
        return {
            entries: this._entriesMemory.getAllocatedRanges(),
            data: this._dataMemory.getAllocatedRanges()
        };
    }
    close() {
        if (this._closed) {
            return;
        }
        this._closed = true;
        this.compact();
        if (this._fd) {
            this._fd.entries.fsync();
            this._fd.data.fsync();
            this._fd.close();
        }
    }
}