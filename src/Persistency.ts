import * as Fs from "fs";
import * as Path from "path";

import { Fd, openFiles, OpenFilesContext, Reader, shake128 } from "./utils";
import { MemoryBlocks, Block } from "./MemoryBlocks";
import { DATA_VERSION, Bytes, EntryHeaderOffsets_V0, EntryOffsets_V0, MAGIC, Values, DataOffsets_V0 } from "./constants";

export type PersistencyOptions = {
    folder:string;
    reclaimDelay?:number;
    autoCompactTimeout?:number;
};
export type PersistencyContext = Partial<{
    now():number; // Inject "world access" dependency for easier testing
    fs:OpenFilesContext;
    setTimeout(cb:()=>void, ms:number):NodeJS.Timeout;
    clearTimeout(timer:NodeJS.Timeout):void;
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
    readonly reclaimDelay;
    private _data = new Map<string, [Entry, ...Entry[]]>();
    private _reclaimEntries:{key:string; entry:Entry; ttl:number}[] = [];
    private _reclaimTimeout:NodeJS.Timeout|null = null;
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
            setTimeout: setTimeout,
            clearTimeout: clearTimeout,
            ...context
        };
        this.entriesFile = Path.join(options.folder, "entries.db");
        this.dataFile = Path.join(options.folder, "data.db");
        if (options.reclaimDelay == null) {
            this.reclaimDelay = 15 * 60 * 1000;
        } else if (options.reclaimDelay > 0) {
            this.reclaimDelay = options.reclaimDelay;
        } else {
            this.reclaimDelay = 0;
            this._context.now = () => 0; // Avoids overhead and applying delays because of clock synchronizing forwards/backwards
        }
        this._fd = this._loadDataSync();
        this._checkReclaim();
        this._compact();
        this._checkTruncate();
    }
    private _readEntry(entriesReader:Reader, fdData:Fd, buffers:{ // Buffers object to avoid allocating buffers each time
        entryHeader:Buffer;
        entry:Buffer;
    }) {
        while (true) {
            const entryLocation = entriesReader.offset;
            try {
                if (entriesReader.read(buffers.entryHeader, false)) {
                    return null;
                }
                if (buffers.entryHeader[EntryHeaderOffsets_V0.ENTRY_VERSION] !== 0) {
                    entriesReader.advance(buffers.entry.length);
                    throw new Error("Invalid entry version");
                }
                entriesReader.read(buffers.entry, true);
                const dataLocation = buffers.entry.readUIntBE(EntryOffsets_V0.LOCATION, Bytes.UINT_48) + (buffers.entry[EntryOffsets_V0.LOCATION + 6] * Values.UINT_48_ROLLOVER); // Maximum read value in nodejs is 6 bytes, so we need a workaround for 7 bytes
                if (dataLocation > 0) {
                    const dataVersion = buffers.entry.readUInt32BE(EntryOffsets_V0.DATA_VERSION);
                    const keySize = buffers.entry.readUInt32BE(EntryOffsets_V0.KEY_SIZE);
                    const valueSize = buffers.entry.readUInt32BE(EntryOffsets_V0.VALUE_SIZE);
                    const dataBuffer = Buffer.allocUnsafe(DataOffsets_V0.SIZE + keySize + valueSize);
                    fdData.read(dataBuffer, dataLocation, true);
                    if (dataBuffer[DataOffsets_V0.VERSION] !== 0) {
                        throw new Error("Invalid data version");
                    }
                    const storedHash = buffers.entryHeader.subarray(EntryHeaderOffsets_V0.HASH, EntryHeaderOffsets_V0.HASH + Bytes.SHAKE_128);
                    const hash = shake128(buffers.entry, dataBuffer);
                    if (!hash.equals(storedHash)) {
                        throw new Error("Invalid data hash");
                    }
                    const key:string = (dataBuffer as any).utf8Slice(DataOffsets_V0.SIZE, DataOffsets_V0.SIZE + keySize); // small optimization non-documented methods
                    const valueLocation = dataLocation + DataOffsets_V0.SIZE + keySize;
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
                    return { key, data: loadingEntry };
                }
            } catch (e) {
                console.error(e);
                // TODO: log invalid entry
            }
        }
    }
    private _loadDataSync() {
        const fd = openFiles({
            entriesFile: this.entriesFile,
            dataFile: this.dataFile
        }, this._context.fs);
        try {
            const buffers = {
                entryHeader: Buffer.allocUnsafe(EntryHeaderOffsets_V0.SIZE),
                entry: Buffer.allocUnsafe(EntryOffsets_V0.SIZE)
            };
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
                    const entry = this._readEntry(entriesReader, fd.data, buffers);
                    if (entry) {
                        const loadingEntries = data.get(entry.key);
                        if (loadingEntries) {
                            loadingEntries.push(entry.data);
                        } else {
                            data.set(entry.key, [ entry.data ]);
                        }
                    } else {
                        break;
                    }
                }
            }
            const allLoadingEntries:LoadingEntry[] = [];
            for (const [ key, loadingEntries ] of data) {
                loadingEntries.sort((a, b) => { // Sort entries based on DataVersion
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
                    if (this.reclaimDelay > 0) {
                        const nextEntry = loadingEntries[i + 1].entry;
                        if (nextEntry.dataVersion === loadingEntries[i].entry.dataVersion) {
                            this._reclaimEntry(key, loadingEntries[i].entry as Required<Entry>);
                        } else {
                            this._reclaimEntryAndData(key, loadingEntries[i].entry as Required<Entry>);
                        }
                        entries.push(loadingEntries[i].entry as Required<Entry>);
                        allLoadingEntries.push(loadingEntries[i]);
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
        if (this._checkReclaim()) {
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
                                    const keySize = lastDataBlock.data.valueLocation - (lastDataBlock.data.dataBlock.start + DataOffsets_V0.SIZE);
                                    const key:string = (dataBuffer as any).utf8Slice(DataOffsets_V0.SIZE, DataOffsets_V0.SIZE + keySize); // small optimization non-documented methods
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
                                    newEntry.valueLocation = newEntry.dataBlock.start + DataOffsets_V0.SIZE + keySize;
                                    entries.push(newEntry);
    
                                    const entryHeaderBuffer = Buffer.allocUnsafe(EntryHeaderOffsets_V0.SIZE);
                                    const entryBuffer = Buffer.allocUnsafe(EntryOffsets_V0.SIZE);
                                    entryHeaderBuffer[EntryHeaderOffsets_V0.ENTRY_VERSION] = 0;
    
                                    const dataLocationByte7 = (newEntry.dataBlock.start / Values.UINT_48_ROLLOVER) | 0;
                                    entryBuffer.writeUIntBE(newEntry.dataBlock.start - (dataLocationByte7 * Values.UINT_48_ROLLOVER), EntryOffsets_V0.LOCATION, Bytes.UINT_48);
                                    entryBuffer[EntryOffsets_V0.LOCATION + 6] = dataLocationByte7;
    
                                    entryBuffer.writeUInt32BE(newEntry.dataVersion, EntryOffsets_V0.DATA_VERSION);
                                    entryBuffer.writeUInt32BE(keySize, EntryOffsets_V0.KEY_SIZE);
                                    entryBuffer.writeUInt32BE((dataSize - DataOffsets_V0.SIZE) - keySize, EntryOffsets_V0.VALUE_SIZE);
                                    const hash = shake128(entryBuffer, dataBuffer);
                                    hash.copy(entryHeaderBuffer, EntryHeaderOffsets_V0.HASH);
    
                                    this._fd.data.write(dataBuffer, newEntry.dataBlock.start);
                                    this._fd.data.fsync();
    
                                    this._fd.entries.write(entryHeaderBuffer, newEntry.block.start);
                                    this._fd.entries.write(entryBuffer, newEntry.block.start + entryHeaderBuffer.length);
                                    this._fd.entries.fsync();

                                    this._checkTruncate();
                                    
                                    this._reclaimEntryAndData(key, lastEntry);
                                    if (dataSize < space) {
                                        blocks[i].space = space - dataSize;
                                    } else {
                                        blocks.splice(i, 1);
                                    }
                                    if (maxSpace === space) {
                                        maxSpace = 0;
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
                        const { location, space, block } = blocks[0];
                        if (lastEntryBlock.start < location) {
                            break;
                        }
                        const entryBuffer = Buffer.allocUnsafe(lastEntryBlock.end - lastEntryBlock.start);
                        this._fd.entries.read(entryBuffer, lastEntryBlock.start, true);
                        const keySize = lastEntryBlock.data.valueLocation - (lastEntryBlock.data.dataBlock.start + DataOffsets_V0.SIZE);
                        const keyBuffer = Buffer.allocUnsafe(keySize);
                        if (keyBuffer.length > 0) {
                            this._fd.data.read(keyBuffer, lastEntryBlock.data.dataBlock.start + DataOffsets_V0.SIZE, true);
                        }
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
                        this._checkTruncate();
                        this._reclaimEntry(key, lastEntryBlock.data);
                        const dataSize = lastEntryBlock.end - lastEntryBlock.start;
                        if (dataSize < space) {
                            blocks[0].space = space - dataSize;
                        } else {
                            blocks.shift();
                        }
                    }
                    lastEntryBlock = lastEntryBlock.prev;
                } while (lastEntryBlock && blocks.length > 0 && lastEntryBlock.start > blocks[0].location);
            }
        }
    }
    private _deleteEntryAndData(entry:Entry) {
        const memoryShrinked = this._deleteEntry(entry);
        return this._dataMemory.free(entry.dataBlock) && memoryShrinked;
    }
    private _deleteEntry(entry:Entry) {
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
    private _reclaimEntryAndData(key:string, entry:Entry) {
        entry.purging = Purging.EntryAndData;
        this._reclaimEntries.push({
            key: key,
            entry: entry,
            ttl: this._context.now() + this.reclaimDelay
        });
        this._checkReclaimTimeout();
    }
    private _reclaimEntry(key:string, entry:Entry) {
        entry.purging = Purging.Entry;
        this._reclaimEntries.push({
            key: key,
            entry: entry,
            ttl: this._context.now() + this.reclaimDelay
        });
        this._checkReclaimTimeout();
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
    private _checkReclaimTimeout() {
        if (this.reclaimDelay > 0) {
            if (this._reclaimEntries.length > 0) {
                if (this._reclaimTimeout == null) {
                    this._reclaimTimeout = this._context.setTimeout(() => {
                        this._reclaimTimeout = null;
                        if (this._checkReclaim()) {
                            this._compact();
                        }
                        this._checkTruncate();
                    }, this.reclaimDelay);
                }
            } else if (this._reclaimTimeout != null) {
                this._context.clearTimeout(this._reclaimTimeout);
                this._reclaimTimeout = null;
            }
        }
    }
    private _checkReclaim() {
        let needsCompact = false;
        if (this._reclaimEntries.length > 0) {
            const now = this._context.now();
            let i = 0;
            while (i < this._reclaimEntries.length) {
                const data = this._reclaimEntries[i];
                if (data.ttl <= now) {
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
                i++; // After the break so _reclaimEntries.splice applies only over reclaimd entries
            }
            if (i > 0) {
                this._reclaimEntries.splice(0, i);
                this._checkReclaimTimeout();
            }
        }
        return needsCompact;
    }
    private _getEntryValue(entry:Entry) {
        const valueBuffer = Buffer.allocUnsafe(entry.dataBlock.end - entry.valueLocation);
        if (valueBuffer.length > 0) {
            this._fd.data.read(valueBuffer, entry.valueLocation, true); // Always read from file so can have more data than available RAM. The OS will handle the caché
        }
        return valueBuffer;
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
        let needsCompact = this._checkReclaim();
        const entries = this._data.get(key);
        const keyBuffer = Buffer.from(key);

        // TODO: Duplicated in compact logic. Fix somehow...
        const partialEntry = this._getFreeEntryLocation({
            block: null,
            dataBlock: null,
            valueLocation: 0,
            dataVersion: 0,
            purging: Purging.None
        });
        const entry = this._getFreeDataLocation(DataOffsets_V0.SIZE +  keyBuffer.length + value.length, partialEntry);
        entry.valueLocation = entry.dataBlock.start + DataOffsets_V0.SIZE + keyBuffer.length;

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

        entryBuffer.writeUInt32BE(entry.dataVersion, EntryOffsets_V0.DATA_VERSION);
        entryBuffer.writeUInt32BE(keyBuffer.length, EntryOffsets_V0.KEY_SIZE);
        entryBuffer.writeUInt32BE(value.length, EntryOffsets_V0.VALUE_SIZE);
        const hash = shake128(entryBuffer, DATA_VERSION, keyBuffer, value);
        hash.copy(entryHeaderBuffer, EntryHeaderOffsets_V0.HASH);

        this._fd.data.write(DATA_VERSION, entry.dataBlock.start);
        this._fd.data.write(keyBuffer, entry.dataBlock.start + DataOffsets_V0.SIZE);
        this._fd.data.write(value, entry.dataBlock.start + DataOffsets_V0.SIZE + keyBuffer.length);
        this._fd.data.fsync();

        this._fd.entries.write(entryHeaderBuffer, entry.block.start);
        this._fd.entries.write(entryBuffer, entry.block.start + entryHeaderBuffer.length);
        this._fd.entries.fsync();

        this._checkTruncate();

        if (entries && this.reclaimDelay <= 0) {
            // Delete old entries after data write
            for (const entry of entries.splice(0, entries.length - 1)) {
                needsCompact = !this._deleteEntryAndData(entry) || needsCompact;
            }
        } else if (lastEntry) {
            this._reclaimEntryAndData(key, lastEntry);
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
                for (let i = 0; i < this._reclaimEntries.length; i++) {
                    if (this._reclaimEntries[i].key === key) {
                        this._reclaimEntries.splice(i--, 1);
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
        if (this._reclaimTimeout != null) {
            this._context.clearTimeout(this._reclaimTimeout);
            this._reclaimTimeout = null;
        }
        if (this._fd) {
            if (this._checkReclaim()) {
                this._compact();
            }
            this._checkTruncate();
            this._fd.entries.fsync();
            this._fd.data.fsync();
            this._fd.close();
        }
    }
}