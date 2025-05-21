import * as Fs from "fs/promises";

import test, { After, asyncMonad } from "arrange-act-assert";

import { assertDeepEqual, assertEqual, tempFolder } from "./testUtils";
import { Persistency, PersistencyContext, PersistencyOptions } from "./Persistency";
import * as constants from "./constants";
import { shake128 } from "./utils";

test.describe("Persistency", test => {
    // helpers
    const entrySize = constants.EntryHeaderOffsets_V0.SIZE + constants.EntryOffsets_V0.SIZE;
    const value1 = Buffer.from([ 0, 1, 2, 3,  4,  5 ]);
    const value2 = Buffer.from([ 6, 7, 8, 9, 10, 11 ]);
    const value3 = Buffer.from([ 0, 2, 4, 6,  8, 10 ]);
    const value4 = Buffer.from([ 1, 3, 5, 7,  9, 11 ]);
    function getEntryOffset(entryI:number) {
        return constants.MAGIC.length + (entrySize * entryI);
    }
    function getDataOffset(keyLength:number, dataI:number) {
        // All values have same length (value1, value2, etc)
        return {
            start: constants.MAGIC.length + ((keyLength + value1.length) * dataI),
            end: constants.MAGIC.length + ((keyLength + value1.length) * (dataI + 1))
        };
    }
    async function overwriteFile(file:string, offset:number, buffer:Buffer) {
        const data = await Fs.readFile(file);
        buffer.copy(data, offset);
        await Fs.writeFile(file, data);
    }
    function overwrite(persistency:Pick<Persistency, "entriesFile"|"dataFile">) {
        return {
            entry: {
                entryVersion(entryI:number, data:Buffer) {
                    return overwriteFile(persistency.entriesFile, getEntryOffset(entryI) + constants.EntryHeaderOffsets_V0.ENTRY_VERSION, data);
                },
                entryHash(entryI:number, data:Buffer) {
                    return overwriteFile(persistency.entriesFile, getEntryOffset(entryI) + constants.EntryHeaderOffsets_V0.ENTRY_HASH, data);
                },
                // TODO: Test del resto de overwrites
                location(entryI:number, data:Buffer) {
                    return overwriteFile(persistency.entriesFile, getEntryOffset(entryI) + constants.EntryHeaderOffsets_V0.SIZE + constants.EntryOffsets_V0.LOCATION, data);
                },
                dataHash(entryI:number, data:Buffer) {
                    return overwriteFile(persistency.entriesFile, getEntryOffset(entryI) + constants.EntryHeaderOffsets_V0.SIZE + constants.EntryOffsets_V0.DATA_HASH, data);
                },
                dataVersion(entryI:number, data:Buffer) {
                    return overwriteFile(persistency.entriesFile, getEntryOffset(entryI) + constants.EntryHeaderOffsets_V0.SIZE + constants.EntryOffsets_V0.DATA_VERSION, data);
                },
                keySize(entryI:number, data:Buffer) {
                    return overwriteFile(persistency.entriesFile, getEntryOffset(entryI) + constants.EntryHeaderOffsets_V0.SIZE + constants.EntryOffsets_V0.KEY_SIZE, data);
                },
                valueSize(entryI:number, data:Buffer) {
                    return overwriteFile(persistency.entriesFile, getEntryOffset(entryI) + constants.EntryHeaderOffsets_V0.SIZE + constants.EntryOffsets_V0.VALUE_SIZE, data);
                }
            }
        };
    }
    async function readEntry(persistency:Pick<Persistency, "entriesFile">, entryI:number) {
        const buffer = await Fs.readFile(persistency.entriesFile);
        return buffer.subarray(getEntryOffset(entryI), getEntryOffset(entryI) + entrySize);
    }
    async function setValueDataVersion(persistency:Pick<Persistency, "entriesFile">, entryI:number, dataVersion:number) {
        const buffer = await Fs.readFile(persistency.entriesFile);
        buffer.writeUint32BE(dataVersion, getEntryOffset(entryI) + constants.EntryHeaderOffsets_V0.SIZE + constants.EntryOffsets_V0.DATA_VERSION);
        await hashEntry(persistency, entryI, buffer);
    }
    async function hashEntry(persistency:Pick<Persistency, "entriesFile">, entryI:number, buffer?:Buffer) {
        buffer = buffer || await Fs.readFile(persistency.entriesFile);
        const entryLocation = getEntryOffset(entryI);
        const entryDataLocation = entryLocation + constants.EntryHeaderOffsets_V0.SIZE;
        const hash = shake128(buffer.subarray(entryDataLocation, entryDataLocation + constants.EntryOffsets_V0.SIZE));
        hash.copy(buffer, entryLocation + constants.EntryHeaderOffsets_V0.ENTRY_HASH);
        await Fs.writeFile(persistency.entriesFile, buffer);
    }
    async function newPersistency(after:After, options?:Partial<PersistencyOptions>|null, mock?:PersistencyContext) {
        const folder = options?.folder || await tempFolder(after);
        const persistency = after(new Persistency({
            folder: folder,
            reclaimTimeout: options?.reclaimTimeout
        }, mock), persistency => persistency.close());
        return { persistency, folder };
    }
    async function setEntryBytes(persistency:Pick<Persistency, "entriesFile">, entryI:number, offset:number, bytes:Buffer) {
        const buffer = await Fs.readFile(persistency.entriesFile);
        bytes.copy(buffer, getEntryOffset(entryI) + offset);
        await Fs.writeFile(persistency.entriesFile, buffer);
    }
    async function getFileSize(file:string) {
        return (await Fs.stat(file)).size;
    }
    async function getFileSizes(persistency:Pick<Persistency, "entriesFile"|"dataFile">) {
        return {
            entries: await getFileSize(persistency.entriesFile),
            data: await getFileSize(persistency.dataFile)
        };
    }
    function newpersistencyContext() {
        let now = Date.now();
        return {
            tick(ms:number) {
                now += ms;
            },
            set(ms:number) {
                now = ms;
            },
            now() {
                return now;
            }
        };
    }
    // end helpers
    test("should set and get data", {
        ARRANGE(after) {
            return newPersistency(after);
        },
        ACT({ persistency }) {
            persistency.set("test", value1);
        },
        ASSERTS: {
            "should have the data"(_, { persistency }) {
                assertDeepEqual(persistency.get("test"), value1);
            },
            "should count the number of entries"(_, { persistency }) {
                assertEqual(persistency.count(), 1);
            }
        }
    });
    test("should load data from file", {
        async ARRANGE(after) {
            const { persistency, folder } = await newPersistency(after);
            persistency.set("test", value1);
            persistency.set("test2", value2);
            persistency.close();
            return { folder };
        },
        ACT({ folder }, after) {
            return newPersistency(after, { folder });
        },
        ASSERTS: {
            "should have first data"({ persistency }) {
                assertDeepEqual(persistency.get("test"), value1);
            },
            "should have second data"({ persistency }) {
                assertDeepEqual(persistency.get("test2"), value2);
            },
            "should count the number of entries"({ persistency }) {
                assertEqual(persistency.count(), 2);
            }
        }
    });
    test("should update data", {
        async ARRANGE(after) {
            const { persistency, folder } = await newPersistency(after);
            persistency.set("test", value1);
            persistency.set("test", value2);
            persistency.close();
            return { folder };
        },
        ACT({ folder }, after) {
            return newPersistency(after, { folder });
        },
        ASSERTS: {
            "should have the correct data"({ persistency }) {
                assertDeepEqual(persistency.get("test"), value2);
            },
            "should count the number of entries"({ persistency }) {
                assertEqual(persistency.count(), 1);
            }
        }
    });
    test("should update data without reloading", {
        ARRANGE(after) {
            return newPersistency(after);
        },
        ACT({ persistency }) {
            persistency.set("test", value1);
            persistency.set("test", value2);
        },
        ASSERTS: {
            "should have the data data"(_, { persistency }) {
                assertDeepEqual(persistency.get("test"), value2);
            },
            "should count the number of entries"(_, { persistency }) {
                assertEqual(persistency.count(), 1);
            }
        }
    });
    test("should delete data", {
        async ARRANGE(after) {
            const { persistency } = await newPersistency(after);
            persistency.set("test", value1);
            persistency.set("test2", value2);
            return { persistency };
        },
        ACT({ persistency }) {
            persistency.delete("test");
        },
        ASSERTS: {
            "should not have first data"(_, { persistency }) {
                assertEqual(persistency.get("test"), null);
            },
            "should have second data"(_, { persistency }) {
                assertDeepEqual(persistency.get("test2"), value2);
            },
            "should count the number of entries"(_, { persistency }) {
                assertEqual(persistency.count(), 1);
            }
        }
    });
    test("should not load previous deleted data from file", {
        async ARRANGE(after) {
            const { persistency, folder: folder } = await newPersistency(after);
            persistency.set("test", value1);
            persistency.set("test2", value2);
            persistency.delete("test"); // delete entry before the last one
            persistency.close();
            return { folder };
        },
        ACT({ folder }, after) {
            return newPersistency(after, { folder });
        },
        ASSERTS: {
            "should not have first data"({ persistency }) {
                assertEqual(persistency.get("test"), null);
            },
            "should have second data"({ persistency }) {
                assertDeepEqual(persistency.get("test2"), value2);
            },
            "should count the number of entries"({ persistency }) {
                assertEqual(persistency.count(), 1);
            }
        }
    });
    test("should not load deleted entry with multiple subentries from file", {
        async ARRANGE(after) {
            const { persistency, folder: folder } = await newPersistency(after);
            persistency.set("test", value1);
            persistency.set("test", value2);
            persistency.delete("test");
            persistency.close();
            return { folder };
        },
        ACT({ folder }, after) {
            return newPersistency(after, { folder });
        },
        ASSERTS: {
            "should get the correct value for the entry"({ persistency }) {
                assertEqual(persistency.get("test"), null);
            },
            "should count the number of entries"({ persistency }) {
                assertEqual(persistency.count(), 0);
            }
        }
    });
    test("should load wrapped around entry when second value is bigger", {
        async ARRANGE(after) {
            const { persistency, folder: folder } = await newPersistency(after);
            persistency.set("test", value1);
            persistency.set("test", value2);
            persistency.close();
            await setValueDataVersion(persistency, 1, 0xFAFBFCFD); // Set value2 dataVersion to 0xFAFBFCFD
            return { folder };
        },
        ACT({ folder }, after) {
            return newPersistency(after, { folder });
        },
        ASSERTS: {
            "should get the correct value for the entry"({ persistency }) {
                assertDeepEqual(persistency.get("test"), value1);
            },
            "should count the number of entries"({ persistency }) {
                assertEqual(persistency.count(), 1);
            }
        }
    });
    test("should load wrapped around entry when first value is bigger", {
        async ARRANGE(after) {
            const { persistency, folder: folder } = await newPersistency(after);
            persistency.set("test", value1);
            persistency.set("test", value2);
            persistency.close();
            await setValueDataVersion(persistency, 0, 0xFAFBFCFD); // Set value1 dataVersion to 0xFAFBFCFD
            return { folder };
        },
        ACT({ folder }, after) {
            return newPersistency(after, { folder });
        },
        ASSERTS: {
            "should get the correct value for the entry"({ persistency }) {
                assertDeepEqual(persistency.get("test"), value2);
            },
            "should count the number of entries"({ persistency }) {
                assertEqual(persistency.count(), 1);
            }
        }
    });
    test("should overwrite deleted entries", {
        async ARRANGE(after) {
            const { persistency } = await newPersistency(after);
            persistency.set("aaa", value1);
            const size = await getFileSizes(persistency);
            persistency.delete("aaa");
            return { persistency, size };
        },
        ACT({ persistency }) {
            persistency.set("bbb", value2);
        },
        ASSERTS: {
            async "entries file size must match"(_, { persistency, size }) {
                assertEqual(await getFileSize(persistency.entriesFile), size.entries);
            },
            async "data file size must match"(_, { persistency, size }) {
                assertEqual(await getFileSize(persistency.dataFile), size.data);
            },
            "should count the number of entries"(_, { persistency }) {
                assertEqual(persistency.count(), 1);
            }
        }
    });
    test("should not overwrite pending data to purge", {
        async ARRANGE(after) {
            const { persistency } = await newPersistency(after);
            persistency.set("aaa", value1);
            persistency.set("aaa", value2);
            const size = await getFileSizes(persistency);
            return { persistency, size };
        },
        ACT({ persistency }) {
            persistency.set("bbb", value1);
        },
        ASSERTS: {
            async "entries file size must be bigger"(_, { persistency, size }) {
                assertEqual(await getFileSize(persistency.entriesFile) > size.entries, true);
            },
            async "data file size must be bigger"(_, { persistency, size }) {
                assertEqual(await getFileSize(persistency.dataFile) > size.data, true);
            },
            "should have aaa value"(_, { persistency }) {
                assertDeepEqual(persistency.get("aaa"), value2);
            },
            "should have bbb value"(_, { persistency }) {
                assertDeepEqual(persistency.get("bbb"), value1);
            },
            "should count the number of entries"(_, { persistency }) {
                assertEqual(persistency.count(), 2);
            }
        }
    });
    test("should purge pending entry after load", {
        async ARRANGE(after) {
            const context = newpersistencyContext();
            const { persistency, folder } = await newPersistency(after, {
                reclaimTimeout: 100
            }, context);
            persistency.set("aaa", value1);
            persistency.set("aaa", value2);
            context.tick(100); // purge entry1, which copies entry2 over entry1 so entry2 is in purge state now
            persistency.set("aaa", value3); // write a new entry, so both entry1 and entry2 are in purge state
            const size = await getFileSizes(persistency);
            persistency.close();
            return { folder, size, context };
        },
        async ACT({ folder, context }, after) {
            const { persistency } = await newPersistency(after, {
                folder: folder,
                reclaimTimeout: 100
            }, context);
            context.tick(100); // purge entry1 and entry2, which copies entry3 over entry1. Entry2 is free
            persistency.set("bbb", value1); // set a new entry which will overwrite entry2
            return { persistency };
        },
        ASSERTS: {
            async "entries file size must match"({ persistency }, { size }) {
                assertEqual(await getFileSize(persistency.entriesFile), size.entries);
            },
            async "data file size must match"({ persistency }, { size }) {
                assertEqual(await getFileSize(persistency.dataFile), size.data);
            },
            "should overwrite data"({ persistency }) {
                // If data is overwritten, then entry was deleted
                assertDeepEqual(persistency.get("bbb"), value1);
            },
            "should count the number of entries"({ persistency }) {
                assertEqual(persistency.count(), 2);
            }
        }
    });
    test("should not overwrite pending data to purge after load", {
        async ARRANGE(after) {
            const context = newpersistencyContext();
            const { persistency, folder } = await newPersistency(after, {
                reclaimTimeout: 100
            }, context);
            persistency.set("aaa", value1);
            persistency.set("aaa", value2);
            context.tick(100); // purge entry1, which copies entry2 over entry1 so entry2 is in purge state now
            persistency.set("aaa", value3); // write a new entry, so both entry1 and entry2 are in purge state
            const size = await getFileSizes(persistency);
            persistency.close();
            return { folder, size };
        },
        async ACT({ folder }, after) {
            const { persistency } = await newPersistency(after, { folder });
            persistency.set("bbb", value1); // write a new entry without purging pending entries in purge state
            return { persistency };
        },
        ASSERTS: {
            async "entries file size must be bigger"({ persistency }, { size }) {
                assertEqual(await getFileSize(persistency.entriesFile) > size.entries, true);
            },
            async "data file size must be bigger"({ persistency }, { size }) {
                assertEqual(await getFileSize(persistency.dataFile) > size.data, true);
            },
            "should have aaa value"({ persistency }) {
                assertDeepEqual(persistency.get("aaa"), value3);
            },
            "should have bbb value"({ persistency }) {
                assertDeepEqual(persistency.get("bbb"), value1);
            },
            "should count the number of entries"({ persistency }) {
                assertEqual(persistency.count(), 2);
            }
        }
    });
    test("should clean purge array when deleting a purging entry", {
        async ARRANGE(after) {
            const context = newpersistencyContext();
            const { persistency } = await newPersistency(after, {
                reclaimTimeout: 100
            }, context);
            persistency.set("aaa", value1);
            persistency.set("aaa", value2);
            return { persistency, context };
        },
        ACT({ persistency, context }) {
            persistency.delete("aaa");
            persistency.set("bbb", value3);
            persistency.set("bbb", value4);
            // if purge array is cleaned, it will not re-clean the set entries
            context.tick(100);
        },
        ASSERTS: {
            "should delete aaa"(_, { persistency }) {
                assertDeepEqual(persistency.get("aaa"), null);
            },
            "should get the correct value for bbb"(_, { persistency }) {
                assertDeepEqual(persistency.get("bbb"), value4);
            },
            "should count the number of entries"(_, { persistency }) {
                assertEqual(persistency.count(), 1);
            }
        }
    });
    test("should overwrite all deleted bytes from the same entry with multiple entries", {
        async ARRANGE(after) {
            const { persistency } = await newPersistency(after);
            persistency.set("aaa", value1);
            persistency.set("aaa", value2);
            const size = await getFileSizes(persistency);
            persistency.delete("aaa");
            return { persistency, size };
        },
        ACT({ persistency }) {
            // if they were deleted, this will re-set them and keep the same size
            persistency.set("bbb", value3);
            persistency.set("ccc", value4);
        },
        ASSERTS: {
            async "entries file size must match"(_, { persistency, size }) {
                assertEqual(await getFileSize(persistency.entriesFile), size.entries);
            },
            async "data file size must match"(_, { persistency, size }) {
                assertEqual(await getFileSize(persistency.dataFile), size.data);
            },
            "should count the number of entries"(_, { persistency }) {
                assertEqual(persistency.count(), 2);
            }
        }
    });
    test("should load allocated blocks on file load", {
        async ARRANGE(after) {
            const { persistency, folder } = await newPersistency(after);
            // all keys same length for easy offset calculation purposes
            persistency.set("test0", value1);
            persistency.set("test1", value2);
            persistency.set("test2", value3);
            persistency.set("test3", value4);
            persistency.set("test4", value1);
            persistency.set("test5", value2);
            persistency.set("test6", value3);
            persistency.set("test7", value4);
            await setEntryBytes(persistency, 1, 3, Buffer.from([ 0x00, 0x01, 0xFF ])); // Invalidate test1
            await setEntryBytes(persistency, 2, 3, Buffer.from([ 0x00, 0x01, 0xFF ])); // Invalidate test2
            await setEntryBytes(persistency, 4, 3, Buffer.from([ 0x00, 0x01, 0xFF ])); // Invalidate test4
            await setEntryBytes(persistency, 7, 3, Buffer.from([ 0x00, 0x01, 0xFF ])); // Invalidate test7
            persistency.close();
            return { folder };
        },
        ACT({ folder }, after) {
            return newPersistency(after, { folder });
        },
        ASSERTS: {
            "should have allocated entries in memory"({ persistency }) {
                assertDeepEqual(persistency.getAllocatedBlocks().entries, [
                    [0, getEntryOffset(0) + entrySize],
                    [getEntryOffset(3), getEntryOffset(3) + entrySize],
                    [getEntryOffset(5), getEntryOffset(6) + entrySize]
                ]);
            },
            "should have allocated data in memory"({ persistency }) {
                assertDeepEqual(persistency.getAllocatedBlocks().data, [
                    [0, getDataOffset(5, 0).end], // keylength 5
                    [getDataOffset(5, 3).start, getDataOffset(5, 3).end], // keylength 5
                    [getDataOffset(5, 5).start, getDataOffset(5, 6).end] // keylength 5
                ]);
            },
            "should count the number of entries"({ persistency }) {
                assertEqual(persistency.count(), 4);
            }
        }
    });
    test.describe("truncate", test => {
        test("should truncate files if freeing a final entry", {
            async ARRANGE(after) {
                const { persistency } = await newPersistency(after);
                persistency.set("test0", value1);
                persistency.set("test1", value2);
                persistency.set("test2", value3);
                persistency.set("test3", value4);
                const fileSizes = await getFileSizes(persistency);
                return { persistency, fileSizes };
            },
            ACT({ persistency }) {
                persistency.delete("test3");
            },
            ASSERTS: {
                "should not have entry3"(_, { persistency }) {
                    assertEqual(persistency.get("entry3"), null);
                },
                async "should truncate entries file"(_, { persistency, fileSizes }) {
                    assertEqual(await getFileSize(persistency.entriesFile) < fileSizes.entries, true);
                },
                async "should truncate data file"(_, { persistency, fileSizes }) {
                    assertEqual(await getFileSize(persistency.dataFile) < fileSizes.data, true);
                },
                "should count the number of entries"(_, { persistency }) {
                    assertEqual(persistency.count(), 3);
                }
            }
        });
        test("should truncate files on load", {
            async ARRANGE(after) {
                const { persistency, folder } = await newPersistency(after);
                persistency.set("test0", value1);
                persistency.set("test1", value2);
                persistency.set("test2", value3);
                persistency.set("test3", value4);
                persistency.close();
                const fileSizes = await getFileSizes(persistency);
                await setEntryBytes(persistency, 3, 3, Buffer.from([ 0x00, 0x01, 0xFF ])); // Invalidate last entry
                return { folder, fileSizes };
            },
            ACT({ folder }, after) {
                return newPersistency(after, { folder });
            },
            ASSERTS: {
                "should not have entry3"({ persistency }) {
                    assertEqual(persistency.get("entry3"), null);
                },
                async "should truncate entries file"({ persistency }, { fileSizes }) {
                    assertEqual(await getFileSize(persistency.entriesFile) < fileSizes.entries, true);
                },
                async "should truncate data file"({ persistency }, { fileSizes }) {
                    assertEqual(await getFileSize(persistency.dataFile) < fileSizes.data, true);
                },
                "should count the number of entries"({ persistency }) {
                    assertEqual(persistency.count(), 3);
                }
            }
        });
    });
    test.describe("magic", test => {
        test("magic is written to files", {
            ACT(_, after) {
                return newPersistency(after);
            },
            ASSERTS: {
                async "should write magic in entries file"({ persistency }) {
                    assertDeepEqual((await Fs.readFile(persistency.entriesFile)).subarray(0, constants.MAGIC.length), constants.MAGIC);
                },
                async "should write magic in data file"({ persistency }) {
                    assertDeepEqual((await Fs.readFile(persistency.dataFile)).subarray(0, constants.MAGIC.length), constants.MAGIC);
                }
            }
        });
        test("should not load if entries magic is invalid", {
            async ARRANGE(after) {
                const { persistency, folder } = await newPersistency(after);
                persistency.set("test0", value1);
                persistency.close();
                const buffer = await Fs.readFile(persistency.entriesFile);
                buffer[0]++; // First magic character
                await Fs.writeFile(persistency.entriesFile, buffer);
                return { folder };
            },
            ACT({ folder }, after) {
                return asyncMonad(() => newPersistency(after, { folder }));
            },
            ASSERT(res) {
                res.should.error({
                    message: "Entries file is not a persistency one"
                });
            }
        });
        test("should not load if data magic is invalid", {
            async ARRANGE(after) {
                const { persistency, folder } = await newPersistency(after);
                persistency.set("test0", value1);
                persistency.close();
                const buffer = await Fs.readFile(persistency.dataFile);
                buffer[0]++; // First magic character
                await Fs.writeFile(persistency.dataFile, buffer);
                return { folder };
            },
            ACT({ folder }, after) {
                return asyncMonad(() => newPersistency(after, { folder }));
            },
            ASSERT(res) {
                res.should.error({
                    message: "Data file is not a persistency one"
                });
            }
        });
    });
    test("should allocate unordered data on load", {
        async ARRANGE(after) {
            const { persistency, folder } = await newPersistency(after);
            persistency.set("test0", value1);
            persistency.set("test1", value2);
            persistency.set("test2", value3);
            persistency.set("test3", value4);
            persistency.set("test4", Buffer.concat([ value1, value2, value3 ]));
            const entryBuffer = await readEntry(persistency, 4);
            await setEntryBytes(persistency, 1, 0, entryBuffer); // move entry 4 to entry 1
            await setEntryBytes(persistency, 4, 0, Buffer.from([ 0x99, 0x99, 0x99, 0x99 ])); // invalidate entry 4
            persistency.close();
            return { folder };
        },
        ACT({ folder }, after) {
            return newPersistency(after, { folder });
        },
        ASSERTS: {
            "should have allocated entries"({ persistency }) {
                assertDeepEqual(persistency.getAllocatedBlocks().entries, [
                    [0, getEntryOffset(3) + entrySize]
                ]);
            },
            "should have allocated data"({ persistency }) {
                assertDeepEqual(persistency.getAllocatedBlocks().data, [
                    [0, getDataOffset(5, 0).end], // keylength 5
                    [getDataOffset(5, 2).start, getDataOffset(5, 4).end + value2.length + value3.length], // keylength 5
                ]);
            },
            "should count the number of entries"({ persistency }) {
                assertEqual(persistency.count(), 4);
            }
        }
    });
    test("should allocate unordered entries on load", {
        async ARRANGE(after) {
            const { persistency, folder } = await newPersistency(after);
            persistency.set("test0", value1);
            persistency.set("test1", value2);
            persistency.set("test0", value3);
            persistency.close();
            return { folder };
        },
        ACT({ folder }, after) {
            return newPersistency(after, { folder });
        },
        ASSERTS: {
            "should have allocated entries"({ persistency }) {
                assertDeepEqual(persistency.getAllocatedBlocks().entries, [
                    [0, getEntryOffset(2) + entrySize]
                ]);
            },
            "should have allocated data"({ persistency }) {
                assertDeepEqual(persistency.getAllocatedBlocks().data, [
                    [0, getDataOffset(5, 2).end], // keylength 5
                ]);
            },
            "should count the number of entries"({ persistency }) {
                assertEqual(persistency.count(), 2);
            }
        }
    });
    test.describe("partial writes", test => {
        test("should invalidate entry if partial file is found", {
            async ARRANGE(after) {
                const { persistency, folder } = await newPersistency(after);
                persistency.set("test0", value1);
                persistency.set("test1", value2);
                persistency.close();
                const fileSize = await getFileSize(persistency.entriesFile);
                await Fs.truncate(persistency.entriesFile, fileSize - 2); // partial write last entry
                return { folder };
            },
            ACT({ folder }, after) {
                return newPersistency(after, { folder });
            },
            ASSERTS: {
                "should have entry 0"({ persistency }) {
                    assertDeepEqual(persistency.get("test0"), value1);
                },
                "should not have entry 1"({ persistency }) {
                    assertEqual(persistency.get("test1"), null);
                },
                "should count the number of entries"({ persistency }) {
                    assertEqual(persistency.count(), 1);
                }
            }
        });
        test("should invalidate entry if entry version is invalid", {
            async ARRANGE(after) {
                const { persistency, folder } = await newPersistency(after);
                persistency.set("test0", value1);
                persistency.set("test1", value2);
                persistency.close();
                await overwrite(persistency).entry.entryVersion(0, Buffer.from([ 0xA0 ]));
                return { folder };
            },
            ACT({ folder }, after) {
                return newPersistency(after, { folder });
            },
            ASSERTS: {
                "should not have entry 0"({ persistency }) {
                    assertDeepEqual(persistency.get("test0"), null);
                },
                "should have entry 1"({ persistency }) {
                    assertDeepEqual(persistency.get("test1"), value2);
                },
                "should count the number of entries"({ persistency }) {
                    assertEqual(persistency.count(), 1);
                }
            }
        });
        test("should invalidate entry if entry hash is invalid", {
            async ARRANGE(after) {
                const { persistency, folder } = await newPersistency(after);
                persistency.set("test0", value1);
                persistency.set("test1", value2);
                persistency.close();
                await overwrite(persistency).entry.entryHash(0, Buffer.from([ 0x00, 0x01 ]));
                return { folder };
            },
            ACT({ folder }, after) {
                return newPersistency(after, { folder });
            },
            ASSERTS: {
                "should not have entry 0"({ persistency }) {
                    assertDeepEqual(persistency.get("test0"), null);
                },
                "should have entry 1"({ persistency }) {
                    assertDeepEqual(persistency.get("test1"), value2);
                },
                "should count the number of entries"({ persistency }) {
                    assertEqual(persistency.count(), 1);
                }
            }
        });
    });
    test.describe("compact", test => {
        test("should purge old entry after time", {
            async ARRANGE(after) {
                const context = newpersistencyContext();
                const { persistency } = await newPersistency(after, {
                    reclaimTimeout: 100
                }, context);
                persistency.set("aaa", value1);
                persistency.set("aaa", value2);
                const size = await getFileSizes(persistency);
                return { persistency, size, context };
            },
            ACT({ persistency, context }) {
                context.tick(100);
                persistency.compact(); // This will copy entry2 over entry1 but entry2 will be tagged as purging
                context.tick(100);
                persistency.compact(); // This will finally remove last entry
            },
            ASSERTS: {
                async "entries file size must reduce"(_, { persistency, size }) {
                    assertEqual(await getFileSize(persistency.entriesFile) < size.entries, true);
                },
                async "data file size must reduce"(_, { persistency, size }) {
                    assertEqual(await getFileSize(persistency.dataFile) < size.data, true);
                },
                "should count the number of entries"(_, { persistency }) {
                    assertEqual(persistency.count(), 1);
                }
            }
        });
    });
    // TODO:
    // Y ya testear todas las posibilidades al cargar (incluyendo que trunca)
    // Testear cuando se escriben datos parciales de absolutamente todos los bytes posibles (entrada y datos)
    // Testear esto además cuando se van a borrar entradas (zero bytes parciales escritos)
});