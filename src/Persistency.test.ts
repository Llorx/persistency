import * as Fs from "fs/promises";
import * as Assert from "assert";
import * as Os from "os";
import * as Path from "path";

import test, { After } from "arrange-act-assert";

import { Persistency, PersistencyContext, PersistencyOptions } from "./Persistency";
import * as constants from "./constants";
import { sha256 } from "./utils";

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
    async function setValueTs(persistency:Pick<Persistency, "entriesFile">, entryI:number, ts:number) {
        const buffer = await Fs.readFile(persistency.entriesFile);
        buffer.writeUint32BE(ts, getEntryOffset(entryI) + constants.EntryHeaderOffsets_V0.SIZE + constants.EntryOffsets_V0.TS);
        await hashEntry(persistency, entryI, buffer);
    }
    async function hashEntry(persistency:Pick<Persistency, "entriesFile">, entryI:number, buffer?:Buffer) {
        buffer = buffer || await Fs.readFile(persistency.entriesFile);
        const entryLocation = getEntryOffset(entryI);
        const entryDataLocation = entryLocation + constants.EntryHeaderOffsets_V0.SIZE;
        const hash = sha256(buffer.subarray(entryDataLocation, entryDataLocation + constants.EntryOffsets_V0.SIZE));
        hash.copy(buffer, entryLocation + constants.EntryHeaderOffsets_V0.ENTRY_HASH);
        await Fs.writeFile(persistency.entriesFile, buffer);
    }
    async function newPersistency(after:After, options?:Partial<PersistencyOptions>|null, mock?:PersistencyContext) {
        const folder = options?.folder || after(await Fs.mkdtemp(Path.join(Os.tmpdir(), "persistency-tests-")), folder => Fs.rm(folder, { recursive: true, force: true }));
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
    function newContextMock() {
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
        ASSERT(_, { persistency }) {
            Assert.deepStrictEqual(persistency.get("test"), value1);
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
                Assert.deepStrictEqual(persistency.get("test"), value1);
            },
            "should have second data"({ persistency }) {
                Assert.deepStrictEqual(persistency.get("test2"), value2);
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
        ASSERT({ persistency }) {
            Assert.deepStrictEqual(persistency.get("test"), value2);
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
        ASSERT(_, { persistency }) {
            Assert.deepStrictEqual(persistency.get("test"), value2);
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
                Assert.strictEqual(persistency.get("test"), null);
            },
            "should have second data"(_, { persistency }) {
                Assert.deepStrictEqual(persistency.get("test2"), value2);
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
                Assert.strictEqual(persistency.get("test"), null);
            },
            "should have second data"({ persistency }) {
                Assert.deepStrictEqual(persistency.get("test2"), value2);
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
        ASSERT({ persistency }) {
            Assert.strictEqual(persistency.get("test"), null);
        }
    });
    test("should load wrapped around entry when second value is bigger", {
        async ARRANGE(after) {
            const { persistency, folder: folder } = await newPersistency(after);
            persistency.set("test", value1);
            persistency.set("test", value2);
            persistency.close();
            await setValueTs(persistency, 1, 0xFAFBFCFD); // Set value2 ts to 0xFAFBFCFD
            return { folder };
        },
        ACT({ folder }, after) {
            return newPersistency(after, { folder });
        },
        ASSERT({ persistency }) {
            Assert.deepStrictEqual(persistency.get("test"), value1);
        }
    });
    test("should load wrapped around entry when first value is bigger", {
        async ARRANGE(after) {
            const { persistency, folder: folder } = await newPersistency(after);
            persistency.set("test", value1);
            persistency.set("test", value2);
            persistency.close();
            await setValueTs(persistency, 0, 0xFAFBFCFD); // Set value1 ts to 0xFAFBFCFD
            return { folder };
        },
        ACT({ folder }, after) {
            return newPersistency(after, { folder });
        },
        ASSERT({ persistency }) {
            Assert.deepStrictEqual(persistency.get("test"), value2);
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
                Assert.strictEqual(await getFileSize(persistency.entriesFile), size.entries);
            },
            async "data file size must match"(_, { persistency, size }) {
                Assert.strictEqual(await getFileSize(persistency.dataFile), size.data);
            }
        }
    });
    test("should purge old entry after time", {
        async ARRANGE(after) {
            const context = newContextMock();
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
            // If data is ready to be purged, next set will overwrite it
            persistency.set("bbb", value1);
        },
        ASSERTS: {
            async "entries file size must match"(_, { persistency, size }) {
                Assert.strictEqual(await getFileSize(persistency.entriesFile), size.entries);
            },
            async "data file size must match"(_, { persistency, size }) {
                Assert.strictEqual(await getFileSize(persistency.dataFile), size.data);
            },
            "should overwrite data"(_, { persistency }) {
                // If data is overwritten, then entry was deleted
                Assert.deepStrictEqual(persistency.get("bbb"), value1);
            }
        }
    });
    test("should clean purge array when deleting a purging entry", {
        async ARRANGE(after) {
            const context = newContextMock();
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
        ASSERT(_, { persistency }) {
            Assert.deepStrictEqual(persistency.get("bbb"), value4);
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
                Assert.strictEqual(await getFileSize(persistency.entriesFile), size.entries);
            },
            async "data file size must match"(_, { persistency, size }) {
                Assert.strictEqual(await getFileSize(persistency.dataFile), size.data);
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
            persistency.delete("test2");
            persistency.delete("test1");
            persistency.delete("test7");
            persistency.delete("test4");
            persistency.close();
            return { folder };
        },
        ACT({ folder }, after) {
            return newPersistency(after, { folder });
        },
        ASSERTS: {
            "should have allocated entries in memory"({ persistency }) {
                Assert.deepStrictEqual(persistency.getAllocatedBlocks().entries, [
                    [0, getEntryOffset(0) + entrySize],
                    [getEntryOffset(3), getEntryOffset(3) + entrySize],
                    [getEntryOffset(5), getEntryOffset(6) + entrySize]
                ]);
            },
            "should have allocated data in memory"({ persistency }) {
                Assert.deepStrictEqual(persistency.getAllocatedBlocks().data, [
                    [0, getDataOffset(5, 0).end], // keylength 5
                    [getDataOffset(5, 3).start, getDataOffset(5, 3).end], // keylength 5
                    [getDataOffset(5, 5).start, getDataOffset(5, 6).end] // keylength 5
                ]);
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
                    Assert.strictEqual(persistency.get("entry3"), null);
                },
                async "should truncate entries file"(_, { persistency, fileSizes }) {
                    Assert.strictEqual(await getFileSize(persistency.entriesFile) < fileSizes.entries, true);
                },
                async "should truncate data file"(_, { persistency, fileSizes }) {
                    Assert.strictEqual(await getFileSize(persistency.dataFile) < fileSizes.data, true);
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
                await setEntryBytes(persistency, 3, 3, Buffer.from([ 0x00, 0x01, 0xFF ])); // Invalidate entry3
                return { folder, fileSizes };
            },
            ACT({ folder }, after) {
                return newPersistency(after, { folder });
            },
            ASSERTS: {
                "should not have entry3"({ persistency }) {
                    Assert.strictEqual(persistency.get("entry3"), null);
                },
                async "should truncate entries file"({ persistency }, { fileSizes }) {
                    Assert.strictEqual(await getFileSize(persistency.entriesFile) < fileSizes.entries, true);
                },
                async "should truncate data file"({ persistency }, { fileSizes }) {
                    Assert.strictEqual(await getFileSize(persistency.dataFile) < fileSizes.data, true);
                }
            }
        });
    });
    // TODO:
    // Testear que count() contiene la cantidad de entradas correspondientes
    // Testear que cuando se purga un dato, desaparece la entry correspondiente del array de entries
    // Testear que el dato desaparece si tooodas las entradas han sido purgadas (no tengo claro cómo puede ocurrir esto)

    // Y ya testear todas las posibilidades al cargar (incluyendo que trunca)
    // Testear cuando se escriben datos parciales de absolutamente todos los bytes posibles (entrada y datos)
    // Testear esto además cuando se van a borrar entradas (bytes parciales escritos)
});