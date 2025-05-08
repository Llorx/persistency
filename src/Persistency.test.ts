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
    const value1 = Buffer.from([ 0, 1, 2, 3,  4,  5 ]);
    const value2 = Buffer.from([ 6, 7, 8, 9, 10, 11 ]);
    const value3 = Buffer.from([ 0, 2, 4, 6,  8, 10 ]);
    const value4 = Buffer.from([ 1, 3, 5, 7,  9, 11 ]);
    async function setValueTs(persistency:Pick<Persistency, "entriesFile">, valueI:number, ts:number) {
        const buffer = await Fs.readFile(persistency.entriesFile);
        const entryLocation = constants.MAGIC.length + ((constants.EntryHeaderOffsets_V0.SIZE + constants.EntryOffsets_V0.SIZE) * valueI);
        buffer.writeUint32BE(ts, entryLocation + constants.EntryHeaderOffsets_V0.SIZE + constants.EntryOffsets_V0.TS);
        await hashEntry(persistency, valueI, buffer);
    }
    async function hashEntry(persistency:Pick<Persistency, "entriesFile">, valueI:number, buffer?:Buffer) {
        buffer = buffer || await Fs.readFile(persistency.entriesFile);
        const entryLocation = constants.MAGIC.length + ((constants.EntryHeaderOffsets_V0.SIZE + constants.EntryOffsets_V0.SIZE) * valueI);
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
    test("Should set and get data", {
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
    test("Should load data from file", {
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
    test("Should update data", {
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
    test("Should update data without reloading", {
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
    test("Should delete data", {
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
    test("Should not load previous deleted data from file", {
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
    test("Should not load deleted entry with multiple subentries from file", {
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
    test("Should load wrapped around entry", {
        async ARRANGE(after) {
            const { persistency, folder: folder } = await newPersistency(after);
            persistency.set("test", value1);
            persistency.set("test", value2);
            persistency.close();
            await setValueTs(persistency, 1, 0xFFFFFFFF); // Set value2 ts to 0xFFFFFFFF
            return { folder };
        },
        ACT({ folder }, after) {
            return newPersistency(after, { folder });
        },
        ASSERT({ persistency }) {
            Assert.deepStrictEqual(persistency.get("test"), value1);
        }
    });
    test("Should overwrite deleted entries", {
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
    test("Should purge old entry after time", {
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
    test("Should clean purge array when deleting a purging entry", {
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
    test("Should overwrite all deleted bytes from the same entry with multiple entries", {
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
    // TODO:
    // Testear que count() contiene la cantidad de entradas correspondientes
    // Testear que cuando se purga un dato, desaparece la entry correspondiente del array de entries
    // Testear que el dato desaparece si tooodas las entradas han sido purgadas (no tengo claro cómo puede ocurrir esto)

    // Testear que al inicio genera los huecos tanto de entries como datas correspondientes.
    // Cómo testear? Pues cargando algo con huecos y después 

    // Y ya testear todas las posibilidades al cargar (incluyendo que trunca)
    // Testear que trunca si hace free de un dato al final
    // Testear cuando se escriben datos parciales
});