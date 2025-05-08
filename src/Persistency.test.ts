import * as Fs from "fs/promises";
import * as Assert from "assert";
import * as Os from "os";
import * as Path from "path";

import test, { After } from "arrange-act-assert";

import { Persistency } from "./Persistency";
import * as constants from "./constants";
import { sha256 } from "./utils";

test.describe("Persistency", test => {
    const value1 = Buffer.from([ 0, 1, 2, 3,  4,  5 ]);
    const value2 = Buffer.from([ 6, 7, 8, 9, 10, 11 ]);
    async function newPersistency(after:After, tmpFolder?:string, reclaimTimeout?:number) {
        tmpFolder = tmpFolder || after(await Fs.mkdtemp(Path.join(Os.tmpdir(), "persistency-tests-")), folder => Fs.rm(folder, { recursive: true, force: true }));
        const persistency = after(new Persistency({
            folder: tmpFolder,
            reclaimTimeout: reclaimTimeout
        }), persistency => persistency.close());
        return { persistency, tmpFolder };
    }
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
            const { persistency, tmpFolder } = await newPersistency(after);
            persistency.set("test", value1);
            persistency.set("test2", value2);
            persistency.close();
            return { tmpFolder };
        },
        ACT({ tmpFolder }, after) {
            return newPersistency(after, tmpFolder);
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
            const { persistency, tmpFolder } = await newPersistency(after);
            persistency.set("test", value1);
            persistency.set("test", value2);
            persistency.close();
            return { tmpFolder };
        },
        ACT({ tmpFolder }, after) {
            return newPersistency(after, tmpFolder);
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
    test("Should not load deleted data from file", {
        async ARRANGE(after) {
            const { persistency, tmpFolder } = await newPersistency(after);
            persistency.set("test", value1);
            persistency.set("test2", value2);
            persistency.delete("test");
            persistency.close();
            return { tmpFolder };
        },
        ACT({ tmpFolder }, after) {
            return newPersistency(after, tmpFolder);
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
            const { persistency, tmpFolder } = await newPersistency(after);
            persistency.set("test", value1);
            persistency.set("test", value2);
            persistency.delete("test");
            persistency.close();
            return { tmpFolder };
        },
        ACT({ tmpFolder }, after) {
            return newPersistency(after, tmpFolder);
        },
        ASSERT({ persistency }) {
            Assert.strictEqual(persistency.get("test"), null);
        }
    });
    test("Should load wrapped around entry", {
        async ARRANGE(after) {
            const { persistency, tmpFolder } = await newPersistency(after);
            persistency.set("test", value1);
            persistency.set("test", value2);
            persistency.close();

            // Set TS of value2 to 0xFFFFFFFF
            const buffer = await Fs.readFile(persistency.entriesFile);
            const entryLocation = constants.MAGIC.length + constants.EntryHeaderOffsets_V0.SIZE + constants.EntryOffsets_V0.SIZE;
            buffer.writeUint32BE(0xFFFFFFFF, entryLocation + constants.EntryHeaderOffsets_V0.SIZE + constants.EntryOffsets_V0.TS);
            const entryDataLocation = entryLocation + constants.EntryHeaderOffsets_V0.SIZE;
            const hash = sha256(buffer.subarray(entryDataLocation, entryDataLocation + constants.EntryOffsets_V0.SIZE));
            hash.copy(buffer, entryLocation + constants.EntryHeaderOffsets_V0.ENTRY_HASH);
            await Fs.writeFile(persistency.entriesFile, buffer);
            return { tmpFolder };
        },
        ACT({ tmpFolder }, after) {
            return newPersistency(after, tmpFolder);
        },
        ASSERT({ persistency }) {
            Assert.deepStrictEqual(persistency.get("test"), value1);
        }
    });
    // TODO:
    // Testear que count() contiene la cantidad de entradas correspondientes
    // Testear que al eliminar, el dato se puede sobreescribir de nuevo
    // Testear que cuando se sobreescribe un dato, se purga el anterior cuando han pasado X milisegundos
    // Testear que si se elimina un dato que está purgando, se elimina de la lista de purgas
    // Testear que cuando se purga un dato, desaparece la entry correspondiente
    // Testear que el dato desaparece si tooodas las entradas han sido purgadas (no tengo claro cómo puede ocurrir esto)
    // Testear que ocupa el siguiente espacio disponible en un entry
    // Testear que puede ocupar el espacio final de un entry varias veces
    // Testear que puede ocupar 2 espacios seguidos con salto de un entry (vamos, probar que aplica el next)
    // testear que si hay sitio para 2 entries y un salto, que da los 2 espacios y después el salto
    // Testear todas las formas de ocupar espacio en un dato (previo, medio y final) y con las mismas cosas que con el entry
    // Testear que al inicio genera los huecos tanto de entries como datas correspondientes
    // Y ya testear todas las posibilidades al cargar (incluyendo que trunca)
    // Testear que trunca si hace free de un dato al final
    // Testear cuando se escriben datos parciales
    // Testear todas las posibles formas de librear espacio existentes. De hecho esto debería ser una clase propia con sus propios tests.
});