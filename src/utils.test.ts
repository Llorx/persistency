import * as Path from "path";

import test, { After, monad } from "arrange-act-assert";

import { assertDeepEqual, assertEqual, newOpenFilesContext, tempFolder } from "./testUtils";
import * as utils from "./utils";

test.describe("utils", test => {
    async function newFiles(after:After) {
        const folder = await tempFolder(after);
        return {
            entries: Path.join(folder, "entriesFile"),
            data: Path.join(folder, "dataFile")
        };
    }
    test("should hash with shake128", {
        ACT() {
            return utils.shake128(Buffer.from([0x00, 0x01, 0x02, 0x03]));
        },
        ASSERT(res) {
            assertDeepEqual(res, Buffer.from([0x0b, 0x0c, 0xc2, 0x8e, 0x60, 0xe3, 0x76, 0x98, 0xb4, 0x11, 0x23, 0x4b, 0x11, 0x58, 0xa5, 0xd4]));
        }
    });
    test.describe("openFiles", test => {
        test("should open files", {
            async ARRANGE(after) {
                const files = await newFiles(after);
                const context = newOpenFilesContext();
                return { files, context };
            },
            ACT({ files, context }, after) {
                return after(utils.openFiles({
                    dataFile: files.data,
                    entriesFile: files.entries,
                }, context), fd => fd.close());
            },
            ASSERTS: {
                "should return a valid object with close method"(res) {
                    assertEqual(typeof res.close, "function");
                },
                "should return a valid object with entries object"(res) {
                    assertEqual(typeof res.entries, "object");
                },
                "should return a valid object with data object"(res) {
                    assertEqual(typeof res.data, "object");
                },
                "should open entries and data files"(_, { files, context }) {
                    assertDeepEqual(
                        context.openSync.splice().map(entry => [entry[0]]), // Remove flags argument
                        [
                            [files.entries],
                            [files.data],
                        ]
                    );
                }
            }
        });
        test("should clear on first error", {
            async ARRANGE(after) {
                const files = await newFiles(after);
                const context = newOpenFilesContext();
                context.openSync.pushNextError(new Error("Error opening"));
                return { files, context };
            },
            ACT({ files, context }, after) {
                return monad(() => after(utils.openFiles({
                    dataFile: files.data,
                    entriesFile: files.entries,
                }, context), fd => fd.close()));
            },
            ASSERTS: {
                "should return an error"(res) {
                    res.should.error({
                        message: "Error opening"
                    });
                },
                "should try to open one files"(_, { context }) {
                    assertEqual(context.openSync.splice().length, 1);
                },
                "should not close any file"(_, { context }) {
                    context.closeSync.assert([]);
                }
            }
        });
        test("should clear on second error", {
            async ARRANGE(after) {
                const files = await newFiles(after);
                const context = newOpenFilesContext();
                context.openSync.pushNextReturn(123);
                context.openSync.pushNextError(new Error("Error opening"));
                return { files, context };
            },
            ACT({ files, context }, after) {
                return monad(() => after(utils.openFiles({
                    dataFile: files.data,
                    entriesFile: files.entries,
                }, context),  fd => fd.close()));
            },
            ASSERTS: {
                "should return an error"(res) {
                    res.should.error({
                        message: "Error opening"
                    });
                },
                "should try to open two files"(_, { context }) {
                    assertEqual(context.openSync.splice().length, 2);
                },
                "should close first file"(_, { context }) {
                    context.closeSync.assert([
                        [123]
                    ]);
                }
            }
        });
        test("should close files when calling close()", {
            async ARRANGE(after) {
                const files = await newFiles(after);
                const context = newOpenFilesContext();
                context.openSync.pushNextReturn(123);
                context.openSync.pushNextReturn(124);
                const fd = after(utils.openFiles({
                    dataFile: files.data,
                    entriesFile: files.entries,
                }, context), fd => fd.close());
                return { fd, context };
            },
            ACT({ fd }) {
                fd.close();
            },
            ASSERT(_, { context }) {
                context.closeSync.assert([
                    [123],
                    [124]
                ]);
            }
        });
    });
    test.describe("reader", test => {
        test("should read all the file bytes", {
            async ARRANGE(after) {
                const files = await newFiles(after);
                const context = newOpenFilesContext();
                const fd = after(utils.openFiles({
                    dataFile: files.data,
                    entriesFile: files.entries,
                }, context), fd => fd.close());
                const data = Buffer.from([0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
                fd.data.write(data, 0);
                const reader = fd.data.reader();
                return { reader, data };
            },
            ACT({ reader }) {
                const read = Buffer.allocUnsafe(7);
                const eof = reader.read(read, false);
                return { eof, read };
            },
            ASSERTS: {
                "should return EOF false"({ eof }) {
                    assertEqual(eof, false);
                },
                "should read the data"({ read }, { data }) {
                    assertDeepEqual(read, data);
                }
            }
        });
        test("should read up to EOF without error if EOF error flag is false", {
            async ARRANGE(after) {
                const files = await newFiles(after);
                const context = newOpenFilesContext();
                const fd = after(utils.openFiles({
                    dataFile: files.data,
                    entriesFile: files.entries,
                }, context), fd => fd.close());
                const data = Buffer.from([0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
                fd.data.write(data, 0);
                const reader = fd.data.reader();
                return { reader, data };
            },
            ACT({ reader }) {
                const read = Buffer.allocUnsafe(7);
                const eof1 = reader.read(read, false);
                const eof2 = reader.read(read, false);
                return { eof1, eof2, read };
            },
            ASSERTS: {
                "should return EOF 1 false"({ eof1 }) {
                    assertEqual(eof1, false);
                },
                "should return EOF 2 true"({ eof2 }) {
                    assertEqual(eof2, true);
                },
                "should read the data"({ read }, { data }) {
                    assertDeepEqual(read, data);
                }
            }
        });
        test("should error on EOF read if EOF error flag is true", {
            async ARRANGE(after) {
                const files = await newFiles(after);
                const context = newOpenFilesContext();
                const fd = after(utils.openFiles({
                    dataFile: files.data,
                    entriesFile: files.entries,
                }, context), fd => fd.close());
                const data = Buffer.from([0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
                fd.data.write(data, 0);
                const reader = fd.data.reader();
                return { reader, data };
            },
            ACT({ reader }) {
                const read = Buffer.allocUnsafe(7);
                const eof1 = reader.read(read, false);
                const eof2 = monad(() => reader.read(read, true));
                return { eof1, eof2, read };
            },
            ASSERTS: {
                "should return EOF 1 false"({ eof1 }) {
                    assertEqual(eof1, false);
                },
                "should return EOF 2 error"({ eof2 }) {
                    eof2.should.error({
                        message: "Invalid file"
                    });
                },
                "should read the data"({ read }, { data }) {
                    assertDeepEqual(read, data);
                }
            }
        });
        test("should error on partial read if EOF flag is false", {
            async ARRANGE(after) {
                const files = await newFiles(after);
                const context = newOpenFilesContext();
                const fd = after(utils.openFiles({
                    dataFile: files.data,
                    entriesFile: files.entries,
                }, context), fd => fd.close());
                const data = Buffer.from([0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
                fd.data.write(data, 0);
                const reader = fd.data.reader();
                return { reader, data };
            },
            ACT({ reader }) {
                const read = Buffer.allocUnsafe(8);
                const eof = monad(() => reader.read(read, false));
                return { eof, read };
            },
            ASSERT({ eof }) {
                eof.should.error({
                    message: "Invalid file"
                });
            }
        });
        test("should error on partial read if EOF flag is true", {
            async ARRANGE(after) {
                const files = await newFiles(after);
                const context = newOpenFilesContext();
                const fd = after(utils.openFiles({
                    dataFile: files.data,
                    entriesFile: files.entries,
                }, context), fd => fd.close());
                const data = Buffer.from([0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
                fd.data.write(data, 0);
                const reader = fd.data.reader();
                return { reader, data };
            },
            ACT({ reader }) {
                const read = Buffer.allocUnsafe(8);
                const eof = monad(() => reader.read(read, true));
                return { eof, read };
            },
            ASSERT({ eof }) {
                eof.should.error({
                    message: "Invalid file"
                });
            }
        });
        test("should read all the file bytes in multiple reads", {
            async ARRANGE(after) {
                const files = await newFiles(after);
                const context = newOpenFilesContext();
                const fd = after(utils.openFiles({
                    dataFile: files.data,
                    entriesFile: files.entries,
                }, context), fd => fd.close());
                const data = Buffer.from([0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
                fd.data.write(data, 0);
                const reader = fd.data.reader();
                return { reader, data };
            },
            ACT({ reader }) {
                const read1 = Buffer.allocUnsafe(3);
                const read2 = Buffer.allocUnsafe(3);
                const read3 = Buffer.allocUnsafe(1);
                const eof1 = reader.read(read1, false);
                const eof2 = reader.read(read2, false);
                const eof3 = reader.read(read3, false);
                return { eof1, eof2, eof3, read1, read2, read3 };
            },
            ASSERTS: {
                "should return EOF 1 false"({ eof1 }) {
                    assertEqual(eof1, false);
                },
                "should return EOF 2 false"({ eof2 }) {
                    assertEqual(eof2, false);
                },
                "should return EOF 3 false"({ eof3 }) {
                    assertEqual(eof3, false);
                },
                "should read the data"({ read1, read2, read3 }, { data }) {
                    assertDeepEqual(Buffer.concat([read1, read2, read3]), data);
                }
            }
        });
        test("should read all the file bytes when context sends data partially", {
            async ARRANGE(after) {
                const files = await newFiles(after);
                const context = newOpenFilesContext();
                context.openSync.pushNextReturn(123);
                context.openSync.pushNextReturn(123);
                context.readSync.pushNextReturn(3);
                context.readSync.pushNextReturn(3);
                context.readSync.pushNextReturn(1);
                const fd = after(utils.openFiles({
                    dataFile: files.data,
                    entriesFile: files.entries,
                }, context), fd => fd.close());
                const reader = fd.data.reader();
                const read = Buffer.allocUnsafe(7);
                return { reader, read, context };
            },
            ACT({ reader, read }) {
                const eof = reader.read(read, false);
                return { eof };
            },
            ASSERTS: {
                "should return EOF false"({ eof }) {
                    assertEqual(eof, false);
                },
                "should read the data multiple times"(_, { context, read }) {
                    context.readSync.assert([
                        [123, read, 0, 7, 0],
                        [123, read, 3, 4, 3],
                        [123, read, 6, 1, 6]
                    ]);
                }
            }
        });
        test("should not re-read after erroring", {
            async ARRANGE(after) {
                const files = await newFiles(after);
                const context = newOpenFilesContext();
                context.openSync.pushNextReturn(123);
                context.openSync.pushNextReturn(123);
                context.readSync.pushNextError(new Error("read error"));
                context.readSync.pushNextError(new Error("read error"));
                const fd = after(utils.openFiles({
                    dataFile: files.data,
                    entriesFile: files.entries,
                }, context), fd => fd.close());
                const reader = fd.data.reader();
                const read = Buffer.allocUnsafe(7);
                return { reader, read, context };
            },
            ACT({ reader, read }) {
                const eof1 = monad(() => reader.read(read, false));
                const eof2 = monad(() => reader.read(read, false));
                return { eof1, eof2 };
            },
            ASSERTS: {
                "should return EOF 1 error"({ eof1 }) {
                    eof1.should.error({
                        message: "read error"
                    });
                },
                "should return EOF 2 true"({ eof2 }) {
                    eof2.should.ok(true);
                },
                "should read the data a single time"(_, { context, read }) {
                    context.readSync.assert([
                        [123, read, 0, 7, 0]
                    ]);
                }
            }
        });
        test("should advance", {
            async ARRANGE(after) {
                const files = await newFiles(after);
                const context = newOpenFilesContext();
                const fd = after(utils.openFiles({
                    dataFile: files.data,
                    entriesFile: files.entries,
                }, context), fd => fd.close());
                const data = Buffer.from([0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
                fd.data.write(data, 0);
                const reader = fd.data.reader();
                return { reader, data };
            },
            ACT({ reader }) {
                const read = Buffer.allocUnsafe(4);
                reader.advance(3);
                const eof = reader.read(read, false);
                return { eof, read };
            },
            ASSERTS: {
                "should return EOF false"({ eof }) {
                    assertEqual(eof, false);
                },
                "should read the data"({ read }, { data }) {
                    assertDeepEqual(read, data.subarray(3));
                }
            }
        });
        test("should offset", {
            async ARRANGE(after) {
                const files = await newFiles(after);
                const context = newOpenFilesContext();
                const fd = after(utils.openFiles({
                    dataFile: files.data,
                    entriesFile: files.entries,
                }, context), fd => fd.close());
                const data = Buffer.from([0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06]);
                fd.data.write(data, 0);
                const reader = fd.data.reader();
                return { reader, data };
            },
            ACT({ reader }) {
                const read = Buffer.allocUnsafe(4);
                const eof = reader.read(read, false);
                return { eof, read };
            },
            ASSERTS: {
                "should return EOF false"({ eof }) {
                    assertEqual(eof, false);
                },
                "should read the data"({ read }, { data }) {
                    assertDeepEqual(read, data.subarray(0, 4));
                },
                "should return the correct offset"(_, { reader }) {
                    assertEqual(reader.offset, 4);
                }
            }
        });
    });
});