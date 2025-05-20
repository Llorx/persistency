import test, { monad } from "arrange-act-assert";

import { assertDeepEqual, assertEqual, newOpenFilesContext } from "./testUtils";
import * as utils from "./utils";
import { strictEqual } from "assert";

test.describe("utils", test => {
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
            ARRANGE() {
                return newOpenFilesContext();
            },
            ACT(context) {
                return utils.openFiles({
                    dataFile: "dataFile",
                    entriesFile: "entriesFile",
                }, context);
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
                "should open entries and data files"(_, context) {
                    assertDeepEqual(
                        context.openSync.splice().map(entry => [entry[0]]), // Remove flags argument
                        [
                            ["entriesFile"],
                            ["dataFile"],
                        ]
                    );
                }
            }
        });
        test("should clear on first error", {
            ARRANGE() {
                const context = newOpenFilesContext();
                context.openSync.pushNextError(new Error("Error opening"));
                return { context };
            },
            ACT({ context }) {
                return monad(() => utils.openFiles({
                    dataFile: "dataFile",
                    entriesFile: "entriesFile",
                }, context));
            },
            ASSERTS: {
                "should return an error"(res) {
                    res.should.error({
                        message: "Error opening"
                    });
                },
                "should try to open one files"(_, { context }) {
                    strictEqual(context.openSync.splice().length, 1);
                },
                "should not close any file"(_, { context }) {
                    context.closeSync.assert([]);
                }
            }
        });
        test("should clear on second error", {
            ARRANGE() {
                const context = newOpenFilesContext();
                context.openSync.pushNextReturn(123);
                context.openSync.pushNextError(new Error("Error opening"));
                return { context };
            },
            ACT({ context }) {
                return monad(() => utils.openFiles({
                    dataFile: "dataFile",
                    entriesFile: "entriesFile",
                }, context));
            },
            ASSERTS: {
                "should return an error"(res) {
                    res.should.error({
                        message: "Error opening"
                    });
                },
                "should try to open two files"(_, { context }) {
                    strictEqual(context.openSync.splice().length, 2);
                },
                "should close first file"(_, { context }) {
                    context.closeSync.assert([
                        [123]
                    ]);
                }
            }
        });
        test("should close files when calling close()", {
            ARRANGE() {
                const context =  newOpenFilesContext();
                context.openSync.pushNextReturn(123);
                context.openSync.pushNextReturn(124);
                const fd = utils.openFiles({
                    dataFile: "dataFile",
                    entriesFile: "entriesFile",
                }, context);
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
});