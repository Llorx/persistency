import * as Assert from "assert";

import test from "arrange-act-assert";

import { FreeBlocks } from "./FreeBlocks";

test.describe("FreeBlocks", test => {
    test.describe("initial allocation", test => {
        test("should add blocks", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.updateAllocation();
                return { allocation, freeBlocks }
            },
            ACT({ allocation }) {
                allocation.add(0, 4);
                allocation.add(6, 8);
            },
            ASSERT(_, { freeBlocks }) {
                Assert.deepStrictEqual(freeBlocks.getFreeBlocks(), [
                    [4, 6],
                    [8, null]
                ]);
            }
        });
        test("should connect contiguous blocks", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.updateAllocation();
                return { allocation, freeBlocks }
            },
            ACT({ allocation }) {
                allocation.add(0, 4);
                allocation.add(4, 6);
                allocation.add(7, 9);
            },
            ASSERT(_, { freeBlocks }) {
                Assert.deepStrictEqual(freeBlocks.getFreeBlocks(), [
                    [6, 7],
                    [9, null]
                ]);
            }
        });
        test("should finish with the last free value", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.updateAllocation();
                allocation.add(0, 4);
                allocation.add(6, 8);
                allocation.add(8, 9);
                return { allocation }
            },
            ACT({ allocation }) {
                return allocation.finish();
            },
            ASSERT(location) {
                Assert.strictEqual(location, 9);
            }
        });
    });
    test.describe("alloc", test => {
        test("should extend first hole where it fits", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.updateAllocation();
                allocation.add(0, 4);
                allocation.add(6, 8);
                return { freeBlocks }
            },
            ACT({ freeBlocks }) {
                return freeBlocks.alloc(1);
            },
            ASSERTS: {
                "should return the correct location"(location) {
                    Assert.strictEqual(location, 4);
                },
                "should return the correct free blocks"(_, { freeBlocks }) {
                    Assert.deepStrictEqual(freeBlocks.getFreeBlocks(), [
                        [5, 6],
                        [8, null]
                    ]);
                }
            }
        });
        test("should merge when the hole is exact the requested size", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.updateAllocation();
                allocation.add(0, 5);
                allocation.add(6, 8);
                return { freeBlocks }
            },
            ACT({ freeBlocks }) {
                return freeBlocks.alloc(1);
            },
            ASSERTS: {
                "should return the correct location"(location) {
                    Assert.strictEqual(location, 5);
                },
                "should return the correct free blocks"(_, { freeBlocks }) {
                    Assert.deepStrictEqual(freeBlocks.getFreeBlocks(), [
                        [8, null]
                    ]);
                }
            }
        });
        test("should extend the last free block", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.updateAllocation();
                allocation.add(0, 5);
                return { freeBlocks }
            },
            ACT({ freeBlocks }) {
                return freeBlocks.alloc(1);
            },
            ASSERTS: {
                "should return the correct location"(location) {
                    Assert.strictEqual(location, 5);
                },
                "should return the correct free blocks"(_, { freeBlocks }) {
                    Assert.deepStrictEqual(freeBlocks.getFreeBlocks(), [
                        [6, null]
                    ]);
                }
            }
        });
        test("should skip holes bigger than needed", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.updateAllocation();
                allocation.add(0, 4);
                allocation.add(6, 8);
                return { freeBlocks }
            },
            ACT({ freeBlocks }) {
                return freeBlocks.alloc(3);
            },
            ASSERTS: {
                "should return the correct location"(location) {
                    Assert.strictEqual(location, 8);
                },
                "should return the correct free blocks"(_, { freeBlocks }) {
                    Assert.deepStrictEqual(freeBlocks.getFreeBlocks(), [
                        [4, 6],
                        [11, null]
                    ]);
                }
            }
        });
        test("should not return the same location 2 times", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.updateAllocation();
                allocation.add(0, 4);
                allocation.add(6, 8);
                allocation.add(8, 9);
                return { freeBlocks }
            },
            ACT({ freeBlocks }) {
                const firstLocation = freeBlocks.alloc(2);
                const secondLocation = freeBlocks.alloc(2);
                return { firstLocation, secondLocation };
            },
            ASSERTS: {
                "first alloc should be 4"({ firstLocation }) {
                    Assert.strictEqual(firstLocation, 4);
                },
                "second alloc should be 9"({ secondLocation }) {
                    Assert.strictEqual(secondLocation, 9);
                },
                "should return the correct free blocks"(_, { freeBlocks }) {
                    Assert.deepStrictEqual(freeBlocks.getFreeBlocks(), [
                        [11, null]
                    ]);
                }
            }
        });
    });
    test.describe("free", test => {
        test("should free before the first next block without touching it", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.updateAllocation();
                allocation.add(0, 10);
                allocation.add(20, 30);
                allocation.add(40, 50);
                return { freeBlocks }
            },
            ACT({ freeBlocks }) {
                return freeBlocks.free(5, 6);
            },
            ASSERTS: {
                "should return null"(res) {
                    Assert.strictEqual(res, null);
                },
                "should return the correct free blocks"(_, { freeBlocks }) {
                    Assert.deepStrictEqual(freeBlocks.getFreeBlocks(), [
                        [5, 6],
                        [10, 20],
                        [30, 40],
                        [50, null]
                    ]);
                }
            }
        });
        test("should free before the first next block touching it", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.updateAllocation();
                allocation.add(0, 10);
                allocation.add(20, 30);
                allocation.add(40, 50);
                return { freeBlocks }
            },
            ACT({ freeBlocks }) {
                return freeBlocks.free(5, 10);
            },
            ASSERTS: {
                "should return null"(res) {
                    Assert.strictEqual(res, null);
                },
                "should return the correct free blocks"(_, { freeBlocks }) {
                    Assert.deepStrictEqual(freeBlocks.getFreeBlocks(), [
                        [5, 20],
                        [30, 40],
                        [50, null]
                    ]);
                }
            }
        });
        test("should free between blocks without touching the start nor end", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.updateAllocation();
                allocation.add(0, 10);
                allocation.add(20, 30);
                allocation.add(40, 50);
                return { freeBlocks }
            },
            ACT({ freeBlocks }) {
                return freeBlocks.free(22, 25);
            },
            ASSERTS: {
                "should return null"(res) {
                    Assert.strictEqual(res, null);
                },
                "should return the correct free blocks"(_, { freeBlocks }) {
                    Assert.deepStrictEqual(freeBlocks.getFreeBlocks(), [
                        [10, 20],
                        [22, 25],
                        [30, 40],
                        [50, null]
                    ]);
                }
            }
        });
        test("should free between blocks touching the start but not end", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.updateAllocation();
                allocation.add(0, 10);
                allocation.add(20, 30);
                allocation.add(40, 50);
                return { freeBlocks }
            },
            ACT({ freeBlocks }) {
                return freeBlocks.free(20, 25);
            },
            ASSERTS: {
                "should return null"(res) {
                    Assert.strictEqual(res, null);
                },
                "should return the correct free blocks"(_, { freeBlocks }) {
                    Assert.deepStrictEqual(freeBlocks.getFreeBlocks(), [
                        [10, 25],
                        [30, 40],
                        [50, null]
                    ]);
                }
            }
        });
        test("should free between blocks without touching the start but touching the end", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.updateAllocation();
                allocation.add(0, 10);
                allocation.add(20, 30);
                allocation.add(40, 50);
                return { freeBlocks }
            },
            ACT({ freeBlocks }) {
                return freeBlocks.free(25, 30);
            },
            ASSERTS: {
                "should return null"(res) {
                    Assert.strictEqual(res, null);
                },
                "should return the correct free blocks"(_, { freeBlocks }) {
                    Assert.deepStrictEqual(freeBlocks.getFreeBlocks(), [
                        [10, 20],
                        [25, 40],
                        [50, null]
                    ]);
                }
            }
        });
        test("should free at the end of the blocks without touching the end", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.updateAllocation();
                allocation.add(0, 10);
                allocation.add(20, 30);
                allocation.add(40, 50);
                return { freeBlocks }
            },
            ACT({ freeBlocks }) {
                return freeBlocks.free(43, 46);
            },
            ASSERTS: {
                "should return null"(res) {
                    Assert.strictEqual(res, null);
                },
                "should return the correct free blocks"(_, { freeBlocks }) {
                    Assert.deepStrictEqual(freeBlocks.getFreeBlocks(), [
                        [10, 20],
                        [30, 40],
                        [43, 46],
                        [50, null]
                    ]);
                }
            }
        });
        test("should free before the last free block without touching the end", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.updateAllocation();
                allocation.add(0, 10);
                allocation.add(20, 30);
                allocation.add(40, 50);
                return { freeBlocks }
            },
            ACT({ freeBlocks }) {
                return freeBlocks.free(40, 45);
            },
            ASSERTS: {
                "should return the new ending value"(res) {
                    Assert.strictEqual(res, null);
                },
                "should return the correct free blocks"(_, { freeBlocks }) {
                    Assert.deepStrictEqual(freeBlocks.getFreeBlocks(), [
                        [10, 20],
                        [30, 45],
                        [50, null]
                    ]);
                }
            }
        });
        test("should free before the last free block touching the end", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.updateAllocation();
                allocation.add(0, 10);
                allocation.add(20, 30);
                allocation.add(40, 50);
                return { freeBlocks }
            },
            ACT({ freeBlocks }) {
                return freeBlocks.free(40, 50);
            },
            ASSERTS: {
                "should return the new ending value"(res) {
                    Assert.strictEqual(res, 30);
                },
                "should return the correct free blocks"(_, { freeBlocks }) {
                    Assert.deepStrictEqual(freeBlocks.getFreeBlocks(), [
                        [10, 20],
                        [30, null]
                    ]);
                }
            }
        });
        test("should free at the end of the blocks without touching the end", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.updateAllocation();
                allocation.add(0, 10);
                allocation.add(20, 30);
                allocation.add(40, 50);
                return { freeBlocks }
            },
            ACT({ freeBlocks }) {
                return freeBlocks.free(43, 46);
            },
            ASSERTS: {
                "should return null"(res) {
                    Assert.strictEqual(res, null);
                },
                "should return the correct free blocks"(_, { freeBlocks }) {
                    Assert.deepStrictEqual(freeBlocks.getFreeBlocks(), [
                        [10, 20],
                        [30, 40],
                        [43, 46],
                        [50, null]
                    ]);
                }
            }
        });
        test("should free at the end of the blocks touching the end", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.updateAllocation();
                allocation.add(0, 10);
                allocation.add(20, 30);
                allocation.add(40, 50);
                return { freeBlocks }
            },
            ACT({ freeBlocks }) {
                return freeBlocks.free(43, 50);
            },
            ASSERTS: {
                "should return the new ending value"(res) {
                    Assert.strictEqual(res, 43);
                },
                "should return the correct free blocks"(_, { freeBlocks }) {
                    Assert.deepStrictEqual(freeBlocks.getFreeBlocks(), [
                        [10, 20],
                        [30, 40],
                        [43, null]
                    ]);
                }
            }
        });
    });
});