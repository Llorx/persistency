import * as Assert from "assert";

import test from "arrange-act-assert";

import { FreeBlocks } from "./FreeBlocks";

test.describe("FreeBlocks", test => {
    test.describe("getAllocatedBlocks", test => {
        test("should return allocated blocks", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.setAllocation();
                allocation.add(0, 4);
                allocation.add(6, 8);
                allocation.add(8, 12);
                allocation.add(13, 14);
                return { freeBlocks }
            },
            ACT({ freeBlocks }) {
                return freeBlocks.getAllocatedBlocks();
            },
            ASSERT(_, { freeBlocks }) {
                Assert.deepStrictEqual(freeBlocks.getAllocatedBlocks(), [
                    [0, 4],
                    [6, 12],
                    [13, 14]
                ]);
            }
        });
        test("should return allocated blocks when first bytes are not allocated", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.setAllocation();
                allocation.add(6, 8);
                allocation.add(8, 12);
                allocation.add(13, 14);
                return { freeBlocks }
            },
            ACT({ freeBlocks }) {
                return freeBlocks.getAllocatedBlocks();
            },
            ASSERT(_, { freeBlocks }) {
                Assert.deepStrictEqual(freeBlocks.getAllocatedBlocks(), [
                    [6, 12],
                    [13, 14]
                ]);
            }
        });
        test("should not return any block if not allocation happened", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                return { freeBlocks }
            },
            ACT({ freeBlocks }) {
                return freeBlocks.getAllocatedBlocks();
            },
            ASSERT(_, { freeBlocks }) {
                Assert.deepStrictEqual(freeBlocks.getAllocatedBlocks(), []);
            }
        });
        test("should return only the first block if only one allocation happened at start", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.setAllocation();
                allocation.add(0, 4);
                return { freeBlocks }
            },
            ACT({ freeBlocks }) {
                return freeBlocks.getAllocatedBlocks();
            },
            ASSERT(_, { freeBlocks }) {
                Assert.deepStrictEqual(freeBlocks.getAllocatedBlocks(), [
                    [0, 4]
                ]);
            }
        });
        test("should return only the first block if only one allocation happened at the middle", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.setAllocation();
                allocation.add(5, 6);
                return { freeBlocks }
            },
            ACT({ freeBlocks }) {
                return freeBlocks.getAllocatedBlocks();
            },
            ASSERT(_, { freeBlocks }) {
                Assert.deepStrictEqual(freeBlocks.getAllocatedBlocks(), [
                    [5, 6]
                ]);
            }
        });
    });
    test.describe("initial allocation", test => {
        test("should add blocks", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.setAllocation();
                return { allocation, freeBlocks }
            },
            ACT({ allocation }) {
                allocation.add(0, 4);
                allocation.add(6, 8);
            },
            ASSERT(_, { freeBlocks }) {
                Assert.deepStrictEqual(freeBlocks.getAllocatedBlocks(), [
                    [0, 4],
                    [6, 8]
                ]);
            }
        });
        test("should connect contiguous blocks", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.setAllocation();
                return { allocation, freeBlocks }
            },
            ACT({ allocation }) {
                allocation.add(0, 4);
                allocation.add(4, 6);
                allocation.add(7, 9);
            },
            ASSERT(_, { freeBlocks }) {
                Assert.deepStrictEqual(freeBlocks.getAllocatedBlocks(), [
                    [0, 6],
                    [7, 9]
                ]);
            }
        });
        test("should finish with the last free value", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.setAllocation();
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
                const allocation = freeBlocks.setAllocation();
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
                    Assert.deepStrictEqual(freeBlocks.getAllocatedBlocks(), [
                        [0, 5],
                        [6, 8]
                    ]);
                }
            }
        });
        test("should merge when the hole is exact the requested size", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.setAllocation();
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
                    Assert.deepStrictEqual(freeBlocks.getAllocatedBlocks(), [
                        [0, 8]
                    ]);
                }
            }
        });
        test("should extend the last free block", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.setAllocation();
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
                    Assert.deepStrictEqual(freeBlocks.getAllocatedBlocks(), [
                        [0, 6]
                    ]);
                }
            }
        });
        test("should skip holes bigger than needed", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.setAllocation();
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
                    Assert.deepStrictEqual(freeBlocks.getAllocatedBlocks(), [
                        [0, 4],
                        [6, 11]
                    ]);
                }
            }
        });
        test("should not return the same location 2 times", {
            ARRANGE() {
                const freeBlocks = new FreeBlocks();
                const allocation = freeBlocks.setAllocation();
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
                    Assert.deepStrictEqual(freeBlocks.getAllocatedBlocks(), [
                        [0, 11]
                    ]);
                }
            }
        });
    });
    test.describe("free", test => {
        test.describe("before first block", test => {
            test("not overlapping the end [free]...[block]", {
                ARRANGE() {
                    const freeBlocks = new FreeBlocks();
                    const allocation = freeBlocks.setAllocation();
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
                        Assert.deepStrictEqual(freeBlocks.getAllocatedBlocks(), [
                            [0, 5],
                            [6, 10],
                            [20, 30],
                            [40, 50]
                        ]);
                    }
                }
            });
            test("overlapping the end [free|block]", {
                ARRANGE() {
                    const freeBlocks = new FreeBlocks();
                    const allocation = freeBlocks.setAllocation();
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
                        Assert.deepStrictEqual(freeBlocks.getAllocatedBlocks(), [
                            [0, 5],
                            [20, 30],
                            [40, 50]
                        ]);
                    }
                }
            });
        });
        test.describe("between blocks", test => {
            test("not overlapping the start nor end [prev]...[free]...[next]", {
                ARRANGE() {
                    const freeBlocks = new FreeBlocks();
                    const allocation = freeBlocks.setAllocation();
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
                        Assert.deepStrictEqual(freeBlocks.getAllocatedBlocks(), [
                            [0, 10],
                            [20, 22],
                            [25, 30],
                            [40, 50]
                        ]);
                    }
                }
            });
            test("overlapping the start but not end [prev|free]...[next]", {
                ARRANGE() {
                    const freeBlocks = new FreeBlocks();
                    const allocation = freeBlocks.setAllocation();
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
                        Assert.deepStrictEqual(freeBlocks.getAllocatedBlocks(), [
                            [0, 10],
                            [25, 30],
                            [40, 50]
                        ]);
                    }
                }
            });
            test("not overlapping the start but overlapping the end [prev]...[free|next]", {
                ARRANGE() {
                    const freeBlocks = new FreeBlocks();
                    const allocation = freeBlocks.setAllocation();
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
                        Assert.deepStrictEqual(freeBlocks.getAllocatedBlocks(), [
                            [0, 10],
                            [20, 25],
                            [40, 50]
                        ]);
                    }
                }
            });
            test("overlapping the start and end [prev|free|next]", {
                ARRANGE() {
                    const freeBlocks = new FreeBlocks();
                    const allocation = freeBlocks.setAllocation();
                    allocation.add(0, 10);
                    allocation.add(20, 30);
                    allocation.add(40, 50);
                    return { freeBlocks }
                },
                ACT({ freeBlocks }) {
                    return freeBlocks.free(20, 30);
                },
                ASSERTS: {
                    "should return null"(res) {
                        Assert.strictEqual(res, null);
                    },
                    "should return the correct free blocks"(_, { freeBlocks }) {
                        Assert.deepStrictEqual(freeBlocks.getAllocatedBlocks(), [
                            [0, 10],
                            [40, 50]
                        ]);
                    }
                }
            });
        });
        test.describe("before the end", test => {
            test("not overlapping the start nor end [prev]...[free]...[end", {
                ARRANGE() {
                    const freeBlocks = new FreeBlocks();
                    const allocation = freeBlocks.setAllocation();
                    allocation.add(0, 10);
                    allocation.add(20, 30);
                    allocation.add(40, 50);
                    return { freeBlocks }
                },
                ACT({ freeBlocks }) {
                    return freeBlocks.free(43, 45);
                },
                ASSERTS: {
                    "should return null"(res) {
                        Assert.strictEqual(res, null);
                    },
                    "should return the correct free blocks"(_, { freeBlocks }) {
                        Assert.deepStrictEqual(freeBlocks.getAllocatedBlocks(), [
                            [0, 10],
                            [20, 30],
                            [40, 43],
                            [45, 50]
                        ]);
                    }
                }
            });
            test("overlapping the start but not overlapping the end [prev|free]...[end", {
                ARRANGE() {
                    const freeBlocks = new FreeBlocks();
                    const allocation = freeBlocks.setAllocation();
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
                        Assert.deepStrictEqual(freeBlocks.getAllocatedBlocks(), [
                            [0, 10],
                            [20, 30],
                            [45, 50]
                        ]);
                    }
                }
            });
            test("not overlapping the start but overlapping the end [prev]...[free|end", {
                ARRANGE() {
                    const freeBlocks = new FreeBlocks();
                    const allocation = freeBlocks.setAllocation();
                    allocation.add(0, 10);
                    allocation.add(20, 30);
                    allocation.add(40, 50);
                    return { freeBlocks }
                },
                ACT({ freeBlocks }) {
                    return freeBlocks.free(45, 50);
                },
                ASSERTS: {
                    "should return the new ending value"(res) {
                        Assert.strictEqual(res, 45);
                    },
                    "should return the correct free blocks"(_, { freeBlocks }) {
                        Assert.deepStrictEqual(freeBlocks.getAllocatedBlocks(), [
                            [0, 10],
                            [20, 30],
                            [40, 45]
                        ]);
                    }
                }
            });
            test("overlapping the start and end [prev|free|end", {
                ARRANGE() {
                    const freeBlocks = new FreeBlocks();
                    const allocation = freeBlocks.setAllocation();
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
                        Assert.deepStrictEqual(freeBlocks.getAllocatedBlocks(), [
                            [0, 10],
                            [20, 30]
                        ]);
                    }
                }
            });
        });
    });
});