import test from "arrange-act-assert";

import { assertDeepEqual, assertEqual } from "./testUtils";
import { MemoryBlocks } from "./MemoryBlocks";

test.describe("MemoryBlocks", test => {
    //const values = [{}, {}, {}, {}] as const;
    test.describe("getFreeBlocks", test => {
        test("should return blocks with free spaces", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(6, 8, null);
                const block2 = allocation.add(8, 12, null);
                const block3 = allocation.add(13, 14, null);
                return { memory, block1, block2, block3 };
            },
            ACT({ memory }) {
                return memory.getFreeBlocks();
            },
            ASSERT(blocks, { block1, block3 }) {
                assertDeepEqual(blocks, {
                    maxSpace: 6,
                    blocks: [
                        {
                            location: 0,
                            space: 6,
                            block: block1
                        },
                        {
                            location: 12,
                            space: 1,
                            block: block3
                        }
                    ]
                });
            }
        });
        test("should return blocks with free spaces after freeing", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(6, 8, null);
                const block2 = allocation.add(8, 12, null);
                const block3 = allocation.add(13, 14, null);
                return { memory, block1, block2, block3 };
            },
            ACT({ memory, block1 }) {
                memory.free(block1);
                return memory.getFreeBlocks();
            },
            ASSERT(blocks, { block2, block3 }) {
                assertDeepEqual(blocks, {
                    maxSpace: 8,
                    blocks: [
                        {
                            location: 0,
                            space: 8,
                            block: block2
                        },
                        {
                            location: 12,
                            space: 1,
                            block: block3
                        }
                    ]
                });
            }
        });
        test("should not return first block if is equal to offset", {
            ARRANGE() {
                const memory = new MemoryBlocks(6);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(6, 8, null);
                const block2 = allocation.add(8, 12, null);
                const block3 = allocation.add(13, 14, null);
                return { memory, block1, block2, block3 };
            },
            ACT({ memory }) {
                return memory.getFreeBlocks();
            },
            ASSERT(blocks, { block3 }) {
                assertDeepEqual(blocks, {
                    maxSpace: 1,
                    blocks: [
                        {
                            location: 12,
                            space: 1,
                            block: block3
                        }
                    ]
                });
            }
        });
        test("should return max space", {
            ARRANGE() {
                const memory = new MemoryBlocks(4);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(6, 8, null);
                const block2 = allocation.add(8, 12, null);
                const block3 = allocation.add(23, 24, null);
                return { memory, block1, block2, block3 };
            },
            ACT({ memory }) {
                return memory.getFreeBlocks();
            },
            ASSERT(blocks, { block1, block3 }) {
                assertDeepEqual(blocks, {
                    maxSpace: 11,
                    blocks: [
                        {
                            location: 4,
                            space: 2,
                            block: block1
                        },
                        {
                            location: 12,
                            space: 11,
                            block: block3
                        }
                    ]
                });
            }
        });
        test("should return first block if is bigger than offset", {
            ARRANGE() {
                const memory = new MemoryBlocks(5);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(6, 8, null);
                const block2 = allocation.add(8, 12, null);
                const block3 = allocation.add(13, 14, null);
                return { memory, block1, block2, block3 };
            },
            ACT({ memory }) {
                return memory.getFreeBlocks();
            },
            ASSERT(blocks, { block1, block3 }) {
                assertDeepEqual(blocks, {
                    maxSpace: 1,
                    blocks: [
                        {
                            location: 5,
                            space: 1,
                            block: block1
                        },
                        {
                            location: 12,
                            space: 1,
                            block: block3
                        }
                    ]
                });
            }
        });
        test("should not return anything if no space is left", {
            ARRANGE() {
                const memory = new MemoryBlocks(6);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(6, 8, null);
                const block2 = allocation.add(8, 13, null);
                const block3 = allocation.add(13, 14, null);
                return { memory, block1, block2, block3 };
            },
            ACT({ memory }) {
                return memory.getFreeBlocks();
            },
            ASSERT(blocks) {
                assertDeepEqual(blocks, {
                    maxSpace: 0,
                    blocks: []
                });
            }
        });
    });
    test.describe("getAllocatedRanges", test => {
        test("should return allocated ranges", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                allocation.add(6, 8, null);
                allocation.add(8, 12, null);
                allocation.add(13, 14, null);
                return { memory };
            },
            ACT({ memory }) {
                return memory.getAllocatedRanges();
            },
            ASSERT(ranges) {
                assertDeepEqual(ranges, [
                    [6, 12],
                    [13, 14]
                ]);
            }
        });
        test("should return allocated ranges including offset", {
            ARRANGE() {
                const memory = new MemoryBlocks(4);
                const allocation = memory.setAllocation();
                allocation.add(6, 8, null);
                allocation.add(8, 12, null);
                allocation.add(13, 14, null);
                return { memory };
            },
            ACT({ memory }) {
                return memory.getAllocatedRanges();
            },
            ASSERT(ranges) {
                assertDeepEqual(ranges, [
                    [0, 4],
                    [6, 12],
                    [13, 14]
                ]);
            }
        });
        test("should return allocated ranges including offset when initial block overlaps offset", {
            ARRANGE() {
                const memory = new MemoryBlocks(6);
                const allocation = memory.setAllocation();
                allocation.add(6, 8, null);
                allocation.add(8, 12, null);
                allocation.add(13, 14, null);
                return { memory };
            },
            ACT({ memory }) {
                return memory.getAllocatedRanges();
            },
            ASSERT(ranges) {
                assertDeepEqual(ranges, [
                    [0, 12],
                    [13, 14]
                ]);
            }
        });
        test("should return empty when no allocated blocks and there's no offset", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                return { memory };
            },
            ACT({ memory }) {
                return memory.getAllocatedRanges();
            },
            ASSERT(ranges) {
                assertDeepEqual(ranges, []);
            }
        });
        test("should return offset only when no allocated blocks", {
            ARRANGE() {
                const memory = new MemoryBlocks(5);
                return { memory };
            },
            ACT({ memory }) {
                return memory.getAllocatedRanges();
            },
            ASSERT(ranges) {
                assertDeepEqual(ranges, [
                    [0, 5]
                ]);
            }
        });
    });
    test.describe("initial allocation", test => {
        test("should add blocks", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                return { allocation, memory }
            },
            ACT({ allocation }) {
                const block1 = allocation.add(0, 4, null);
                const block2 = allocation.add(6, 8, null);
                return { block1, block2 };
            },
            ASSERTS: {
                "should get block1"({ block1, block2 }) {
                    assertDeepEqual(block1, {
                        start: 0,
                        end: 4,
                        prev: null,
                        next: block2,
                        data: null
                    });
                },
                "should get block2"({ block1, block2 }) {
                    assertDeepEqual(block2, {
                        start: 6,
                        end: 8,
                        prev: block1,
                        next: null,
                        data: null
                    });
                },
                "should get ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [0, 4],
                        [6, 8]
                    ]);
                }
            }
        });
        test("should connect contiguous blocks and add range gaps", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                return { allocation, memory }
            },
            ACT({ allocation }) {
                const block1 = allocation.add(0, 4, null);
                const block2 = allocation.add(4, 6, null);
                const block3 = allocation.add(7, 9, null);
                const block4 = allocation.add(10, 11, null);
                return { block1, block2, block3, block4 };
            },
            ASSERTS: {
                "should get block1"({ block1, block2 }) {
                    assertDeepEqual(block1, {
                        start: 0,
                        end: 4,
                        prev: null,
                        next: block2,
                        data: null
                    });
                },
                "should get block2"({ block1, block2, block3 }) {
                    assertDeepEqual(block2, {
                        start: 4,
                        end: 6,
                        prev: block1,
                        next: block3,
                        data: null
                    });
                },
                "should get block3"({  block2, block3, block4 }) {
                    assertDeepEqual(block3, {
                        start: 7,
                        end: 9,
                        prev: block2,
                        next: block4,
                        data: null
                    });
                },
                "should get block4"({  block3, block4 }) {
                    assertDeepEqual(block4, {
                        start: 10,
                        end: 11,
                        prev: block3,
                        next: null,
                        data: null
                    });
                },
                "should get ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [0, 6],
                        [7, 9],
                        [10, 11]
                    ]);
                }
            }
        });
    });
    test.describe("allocAfter", test => {
        test("should alloc a block after another block between blocks", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(5, 10, null);
                const block2 = allocation.add(20, 28, null);
                const block3 = allocation.add(50, 90, null);
                return { memory, block1, block2, block3 };
            },
            ACT({ memory, block2 }) {
                return memory.allocAfter(block2, 3, {})
            },
            ASSERTS: {
                "should get the block"(block, { block2, block3 }) {
                    assertDeepEqual(block, {
                        start: 28,
                        end: 31,
                        prev: block2,
                        next: block3,
                        data: {}
                    });
                },
                "should get ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [5, 10],
                        [20, 31],
                        [50, 90]
                    ]);
                }
            }
        });
        test("should alloc a block after another block between blocks and merge", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(5, 10, null);
                const block2 = allocation.add(20, 28, null);
                const block3 = allocation.add(31, 90, null);
                return { memory, block1, block2, block3 };
            },
            ACT({ memory, block2 }) {
                return memory.allocAfter(block2, 3, {})
            },
            ASSERTS: {
                "should get the block"(block, { block2, block3 }) {
                    assertDeepEqual(block, {
                        start: 28,
                        end: 31,
                        prev: block2,
                        next: block3,
                        data: {}
                    });
                },
                "should get ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [5, 10],
                        [20, 90]
                    ]);
                }
            }
        });
        test("should alloc a block after another block at the end", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(5, 10, null);
                const block2 = allocation.add(20, 28, null);
                const block3 = allocation.add(50, 90, null);
                return { memory, block1, block2, block3 };
            },
            ACT({ memory, block3 }) {
                return memory.allocAfter(block3, 3, {})
            },
            ASSERTS: {
                "should get the block"(block, { block3 }) {
                    assertDeepEqual(block, {
                        start: 90,
                        end: 93,
                        prev: block3,
                        next: null,
                        data: {}
                    });
                },
                "should get ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [5, 10],
                        [20, 28],
                        [50, 93]
                    ]);
                }
            }
        });
    });
    test.describe("alloc", test => {
        test("should alloc a block", {
            ARRANGE() {
                return new MemoryBlocks(0);
            },
            ACT(memory) {
                return memory.alloc(10, {})
            },
            ASSERTS: {
                "should get the block"(block) {
                    assertDeepEqual(block, {
                        start: 0,
                        end: 10,
                        prev: null,
                        next: null,
                        data: {}
                    });
                },
                "should get ranges"(_, memory) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [0, 10]
                    ]);
                }
            }
        });
        test("should apply the initial offset", {
            ARRANGE() {
                return new MemoryBlocks(5);
            },
            ACT(memory) {
                return memory.alloc(10, {})
            },
            ASSERTS: {
                "should get the block"(block) {
                    assertDeepEqual(block, {
                        start: 5,
                        end: 15,
                        prev: null,
                        next: null,
                        data: {}
                    });
                },
                "should get ranges"(_, memory) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [0, 15]
                    ]);
                }
            }
        });
        test("should apply the initial offset when allocating before the first block with a size smaller than the space free", {
            ARRANGE() {
                const memory = new MemoryBlocks(5);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(7, 8, null);
                return { memory, block1 };
            },
            ACT({ memory }) {
                return memory.alloc(1, null);
            },
            ASSERTS: {
                "should get the block"(block, { block1 }) {
                    assertDeepEqual(block, {
                        start: 5,
                        end: 6,
                        prev: null,
                        next: block1,
                        data: null
                    });
                },
                "should get ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [0, 6],
                        [7, 8]
                    ]);
                }
            }
        });
        test("should apply the initial offset when allocating before the first block with a size equal to the space free", {
            ARRANGE() {
                const memory = new MemoryBlocks(5);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(7, 8, null);
                return { memory, block1 };
            },
            ACT({ memory }) {
                return memory.alloc(2, null);
            },
            ASSERTS: {
                "should get the block"(block, { block1 }) {
                    assertDeepEqual(block, {
                        start: 5,
                        end: 7,
                        prev: null,
                        next: block1,
                        data: null
                    });
                },
                "should get ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [0, 8]
                    ]);
                }
            }
        });
        test("should alloc contiguous blocks", {
            ARRANGE() {
                return new MemoryBlocks(5);
            },
            ACT(memory) {
                const block1 = memory.alloc(10, {});
                const block2 = memory.alloc(2, {});
                const block3 = memory.alloc(3, {});
                return { block1, block2, block3 };
            },
            ASSERTS: {
                "should alloc block 1"({ block1, block2 }) {
                    assertDeepEqual(block1, {
                        start: 5,
                        end: 15,
                        prev: null,
                        next: block2,
                        data: {}
                    });
                },
                "should alloc block 2"({ block1, block2, block3 }) {
                    assertDeepEqual(block2, {
                        start: 15,
                        end: 17,
                        prev: block1,
                        next: block3,
                        data: {}
                    });
                },
                "should alloc block 3"({ block2, block3 }) {
                    assertDeepEqual(block3, {
                        start: 17,
                        end: 20,
                        prev: block2,
                        next: null,
                        data: {}
                    });
                },
                "should get ranges"(_, memory) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [0, 20]
                    ]);
                }
            }
        });
        test("should extend first gap where it fits", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(0, 4, null);
                const block2 = allocation.add(6, 8, null);
                return { memory, block1, block2 }
            },
            ACT({ memory }) {
                return memory.alloc(1, null);
            },
            ASSERTS: {
                "should return the correct block"(block, { block1, block2 }) {
                    assertDeepEqual(block, {
                        start: 4,
                        end: 5,
                        prev: block1,
                        next: block2,
                        data: null
                    });
                },
                "block1 should point to new block"(block, { block1 }) {
                    assertDeepEqual(block1, {
                        start: 0,
                        end: 4,
                        prev: null,
                        next: block,
                        data: null
                    });
                },
                "block2 should point to new block"(block, { block2 }) {
                    assertDeepEqual(block2, {
                        start: 6,
                        end: 8,
                        prev: block,
                        next: null,
                        data: null
                    });
                },
                "should return the correct ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [0, 5],
                        [6, 8]
                    ]);
                }
            }
        });
        test("should extend contiguous gaps where it fits", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(0, 4, null);
                const block2 = allocation.add(7, 8, null);
                return { memory, block1, block2 }
            },
            ACT({ memory }) {
                const newBlock1 = memory.alloc(1, null);
                const newBlock2 = memory.alloc(1, null);
                return { newBlock1, newBlock2 };
            },
            ASSERTS: {
                "should return the correct first block"({ newBlock1, newBlock2 }, { block1 }) {
                    assertDeepEqual(newBlock1, {
                        start: 4,
                        end: 5,
                        prev: block1,
                        next: newBlock2,
                        data: null
                    });
                },
                "should return the correct second block"({ newBlock1, newBlock2 }, { block2 }) {
                    assertDeepEqual(newBlock2, {
                        start: 5,
                        end: 6,
                        prev: newBlock1,
                        next: block2,
                        data: null
                    });
                },
                "block1 should point to new block 1"({ newBlock1 }, { block1 }) {
                    assertDeepEqual(block1, {
                        start: 0,
                        end: 4,
                        prev: null,
                        next: newBlock1,
                        data: null
                    });
                },
                "block2 should point to new block 2"({ newBlock2 }, { block2 }) {
                    assertDeepEqual(block2, {
                        start: 7,
                        end: 8,
                        prev: newBlock2,
                        next: null,
                        data: null
                    });
                },
                "should return the correct ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [0, 6],
                        [7, 8]
                    ]);
                }
            }
        });
        test("should extend contiguous gaps and merge ranges", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(0, 4, null);
                const block2 = allocation.add(6, 8, null);
                return { memory, block1, block2 }
            },
            ACT({ memory }) {
                const newBlock1 = memory.alloc(1, null);
                const newBlock2 = memory.alloc(1, null);
                return { newBlock1, newBlock2 };
            },
            ASSERTS: {
                "should return the correct first block"({ newBlock1, newBlock2 }, { block1 }) {
                    assertDeepEqual(newBlock1, {
                        start: 4,
                        end: 5,
                        prev: block1,
                        next: newBlock2,
                        data: null
                    });
                },
                "should return the correct second block"({ newBlock1, newBlock2 }, { block2 }) {
                    assertDeepEqual(newBlock2, {
                        start: 5,
                        end: 6,
                        prev: newBlock1,
                        next: block2,
                        data: null
                    });
                },
                "block1 should point to new block 1"({ newBlock1 }, { block1 }) {
                    assertDeepEqual(block1, {
                        start: 0,
                        end: 4,
                        prev: null,
                        next: newBlock1,
                        data: null
                    });
                },
                "block2 should point to new block 2"({ newBlock2 }, { block2 }) {
                    assertDeepEqual(block2, {
                        start: 6,
                        end: 8,
                        prev: newBlock2,
                        next: null,
                        data: null
                    });
                },
                "should return the correct ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [0, 8]
                    ]);
                }
            }
        });
        test("should merge when the gap is exact the requested size", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(0, 5, null);
                const block2 = allocation.add(6, 8, null);
                return { memory, block1, block2 }
            },
            ACT({ memory }) {
                return memory.alloc(1, null);
            },
            ASSERTS: {
                "should return the correct block"(block, { block1, block2 }) {
                    assertDeepEqual(block, {
                        start: 5,
                        end: 6,
                        prev: block1,
                        next: block2,
                        data: null
                    });
                },
                "block1 should point to new block"(block, { block1 }) {
                    assertDeepEqual(block1, {
                        start: 0,
                        end: 5,
                        prev: null,
                        next: block,
                        data: null
                    });
                },
                "block2 should point to new block"(block, { block2 }) {
                    assertDeepEqual(block2, {
                        start: 6,
                        end: 8,
                        prev: block,
                        next: null,
                        data: null
                    });
                },
                "should return the correct ranges"(_, { memory: freeBlocks }) {
                    assertDeepEqual(freeBlocks.getAllocatedRanges(), [
                        [0, 8]
                    ]);
                }
            }
        });
        test("should extend the last free range", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(0, 5, null);
                return { memory, block1 }
            },
            ACT({ memory }) {
                return memory.alloc(1, null);
            },
            ASSERTS: {
                "should return the correct block"(block, { block1 }) {
                    assertDeepEqual(block, {
                        start: 5,
                        end: 6,
                        prev: block1,
                        next: null,
                        data: null
                    });
                },
                "block1 should point to new block"(block, { block1 }) {
                    assertDeepEqual(block1, {
                        start: 0,
                        end: 5,
                        prev: null,
                        next: block,
                        data: null
                    });
                },
                "should return the correct free ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [0, 6]
                    ]);
                }
            }
        });
        test("should skip gaps bigger than requested", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(0, 4, null);
                const block2 = allocation.add(6, 8, null);
                return { memory, block1, block2 }
            },
            ACT({ memory }) {
                return memory.alloc(3, null);
            },
            ASSERTS: {
                "should return the correct block"(block, { block2 }) {
                    assertDeepEqual(block, {
                        start: 8,
                        end: 11,
                        prev: block2,
                        next: null,
                        data: null
                    });
                },
                "block2 should point to new block"(block, { block1, block2 }) {
                    assertDeepEqual(block2, {
                        start: 6,
                        end: 8,
                        prev: block1,
                        next: block,
                        data: null
                    });
                },
                "should return the correct allocated ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [0, 4],
                        [6, 11]
                    ]);
                }
            }
        });
        test("should be able to free after extending a range", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(0, 4, null);
                const lastBlock = allocation.add(6, 8, null);
                const block2 = memory.alloc(1, null);
                return { memory, block1, lastBlock, block2 }
            },
            ACT({ memory, lastBlock }) {
                return memory.free(lastBlock);
            },
            ASSERTS: {
                "should return the if memory shrinked"(res) {
                    assertDeepEqual(res, true);
                },
                "block2 should point to block1"(_, { block1, block2 }) {
                    assertDeepEqual(block2, {
                        start: 4,
                        end: 5,
                        prev: block1,
                        next: null,
                        data: null
                    });
                },
                "should return the correct allocated ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [0, 5]
                    ]);
                }
            }
        });
        test("should be able to free after merging ranges", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(0, 4, null);
                const lastBlock = allocation.add(6, 8, null);
                const block2 = memory.alloc(2, null);
                return { memory, block1, lastBlock, block2 }
            },
            ACT({ memory, lastBlock }) {
                return memory.free(lastBlock);
            },
            ASSERTS: {
                "should return if memory shrinked"(res) {
                    assertDeepEqual(res, true);
                },
                "block2 should point to block1"(_, { block1, block2 }) {
                    assertDeepEqual(block2, {
                        start: 4,
                        end: 6,
                        prev: block1,
                        next: null,
                        data: null
                    });
                },
                "should return the correct allocated ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [0, 6]
                    ]);
                }
            }
        });
    });
    test.describe("free", test => {
        test("should free a block at the start of a range", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(0, 5, null);
                const block2 = allocation.add(6, 8, null);
                const block3 = allocation.add(8, 9, null);
                const block4 = allocation.add(9, 10, null);
                return { memory, block1, block2, block3, block4 }
            },
            ACT({ memory, block2 }) {
                return memory.free(block2);
            },
            ASSERTS: {
                "should return if memory shrinked"(res) {
                    assertDeepEqual(res, false);
                },
                "block1 should point to block3"(_, { block1, block3 }) {
                    assertDeepEqual(block1, {
                        start: 0,
                        end: 5,
                        prev: null,
                        next: block3,
                        data: null
                    });
                },
                "block3 should point to block1"(_, { block1, block3, block4 }) {
                    assertDeepEqual(block3, {
                        start: 8,
                        end: 9,
                        prev: block1,
                        next: block4,
                        data: null
                    });
                },
                "should return the correct allocated ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [0, 5],
                        [8, 10]
                    ]);
                }
            }
        });
        test("should able to alloc a new block at the start of a range after freeing it", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(0, 5, null);
                const block2 = allocation.add(6, 8, null);
                const block3 = allocation.add(8, 9, null);
                const block4 = allocation.add(9, 10, null);
                memory.free(block2);
                return { memory, block1, block2, block3, block4 }
            },
            ACT({ memory }) {
                return memory.alloc(3, null);
            },
            ASSERTS: {
                "should return the correct block"(block, { block1, block3 }) {
                    assertDeepEqual(block, {
                        start: 5,
                        end: 8,
                        prev: block1,
                        next: block3,
                        data: null
                    });
                },
                "block1 should point to new block"(block, { block1 }) {
                    assertDeepEqual(block1, {
                        start: 0,
                        end: 5,
                        prev: null,
                        next: block,
                        data: null
                    });
                },
                "block3 should point to new block"(block, { block3, block4 }) {
                    assertDeepEqual(block3, {
                        start: 8,
                        end: 9,
                        prev: block,
                        next: block4,
                        data: null
                    });
                },
                "should return the correct allocated ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [0, 10]
                    ]);
                }
            }
        });
        test("should free a block at the end of a range", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(0, 5, null);
                const block2 = allocation.add(5, 8, null);
                const block3 = allocation.add(8, 9, null);
                const block4 = allocation.add(10, 11, null);
                return { memory, block1, block2, block3, block4 }
            },
            ACT({ memory, block3 }) {
                return memory.free(block3);
            },
            ASSERTS: {
                "should return if memory shrinked"(res) {
                    assertDeepEqual(res, false);
                },
                "block2 should point to block4"(_, { block1, block2, block4 }) {
                    assertDeepEqual(block2, {
                        start: 5,
                        end: 8,
                        prev: block1,
                        next: block4,
                        data: null
                    });
                },
                "block4 should point to block2"(_, { block2, block4 }) {
                    assertDeepEqual(block4, {
                        start: 10,
                        end: 11,
                        prev: block2,
                        next: null,
                        data: null
                    });
                },
                "should return the correct allocated ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [0, 8],
                        [10, 11]
                    ]);
                }
            }
        });
        test("should able to alloc a new block at the end of a range after freeing it", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(0, 5, null);
                const block2 = allocation.add(5, 8, null);
                const block3 = allocation.add(8, 9, null);
                const block4 = allocation.add(12, 13, null);
                memory.free(block3);
                return { memory, block1, block2, block3, block4 }
            },
            ACT({ memory }) {
                return memory.alloc(2, null);
            },
            ASSERTS: {
                "should return the correct block"(block, { block2, block4 }) {
                    assertDeepEqual(block, {
                        start: 8,
                        end: 10,
                        prev: block2,
                        next: block4,
                        data: null
                    });
                },
                "block2 should point to new block"(block, { block1, block2 }) {
                    assertDeepEqual(block2, {
                        start: 5,
                        end: 8,
                        prev: block1,
                        next: block,
                        data: null
                    });
                },
                "block4 should point to new block"(block, { block4 }) {
                    assertDeepEqual(block4, {
                        start: 12,
                        end: 13,
                        prev: block,
                        next: null,
                        data: null
                    });
                },
                "should return the correct allocated ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [0, 10],
                        [12, 13]
                    ]);
                }
            }
        });
        test("should free a block in the middle of a range", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(0, 5, null);
                const block2 = allocation.add(5, 8, null);
                const block3 = allocation.add(8, 9, null);
                const block4 = allocation.add(10, 11, null);
                return { memory, block1, block2, block3, block4 }
            },
            ACT({ memory, block2 }) {
                return memory.free(block2);
            },
            ASSERTS: {
                "should return if memory shrinked"(res) {
                    assertDeepEqual(res, false);
                },
                "block1 should point to block3"(_, { block1, block3 }) {
                    assertDeepEqual(block1, {
                        start: 0,
                        end: 5,
                        prev: null,
                        next: block3,
                        data: null
                    });
                },
                "block3 should point to block1"(_, { block1, block3, block4 }) {
                    assertDeepEqual(block3, {
                        start: 8,
                        end: 9,
                        prev: block1,
                        next: block4,
                        data: null
                    });
                },
                "should return the correct allocated ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [0, 5],
                        [8, 9],
                        [10, 11]
                    ]);
                }
            }
        });
        test("should free first range", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(0, 5, null);
                const block2 = allocation.add(5, 8, null);
                const block3 = allocation.add(9, 10, null);
                const block4 = allocation.add(10, 11, null);
                return { memory, block1, block2, block3, block4 }
            },
            ACT({ memory, block1, block2 }) {
                const res1 = memory.free(block1);
                const res2 = memory.free(block2);
                return { res1, res2 };
            },
            ASSERTS: {
                "first free should return if memory shrinked"({ res1 }) {
                    assertDeepEqual(res1, false);
                },
                "second free should return if memory shrinked"({ res2 }) {
                    assertDeepEqual(res2, false);
                },
                "block3 should point to block4"(_, { block3, block4 }) {
                    assertDeepEqual(block3, {
                        start: 9,
                        end: 10,
                        prev: null,
                        next: block4,
                        data: null
                    });
                },
                "block4 should point to block3"(_, { block3, block4 }) {
                    assertDeepEqual(block4, {
                        start: 10,
                        end: 11,
                        prev: block3,
                        next: null,
                        data: null
                    });
                },
                "should return the correct allocated ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [9, 11]
                    ]);
                }
            }
        });
        test("should free a range after splitting a range", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(4, 5, null);
                const block2 = allocation.add(5, 6, null);
                const block3 = allocation.add(6, 7, null);
                const block4 = allocation.add(8, 9, null);
                return { memory, block1, block2, block3, block4 }
            },
            ACT({ memory, block2, block3 }) {
                const res1 = memory.free(block2);
                const res2 = memory.free(block3);
                return { res1, res2 };
            },
            ASSERTS: {
                "first free should return if memory shrinked"({ res1 }) {
                    assertDeepEqual(res1, false);
                },
                "second free should return if memory shrinked"({ res2 }) {
                    assertDeepEqual(res2, false);
                },
                "block1 should point to block4"(_, { block1, block4 }) {
                    assertDeepEqual(block1, {
                        start: 4,
                        end: 5,
                        prev: null,
                        next: block4,
                        data: null
                    });
                },
                "block4 should point to block1"(_, { block1, block4 }) {
                    assertDeepEqual(block4, {
                        start: 8,
                        end: 9,
                        prev: block1,
                        next: null,
                        data: null
                    });
                },
                "should return the correct allocated ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [4, 5],
                        [8, 9]
                    ]);
                }
            }
        });
        test("should free a range after freeing a previous range", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(4, 5, null);
                const block2 = allocation.add(6, 7, null);
                const block3 = allocation.add(8, 9, null);
                const block4 = allocation.add(10, 11, null);
                return { memory, block1, block2, block3, block4 }
            },
            ACT({ memory, block2, block3 }) {
                const res1 = memory.free(block2);
                const res2 = memory.free(block3);
                return { res1, res2 };
            },
            ASSERTS: {
                "first free should return if memory shrinked"({ res1 }) {
                    assertDeepEqual(res1, false);
                },
                "second free should return if memory shrinked"({ res2 }) {
                    assertDeepEqual(res2, false);
                },
                "block1 should point to block4"(_, { block1, block4 }) {
                    assertDeepEqual(block1, {
                        start: 4,
                        end: 5,
                        prev: null,
                        next: block4,
                        data: null
                    });
                },
                "block4 should point to block1"(_, { block1, block4 }) {
                    assertDeepEqual(block4, {
                        start: 10,
                        end: 11,
                        prev: block1,
                        next: null,
                        data: null
                    });
                },
                "should return the correct allocated ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [4, 5],
                        [10, 11]
                    ]);
                }
            }
        });
        test("should return the new size if freeing at the end of the memory", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(4, 5, null);
                const block2 = allocation.add(6, 7, null);
                const block3 = allocation.add(8, 9, null);
                const block4 = allocation.add(10, 11, null);
                return { memory, block1, block2, block3, block4 }
            },
            ACT({ memory, block4 }) {
                return memory.free(block4);
            },
            ASSERTS: {
                "free should return if memory shrinked"(res) {
                    assertDeepEqual(res, true);
                },
                "block3 should not point to block4"(_, { block2, block3 }) {
                    assertDeepEqual(block3, {
                        start: 8,
                        end: 9,
                        prev: block2,
                        next: null,
                        data: null
                    });
                },
                "should return the correct allocated ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [4, 5],
                        [6, 7],
                        [8, 9]
                    ]);
                }
            }
        });
        test("should return the new size if freeing at the end of the memory after freeing a previous block", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(4, 5, null);
                const block2 = allocation.add(6, 7, null);
                const block3 = allocation.add(8, 9, null);
                const block4 = allocation.add(10, 11, null);
                return { memory, block1, block2, block3, block4 }
            },
            ACT({ memory, block3, block4 }) {
                const res1 = memory.free(block3);
                const res2 = memory.free(block4);
                return { res1, res2 };
            },
            ASSERTS: {
                "first free should return if memory shrinked"({ res1 }) {
                    assertDeepEqual(res1, false);
                },
                "second free should return if memory shrinked"({ res2 }) {
                    assertDeepEqual(res2, true);
                },
                "block2 should not point to block1"(_, { block1, block2 }) {
                    assertDeepEqual(block2, {
                        start: 6,
                        end: 7,
                        prev: block1,
                        next: null,
                        data: null
                    });
                },
                "should return the correct allocated ranges"(_, { memory }) {
                    assertDeepEqual(memory.getAllocatedRanges(), [
                        [4, 5],
                        [6, 7]
                    ]);
                }
            }
        });
    });
    test.describe("getLastBlock", test => {
        test("should not get last block if not initialized", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                return { memory }
            },
            ACT({ memory }) {
                return memory.getLastBlock();
            },
            ASSERT(block) {
                assertEqual(block, null);
            }
        });
        test("should get last block after initializing only one block", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(4, 5, null);
                return { memory, block1 }
            },
            ACT({ memory }) {
                return memory.getLastBlock();
            },
            ASSERT(block, { block1 }) {
                assertEqual(block, block1);
            }
        });
        test("should get last block after initializing multiple blocks", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(4, 5, null);
                const block2 = allocation.add(5, 6, null);
                const block3 = allocation.add(8, 9, null);
                const block4 = allocation.add(10, 11, null);
                return { memory, block1, block2, block3, block4 }
            },
            ACT({ memory }) {
                return memory.getLastBlock();
            },
            ASSERT(block, { block4 }) {
                assertEqual(block, block4);
            }
        });
        test("should get last block after allocating one block", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const block1 = memory.alloc(4, null);
                return { memory, block1 }
            },
            ACT({ memory }) {
                return memory.getLastBlock();
            },
            ASSERT(block, { block1 }) {
                assertEqual(block, block1);
            }
        });
        test("should get last block after allocating multiple block", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const block1 = memory.alloc(4, null);
                const block2 = memory.alloc(5, null);
                return { memory, block1, block2 }
            },
            ACT({ memory }) {
                return memory.getLastBlock();
            },
            ASSERT(block, { block2 }) {
                assertEqual(block, block2);
            }
        });
        test("should get last block after allocating a block before last block", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(4, 5, null);
                memory.alloc(3, null);
                return { memory, block1 }
            },
            ACT({ memory }) {
                return memory.getLastBlock();
            },
            ASSERT(block, { block1 }) {
                assertEqual(block, block1);
            }
        });
        test("should get a new last block after freeing the current last block", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(4, 5, null);
                const block2 = allocation.add(5, 6, null);
                const block3 = allocation.add(8, 9, null);
                const block4 = allocation.add(10, 11, null);
                return { memory, block1, block2, block3, block4 };
            },
            ACT({ memory, block4 }) {
                memory.free(block4);
                return memory.getLastBlock();
            },
            ASSERT(block, { block3 }) {
                assertEqual(block, block3);
            }
        });
        test("should get the last block after freeing a block before the last block", {
            ARRANGE() {
                const memory = new MemoryBlocks(0);
                const allocation = memory.setAllocation();
                const block1 = allocation.add(4, 5, null);
                const block2 = allocation.add(5, 6, null);
                const block3 = allocation.add(8, 9, null);
                const block4 = allocation.add(10, 11, null);
                return { memory, block1, block2, block3, block4 };
            },
            ACT({ memory, block3 }) {
                memory.free(block3);
                return memory.getLastBlock();
            },
            ASSERT(block, { block4 }) {
                assertEqual(block, block4);
            }
        });
    });
});