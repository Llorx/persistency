import * as Assert from "assert";

import test from "arrange-act-assert";

import { FreeBlocks } from "./FreeBlocks";

test.describe("FreeBlocks", test => {
    test("should update initial allocation", {
        ARRANGE() {
            const freeBlocks = new FreeBlocks();
            const allocation = freeBlocks.updateAllocation();
            return { allocation, freeBlocks }
        },
        ACT({ allocation }) {
            allocation.add(0, 4);
        },
        ASSERT(_, { freeBlocks }) {
            Assert.deepStrictEqual(freeBlocks.alloc(1), 4);
        }
    });
});