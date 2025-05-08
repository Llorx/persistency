type FreeBlock = {
    start:number;
} & ({
    end:number;
    next:FreeBlock;
} | {
    end:null;
    next:null;
});
export class FreeBlocks {
    private _next:FreeBlock = {
        start: 0,
        end: null,
        next: null
    };
    updateAllocation() {
        // Assume updating in ascending order, so "next" is always going to be the last block
        let next = this._next;
        return {
            add(start:number, end:number) {
                if (next.start === start) {
                    next.start = end;
                } else {
                    next.end = start;
                    next = next.next = {
                        start: end,
                        end: null,
                        next: null
                    };
                }
            },
            finish() {
                return next.start;
            }
        };
    }
    alloc(size:number) {
        let block:FreeBlock|null = this._next;
        do {
            if (block.end == null) {
                const location = block.start;
                block.start += size;
                return location;
            } else if (block.end - block.start >= size) {
                const location = block.start;
                block.start += size;
                if (block.start >= block.end) {
                    block.start = block.next.start;
                    block.end = block.next.end!;
                    block.next = block.next.next!;
                }
                return location;
            }
            block = block.next;
        } while(block);
        throw new Error("Free data location not found"); // Never should happen
    }
    free(start:number, end:number) {
        let next:FreeBlock|null = this._next;
        do {
            if (next.start > end) {
                const swapNext = next;
                next = {
                    start: swapNext.start,
                    end: swapNext.end!,
                    next: swapNext.next!
                };
                swapNext.start = start;
                swapNext.end = end;
                swapNext.next = next;
                break;
            } else if (next.end === start) {
                next.end = end;
                if (next.end >= next.next.start) {
                    next.end = next.next.end!;
                    if (next.next.end == null) {
                        next.next = next.next.next!;
                        return next.start;
                    } else {
                        next.next = next.next.next!;
                    }
                }
                break;
            } else if (next.start === end) {
                next.start = start;
                if (next.end == null) {
                    return next.start;
                }
                break;
            }
            next = next.next;
        } while (next);
        return null;
    }
    getFreeBlocks() {
        const blocks:[start:number, end:number|null][] = [];
        let next:FreeBlock|null = this._next;
        do {
            blocks.push([next.start, next.end]);
            next = next.next;
        } while (next);
        return blocks;
    }
}