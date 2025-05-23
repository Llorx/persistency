type BlockRange<T> = {
    first:Block<T>;
    last:Block<T>;
    prev:BlockRange<T>|null;
    next:BlockRange<T>|null;
};
export type Block<T> = {
    start:number;
    end:number;
    prev:Block<T>|null;
    next:Block<T>|null;
    data:T;
};
export class MemoryBlocks<T = any> {
    private _firstRange:BlockRange<T>|null = null;
    private _lastBlock:Block<T>|null = null;
    constructor(readonly offset:number) {}
    private _mergeRange(range:BlockRange<T>) {
        range.last = range.next!.last;
        range.next = range.next!.next;
        if (range.next) {
            range.next.prev = range;
        }
    }
    private _splitRange(range:BlockRange<T>, currentLast:Block<T>, nextFirst:Block<T>) {
        range.next = {
            first: nextFirst,
            last: range.last,
            prev: range,
            next: range.next
        };
        range.last = currentLast;
        if (range.next.next) {
            range.next.next.prev = range.next;
        }
    }
    getLastBlock() {
        return this._lastBlock;
    }
    setAllocation() {
        // Assume updating in ascending order, so "next" is always going to be the last block
        let next:BlockRange<T>|null = null;
        return {
            add: (start:number, end:number, data:T) => {
                if (!next) {
                    const block:Block<T> = {
                        start: start,
                        end: end,
                        prev: null,
                        next: null,
                        data: data
                    };
                    next = this._firstRange = {
                        first: block,
                        last: block,
                        prev: null,
                        next: null
                    };
                    this._lastBlock = block;
                    return block;
                }
                const block:Block<T> = {
                    start: start,
                    end: end,
                    prev: next.last,
                    next: null,
                    data: data
                };
                next.last.next = block;
                if (next.last.end === start) {
                    next.last = block;
                } else {
                    next.next = {
                        first: block,
                        last: block,
                        prev: next,
                        next: null
                    };
                    next = next.next;
                }
                this._lastBlock = block;
                return block;
            }
        };
    }
    alloc(size:number, data:T) {
        if (!this._firstRange) {
            // First alloc
            const block:Block<T> = {
                start: this.offset,
                end: this.offset + size,
                prev: null,
                next: null,
                data: data
            };
            this._firstRange = {
                first: block,
                last: block,
                prev: null,
                next: null
            };
            this._lastBlock = block;
            return block;
        }
        if (this._firstRange.first.start - this.offset >= size) {
            // Alloc before first block but after offset
            const block:Block<T> = {
                start: this.offset,
                end: this.offset + size,
                prev: null,
                next: this._firstRange.first,
                data: data
            };
            this._firstRange.first.prev = block;
            if (block.end === this._firstRange.first.start) {
                this._firstRange.first = block;
            } else {
                this._firstRange.prev = {
                    first: block,
                    last: block,
                    prev: this._firstRange.prev,
                    next: this._firstRange
                };
                this._firstRange = this._firstRange.prev;
            }
            return block;
        }
        let range:BlockRange<T>|null = this._firstRange;
        do {
            if (!range.next) {
                // If it is the last range, extend the end
                const block:Block<T> = {
                    start: range.last.end,
                    end: range.last.end + size,
                    prev: range.last,
                    next: null,
                    data: data
                };
                range.last.next = block;
                this._lastBlock = block;
                return range.last = block;
            } else {
                if (range.next.first.start - range.last.end >= size) { // If there's enough space before the next range
                    // extend the range end
                    const block:Block<T> = {
                        start: range.last.end,
                        end: range.last.end + size,
                        prev: range.last,
                        next: range.next.first,
                        data: data
                    };
                    range.last.next = block;
                    range.next.first.prev = block;
                    range.last = block;
                    // If it fills the gap perfectly, merge the ranges to a single range
                    if (range.last.end === range.next.first.start) {
                        this._mergeRange(range);
                    }
                    return block;
                }
            }
            range = range.next;
        } while(range);
        throw new Error("Free data location not found"); // Never should happen
    }
    free(block:Block<T>) {
        let range:BlockRange<T>|null = this._firstRange!;
        do {
            if (range.last.end > block.start) {
                if (range.first === block) {
                    if (range.last === block) {
                        // Remove range
                        if (range.prev) {
                            range.prev.next = range.next;
                        } else {
                            // If not previous range, it is first range
                            this._firstRange = range.next;
                        }
                        if (range.next) {
                            range.next.prev = range.prev;
                        }
                    } else {
                        // shrink range start
                        range.first = block.next!;
                    }
                } else if (range.last === block) {
                    // shrink range end
                    range.last = block.prev!;
                } else {
                    this._splitRange(range, block.prev!, block.next!);
                }
                break;
            }
            range = range.next;
        } while (range);
        // Remove block
        if (block.prev) {
            block.prev.next = block.next;
        }
        if (block.next) {
            block.next.prev = block.prev;
            return false;
        } else {
            this._lastBlock = block.prev;
            return true;
        }
    }
    allocAfter(block:Block<T>, size:number, data:T) {
        const newBlock:Block<T> = {
            start: block.end,
            end: block.end + size,
            prev: block,
            next: block.next,
            data: data
        };
        block.next = newBlock;
        if (newBlock.next) {
            newBlock.next.prev = newBlock;
        }
        let range:BlockRange<T>|null = this._firstRange!;
        do {
            if (range.last === block) {
                range.last = newBlock;
                if (range.next && range.next.first.start === range.last.end) {
                    this._mergeRange(range);
                }
                break;
            }
            range = range.next;
        } while (range);
        return newBlock;
    }
    getFreeBlocks() {
        const blocks:{
            location:number;
            space:number;
            block:Block<T>;
        }[] = [];
        let maxSpace = 0;
        let range = this._firstRange;
        if (range) {
            if (range.first.start > this.offset) {
                const space = range.first.start - this.offset;
                maxSpace = space;
                blocks.push({
                    location: this.offset,
                    space: space,
                    block: range.first
                });
            }
            range = range.next;
        }
        while (range) {
            const space = range.first.start - range.first.prev!.end;
            if (space > maxSpace) {
                maxSpace = space;
            }
            blocks.push({
                location: range.first.prev!.end,
                space: space,
                block: range.first
            });
            range = range.next;
        }
        return { maxSpace, blocks };
    }
    getAllocatedRanges() {
        const blocks:[start:number, end:number][] = [];
        let range = this._firstRange;
        if (this.offset > 0) {
            if (range && range.first.start === this.offset) {
                blocks.push([ 0, range.last.end ]);
                range = range.next;
            } else {
                blocks.push([ 0, this.offset ]);
            }
        }
        while (range) {
            blocks.push([ range.first.start, range.last.end ]);
            range = range.next;
        }
        return blocks;
    }
}