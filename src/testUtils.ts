import * as Assert from "assert";

// Force equal types
export function assertDeepEqual<T>(a:T, b:NoInfer<T>) {
    return Assert.deepStrictEqual(a, b);
}
export function assertEqual<T>(a:T, b:NoInfer<T>) {
    return Assert.strictEqual(a, b);
}