import * as Assert from "assert";
import * as Fs from "fs";
import * as Path from "path";
import * as Os from "os";

import { After } from "arrange-act-assert";
import { OpenFilesContext } from "./utils";

const SPIED_OBJECT = Symbol();

// Force equal types
export function assertDeepEqual<T>(a:T, b:NoInfer<T>) {
    return Assert.deepStrictEqual(a, b);
}
export function assertEqual<T>(a:T, b:NoInfer<T>) {
    return Assert.strictEqual(a, b);
}

type SpyMethod<ARGS extends any[], RET> = {
    assert(calls:ARGS[]):void;
    splice():ARGS[];
    pushNextReturn(ret:RET):void;
    pushNextError(error:Error):void;
};
type SpyObjectProperty<T> = T extends (...args:infer ARGS)=>infer RET ? T & SpyMethod<ARGS, RET> : T extends object ? SpiedObject<T> : T;
type SpiedObject<T extends object> = {[K in keyof T]:SpyObjectProperty<T[K]>};
function spy<T extends object>(obj:T) {
    let proxy = SPIED_OBJECT in obj && obj[SPIED_OBJECT] as SpiedObject<T>;
    if (proxy) {
        // Avoid spying same object twice
        return proxy;
    }
    const proxies = new Map<string|symbol, (...args: any) => void>();
    proxy = new Proxy(obj, {
        has(target, p) {
            if (p === SPIED_OBJECT) {
                return true;
            }
            return p in target;
        },
        get(target, p) {
            if (p === SPIED_OBJECT) {
                return proxy;
            }
            const res = target[p as keyof T];
            if (typeof res === "function") {
                // Wrap function to catch calls and mock return value
                let proxy = proxies.get(p);
                if (proxy) {
                    return proxy;
                }
                const methodCalls:any[][] = [];
                const mockReturn:({value:any}|{error:Error})[] = [];
                proxy = new Proxy((...args:any[]) => {
                    methodCalls.push(args);
                    const next = mockReturn.shift();
                    if (next != null) {
                        if ("error" in next) {
                            throw next.error;
                        } else {
                            return next.value;
                        }
                    }
                    return res(...args);
                }, {
                    get(target, p) {
                        if (p === "assert") {
                            return (args:any[]) => {
                                assertDeepEqual(methodCalls.splice(0), args);
                            };
                        } else if (p === "pushNextReturn") {
                            return (value:any) => {
                                mockReturn.push({value});
                            };
                        } else if (p === "pushNextError") {
                            return (error:Error) => {
                                mockReturn.push({error});
                            };
                        } else if (p === "splice") {
                            return () => {
                                return methodCalls.splice(0);
                            };
                        } else {
                            return target[p as keyof typeof target];
                        }
                    }
                });
                proxies.set(p, proxy);
                return proxy;
            } else if (res != null && typeof res === "object") {
                // Recursive spy objects
                return spy(res);
            } else {
                return res;
            }
        }
    }) as SpiedObject<T>;
    return proxy;
}

export function newOpenFilesContext() {
    return spy(Fs as OpenFilesContext);
}

export async function tempFolder(after:After) {
    return after(await Fs.promises.mkdtemp(Path.join(Os.tmpdir(), "persistency-tests-")), folder => Fs.promises.rm(folder, { recursive: true, force: true }));
}