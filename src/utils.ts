import * as Fs from "fs";
import * as Crypto from "crypto";

type ReadContext = {
    readSync(fd:number, buffer:Buffer, offset:number, length:number, position:number):number;
}
function read(fd:number, buffer:Buffer, position:number, read0Error:boolean, context:ReadContext) {
    let offset = 0;
    do {
        const bytesRead = context.readSync(fd, buffer, offset, buffer.length - offset, position + offset);
        offset += bytesRead;
        if (bytesRead === 0) {
            if (read0Error || offset > 0) {
                // Partial read always fails
                throw new Error("Invalid file");
            }
            return true;
        }
    } while (offset !== buffer.length);
    return false;
}
function reader(fd:number, context:ReadContext) {
    let offset = 0;
    let done = false;
    return {
        get offset() {
            return offset;
        },
        advance(count:number) {
            offset += count;
        },
        read(buffer:Buffer, read0Error:boolean) {
            if (done) {
                return true;
            }
            try {
                const result = read(fd, buffer, offset, read0Error, context);
                offset += buffer.length;
                return result;
            } catch (e) {
                done = true;
                throw e;
            }
        }
    };
}
type FdActionContext = {
    writeSync(fd:number, buffer:Buffer, offset:number, length:number, position:number):void;
    fsyncSync(fd:number):void;
    ftruncateSync(fd:number, length:number):void;
} & ReadContext;
function fdAction(fd:number, context:FdActionContext) {
    return {
        reader() {
            return reader(fd, context);
        },
        write(buffer:Buffer, position:number) {
            context.writeSync(fd, buffer, 0, buffer.length, position);
        },
        read(buffer:Buffer, position:number, errorOnEOF:boolean) {
            return read(fd, buffer, position, errorOnEOF, context);
        },
        fsync() {
            context.fsyncSync(fd);
        },
        truncate(len:number) {
            context.ftruncateSync(fd, len);
        }
    };
}
export function shake128(buffer:Buffer, buffer2?:Buffer) {
    const hash = Crypto.createHash("shake128");
    hash.update(buffer);
    if (buffer2) {
        hash.update(buffer2);
    }
    return hash.digest();
}
export type OpenFilesContext = {
    openSync(path:Fs.PathLike, flags:Fs.OpenMode):number;
    closeSync(fd:number):void;
} & FdActionContext;
export function openFiles(options:{entriesFile:string; dataFile:string;}, context:OpenFilesContext) {
    let entriesFd;
    let dataFd;
    try {
        entriesFd = context.openSync(options.entriesFile, Fs.constants.O_CREAT | Fs.constants.O_RDWR);
        dataFd = context.openSync(options.dataFile, Fs.constants.O_CREAT | Fs.constants.O_RDWR);
    } catch (e) {
        if (entriesFd != null) {
            try {
                context.closeSync(entriesFd);
            } catch (e) {}
        }
        if (dataFd != null) {
            try {
                context.closeSync(dataFd);
            } catch (e) {}
        }
        console.error(e);
        // TODO: log invalid reloading persistency
        throw e;
    }
    return {
        close() {
            if (entriesFd != null) {
                try {
                    context.closeSync(entriesFd);
                } catch (e) {}
            }
            if (dataFd != null) {
                try {
                    context.closeSync(dataFd);
                } catch (e) {}
            }
        },
        entries: fdAction(entriesFd, context),
        data: fdAction(dataFd, context)
    };
}