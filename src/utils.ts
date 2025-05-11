import * as Fs from "fs";
import * as Crypto from "crypto";

function read(fd:number, buffer:Buffer, position:number, emptyBufferError:boolean) {
    let offset = 0;
    do {
        const bytesRead = Fs.readSync(fd, buffer, offset, buffer.length - offset, position + offset);
        offset += bytesRead;
        if (bytesRead === 0) {
            if (emptyBufferError || offset > 0) {
                throw new Error("Invalid file");
            }
            return false;
        }
    } while (offset !== buffer.length);
    return true;
}
function reader(fd:number) {
    let offset = 0;
    let done = false;
    return {
        get offset() {
            return offset;
        },
        advance(count:number) {
            offset += count;
        },
        read(buffer:Buffer, emptyBufferError:boolean) {
            if (done) {
                return false;
            }
            try {
                const result = read(fd, buffer, offset, emptyBufferError);
                offset += buffer.length;
                return result;
            } catch (e) {
                done = true;
                throw e;
            }
        }
    };
}
export function sha256(buffer:Buffer, buffer2?:Buffer) {
    const hash = Crypto.createHash("sha256");
    hash.update(buffer);
    if (buffer2) {
        hash.update(buffer2);
    }
    return hash.digest();
}
export function fdAction(fd:number) {
    return {
        reader() {
            return reader(fd);
        },
        write(buffer:Buffer, position:number) {
            Fs.writeSync(fd, buffer, 0, buffer.length, position);
        },
        read(buffer:Buffer, position:number, errorOnEOF:boolean) {
            return read(fd, buffer, position, errorOnEOF);
        },
        fsync() {
            Fs.fsyncSync(fd);
        },
        truncate(len:number) {
            Fs.ftruncateSync(fd, len);
        }
    };
}
export function openFiles(options:{entriesFile:string; dataFile:string;}) {
    let entriesFd;
    let dataFd;
    try {
        entriesFd = Fs.openSync(options.entriesFile, Fs.constants.O_CREAT | Fs.constants.O_RDWR);
        dataFd = Fs.openSync(options.dataFile, Fs.constants.O_CREAT | Fs.constants.O_RDWR);
    } catch (e) {
        if (entriesFd != null) {
            try {
                Fs.closeSync(entriesFd);
            } catch (e) {}
        }
        if (dataFd != null) {
            try {
                Fs.closeSync(dataFd);
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
                    Fs.closeSync(entriesFd);
                } catch (e) {}
            }
            if (dataFd != null) {
                try {
                    Fs.closeSync(dataFd);
                } catch (e) {}
            }
        },
        entries: fdAction(entriesFd),
        data: fdAction(dataFd)
    };
}