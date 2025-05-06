# persistency
Zero-dependency resilient key-value database.

# Motivation
I just wanted a way to store resilient data without installing native libraries, so I designed a library that reduces the amount of data loss after a power outage.

To achieve resiliency in the event of a crash or power outage, this library applies:
- [Copy-on-write](https://en.wikipedia.org/wiki/Copy-on-write): Each data update is written in a different sector instead of overwriting the previous data, so if the crash happens during this process, the old data is kept intact.
- Data hashing: Everything is hashed with [Sha-256](https://en.wikipedia.org/wiki/SHA-2) to verify that the data loaded is exactly the same as the data saved.
- Data versioning: Everything is versioned to ensure that the latest updated entry is loaded over the oldest ones.
- [fsync](https://man7.org/linux/man-pages/man2/fsync.2.html): Everything is flushed to the disk after a modification, to increase the certainty that data has been persisted to disk.
- Delayed empty space reclamation: To avoid growing the data infinitely, the library will reclaim old data and overwrite it with new data, but after sending the data to the disk by using [fsync](https://man7.org/linux/man-pages/man2/fsync.2.html), the disk could still have an internal cache that can lead to data loss in the case of a power outage. Not having any feedback mechanism for the device to notify us about this cache flush, the only way that we can *ensure* data persistency to the disk is with *time*. After a section of data is freed to reuse, the library will wait a configurable amount of time before reclaiming the data for overwriting.

With all these measures, the data integrity and consistency is guaranteed.

Also, the only cached information in memory is the metadata of the entries. The data is loaded from the disk on access, so you can store more data than RAM available.

# Documentation
The tool is pretty straightforward:
```typescript
import { Persistency } from "persistency";

const persistency = new Persistency({
    folder: "/path/to/data/folder",
    reclaimTimeout: 10000 // Milliseconds. Optional. 10 seconds by default
});

// The library works with buffers, so you can use any serialization library, like pacopack
const buffer1 = Buffer.from(JSON.stringify({ data: 1 }));
persistency.set("myKey", buffer1); // set data

const buffer2 = Buffer.from(JSON.stringify({ data: 2 }));
persistency.set("myKey", buffer2); // update data

const gotBuffer = persistency.get("myKey"); // get latest data. Returns null if no data is found
if (gotBuffer != null) {
    console.log("myKey is:", JSON.parse(String(gotBuffer)));
}

persistency.delete("myKey"); // Delete data. Returns true if found
```