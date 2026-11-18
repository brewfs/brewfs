# Per-directory TTL experiment for an immutable Pack lower

A Linux FUSE adapter exposes two directories from a read-only lower filesystem. A private Redis instance holds upper entries. Run the experiment with two directories in a mounted BrewFS `packed-snapshot` and a direct child file in each.

| Directory state | Directory entries and attributes | First upper creation |
| --- | --- | --- |
| Sealed | 1 hour, including missing names | Register each mount before it can issue a long-lived reply |
| Draining | 0 seconds for new replies | Wait for every registered mount to invalidate its cached entries and inodes |
| Writable | 1 second | Publish the upper entry and directory modification time together in Redis |

A mount records the names for which it issued long replies, including negative lookups. The first writer closes registration in a Redis transaction. Each reader stops issuing long replies, invalidates the affected names, inodes and parent directory in the Linux kernel, and then removes its registration. New mounts use a zero TTL while revocation is in progress. The writer publishes the upper entry after all acknowledgements. Subsequent creates in that directory use the short-TTL policy. Sibling directories keep their own grants.

The invalidation worker runs outside FUSE request handlers and waits for kernel acknowledgement. A paused reader holds up the first write. Retrying with the same operation and payload resumes the pending transition. The experiment uses a private Redis database and a single coordinator for each first create. Writes that bypass the coordinator can leave other mounts with a cached missing name. [FUSE invalidation API](https://pyfuse3.readthedocs.io/en/latest/fuse_api.html#pyfuse3.invalidate_entry)

## Run

Install `pyfuse3`, `redis` and `trio` in a Python environment on Linux with FUSE 3, `fusermount3` and `redis-server`.

```text
python exercise.py --changed /mnt/pack/small/d0000 --existing f0000000 --sibling /mnt/pack/small/d0001 --sibling-file f0001000
```

With no arguments, the program creates two temporary lower directories. Each run starts a private Redis server on a Unix socket and three independent FUSE mounts. It checks file reads and directory listings, positive and negative cache hits, publication with one paused reader, a late-joining reader, retry and competing-writer handling, sibling isolation, and visibility after a second write. A separate check writes directly to the private Redis upper and observes a previously cached missing name.

This prototype covers creating a new regular file through the coordinator. Its FUSE adapter reads existing lower files and upper entries. Integrating this policy with BrewFS workspace mutations also requires handling rename across parents, unlink, copy-up, permission changes, reader recovery and the cost of invalidating a large directory.
