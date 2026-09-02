# Migrating from v4 to v5

v5 removes callback support entirely and upgrades the underlying Redis client from node-redis v3 to v6. v4 supports both callbacks and promises and emits a `DeprecationWarning` for every callback-style usage — migrate to v4 first, clear the warnings, and the jump to v5 is a version bump.

## Finding callback usage

On v4, each callback-style call emits a once-per-process `DeprecationWarning` naming the function. Run your service with `--trace-deprecation` to get a stack trace pointing at each call site:

```
node --trace-deprecation index.js
```

On v5, any remaining callback usage rejects with a `TypeError` instead of silently doing nothing:

```
TypeError: pettyCache.get: callbacks were removed in petty-cache v5. Use the returned promise instead.
```

## Method callbacks → promises

Every function returns a promise. The final-callback parameter is gone from every signature.

**Before**

```javascript
pettyCache.get('key', function(err, value) {
    if (err) {
        // Handle error
    }

    console.log(value);
});
```

**After**

```javascript
const value = await pettyCache.get('key');
```

Error handling moves from the err-first parameter to `try`/`catch` (or `.catch`):

```javascript
try {
    await pettyCache.patch('key', { a: 1 });
} catch (err) {
    // Handle error (e.g. the key does not exist)
}
```

## Fire-and-forget calls

Calls that previously omitted the callback to ignore the result now return a promise. An ignored rejected promise crashes modern Node.js processes, so handle it explicitly:

**Before**

```javascript
pettyCache.semaphore.reset(key);
```

**After**

```javascript
pettyCache.semaphore.reset(key).catch(() => {});
```

Prefer `await` where the calling code can be async.

## Cache-miss functions must be async

Callback-style cache-miss functions are no longer supported by `fetch`, `fetchAndRefresh`, and `bulkFetch`, and neither are callback-style `size` functions for `semaphore.retrieveOrCreate`. Return the value (or a promise) instead of calling a callback:

**Before**

```javascript
pettyCache.fetch('key', function(callback) {
    fs.readFile('file.txt', callback);
}, function(err, value) {
    console.log(value);
});

pettyCache.bulkFetch(['a', 'b'], function(keys, callback) {
    callback(null, { a: 1, b: 2 });
}, function(err, values) {
    console.log(values);
});
```

**After**

```javascript
const value = await pettyCache.fetch('key', async () => {
    return await fs.readFile('file.txt');
});

const values = await pettyCache.bulkFetch(['a', 'b'], async (keys) => {
    return { a: 1, b: 2 };
});
```

Plain-return functions (`() => value`) also work anywhere an async function is accepted.

## Redis client v3 → v6

petty-cache now uses [node-redis](https://www.npmjs.com/package/redis) v6 and connects the client automatically.

**The constructor takes a node-redis v6 options object.** The v4 `(port, host, options)` signature is gone, and so is the translation of v3 option names.

**Before**

```javascript
const pettyCache = new PettyCache(process.env.redisPort, process.env.redisHost, { auth_pass: process.env.redisPassword, enable_offline_queue: false });
```

**After**

```javascript
const pettyCache = new PettyCache({ disableOfflineQueue: true, password: process.env.redisPassword, socket: { host: process.env.redisHost, port: process.env.redisPort } });
```

Both old forms throw a `TypeError` that names what to change:

```
TypeError: petty-cache v5 takes a node-redis options object. The (port, host, options)
signature was removed; see docs/v4-to-v5.md.

TypeError: petty-cache v5 takes a node-redis options object. auth_pass, host, port are
node-redis v3 options and would be ignored; see docs/v4-to-v5.md.
```

They throw rather than translate deliberately. v6 ignores unknown top-level options, so a v3 options object silently yields an unauthenticated client pointed at localhost:6379 — a cache that never hits and never errors. Failing at construction is the whole point of the breaking change.

Option names that moved:

| v3 | v6 |
| --- | --- |
| `auth_pass` | `password` |
| `host`, `port`, `path` | `socket.host`, `socket.port`, `socket.path` |
| `db` | `database` |
| `enable_offline_queue: false` | `disableOfflineQueue: true` |
| `connect_timeout` | `socket.connectTimeout` |
| `socket_keepalive`, `socket_initial_delay` | `socket.keepAlive`, `socket.keepAliveInitialDelay` |
| `family` | `socket.family` (a number, not `'IPv4'`) |
| `tls: {...}` | `socket.tls: true` plus the TLS options on `socket` |
| `retry_strategy` | `socket.reconnectStrategy` |

**Injected clients must be node-redis v6 clients.** `new PettyCache(redisClient)` requires a client created by `redis@6`'s `createClient()`. petty-cache connects it if it isn't already connected.

## New: close()

v5 adds a shutdown API. `await pettyCache.close()` stops the background refresh intervals started by `fetchAndRefresh` and gracefully closes the Redis client connection.
