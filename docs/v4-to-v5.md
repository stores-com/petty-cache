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

- **Positional constructor arguments keep working.** `new PettyCache(port, host, options)` is translated to node-redis options. No change needed for this form.
- **Common v3 options are translated.** `auth_pass`, `connect_timeout`, `db`, `enable_offline_queue`, `family`, `host`, `path`, `port`, `socket_initial_delay`, `socket_keepalive`, and `tls` are translated to their node-redis v6 equivalents, whether passed positionally or as an options object. Options already in the v6 shape pass through untouched.
- **Options with no v6 equivalent throw.** `detect_buffers`, `prefix`, `rename_commands`, `retry_strategy`, `return_buffers`, and `string_numbers` throw a `TypeError` rather than being silently ignored. Reimplement those with node-redis v6 options (e.g. `retry_strategy` becomes `socket.reconnectStrategy`, which receives the retry count and returns a delay in milliseconds).
- **Injected clients must be node-redis v6 clients.** `new PettyCache(redisClient)` requires a client created by `redis@6`'s `createClient()`. petty-cache connects it if it isn't already connected.

## New: close()

v5 adds a shutdown API. `await pettyCache.close()` stops the background refresh intervals started by `fetchAndRefresh` and gracefully closes the Redis client connection.
