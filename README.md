# petty-cache

[![Build Status](https://github.com/stores-com/petty-cache/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/stores-com/petty-cache/actions?query=workflow%3Abuild+branch%3Amain)
[![Coverage Status](https://coveralls.io/repos/github/stores-com/petty-cache/badge.svg?branch=main&t=Pc1x8G)](https://coveralls.io/github/stores-com/petty-cache?branch=main)
[![npm version](https://img.shields.io/npm/v/petty-cache)](https://www.npmjs.com/package/petty-cache)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A cache module for Node.js that uses a two-level cache (in-memory cache for recently accessed data plus Redis for distributed caching) with automatic serialization plus some extra features to avoid cache stampedes and thundering herds.

Also includes mutex and semaphore distributed locking primitives.

As of v5, every function returns a promise and callbacks are no longer supported — passing a callback rejects with a `TypeError`. Cache-miss functions must be async (or plain-return) functions. If you need callback support, use v4, which supports both styles and emits deprecation warnings for callback usage. See the [v4 to v5 migration guide](docs/v4-to-v5.md).

## Features

**Two-level cache**
Data is cached for 2 to 5 seconds in memory to reduce the amount of calls to Redis.

**Jitter**
By default, cache values expire from Redis at a random time between 30 and 60 seconds. This helps to prevent a large amount of keys from expiring at the same time in order to avoid thundering herds (http://en.wikipedia.org/wiki/Thundering_herd_problem).

**Double-checked locking**
Functions executed on cache misses are wrapped in double-checked locking (http://en.wikipedia.org/wiki/Double-checked_locking). This ensures the function called on cache miss will only be executed once in order to prevent cache stampedes (http://en.wikipedia.org/wiki/Cache_stampede).

**Mutex**
Provides a distributed lock (mutex) with the ability to retry a specified number of times after a specified interval of time when acquiring a lock.

**Semaphore**
Provides a pool of distributed locks with the ability to release a slot back to the pool or remove the slot from the pool so that it's not used again.

## Getting Started

```javascript
// Setup petty-cache
const PettyCache = require('petty-cache');
const pettyCache = new PettyCache();

// Fetch some data
const value = await pettyCache.fetch('key', async () => {
    // This function is called on a cache miss
    return await fs.readFile('file.txt');
});
```

## API

### new PettyCache([options])

Creates a new petty-cache client backed by [node-redis](https://www.npmjs.com/package/redis) v6 and connects it automatically. `options` is passed to [redis.createClient()](https://www.npmjs.com/package/redis) untouched.

The v4 `(port, host, options)` signature and node-redis v3 option names were removed in v5. Both throw a `TypeError` naming what to change rather than being translated, because a mistranslated option silently produces an unauthenticated client pointed at localhost.

**Example**
```javascript
const pettyCache = new PettyCache({ password: 'secret', socket: { host: 'localhost', port: 6379 } });
```

### new PettyCache(RedisClient)

Alternatively, you can inject your own node-redis v6 client into Petty Cache. If the client isn't already connected, petty-cache connects it.

**Example**
```javascript
const redisClient = redis.createClient();
const pettyCache = new PettyCache(redisClient);
```

### pettyCache.bulkFetch(keys, cacheMissFunction, [options])

Attempts to retrieve the values of the keys specified in the `keys` array. Any keys that aren't found are passed to cacheMissFunction as an array. `cacheMissFunction` should retrieve the expected values for the missing keys from another source and return an object, expecting the keys of the object to be the keys passed to `cacheMissFunction` and the values to be the values that should be stored in cache for the corresponding key. Resolves with a key-value hash of all requested keys.

**Example**

```javascript
// Let's assume a and b are already cached as 1 and 2
const values = await pettyCache.bulkFetch(['a', 'b', 'c', 'd'], async (keys) => {
    const results = {};

    keys.forEach(function(key) {
        results[key] = key.toUpperCase();
    });

    return results;
});

console.log(values); // {a: 1, b: 2, c: 'C', d: 'D'}
```

**Options**

```
{
    ttl: 30000 // How long it should take for the cache entry to expire in milliseconds. Defaults to a random value between 30000 and 60000 (for jitter).
}
```

```
{
    // TTL can optional be specified with a range to pick a random value between `min` and `max` (for jitter).
    ttl: {
        min: 5000,
        max: 10000
    }
}
```

### pettyCache.bulkGet(keys)

Attempts to retrieve the values of the keys specified in the `keys` array. Resolves with a key-value hash of all specified keys with either the corresponding values from cache or `null` if a key was not found.

**Example**

```javascript
const values = await pettyCache.bulkGet(['key1', 'key2', 'key3']);
```

### pettyCache.bulkSet(values, [options])

Unconditionally sets the values for the specified keys.

**Example**

```javascript
await pettyCache.bulkSet({ key1: 'one', key2: 2, key3: 'three' });
```

**Options**

```
{
    ttl: 30000 // How long it should take for the cache entries to expire in milliseconds. Defaults to a random value between 30000 and 60000 (for jitter).
}
```

```
{
    // TTL can optional be specified with a range to pick a random value between `min` and `max` (for jitter).
    ttl: {
        min: 5000,
        max: 10000
    }
}
```

### pettyCache.close()

Stops the background refresh intervals started by `pettyCache.fetchAndRefresh` and gracefully closes the Redis client connection.

**Example**

```javascript
await pettyCache.close();
```

### pettyCache.del(key)

Deletes a value from both the in-memory cache and Redis.

**Example**

```javascript
await pettyCache.del('key');
```

### pettyCache.fetch(key, cacheMissFunction, [options])

Attempts to retrieve the value from cache at the specified key. If it doesn't exist, it executes the specified cacheMissFunction, which should retrieve the expected value for the key from another source and return it. Either way, resolves with the resulting value.

**Example**

```javascript
const value = await pettyCache.fetch('key', async () => {
    // This function is called on a cache miss
    return await fs.readFile('file.txt');
});
```

**Options**

```
{
    ttl: 30000 // How long it should take for the cache entry to expire in milliseconds. Defaults to a random value between 30000 and 60000 (for jitter).
}
```

```
{
    // TTL can optional be specified with a range to pick a random value between `min` and `max` (for jitter).
    ttl: {
        min: 5000,
        max: 10000
    }
}
```

### pettyCache.fetchAndRefresh(key, cacheMissFunction, [options])

Similar to `pettyCache.fetch` but this method continually refreshes the data in cache by executing the specified cacheMissFunction before the TTL expires.

**Example**

```javascript
const value = await pettyCache.fetchAndRefresh('key', async () => {
    // This function is called on a cache miss and every TTL/2 milliseconds
    return await fs.readFile('file.txt');
});
```

**Options**

```
{
    ttl: 30000 // How long it should take for the cache entry to expire in milliseconds. Defaults to a random value between 30000 and 60000 (for jitter).
}
```

```
{
    // TTL can optional be specified with a range to pick a random value between `min` and `max` (for jitter).
    ttl: {
        min: 5000,
        max: 10000
    }
}
```

### pettyCache.get(key)

Attempts to retrieve the value from cache at the specified key. Resolves with `null` if the key doesn't exist.

**Example**

```javascript
const value = await pettyCache.get('key');
```

### pettyCache.patch(key, value, [options])

Updates an object at the given key with the property values provided. Rejects if the key does not exist.

**Example**

```javascript
await pettyCache.patch('key', { a: 1 });

// The object stored at 'key' now has a property 'a' with the value 1. Its other values are intact.
```

**Options**

```
{
    ttl: 30000 // How long it should take for the cache entry to expire in milliseconds. Defaults to a random value between 30000 and 60000 (for jitter).
}
```

```
{
    // TTL can optional be specified with a range to pick a random value between `min` and `max` (for jitter).
    ttl: {
        min: 5000,
        max: 10000
    }
}
```

### pettyCache.set(key, value, [options])

Unconditionally sets a value for a given key.

**Example**

```javascript
await pettyCache.set('key', { a: 'b' });
```

**Options**

```
{
    ttl: 30000 // How long it should take for the cache entry to expire in milliseconds. Defaults to a random value between 30000 and 60000 (for jitter).
}
```

```
{
    // TTL can optional be specified with a range to pick a random value between `min` and `max` (for jitter).
    ttl: {
        min: 5000,
        max: 10000
    }
}
```

### PettyCache.parse(text)

Parses a JSON string produced by `PettyCache.stringify()`, restoring `NaN`, `null`, and `undefined` values. This is the deserializer petty-cache uses for values coming back from Redis.

```javascript
const value = PettyCache.parse('{"a":"__null"}'); // { a: null }
```

### PettyCache.stringify(value)

Serializes a value to JSON, encoding `NaN`, `null`, and `undefined` as sentinel strings so they survive the round trip to Redis and back through `PettyCache.parse()`.

```javascript
const text = PettyCache.stringify({ a: null }); // '{"a":"__null"}'
```

## Mutex

### pettyCache.mutex.lock(key, [options])

Attempts to acquire a distributed lock for the specified key. Optionally retries a specified number of times by waiting a specified amount of time between attempts.

```javascript
await pettyCache.mutex.lock('key', { retry: { interval: 100, times: 5 }, ttl: 1000 });

// We were able to acquire the lock. Do work and then unlock.
await pettyCache.mutex.unlock('key');
```

**Options**

```javascript
{
    retry: {
        interval: 100, // The time in milliseconds between attempts to acquire the lock.
        times: 1 // The number of attempts to acquire the lock.
    },
    ttl: 1000 // The maximum amount of time to keep the lock locked before automatically being unlocked.
}
```

### pettyCache.mutex.unlock(key)

Releases the distributed lock for the specified key.

```javascript
await pettyCache.mutex.unlock('key');
```

## Semaphore

Provides a pool of distributed locks. Once a consumer acquires a lock they have the ability to release the lock back to the pool or mark the lock as "consumed" so that it's not used again.

**Example**

```javascript
// Create a new semaphore
await pettyCache.semaphore.retrieveOrCreate('key', { size: 10 });

// Acquire a lock from the semaphore's pool
const index = await pettyCache.semaphore.acquireLock('key', { retry: { interval: 100, times: 5 }, ttl: 1000 });

// We were able to acquire a lock from the semaphore's pool. Do work and then release the lock.
await pettyCache.semaphore.releaseLock('key', index);

// Or, rather than releasing the lock back to the semaphore's pool you can mark the lock as "consumed" to prevent it from being used again.
await pettyCache.semaphore.consumeLock('key', index);
```

### pettyCache.semaphore.acquireLock(key, [options])

Attempts to acquire a lock from the semaphore's pool. Optionally retries a specified number of times by waiting a specified amount of time between attempts. Resolves with the index of the acquired slot.

```javascript
const index = await pettyCache.semaphore.acquireLock('key', { retry: { interval: 100, times: 5 }, ttl: 1000 });
```

**Options**

```javascript
{
    retry: {
        interval: 100, // The time in milliseconds between attempts to acquire the lock.
        times: 1 // The number of attempts to acquire the lock.
    },
    ttl: 1000 // The maximum amount of time to keep the lock locked before automatically being unlocked.
}
```

### pettyCache.semaphore.consumeLock(key, index)

Mark the lock at the specified index as "consumed" to prevent it from being used again.

```javascript
await pettyCache.semaphore.consumeLock('key', index);
```

### pettyCache.semaphore.expand(key, size)

Expand the number of locks in the specified semaphore's pool.

```javascript
await pettyCache.semaphore.expand(key, 100);
```

### pettyCache.semaphore.releaseLock(key, index)

Releases the lock at the specified index back to the semaphore's pool so that it can be used again.

```javascript
await pettyCache.semaphore.releaseLock('key', index);
```

### pettyCache.semaphore.reset(key)

Resets all locks in the semaphore's pool to available, releasing them all (even those that have been marked as "consumed"). The pool keeps its current size, including any expansions. Resolves with the reset pool.

```javascript
const pool = await pettyCache.semaphore.reset('key');
```

### pettyCache.semaphore.retrieveOrCreate(key, [options])

Retrieves a previously created semaphore or creates a new semaphore with the optionally specified number of locks in its pool. Resolves with the semaphore's pool.

```javascript
const semaphore = await pettyCache.semaphore.retrieveOrCreate('key', { size: 10 });
```

**Options**

```javascript
{
    size: 1 // The number of locks to create in the semaphore's pool. Optionally, size can be an async function that resolves the size.
}
```
