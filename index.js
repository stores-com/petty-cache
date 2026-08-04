const timers = require('node:timers/promises');
const util = require('node:util');

const lock = require('lock').Lock();
const memoryCache = require('memory-cache');
const redis = require('redis');

/**
 * Acquires the in-process lock for the given key and resolves once it's held.
 * @param {string} key
 * @returns {Promise<Function>} Resolves with a function that releases the lock.
 */
function acquireLock(key) {
    return new Promise(resolve => {
        lock(key, release => resolve(release()));
    });
}

/**
 * Throws if any of the given arguments is a function, catching legacy callback-style calls.
 * @param {string} name - The public method name shown in the error.
 * @param {...*} args - Arguments beyond the method's supported signature.
 */
function assertNoCallback(name, ...args) {
    if (args.some(arg => typeof arg === 'function')) {
        throw new TypeError(`${name}: callbacks were removed in petty-cache v5. Use the returned promise instead.`);
    }
}

/**
 * Returns a random integer between min and max, inclusive.
 * @param {number} min
 * @param {number} max
 * @returns {number}
 */
function random(min, max) {
    if (min === max) {
        return min;
    }

    return Math.floor(Math.random() * (max - min + 1) + min);
}

/**
 * Creates a new PettyCache instance backed by Redis.
 * Accepts the same arguments as redis.createClient(), or an existing RedisClient instance.
 * @param {...*} args - Either a RedisClient instance, or arguments forwarded to redis.createClient().
 */
function PettyCache() {
    const intervals = {};
    let redisClient;

    if (arguments[0] instanceof redis.RedisClient) {
        redisClient = arguments[0];
    } else {
        redisClient = redis.createClient(...arguments);
    }

    //eslint-disable-next-line no-console
    redisClient.on('error', err => console.warn(`Warning: Redis reported a client error: ${err}`));

    // Promisify per call rather than once at construction so that wrappers applied to the
    // client's methods later (APM instrumentation, test stubs) are respected
    const delAsync = (...args) => util.promisify(redisClient.del).apply(redisClient, args);
    const getAsync = (...args) => util.promisify(redisClient.get).apply(redisClient, args);
    const mgetAsync = (...args) => util.promisify(redisClient.mget).apply(redisClient, args);
    const psetexAsync = (...args) => util.promisify(redisClient.psetex).apply(redisClient, args);
    const setAsync = (...args) => util.promisify(redisClient.set).apply(redisClient, args);

    /**
     * Fetches multiple keys from Redis.
     * @param {string[]} keys
     * @returns {Promise<Object>} Resolves with an object mapping each key to {exists, value}.
     */
    async function bulkGetFromRedis(keys) {
        // Try to get values from Redis
        const data = await mgetAsync(keys);

        const values = {};

        for (let i = 0; i < keys.length; i++) {
            const key = keys[i];
            const value = data[i];

            if (value === null) {
                values[key] = { exists: false };
                continue;
            }

            values[key] = { exists: true, value: PettyCache.parse(value) };
        }

        return values;
    }

    /**
     * Fetches a single key from the in-process memory cache.
     * @param {string} key
     * @returns {{exists: boolean, value: *}}
     */
    function getFromMemoryCache(key) {
        // Try to get value from memory cache
        const value = memoryCache.get(key);

        // Return value from the memory cache if it's not null
        if (value !== null) {
            return { exists: true, value };
        }

        // If the key exists, the value in the memory cache is null
        if (memoryCache.keys().includes(key)) {
            return { exists: true, value: null };
        }

        // The key wasn't found in memory cache
        return { exists: false };
    }

    /**
     * Fetches a single key from Redis.
     * @param {string} key
     * @returns {Promise<{exists: boolean, value: *}>}
     */
    async function getFromRedis(key) {
        // Try to get value from Redis
        const data = await getAsync(key);

        // Return if the key wasn't found in Redis
        if (data === null) {
            return { exists: false };
        }

        return { exists: true, value: PettyCache.parse(data) };
    }

    /**
     * Resolves TTL options into a {min, max} object in milliseconds. Defaults to 30–60 seconds.
     * @param {Object} options
     * @param {number|Object} [options.ttl] - Fixed ms value, or an object with min/max properties.
     * @returns {{min: number, max: number}}
     */
    function getTtl(options) {
        // Default TTL is 30-60 seconds
        const ttl = {
            max: 60000,
            min: 30000
        };

        if (Object.hasOwn(options, 'ttl')) {
            if (typeof options.ttl === 'number') {
                ttl.max = options.ttl;
                ttl.min = options.ttl;
            } else {
                if (Object.hasOwn(options.ttl, 'max')) {
                    ttl.max = options.ttl.max;
                }

                if (Object.hasOwn(options.ttl, 'min')) {
                    ttl.min = options.ttl.min;
                }
            }
        }

        return ttl;
    }

    /**
     * Returns data from cache for each key if available; otherwise executes func for the missing keys
     * and stores the results in cache before returning.
     * @param {Array} keys - An array of cache keys.
     * @param {Function} func - Called with the missing keys: async func(keys).
     * @param {Object} [options] - Optional settings.
     * @param {number|Object} [options.ttl] - TTL in ms, or object with min/max properties.
     * @returns {Promise<Object>} Resolves with an object mapping each key to its cached value.
     */
    this.bulkFetch = async (keys, func, options = {}, ...rest) => {
        assertNoCallback('pettyCache.bulkFetch', options, ...rest);

        // If there aren't any keys, return
        if (!keys.length) {
            return {};
        }

        const _keys = Array.from(new Set(keys));
        const values = {};

        // Try to get values from memory cache
        for (let i = _keys.length - 1; i >= 0; i--) {
            const key = _keys[i];
            const result = getFromMemoryCache(key);

            if (result.exists) {
                values[key] = result.value;
                _keys.splice(i, 1);
            }
        }

        // If there aren't any keys left, return
        if (!_keys.length) {
            return values;
        }

        // Try to get values from Redis
        const results = await bulkGetFromRedis(_keys);

        for (let i = _keys.length - 1; i >= 0; i--) {
            const key = _keys[i];
            const result = results[key];

            if (result.exists) {
                _keys.splice(i, 1);
                values[key] = result.value;

                // Store value in memory cache with a short expiration
                memoryCache.put(key, result.value, random(2000, 5000));
            }
        }

        // If there aren't any keys left, return
        if (!_keys.length) {
            return values;
        }

        // Execute the specified function for remaining keys
        const data = await func(_keys);

        Object.keys(data).forEach(key => values[key] = data[key]);

        await this.bulkSet(data, options);

        return values;
    };

    /**
     * Gets cached values for an array of keys.
     * @param {Array} keys - An array of cache keys.
     * @returns {Promise<Object>} Resolves with an object mapping each key to its value, or null if not found.
     */
    this.bulkGet = async (keys, ...rest) => {
        assertNoCallback('pettyCache.bulkGet', ...rest);

        // If there aren't any keys, return
        if (!keys.length) {
            return {};
        }

        const _keys = Array.from(new Set(keys));
        const values = {};

        // Try to get values from memory cache
        for (let i = _keys.length - 1; i >= 0; i--) {
            const key = _keys[i];
            const result = getFromMemoryCache(key);

            if (result.exists) {
                values[key] = result.value;
                _keys.splice(i, 1);
            }
        }

        // If there aren't any keys left, return
        if (!_keys.length) {
            return values;
        }

        // Try to get values from Redis
        const results = await bulkGetFromRedis(_keys);

        for (let i = 0; i < _keys.length; i++) {
            const key = _keys[i];
            const result = results[key];

            if (!result.exists) {
                values[key] = null;
                continue;
            }

            values[key] = result.value;

            // Store value in memory cache with a short expiration
            memoryCache.put(key, result.value, random(2000, 5000));
        }

        return values;
    };

    /**
     * Sets multiple key/value pairs in cache simultaneously.
     * @param {Object} values - An object mapping cache keys to their values.
     * @param {Object} [options] - Optional settings.
     * @param {number|Object} [options.ttl] - TTL in ms, or object with min/max properties.
     * @returns {Promise}
     */
    this.bulkSet = async (values, options = {}, ...rest) => {
        assertNoCallback('pettyCache.bulkSet', options, ...rest);

        // Get TTL based on specified options
        const ttl = getTtl(options);

        // Redis does not have a MSETEX command so we batch commands: http://redis.js.org/#api-clientbatchcommands
        const batch = redisClient.batch();

        Object.keys(values).forEach(key => {
            const value = values[key];

            // Store value in memory cache with a short expiration
            memoryCache.put(key, value, random(2000, 5000));

            // Add Redis command
            batch.psetex(key, random(ttl.min, ttl.max), PettyCache.stringify(value));
        });

        await util.promisify(batch.exec).call(batch);
    };

    /**
     * Deletes a key from both the memory cache and Redis.
     * @param {string} key - The cache key to delete.
     * @returns {Promise}
     */
    this.del = async (key, ...rest) => {
        assertNoCallback('pettyCache.del', ...rest);

        await delAsync(key);
        memoryCache.del(key);
    };

    /**
     * Returns data from cache if available; otherwise executes func, stores the result, and returns it.
     * Uses double-checked locking to prevent cache stampedes.
     * @param {string} key - The cache key.
     * @param {Function} func - Called on cache miss: async func().
     * @param {Object} [options] - Optional settings.
     * @param {number|Object} [options.ttl] - TTL in ms, or object with min/max properties.
     * @returns {Promise<*>} Resolves with the cached or newly fetched value.
     */
    this.fetch = async (key, func, options = {}, ...rest) => {
        assertNoCallback('pettyCache.fetch', options, ...rest);

        // Try to get value from memory cache
        let result = getFromMemoryCache(key);

        // Return value from memory cache if it exists
        if (result.exists) {
            return result.value;
        }

        // Double-checked locking: http://en.wikipedia.org/wiki/Double-checked_locking
        const releaseMemoryCacheLock = await acquireLock(`fetch-memory-cache-lock-${key}`);

        try {
            // Try to get value from memory cache
            result = getFromMemoryCache(key);

            // Return value from memory cache if it exists
            if (result.exists) {
                return result.value;
            }

            // Try to get value from Redis
            result = await getFromRedis(key);

            // Return value from Redis if it exists
            if (result.exists) {
                memoryCache.put(key, result.value, random(2000, 5000));
                return result.value;
            }

            // Double-checked locking: http://en.wikipedia.org/wiki/Double-checked_locking
            const releaseRedisLock = await acquireLock(`fetch-redis-lock-${key}`);

            try {
                // Try to get value from memory cache
                result = getFromMemoryCache(key);

                // Return value from memory cache if it exists
                if (result.exists) {
                    return result.value;
                }

                // Try to get value from Redis
                result = await getFromRedis(key);

                // Return value from Redis if it exists
                if (result.exists) {
                    memoryCache.put(key, result.value, random(2000, 5000));
                    return result.value;
                }

                // Execute the specified function and place the results in cache before returning the data
                const data = await func();

                await this.set(key, data, options);

                return data;
            } finally {
                releaseRedisLock();
            }
        } finally {
            releaseMemoryCacheLock();
        }
    };

    /**
     * Like fetch(), but also sets up a background interval to proactively refresh the cached value
     * before it expires, preventing cache misses under sustained load.
     * @param {string} key - The cache key.
     * @param {Function} func - Called on cache miss and on each refresh interval: async func().
     * @param {Object} [options] - Optional settings.
     * @param {number|Object} [options.ttl] - TTL in ms, or object with min/max properties.
     * @returns {Promise<*>} Resolves with the cached or newly fetched value.
     */
    this.fetchAndRefresh = async (key, func, options = {}, ...rest) => {
        assertNoCallback('pettyCache.fetchAndRefresh', options, ...rest);

        // Get TTL based on specified options
        const ttl = getTtl(options);

        if (!intervals[key]) {
            const delay = ttl.min / 2;

            intervals[key] = setInterval(async () => {
                // This distributed lock prevents multiple clients from executing func at the same time
                try {
                    await this.mutex.lock(`interval-${key}`, { ttl: delay - 100 });
                } catch (err) {
                    return;
                }

                // Execute the specified function and update cache, trying again next interval on failure
                try {
                    const data = await func();

                    await this.set(key, data, options);
                } catch (err) {
                    return;
                }
            }, delay);
        }

        return this.fetch(key, func, options);
    };

    /**
     * Gets a cached value.
     * @param {string} key - The cache key.
     * @returns {Promise<*>} Resolves with the cached value, or null if not found.
     */
    this.get = async (key, ...rest) => {
        assertNoCallback('pettyCache.get', ...rest);

        // Try to get value from memory cache
        let result = getFromMemoryCache(key);

        // Return value from memory cache if it exists
        if (result.exists) {
            return result.value;
        }

        // Double-checked locking: http://en.wikipedia.org/wiki/Double-checked_locking
        const releaseMemoryCacheLock = await acquireLock(`get-memory-cache-lock-${key}`);

        try {
            // Try to get value from memory cache
            result = getFromMemoryCache(key);

            // Return value from memory cache if it exists
            if (result.exists) {
                return result.value;
            }

            // Try to get value from Redis
            result = await getFromRedis(key);

            // Return null if the key wasn't found in Redis
            if (!result.exists) {
                return null;
            }

            // Store value in memory cache with a short expiration
            memoryCache.put(key, result.value, random(2000, 5000));

            return result.value;
        } finally {
            releaseMemoryCacheLock();
        }
    };

    this.mutex = {
        /**
         * Acquires a distributed mutex lock in Redis.
         * @param {string} key - The lock key.
         * @param {Object} [options] - Optional settings.
         * @param {number} [options.ttl=1000] - Lock TTL in ms.
         * @param {Object} [options.retry] - Retry options.
         * @param {number} [options.retry.times=1] - Number of acquisition attempts.
         * @param {number} [options.retry.interval=100] - Delay between retries in ms.
         * @returns {Promise}
         */
        lock: async (key, options = {}, ...rest) => {
            assertNoCallback('pettyCache.mutex.lock', options, ...rest);

            options.retry = Object.hasOwn(options, 'retry') ? options.retry : {};
            options.retry.interval = Object.hasOwn(options.retry, 'interval') ? options.retry.interval : 100;
            options.retry.times = Object.hasOwn(options.retry, 'times') ? options.retry.times : 1;
            options.ttl = Object.hasOwn(options, 'ttl') ? options.ttl : 1000;

            const attempt = async () => {
                const res = await setAsync(key, '1', 'NX', 'PX', options.ttl);

                if (!res) {
                    throw new Error();
                }

                if (res !== 'OK') {
                    throw new Error(res);
                }
            };

            let attempts = options.retry.times;

            while (attempts > 1) {
                try {
                    return await attempt();
                } catch (err) {
                    attempts--;
                    await timers.setTimeout(options.retry.interval);
                }
            }

            return attempt();
        },
        /**
         * Releases a distributed mutex lock in Redis.
         * @param {string} key - The lock key to release.
         * @returns {Promise}
         */
        unlock: async (key, ...rest) => {
            assertNoCallback('pettyCache.mutex.unlock', ...rest);

            await delAsync(key);
        }
    };

    /**
     * Updates specific properties of a cached object without replacing the whole value.
     * @param {string} key - The cache key of the object to patch.
     * @param {Object} value - Properties to merge into the cached object.
     * @param {Object} [options] - Optional settings passed to set().
     * @param {number|Object} [options.ttl] - TTL in ms, or object with min/max properties.
     * @returns {Promise}
     */
    this.patch = async (key, value, options = {}, ...rest) => {
        assertNoCallback('pettyCache.patch', options, ...rest);

        const data = await this.get(key);

        if (!data) {
            throw new Error(`Key ${key} does not exist`);
        }

        for (let k in value) {
            data[k] = value[k];
        }

        await this.set(key, data, options);
    };

    this.semaphore = {
        /**
         * Acquires a slot in an existing semaphore pool. Retries if no slot is currently available.
         * @param {string} key - The semaphore key.
         * @param {Object} [options] - Optional settings.
         * @param {number} [options.ttl=1000] - Slot TTL in ms; expired slots may be reclaimed.
         * @param {Object} [options.retry] - Retry options.
         * @param {number} [options.retry.times=1] - Number of acquisition attempts.
         * @param {number} [options.retry.interval=100] - Delay between retries in ms.
         * @returns {Promise<number>} Resolves with the acquired slot index.
         */
        acquireLock: async (key, options = {}, ...rest) => {
            assertNoCallback('pettyCache.semaphore.acquireLock', options, ...rest);

            options.retry = Object.hasOwn(options, 'retry') ? options.retry : {};
            options.retry.interval = Object.hasOwn(options.retry, 'interval') ? options.retry.interval : 100;
            options.retry.times = Object.hasOwn(options.retry, 'times') ? options.retry.times : 1;
            options.ttl = Object.hasOwn(options, 'ttl') ? options.ttl : 1000;

            const attempt = async () => {
                // Mutex lock around semaphore
                await this.mutex.lock(`lock:${key}`, { retry: { times: 100 } });

                try {
                    const data = await getAsync(key);

                    // If we don't have a previously created semaphore, return error
                    if (!data) {
                        throw new Error(`Semaphore ${key} doesn't exist.`);
                    }

                    const pool = JSON.parse(data);

                    // Try to find a slot that's available.
                    let index = pool.findIndex(s => s.status === 'available');

                    if (index === -1) {
                        index = pool.findIndex(s => s.ttl <= Date.now());
                    }

                    // If we don't have an available slot, return error
                    if (index === -1) {
                        throw new Error(`Semaphore ${key} doesn't have any available slots.`);
                    }

                    pool[index] = { status: 'acquired', ttl: Date.now() + options.ttl };

                    await setAsync(key, JSON.stringify(pool));

                    return index;
                } finally {
                    // Unlock errors are ignored; the mutex lock expires via its TTL
                    await this.mutex.unlock(`lock:${key}`).catch(() => {});
                }
            };

            let attempts = options.retry.times;

            while (attempts > 1) {
                try {
                    return await attempt();
                } catch (err) {
                    attempts--;
                    await timers.setTimeout(options.retry.interval);
                }
            }

            return attempt();
        },
        /**
         * Permanently consumes a semaphore slot, marking it consumed rather than available.
         * Ensures at least one slot always remains non-consumed.
         * @param {string} key - The semaphore key.
         * @param {number} index - The slot index to consume.
         * @returns {Promise}
         */
        consumeLock: async (key, index, ...rest) => {
            assertNoCallback('pettyCache.semaphore.consumeLock', ...rest);

            // Mutex lock around semaphore
            await this.mutex.lock(`lock:${key}`, { retry: { times: 100 } });

            try {
                const data = await getAsync(key);

                // If we don't have a previously created semaphore, return error
                if (!data) {
                    throw new Error(`Semaphore ${key} doesn't exist.`);
                }

                const pool = JSON.parse(data);

                // Ensure index exists.
                if (pool.length <= index) {
                    throw new Error(`Index ${index} for semaphore ${key} is invalid.`);
                }

                pool[index] = { status: 'consumed' };

                // Ensure at least one slot isn't consumed
                if (pool.every(s => s.status === 'consumed')) {
                    pool[index] = { status: 'available' };
                }

                await setAsync(key, JSON.stringify(pool));
            } finally {
                // Unlock errors are ignored; the mutex lock expires via its TTL
                await this.mutex.unlock(`lock:${key}`).catch(() => {});
            }
        },
        /**
         * Increases the size of an existing semaphore pool. Cannot shrink a pool.
         * @param {string} key - The semaphore key.
         * @param {number} size - The desired pool size (must be >= current size).
         * @returns {Promise}
         */
        expand: async (key, size, ...rest) => {
            assertNoCallback('pettyCache.semaphore.expand', ...rest);

            // Mutex lock around semaphore
            await this.mutex.lock(`lock:${key}`, { retry: { times: 100 } });

            try {
                const data = await getAsync(key);

                // If we don't have a previously created semaphore, return error
                if (!data) {
                    throw new Error(`Semaphore ${key} doesn't exist.`);
                }

                let pool = JSON.parse(data);

                if (pool.length > size) {
                    throw new Error(`Cannot shrink pool, size is ${pool.length} and you requested a size of ${size}.`);
                }

                if (pool.length === size) {
                    return;
                }

                pool = pool.concat(Array(size - pool.length).fill({ status: 'available' }));

                await setAsync(key, JSON.stringify(pool));
            } finally {
                // Unlock errors are ignored; the mutex lock expires via its TTL
                await this.mutex.unlock(`lock:${key}`).catch(() => {});
            }
        },
        /**
         * Releases an acquired semaphore slot, marking it available again.
         * @param {string} key - The semaphore key.
         * @param {number} index - The slot index to release.
         * @returns {Promise}
         */
        releaseLock: async (key, index, ...rest) => {
            assertNoCallback('pettyCache.semaphore.releaseLock', ...rest);

            // Mutex lock around semaphore
            await this.mutex.lock(`lock:${key}`, { retry: { times: 100 } });

            try {
                const data = await getAsync(key);

                // If we don't have a previously created semaphore, return error
                if (!data) {
                    throw new Error(`Semaphore ${key} doesn't exist.`);
                }

                const pool = JSON.parse(data);

                // Ensure index exists.
                if (pool.length <= index) {
                    throw new Error(`Index ${index} for semaphore ${key} is invalid.`);
                }

                pool[index] = { status: 'available' };

                await setAsync(key, JSON.stringify(pool));
            } finally {
                // Unlock errors are ignored; the mutex lock expires via its TTL
                await this.mutex.unlock(`lock:${key}`).catch(() => {});
            }
        },
        /**
         * Resets all slots in an existing semaphore pool to available.
         * @param {string} key - The semaphore key.
         * @returns {Promise<Array>} Resolves with the reset pool.
         */
        reset: async (key, ...rest) => {
            assertNoCallback('pettyCache.semaphore.reset', ...rest);

            // Mutex lock around semaphore
            await this.mutex.lock(`lock:${key}`, { retry: { times: 100 } });

            try {
                // Try to get previously created semaphore
                const data = await getAsync(key);

                // If we don't have a previously created semaphore, return error
                if (!data) {
                    throw new Error(`Semaphore ${key} doesn't exist.`);
                }

                let pool = JSON.parse(data);
                pool = Array(pool.length).fill({ status: 'available' });

                await setAsync(key, JSON.stringify(pool));

                return pool;
            } finally {
                // Unlock errors are ignored; the mutex lock expires via its TTL
                await this.mutex.unlock(`lock:${key}`).catch(() => {});
            }
        },
        /**
         * Retrieves an existing semaphore pool, or creates one if it doesn't exist.
         * @param {string} key - The semaphore key.
         * @param {Object} [options] - Optional settings.
         * @param {number|Function} [options.size=1] - Pool size, or an async function that resolves the size.
         * @returns {Promise<Array>} Resolves with the semaphore pool.
         */
        retrieveOrCreate: async (key, options = {}, ...rest) => {
            assertNoCallback('pettyCache.semaphore.retrieveOrCreate', options, ...rest);

            // Mutex lock around semaphore retrival or creation
            await this.mutex.lock(`lock:${key}`, { retry: { times: 100 } });

            try {
                // Try to get previously created semaphore
                const data = await getAsync(key);

                // If we retreived a previously created semaphore, return it
                if (data) {
                    return JSON.parse(data);
                }

                let size;

                if (typeof options.size === 'function') {
                    size = await options.size();
                } else {
                    size = Object.hasOwn(options, 'size') ? options.size : 1;
                }

                const pool = Array(Math.max(size, 1)).fill({ status: 'available' });

                await setAsync(key, JSON.stringify(pool));

                return pool;
            } finally {
                // Unlock errors are ignored; the mutex lock expires via its TTL
                await this.mutex.unlock(`lock:${key}`).catch(() => {});
            }
        }
    };

    /**
     * Stores a value in both the memory cache and Redis.
     * @param {string} key - The cache key.
     * @param {*} value - The value to cache.
     * @param {Object} [options] - Optional settings.
     * @param {number|Object} [options.ttl] - TTL in ms, or object with min/max properties.
     * @returns {Promise}
     */
    this.set = async (key, value, options = {}, ...rest) => {
        assertNoCallback('pettyCache.set', options, ...rest);

        // Get TTL based on specified options
        const ttl = getTtl(options);

        // Store value in memory cache with a short expiration
        memoryCache.put(key, value, random(2000, 5000));

        // Store value in Redis
        await psetexAsync(key, random(ttl.min, ttl.max), PettyCache.stringify(value));
    };
}

/**
 * Parses a JSON string produced by PettyCache.stringify(), restoring NaN, null, and undefined.
 * @param {string} text
 * @returns {*}
 */
PettyCache.parse = (text) => {
    return JSON.parse(text, (k, v) => {
        if (v === '__NaN') {
            return NaN;
        } else if (v === '__null') {
            return null;
        } else if (v === '__undefined') {
            return undefined;
        }

        return v;
    });
};

/**
 * Serializes a value to JSON, encoding NaN, null, and undefined as sentinel strings
 * so they survive a Redis round-trip and can be restored by PettyCache.parse().
 * @param {*} value
 * @returns {string}
 */
PettyCache.stringify = (value) => {
    return JSON.stringify(value, (k, v) => {
        if (typeof v === 'number' && isNaN(v)) {
            return '__NaN';
        } else if (v === null) {
            return '__null';
        } else if (v === undefined) {
            return '__undefined';
        }

        return v;
    });
};

module.exports = PettyCache;
