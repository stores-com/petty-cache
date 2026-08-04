const timers = require('node:timers/promises');
const util = require('node:util');

const async = require('async');
const lock = require('lock').Lock();
const memoryCache = require('memory-cache');
const redis = require('redis');

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
    const getAsync = (...args) => util.promisify(redisClient.get).apply(redisClient, args);
    const mgetAsync = (...args) => util.promisify(redisClient.mget).apply(redisClient, args);
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

        if (Object.prototype.hasOwnProperty.call(options, 'ttl')) {
            if (typeof options.ttl === 'number') {
                ttl.max = options.ttl;
                ttl.min = options.ttl;
            } else {
                if (Object.prototype.hasOwnProperty.call(options.ttl, 'max')) {
                    ttl.max = options.ttl.max;
                }

                if (Object.prototype.hasOwnProperty.call(options.ttl, 'min')) {
                    ttl.min = options.ttl.min;
                }
            }
        }

        return ttl;
    }

    /**
     * Returns data from cache for each key if available; otherwise executes func for the missing keys
     * and stores the results in cache before returning. Supports both callback and promise styles.
     * @param {Array} keys - An array of cache keys.
     * @param {Function} func - Function called with missing keys and a callback: (keys, callback).
     * @param {Object} [options] - Optional settings.
     * @param {number|Object} [options.ttl] - TTL in ms, or object with min/max properties.
     * @param {Function} [callback] - Optional callback(err, values). If omitted, returns a Promise.
     * @returns {Promise|undefined} Resolves with an object mapping each key to its cached value.
     */
    this.bulkFetch = (keys, func, options = {}, callback) => {
        if (typeof options === 'function') {
            callback = options;
            options = {};
        }

        const executor = async () => {
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
            const data = await new Promise((resolve, reject) => {
                func(_keys, (err, data) => {
                    if (err) {
                        return reject(err);
                    }

                    resolve(data);
                });
            });

            Object.keys(data).forEach(key => values[key] = data[key]);

            await this.bulkSet(data, options);

            return values;
        };

        if (callback) {
            executor().then(result => callback(null, result)).catch(callback);
        } else {
            return executor();
        }
    };

    /**
     * Gets cached values for an array of keys. Supports both callback and promise styles.
     * @param {Array} keys - An array of cache keys.
     * @param {Function} [callback] - Optional callback(err, values). If omitted, returns a Promise.
     * @returns {Promise|undefined} Resolves with an object mapping each key to its value, or null if not found.
     */
    this.bulkGet = (keys, callback) => {
        const executor = async () => {
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

        if (callback) {
            executor().then(result => callback(null, result)).catch(callback);
        } else {
            return executor();
        }
    };

    /**
     * Sets multiple key/value pairs in cache simultaneously. Supports both callback and promise styles.
     * @param {Object} values - An object mapping cache keys to their values.
     * @param {Object} [options] - Optional settings.
     * @param {number|Object} [options.ttl] - TTL in ms, or object with min/max properties.
     * @param {Function} [callback] - Optional callback(err). If omitted, returns a Promise.
     * @returns {Promise|undefined}
     */
    this.bulkSet = (values, options = {}, callback) => {
        if (typeof options === 'function') {
            callback = options;
            options = {};
        }

        const executor = () => {
            return new Promise((resolve, reject) => {
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

                batch.exec((err) => {
                    if (err) {
                        return reject(err);
                    }

                    resolve();
                });
            });
        };

        if (callback) {
            executor().then(result => callback(null, result)).catch(callback);
        } else {
            return executor();
        }
    };

    /**
     * Deletes a key from both the memory cache and Redis. Supports both callback and promise styles.
     * @param {string} key - The cache key to delete.
     * @param {Function} [callback] - Optional callback(err). If omitted, returns a Promise.
     * @returns {Promise|undefined}
     */
    this.del = (key, callback) => {
        const executor = () => {
            return new Promise((resolve, reject) => {
                redisClient.del(key, (err) => {
                    if (err) {
                        return reject(err);
                    }

                    memoryCache.del(key);
                    resolve();
                });
            });
        };

        if (callback) {
            executor().then(result => callback(null, result)).catch(callback);
        } else {
            return executor();
        }
    };

    /**
     * Returns data from cache if available; otherwise executes func, stores the result, and returns it.
     * Uses double-checked locking to prevent cache stampedes. Supports async and callback func signatures,
     * and both callback and promise styles.
     * @param {string} key - The cache key.
     * @param {Function} func - Called on cache miss. Use func(callback) for callbacks or async func() for promises.
     * @param {Object} [options] - Optional settings.
     * @param {number|Object} [options.ttl] - TTL in ms, or object with min/max properties.
     * @param {Function} [callback] - Optional callback(err, value). If omitted, returns a Promise.
     * @returns {Promise|undefined} Resolves with the cached or newly fetched value.
     */
    this.fetch = (key, func, options = {}, callback) => {
        if (typeof options === 'function') {
            callback = options;
            options = {};
        }

        const executor = async () => {
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
                    const data = await executeFunc(func);

                    await this.set(key, data, options);

                    return data;
                } finally {
                    releaseRedisLock();
                }
            } finally {
                releaseMemoryCacheLock();
            }
        };

        if (callback) {
            executor().then(result => callback(null, result)).catch(callback);
        } else {
            return executor();
        }
    };

    /**
     * Like fetch(), but also sets up a background interval to proactively refresh the cached value
     * before it expires, preventing cache misses under sustained load. Supports async and callback
     * func signatures, and both callback and promise styles.
     * @param {string} key - The cache key.
     * @param {Function} func - Called on cache miss and on each refresh interval. Use func(callback) for callbacks or async func() for promises.
     * @param {Object} [options] - Optional settings.
     * @param {number|Object} [options.ttl] - TTL in ms, or object with min/max properties.
     * @param {Function} [callback] - Optional callback(err, value). If omitted, returns a Promise.
     * @returns {Promise|undefined} Resolves with the cached or newly fetched value.
     */
    this.fetchAndRefresh = (key, func, options = {}, callback) => {
        if (typeof options === 'function') {
            callback = options;
            options = {};
        }

        // Get TTL based on specified options
        const ttl = getTtl(options);

        const _this = this;

        if (!intervals[key]) {
            const delay = ttl.min / 2;

            intervals[key] = setInterval(async () => {
                // This distributed lock prevents multiple clients from executing func at the same time
                try {
                    await _this.mutex.lock(`interval-${key}`, { ttl: delay - 100 });
                } catch (err) {
                    return;
                }

                // Execute the specified function and update cache, trying again next interval on failure
                try {
                    const data = await executeFunc(func);

                    await _this.set(key, data, options);
                } catch (err) {
                    return;
                }
            }, delay);
        }

        return this.fetch(key, func, options, callback);
    };

    /**
     * Gets a cached value. Supports both callback and promise styles.
     * @param {string} key - The cache key.
     * @param {Function} [callback] - Optional callback(err, value). If omitted, returns a Promise.
     * @returns {Promise|undefined} Resolves with the cached value, or null if not found.
     */
    this.get = (key, callback) => {
        const executor = async () => {
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

        if (callback) {
            executor().then(result => callback(null, result)).catch(callback);
        } else {
            return executor();
        }
    };

    this.mutex = {
        /**
         * Acquires a distributed mutex lock in Redis. Supports both callback and promise styles.
         * @param {string} key - The lock key.
         * @param {Object} [options] - Optional settings.
         * @param {number} [options.ttl=1000] - Lock TTL in ms.
         * @param {Object} [options.retry] - Retry options.
         * @param {number} [options.retry.times=1] - Number of acquisition attempts.
         * @param {number} [options.retry.interval=100] - Delay between retries in ms.
         * @param {Function} [callback] - Optional callback(err). If omitted, returns a Promise.
         * @returns {Promise|undefined}
         */
        lock: (key, options = {}, callback) => {
            // Options are optional
            if (!callback && typeof options === 'function') {
                callback = options;
                options = {};
            }

            options.retry = Object.hasOwn(options, 'retry') ? options.retry : {};
            options.retry.interval = Object.hasOwn(options.retry, 'interval') ? options.retry.interval : 100;
            options.retry.times = Object.hasOwn(options.retry, 'times') ? options.retry.times : 1;
            options.ttl = Object.hasOwn(options, 'ttl') ? options.ttl : 1000;

            const executor = () => {
                return new Promise((resolve, reject) => {
                    async.retry({ interval: options.retry.interval, times: options.retry.times }, callback => {
                        redisClient.set(key, '1', 'NX', 'PX', options.ttl, (err, res) => {
                            if (err) {
                                return callback(err);
                            }

                            if (!res) {
                                return callback(new Error());
                            }

                            if (res !== 'OK') {
                                return callback(new Error(res));
                            }

                            callback();
                        });
                    }, (err) => {
                        if (err) {
                            return reject(err);
                        }

                        resolve();
                    });
                });
            };

            if (callback) {
                executor().then(result => callback(null, result)).catch(callback);
            } else {
                return executor();
            }
        },
        /**
         * Releases a distributed mutex lock in Redis. Supports both callback and promise styles.
         * @param {string} key - The lock key to release.
         * @param {Function} [callback] - Optional callback(err). If omitted, returns a Promise.
         * @returns {Promise|undefined}
         */
        unlock: (key, callback) => {
            const executor = () => {
                return new Promise((resolve, reject) => {
                    redisClient.del(key, (err) => {
                        if (err) {
                            return reject(err);
                        }

                        resolve();
                    });
                });
            };

            if (callback) {
                executor().then(result => callback(null, result)).catch(callback);
            } else {
                return executor();
            }
        }
    };

    /**
     * Updates specific properties of a cached object without replacing the whole value.
     * Supports both callback and promise styles.
     * @param {string} key - The cache key of the object to patch.
     * @param {Object} value - Properties to merge into the cached object.
     * @param {Object} [options] - Optional settings passed to set().
     * @param {number|Object} [options.ttl] - TTL in ms, or object with min/max properties.
     * @param {Function} [callback] - Optional callback(err). If omitted, returns a Promise.
     * @returns {Promise|undefined}
     */
    this.patch = (key, value, options = {}, callback) => {
        if (!callback && typeof options === 'function') {
            callback = options;
            options = {};
        }

        const _this = this;

        const executor = () => {
            return new Promise((resolve, reject) => {
                _this.get(key, (err, data) => {
                    if (err) {
                        return reject(err);
                    }

                    if (!data) {
                        return reject(new Error(`Key ${key} does not exist`));
                    }

                    for (let k in value) {
                        data[k] = value[k];
                    }

                    _this.set(key, data, options, (err) => {
                        if (err) {
                            return reject(err);
                        }

                        resolve();
                    });
                });
            });
        };

        if (callback) {
            executor().then(result => callback(null, result)).catch(callback);
        } else {
            return executor();
        }
    };

    this.semaphore = {
        /**
         * Acquires a slot in an existing semaphore pool. Retries if no slot is currently available.
         * Supports both callback and promise styles.
         * @param {string} key - The semaphore key.
         * @param {Object} [options] - Optional settings.
         * @param {number} [options.ttl=1000] - Slot TTL in ms; expired slots may be reclaimed.
         * @param {Object} [options.retry] - Retry options.
         * @param {number} [options.retry.times=1] - Number of acquisition attempts.
         * @param {number} [options.retry.interval=100] - Delay between retries in ms.
         * @param {Function} [callback] - Optional callback(err, index). If omitted, returns a Promise.
         * @returns {Promise|undefined} Resolves with the acquired slot index.
         */
        acquireLock: (key, options = {}, callback) => {
            // Options are optional
            if (!callback && typeof options === 'function') {
                callback = options;
                options = {};
            }

            options.retry = Object.prototype.hasOwnProperty.call(options, 'retry') ? options.retry : {};
            options.retry.interval = Object.prototype.hasOwnProperty.call(options.retry, 'interval') ? options.retry.interval : 100;
            options.retry.times = Object.prototype.hasOwnProperty.call(options.retry, 'times') ? options.retry.times : 1;
            options.ttl = Object.prototype.hasOwnProperty.call(options, 'ttl') ? options.ttl : 1000;

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

            const executor = async () => {
                for (let attempts = options.retry.times; ; attempts--) {
                    try {
                        return await attempt();
                    } catch (err) {
                        if (attempts <= 1) {
                            throw err;
                        }

                        await timers.setTimeout(options.retry.interval);
                    }
                }
            };

            if (callback) {
                executor().then(result => callback(null, result)).catch(callback);
            } else {
                return executor();
            }
        },
        /**
         * Permanently consumes a semaphore slot, marking it consumed rather than available.
         * Ensures at least one slot always remains non-consumed. Supports both callback and promise styles.
         * @param {string} key - The semaphore key.
         * @param {number} index - The slot index to consume.
         * @param {Function} [callback] - Optional callback(err). If omitted, returns a Promise.
         * @returns {Promise|undefined}
         */
        consumeLock: (key, index, callback) => {
            const executor = async () => {
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
            };

            if (callback) {
                executor().then(result => callback(null, result)).catch(callback);
            } else {
                return executor();
            }
        },
        /**
         * Increases the size of an existing semaphore pool. Cannot shrink a pool.
         * Supports both callback and promise styles.
         * @param {string} key - The semaphore key.
         * @param {number} size - The desired pool size (must be >= current size).
         * @param {Function} [callback] - Optional callback(err). If omitted, returns a Promise.
         * @returns {Promise|undefined}
         */
        expand: (key, size, callback) => {
            const executor = async () => {
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
            };

            if (callback) {
                executor().then(result => callback(null, result)).catch(callback);
            } else {
                return executor();
            }
        },
        /**
         * Releases an acquired semaphore slot, marking it available again.
         * Supports both callback and promise styles.
         * @param {string} key - The semaphore key.
         * @param {number} index - The slot index to release.
         * @param {Function} [callback] - Optional callback(err). If omitted, returns a Promise.
         * @returns {Promise|undefined}
         */
        releaseLock: (key, index, callback) => {
            const executor = async () => {
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
            };

            if (callback) {
                executor().then(result => callback(null, result)).catch(callback);
            } else {
                return executor();
            }
        },
        /**
         * Resets all slots in an existing semaphore pool to available.
         * Supports both callback and promise styles.
         * @param {string} key - The semaphore key.
         * @param {Function} [callback] - Optional callback(err, pool). If omitted, returns a Promise.
         * @returns {Promise|undefined} Resolves with the reset pool.
         */
        reset: (key, callback) => {
            const executor = async () => {
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
            };

            if (callback) {
                executor().then(result => callback(null, result)).catch(callback);
            } else {
                return executor();
            }
        },
        /**
         * Retrieves an existing semaphore pool, or creates one if it doesn't exist.
         * Supports both callback and promise styles.
         * @param {string} key - The semaphore key.
         * @param {Object} [options] - Optional settings.
         * @param {number|Function} [options.size=1] - Pool size, or a function that resolves the size. Use size(callback) for callbacks or async size() for promises.
         * @param {Function} [callback] - Optional callback(err, pool). If omitted, returns a Promise.
         * @returns {Promise|undefined} Resolves with the semaphore pool.
         */
        retrieveOrCreate: (key, options = {}, callback) => {
            // Options are optional
            if (!callback && typeof options === 'function') {
                callback = options;
                options = {};
            }

            const executor = async () => {
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
                        size = await executeFunc(options.size);
                    } else {
                        size = Object.prototype.hasOwnProperty.call(options, 'size') ? options.size : 1;
                    }

                    const pool = Array(Math.max(size, 1)).fill({ status: 'available' });

                    await setAsync(key, JSON.stringify(pool));

                    return pool;
                } finally {
                    // Unlock errors are ignored; the mutex lock expires via its TTL
                    await this.mutex.unlock(`lock:${key}`).catch(() => {});
                }
            };

            if (callback) {
                executor().then(result => callback(null, result)).catch(callback);
            } else {
                return executor();
            }
        }
    };

    /**
     * Stores a value in both the memory cache and Redis. Supports both callback and promise styles.
     * @param {string} key - The cache key.
     * @param {*} value - The value to cache.
     * @param {Object} [options] - Optional settings.
     * @param {number|Object} [options.ttl] - TTL in ms, or object with min/max properties.
     * @param {Function} [callback] - Optional callback(err). If omitted, returns a Promise.
     * @returns {Promise|undefined}
     */
    this.set = (key, value, options = {}, callback) => {
        if (typeof options === 'function') {
            callback = options;
            options = {};
        }

        // Get TTL based on specified options
        const ttl = getTtl(options);

        const executor = () => {
            return new Promise((resolve, reject) => {
                // Store value in memory cache with a short expiration
                memoryCache.put(key, value, random(2000, 5000));

                // Store value in Redis
                redisClient.psetex(key, random(ttl.min, ttl.max), PettyCache.stringify(value), (err) => {
                    if (err) {
                        return reject(err);
                    }

                    resolve();
                });
            });
        };

        if (callback) {
            executor().then(result => callback(null, result)).catch(callback);
        } else {
            return executor();
        }
    };

}

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
 * Executes a cache-miss function, supporting both async and callback signatures.
 * @param {Function} func - Use func(callback) for callbacks or async func() for promises.
 * @returns {Promise<*>} Resolves with the value produced by func.
 */
async function executeFunc(func) {
    // If the function doesn't have any arguments, there wasn't a callback provided
    if (func.length === 0) {
        return func();
    }

    // If the function has arguments, there was a callback provided
    return new Promise((resolve, reject) => {
        func((err, data) => {
            if (err) {
                return reject(err);
            }

            resolve(data);
        });
    });
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
