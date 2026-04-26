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

    /**
     * Fetches multiple keys from Redis.
     * @param {string[]} keys
     * @param {Function} callback - callback(err, values) where values maps each key to {exists, value}.
     */
    function bulkGetFromRedis(keys, callback) {
        // Try to get values from Redis
        redisClient.mget(keys, (err, data) => {
            if (err) {
                return callback(err);
            }

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

            callback(null, values);
        });
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
     * @param {Function} callback - callback(err, {exists, value}).
     */
    function getFromRedis(key, callback) {
        // Try to get value from Redis
        redisClient.get(key, (err, data) => {
            if (err) {
                return callback(err);
            }

            // Return if the key wasn't found in Redis
            if (data === null) {
                return callback(null, { exists: false });
            }

            callback(null, { exists: true, value: PettyCache.parse(data) });
        });
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

        const executor = () => {
            return new Promise((resolve, reject) => {
                // If there aren't any keys, return
                if (!keys.length) {
                    return resolve({});
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
                    return resolve(values);
                }

                // Try to get values from Redis
                bulkGetFromRedis(_keys, (err, results) => {
                    if (err) {
                        return reject(err);
                    }

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
                        return resolve(values);
                    }

                    // Execute the specified function for remaining keys
                    func(_keys, (err, data) => {
                        if (err) {
                            return reject(err);
                        }

                        Object.keys(data).forEach(key => values[key] = data[key]);

                        this.bulkSet(data, options, err => {
                            if (err) {
                                return reject(err);
                            }

                            resolve(values);
                        });
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

    /**
     * Gets cached values for an array of keys. Supports both callback and promise styles.
     * @param {Array} keys - An array of cache keys.
     * @param {Function} [callback] - Optional callback(err, values). If omitted, returns a Promise.
     * @returns {Promise|undefined} Resolves with an object mapping each key to its value, or null if not found.
     */
    this.bulkGet = (keys, callback) => {
        const executor = () => {
            return new Promise((resolve, reject) => {
                // If there aren't any keys, return
                if (!keys.length) {
                    return resolve({});
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
                    return resolve(values);
                }

                // Try to get values from Redis
                bulkGetFromRedis(_keys, (err, results) => {
                    if (err) {
                        return reject(err);
                    }

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

                    resolve(values);
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
     * Uses double-checked locking to prevent cache stampedes. Supports async and callback func signatures.
     * @param {string} key - The cache key.
     * @param {Function} func - Called on cache miss. Use func(callback) for callbacks or async func() for promises.
     * @param {Object} [options] - Optional settings.
     * @param {number|Object} [options.ttl] - TTL in ms, or object with min/max properties.
     * @param {Function} [callback] - Optional callback(err, value). Defaults to a noop.
     */
    this.fetch = (key, func, options = {}, callback) => {
        if (typeof options === 'function') {
            callback = options;
            options = {};
        }

        // Default callback is a noop
        callback = callback || (() => {});

        // Try to get value from memory cache
        let result = getFromMemoryCache(key);

        // Return value from memory cache if it exists
        if (result.exists) {
            return callback(null, result.value);
        }

        const _this = this;

        // Double-checked locking: http://en.wikipedia.org/wiki/Double-checked_locking
        lock(`fetch-memory-cache-lock-${key}`, (releaseMemoryCacheLock) => {
            async.reflect((callback) => {
                // Try to get value from memory cache
                result = getFromMemoryCache(key);

                // Return value from memory cache if it exists
                if (result.exists) {
                    return callback(null, result.value);
                }

                // Try to get value from Redis
                getFromRedis(key, (err, result) => {
                    if (err) {
                        return callback(err);
                    }

                    // Return value from Redis if it exists
                    if (result.exists) {
                        memoryCache.put(key, result.value, random(2000, 5000));
                        return callback(null, result.value);
                    }

                    // Double-checked locking: http://en.wikipedia.org/wiki/Double-checked_locking
                    lock(`fetch-redis-lock-${key}`, (releaseRedisLock) => {
                        async.reflect((callback) => {
                            // Try to get value from memory cache
                            result = getFromMemoryCache(key);

                            // Return value from memory cache if it exists
                            if (result.exists) {
                                return callback(null, result.value);
                            }

                            // Try to get value from Redis
                            getFromRedis(key, async (err, result) => {
                                if (err) {
                                    return callback(err);
                                }

                                // Return value from Redis if it exists
                                if (result.exists) {
                                    memoryCache.put(key, result.value, random(2000, 5000));
                                    return callback(null, result.value);
                                }

                                // Execute the specified function and place the results in cache before returning the data
                                if (func.length === 0) {
                                    // If the function doesn't have any arguments, there wasn't a callback provided
                                    try {
                                        const data = await func();

                                        _this.set(key, data, options, (err) => {
                                            callback(err, data);
                                        });
                                    } catch(err) {
                                        callback(err);
                                    }
                                } else {
                                    // If the function has arguments, there was a callback provided
                                    func((err, data) => {
                                        if (err) {
                                            return callback(err);
                                        }

                                        _this.set(key, data, options, (err) => {
                                            callback(err, data);
                                        });
                                    });
                                }
                            });
                        })(releaseRedisLock((err, result) => {
                            if (result.error) {
                                return callback(result.error);
                            }

                            callback(null, result.value);
                        }));
                    });
                });
            })(releaseMemoryCacheLock((err, result) => {
                if (result.error) {
                    return callback(result.error);
                }

                callback(null, result.value);
            }));
        });
    };

    /**
     * Like fetch(), but also sets up a background interval to proactively refresh the cached value
     * before it expires, preventing cache misses under sustained load.
     * @param {string} key - The cache key.
     * @param {Function} func - Called on cache miss and on each refresh interval: func(callback).
     * @param {Object} [options] - Optional settings.
     * @param {number|Object} [options.ttl] - TTL in ms, or object with min/max properties.
     * @param {Function} [callback] - Optional callback(err, value). Defaults to a noop.
     */
    this.fetchAndRefresh = (key, func, options = {}, callback) => {
        if (typeof options === 'function') {
            callback = options;
            options = {};
        }

        // Get TTL based on specified options
        const ttl = getTtl(options);

        // Default callback is a noop
        callback = callback || (() => {});

        const _this = this;

        if (!intervals[key]) {
            const delay = ttl.min / 2;

            intervals[key] = setInterval(() => {
                // This distributed lock prevents multiple clients from executing func at the same time
                _this.mutex.lock(`interval-${key}`, { ttl: delay - 100 }, (err) => {
                    if (err) {
                        return;
                    }

                    // Execute the specified function and update cache
                    func((err, data) => {
                        if (err) {
                            return;
                        }

                        _this.set(key, data, options);
                    });
                });
            }, delay);
        }

        this.fetch(key, func, options, callback);
    };

    /**
     * Gets a cached value. Supports both callback and promise styles.
     * @param {string} key - The cache key.
     * @param {Function} [callback] - Optional callback(err, value). If omitted, returns a Promise.
     * @returns {Promise|undefined} Resolves with the cached value, or null if not found.
     */
    this.get = (key, callback) => {
        const executor = () => {
            return new Promise((resolve, reject) => {
                // Try to get value from memory cache
                let result = getFromMemoryCache(key);

                // Return value from memory cache if it exists
                if (result.exists) {
                    return resolve(result.value);
                }

                // Double-checked locking: http://en.wikipedia.org/wiki/Double-checked_locking
                lock(`get-memory-cache-lock-${key}`, (releaseMemoryCacheLock) => {
                    async.reflect((callback) => {
                        // Try to get value from memory cache
                        result = getFromMemoryCache(key);

                        // Return value from memory cache if it exists
                        if (result.exists) {
                            return callback(null, result.value);
                        }

                        getFromRedis(key, (err, result) => {
                            if (err) {
                                return callback(err);
                            }

                            if (!result.exists) {
                                return callback(null, null);
                            }

                            memoryCache.put(key, result.value, random(2000, 5000));
                            callback(null, result.value);
                        });
                    })(releaseMemoryCacheLock((err, result) => {
                        if (result.error) {
                            return reject(result.error);
                        }

                        resolve(result.value);
                    }));
                });
            });
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
         * @param {string} key - The semaphore key.
         * @param {Object} [options] - Optional settings.
         * @param {number} [options.ttl=1000] - Slot TTL in ms; expired slots may be reclaimed.
         * @param {Object} [options.retry] - Retry options.
         * @param {number} [options.retry.times=1] - Number of acquisition attempts.
         * @param {number} [options.retry.interval=100] - Delay between retries in ms.
         * @param {Function} callback - callback(err, index) where index is the acquired slot number.
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

            const _this = this;

            async.retry({ interval: options.retry.interval, times: options.retry.times }, (callback) => {
                // Mutex lock around semaphore
                _this.mutex.lock(`lock:${key}`, { retry: { times: 100 } }, (err) => {
                    if (err) {
                        return callback(err);
                    }

                    redisClient.get(key, (err, data) => {
                        // If we encountered an error, unlock the mutex lock and return error
                        if (err) {
                            return _this.mutex.unlock(`lock:${key}`, () => { callback(err); });
                        }

                        // If we don't have a previously created semaphore, unlock the mutex lock and return error
                        if (!data) {
                            return _this.mutex.unlock(`lock:${key}`, () => { callback(new Error(`Semaphore ${key} doesn't exist.`)); });
                        }

                        const pool = JSON.parse(data);

                        // Try to find a slot that's available.
                        let index = pool.findIndex(s => s.status === 'available');

                        if (index === -1) {
                            index = pool.findIndex(s => s.ttl <= Date.now());
                        }

                        // If we don't have a previously created semaphore, unlock the mutex lock and return error
                        if (index === -1) {
                            return _this.mutex.unlock(`lock:${key}`, () => { callback(new Error(`Semaphore ${key} doesn't have any available slots.`)); });
                        }

                        pool[index] = { status: 'acquired', ttl: Date.now() + options.ttl };

                        redisClient.set(key, JSON.stringify(pool), (err) => {
                            if (err) {
                                return _this.mutex.unlock(`lock:${key}`, () => { callback(err); });
                            }

                            _this.mutex.unlock(`lock:${key}`, () => { callback(null, index); });
                        });
                    });
                });
            }, callback);
        },
        /**
         * Permanently consumes a semaphore slot, marking it consumed rather than available.
         * Ensures at least one slot always remains non-consumed.
         * @param {string} key - The semaphore key.
         * @param {number} index - The slot index to consume.
         * @param {Function} [callback] - Optional callback(err). Defaults to a noop.
         */
        consumeLock: (key, index, callback) => {
            callback = callback || (() => {});

            const _this = this;

            // Mutex lock around semaphore
            _this.mutex.lock(`lock:${key}`, { retry: { times: 100 } }, (err) => {
                if (err) {
                    return callback(err);
                }

                redisClient.get(key, (err, data) => {
                    // If we encountered an error, unlock the mutex lock and return error
                    if (err) {
                        return _this.mutex.unlock(`lock:${key}`, () => { callback(err); });
                    }

                    // If we don't have a previously created semaphore, unlock the mutex lock and return error
                    if (!data) {
                        return _this.mutex.unlock(`lock:${key}`, () => { callback(new Error(`Semaphore ${key} doesn't exist.`)); });
                    }

                    const pool = JSON.parse(data);

                    // Ensure index exists.
                    if (pool.length <= index) {
                        return _this.mutex.unlock(`lock:${key}`, () => { callback(new Error(`Index ${index} for semaphore ${key} is invalid.`)); });
                    }

                    pool[index] = { status: 'consumed' };

                    // Ensure at least one slot isn't consumed
                    if (pool.every(s => s.status === 'consumed')) {
                        pool[index] = { status: 'available' };
                    }

                    redisClient.set(key, JSON.stringify(pool), (err) => {
                        if (err) {
                            return _this.mutex.unlock(`lock:${key}`, () => { callback(err); });
                        }

                        _this.mutex.unlock(`lock:${key}`, () => { callback(); });
                    });
                });
            });
        },
        /**
         * Increases the size of an existing semaphore pool. Cannot shrink a pool.
         * @param {string} key - The semaphore key.
         * @param {number} size - The desired pool size (must be >= current size).
         * @param {Function} [callback] - Optional callback(err). Defaults to a noop.
         */
        expand: (key, size, callback) => {
            callback = callback || (() => {});

            const _this = this;

            _this.mutex.lock(`lock:${key}`, { retry: { times: 100 } }, (err) => {
                if (err) {
                    return callback(err);
                }

                redisClient.get(key, (err, data) => {
                    // If we encountered an error, unlock the mutex lock and return error
                    if (err) {
                        return _this.mutex.unlock(`lock:${key}`, () => { callback(err); });
                    }

                    // If we don't have a previously created semaphore, unlock the mutex lock and return error
                    if (!data) {
                        return _this.mutex.unlock(`lock:${key}`, () => { callback(new Error(`Semaphore ${key} doesn't exist.`)); });
                    }

                    let pool = JSON.parse(data);

                    if (pool.length > size) {
                        return _this.mutex.unlock(`lock:${key}`, () => { callback(new Error(`Cannot shrink pool, size is ${pool.length} and you requested a size of ${size}.`)); });
                    }

                    if (pool.length === size) {
                        return _this.mutex.unlock(`lock:${key}`, () => callback());
                    }

                    pool = pool.concat(Array(size - pool.length).fill({ status: 'available' }));

                    redisClient.set(key, JSON.stringify(pool), (err) => {
                        if (err) {
                            return _this.mutex.unlock(`lock:${key}`, () => { callback(err); });
                        }

                        _this.mutex.unlock(`lock:${key}`, () => { callback(); });
                    });
                });
            });
        },
        /**
         * Releases an acquired semaphore slot, marking it available again.
         * @param {string} key - The semaphore key.
         * @param {number} index - The slot index to release.
         * @param {Function} [callback] - Optional callback(err). Defaults to a noop.
         */
        releaseLock: (key, index, callback) => {
            callback = callback || (() => {});

            const _this = this;

            // Mutex lock around semaphore
            _this.mutex.lock(`lock:${key}`, { retry: { times: 100 } }, (err) => {
                if (err) {
                    return callback(err);
                }

                redisClient.get(key, (err, data) => {
                    // If we encountered an error, unlock the mutex lock and return error
                    if (err) {
                        return _this.mutex.unlock(`lock:${key}`, () => { callback(err); });
                    }

                    // If we don't have a previously created semaphore, unlock the mutex lock and return error
                    if (!data) {
                        return _this.mutex.unlock(`lock:${key}`, () => { callback(new Error(`Semaphore ${key} doesn't exist.`)); });
                    }

                    const pool = JSON.parse(data);

                    // Ensure index exists.
                    if (pool.length <= index) {
                        return _this.mutex.unlock(`lock:${key}`, () => { callback(new Error(`Index ${index} for semaphore ${key} is invalid.`)); });
                    }

                    pool[index] = { status: 'available' };

                    redisClient.set(key, JSON.stringify(pool), (err) => {
                        if (err) {
                            return _this.mutex.unlock(`lock:${key}`, () => { callback(err); });
                        }

                        _this.mutex.unlock(`lock:${key}`, () => { callback(); });
                    });
                });
            });
        },
        /**
         * Resets all slots in an existing semaphore pool to available.
         * @param {string} key - The semaphore key.
         * @param {Function} [callback] - Optional callback(err, pool). Defaults to a noop.
         */
        reset: (key, callback) => {
            callback = callback || (() => {});

            const _this = this;

            // Mutex lock around semaphore
            this.mutex.lock(`lock:${key}`, { retry: { times: 100 } }, (err) => {
                if (err) {
                    return callback(err);
                }

                // Try to get previously created semaphore
                redisClient.get(key, (err, data) => {
                    // If we encountered an error, unlock the mutex lock and return error
                    if (err) {
                        return _this.mutex.unlock(`lock:${key}`, () => { callback(err); });
                    }

                    // If we don't have a previously created semaphore, unlock the mutex lock and return error
                    if (!data) {
                        return _this.mutex.unlock(`lock:${key}`, () => { callback(new Error(`Semaphore ${key} doesn't exist.`)); });
                    }

                    let pool = JSON.parse(data);
                    pool = Array(pool.length).fill({ status: 'available' });

                    redisClient.set(key, JSON.stringify(pool), (err) => {
                        if (err) {
                            return _this.mutex.unlock(`lock:${key}`, () => { callback(err); });
                        }

                        _this.mutex.unlock(`lock:${key}`, () => { callback(null, pool); });
                    });
                });
            });
        },
        /**
         * Retrieves an existing semaphore pool, or creates one if it doesn't exist.
         * @param {string} key - The semaphore key.
         * @param {Object} [options] - Optional settings.
         * @param {number|Function} [options.size=1] - Pool size, or a function(callback) that resolves the size.
         * @param {Function} [callback] - Optional callback(err, pool). Defaults to a noop.
         */
        retrieveOrCreate: (key, options = {}, callback) => {
            // Options are optional
            if (!callback && typeof options === 'function') {
                callback = options;
                options = {};
            }

            callback = callback || (() => {});

            const _this = this;

            // Mutex lock around semaphore retrival or creation
            this.mutex.lock(`lock:${key}`, { retry: { times: 100 } }, (err) => {
                if (err) {
                    return callback(err);
                }

                // Try to get previously created semaphore
                redisClient.get(key, (err, data) => {
                    // If we encountered an error, unlock the mutex lock and return error
                    if (err) {
                        return _this.mutex.unlock(`lock:${key}`, () => { callback(err); });
                    }

                    // If we retreived a previously created semaphore, unlock the mutex lock and return
                    if (data) {
                        return _this.mutex.unlock(`lock:${key}`, () => { callback(null, JSON.parse(data)); });
                    }

                    const getSize = (callback) => {
                        if (typeof options.size === 'function') {
                            return options.size(callback);
                        }

                        callback(null, Object.prototype.hasOwnProperty.call(options, 'size') ? options.size : 1);
                    };

                    getSize((err, size) => {
                        // If we encountered an error, unlock the mutex lock and return error
                        if (err) {
                            return _this.mutex.unlock(`lock:${key}`, () => { callback(err); });
                        }

                        const pool = Array(Math.max(size, 1)).fill({ status: 'available' });

                        redisClient.set(key, JSON.stringify(pool), (err) => {
                            if (err) {
                                return _this.mutex.unlock(`lock:${key}`, () => { callback(err); });
                            }

                            _this.mutex.unlock(`lock:${key}`, () => { callback(null, pool); });
                        });
                    });
                });
            });
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

    /**
     * Distributed throttle: invokes `fn` only if no other call has claimed the same key
     * within the last `ttl` milliseconds, coalescing calls across multiple processes via
     * Redis. The first caller in a window wins the claim and `fn` runs to completion;
     * subsequent calls within the window are no-ops (they return immediately without
     * invoking `fn`). After the window's TTL expires, the next caller can claim again.
     *
     * The returned Promise resolves only after `fn` has resolved (for the winning caller)
     * or immediately (for absorbed callers). Errors thrown by `fn` propagate to the
     * caller — useful for callers that need to know whether the work succeeded so they
     * can NACK upstream messages, etc.
     *
     * @param {string} key - The Redis key. Callers compose their own naming convention.
     * @param {Object} options
     * @param {number} options.ttl - Throttle window in milliseconds. Required.
     * @param {Function} fn - Async function invoked once per window if this caller wins
     *                        the claim. Awaited before the returned Promise resolves.
     * @returns {Promise<void>}
     */
    this.throttle = async (key, options, fn) => {
        const ttl = options.ttl;

        const won = await new Promise((resolve, reject) => {
            redisClient.set(key, '1', 'NX', 'PX', ttl, (err, res) => {
                if (err) {
                    return reject(err);
                }

                resolve(res === 'OK');
            });
        });

        if (!won) {
            return;
        }

        await fn();
    };

    // Semaphore functions need to be bound to the main PettyCache object
    for (const method in this.semaphore) {
        this.semaphore[method] = this.semaphore[method].bind(this);
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
