const test = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const async = require('async');
const memoryCache = require('memory-cache');
const redis = require('redis');

const PettyCache = require('../index.js');

const redisClient = redis.createClient();
const pettyCache = new PettyCache(redisClient);

test('petty-cache', async (t) => {
    t.test('new PettyCache()', { concurrency: true }, async (t) => {
    t.test('new PettyCache()', { timeout: 7000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();
        const newPettyCache = new PettyCache();

        newPettyCache.fetch(key, (callback) => {
            return callback(null, { foo: 'bar' });
        }, () => {
            newPettyCache.fetch(key, () => {
                throw 'This function should not be called';
            }, (err, data) => {
                assert.equal(data.foo, 'bar');

                // Wait for memory cache to expire
                setTimeout(() => {
                    newPettyCache.fetch(key, () => {
                        throw 'This function should not be called';
                    }, (err, data) => {
                        assert.strictEqual(data.foo, 'bar');
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});

    t.test('new PettyCache(port, host)', { timeout: 7000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();
        const newPettyCache = new PettyCache(6379, 'localhost');

        newPettyCache.fetch(key, (callback) => {
            return callback(null, { foo: 'bar' });
        }, () => {
            newPettyCache.fetch(key, () => {
                throw 'This function should not be called';
            }, (err, data) => {
                assert.equal(data.foo, 'bar');

                // Wait for memory cache to expire
                setTimeout(() => {
                    newPettyCache.fetch(key, () => {
                        throw 'This function should not be called';
                    }, (err, data) => {
                        assert.strictEqual(data.foo, 'bar');
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});

    t.test('new PettyCache(redisClient)', { timeout: 7000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();
        const redisClient = redis.createClient();
        const newPettyCache = new PettyCache(redisClient);

        newPettyCache.fetch(key, (callback) => {
            return callback(null, { foo: 'bar' });
        }, () => {
            newPettyCache.fetch(key, () => {
                throw 'This function should not be called';
            }, (err, data) => {
                assert.equal(data.foo, 'bar');

                // Wait for memory cache to expire
                setTimeout(() => {
                    newPettyCache.fetch(key, () => {
                        throw 'This function should not be called';
                    }, (err, data) => {
                        assert.strictEqual(data.foo, 'bar');
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});
});

    t.test('memory-cache', { concurrency: true }, async (t) => {
    t.test('memoryCache.put(key, \'\')', async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        memoryCache.put(key, '', 1000);
        assert(memoryCache.keys().includes(key));
        assert.strictEqual(memoryCache.get(key), '');

        // Wait for memory cache to expire
        setTimeout(() => {
            assert(!memoryCache.keys().includes(key));
            assert.strictEqual(memoryCache.get(key), null);
            resolve();
        }, 1001);
    });
});

    t.test('memoryCache.put(key, 0)', async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        memoryCache.put(key, 0, 1000);
        assert(memoryCache.keys().includes(key));
        assert.strictEqual(memoryCache.get(key), 0);

        // Wait for memory cache to expire
        setTimeout(() => {
            assert(!memoryCache.keys().includes(key));
            assert.strictEqual(memoryCache.get(key), null);
            resolve();
        }, 1001);
    });
});

    t.test('memoryCache.put(key, false)', async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        memoryCache.put(key, false, 1000);
        assert(memoryCache.keys().includes(key));
        assert.strictEqual(memoryCache.get(key), false);

        // Wait for memory cache to expire
        setTimeout(() => {
            assert(!memoryCache.keys().includes(key));
            assert.strictEqual(memoryCache.get(key), null);
            resolve();
        }, 1001);
    });
});

    t.test('memoryCache.put(key, NaN)', async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        memoryCache.put(key, NaN, 1000);
        assert(memoryCache.keys().includes(key));
        assert(isNaN(memoryCache.get(key)));

        // Wait for memory cache to expire
        setTimeout(() => {
            assert(!memoryCache.keys().includes(key));
            assert.strictEqual(memoryCache.get(key), null);
            resolve();
        }, 1001);
    });
});

    t.test('memoryCache.put(key, null)', async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        memoryCache.put(key, null, 1000);
        assert(memoryCache.keys().includes(key));
        assert.strictEqual(memoryCache.get(key), null);

        // Wait for memory cache to expire
        setTimeout(() => {
            assert(!memoryCache.keys().includes(key));
            assert.strictEqual(memoryCache.get(key), null);
            resolve();
        }, 1001);
    });
});

    t.test('memoryCache.put(key, undefined)', async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        memoryCache.put(key, undefined, 1000);
        assert(memoryCache.keys().includes(key));
        assert.strictEqual(memoryCache.get(key), undefined);

        // Wait for memory cache to expire
        setTimeout(() => {
            assert(!memoryCache.keys().includes(key));
            assert.strictEqual(memoryCache.get(key), null);
            resolve();
        }, 1001);
    });
});
});

    t.test('PettyCache.bulkFetch', { concurrency: true }, async (t) => {
    t.test('PettyCache.bulkFetch', { timeout: 7000 }, async () => {
 await new Promise((resolve) => {
        pettyCache.set('a', 1, () => {
            pettyCache.set('b', '2', () => {
                pettyCache.bulkFetch(['a', 'b', 'c', 'd'], (keys, callback) => {
                    assert(keys.length === 2);

                    callback(null, { 'c': [3], 'd': { num: 4 } });
                }, (err, values) => {
                    assert.strictEqual(values.a, 1);
                    assert.strictEqual(values.b, '2');
                    assert.strictEqual(values.c[0], 3);
                    assert.strictEqual(values.d.num, 4);

                    // Call bulkFetch again to ensure memory serialization is working as expected.
                    pettyCache.bulkFetch(['a', 'b', 'c', 'd'], () => {
                        throw 'This function should not be called';
                    }, (err, values) => {
                        assert.strictEqual(values.a, 1);
                        assert.strictEqual(values.b, '2');
                        assert.strictEqual(values.c[0], 3);
                        assert.strictEqual(values.d.num, 4);

                        // Wait for memory cache to expire
                        setTimeout(() => {
                            pettyCache.bulkFetch(['a', 'b', 'c', 'd'], () => {
                                throw 'This function should not be called';
                            }, (err, values) => {
                                assert.strictEqual(values.a, 1);
                                assert.strictEqual(values.b, '2');
                                assert.strictEqual(values.c[0], 3);
                                assert.strictEqual(values.d.num, 4);

                                // Call bulkFetch again to ensure memory serialization is working as expected.
                                pettyCache.bulkFetch(['a', 'b', 'c', 'd'], () => {
                                    throw 'This function should not be called';
                                }, (err, values) => {
                                    assert.strictEqual(values.a, 1);
                                    assert.strictEqual(values.b, '2');
                                    assert.strictEqual(values.c[0], 3);
                                    assert.strictEqual(values.d.num, 4);
                                    resolve();
                                });
                            });
                        }, 5001);
                    });
                });
            });
        });
    });
});

    t.test('PettyCache.bulkFetch should cache null values returned by func', { timeout: 7000 }, async () => {
 await new Promise((resolve) => {
        const key1 = Math.random().toString();
        const key2 = Math.random().toString();

        pettyCache.bulkFetch([key1, key2], (keys, callback) => {
            assert.strictEqual(keys.length, 2);
            assert(keys.some(k => k === key1));
            assert(keys.some(k => k === key2));

            const values = {};

            values[key1] = '1';
            values[key2] = null;

            callback(null, values);
        }, (err) => {
            assert.ifError(err);

            pettyCache.bulkFetch([key1, key2], () => {
                throw 'This function should not be called';
            }, (err, data) => {
                assert.strictEqual(Object.keys(data).length, 2);
                assert.strictEqual(data[key1], '1');
                assert.strictEqual(data[key2], null);

                // Wait for memory cache to expire
                setTimeout(() => {
                    pettyCache.bulkFetch([key1, key2], () => {
                        throw 'This function should not be called';
                    }, (err, data) => {
                        assert.strictEqual(Object.keys(data).length, 2);
                        assert.strictEqual(data[key1], '1');
                        assert.strictEqual(data[key2], null);

                        resolve();
                    });
                }, 6000);
            });
        });
    });
});

    t.test('PettyCache.bulkFetch should return empty object when no keys are passed', async () => {
 await new Promise((resolve) => {
        pettyCache.bulkFetch([], () => {
            throw 'This function should not be called';
        }, (err, values) => {
            assert.ifError(err);
            assert.deepEqual(values, {});
            resolve();
        });
    });
});

    t.test('PettyCache.bulkFetch should return error if func returns error', async () => {
 await new Promise((resolve) => {
        pettyCache.bulkFetch([Math.random().toString()], (keys, callback) => {
            callback(new Error('PettyCache.bulkFetch should return error if func returns error'));
        }, (err, values) => {
            assert(err);
            assert.strictEqual(err.message, 'PettyCache.bulkFetch should return error if func returns error');
            assert(!values);
            resolve();
        });
    });
});

    t.test('PettyCache.bulkFetch should run func again after TTL', { timeout: 7000 }, async () => {
 await new Promise((resolve) => {
        const keys = [Math.random().toString(), Math.random().toString()];
        let numberOfFuncCalls = 0;

        const func = (keys, callback) => {
            numberOfFuncCalls++;

            const results = {};
            results[keys[0]] = numberOfFuncCalls;
            results[keys[1]] = numberOfFuncCalls;

            callback(null, results);
        };

        pettyCache.bulkFetch(keys, func, { ttl: 6000 }, (err, results) => {
            assert.ifError(err);
            assert.strictEqual(results[keys[0]], 1);
            assert.strictEqual(results[keys[1]], 1);

            pettyCache.bulkGet(keys, (err, results) => {
                assert.ifError(err);
                assert.strictEqual(results[keys[0]], 1);
                assert.strictEqual(results[keys[1]], 1);
            });

            setTimeout(() => {
                pettyCache.bulkGet(keys, (err, results) => {
                    assert.ifError(err);
                    assert.strictEqual(results[keys[0]], null);
                    assert.strictEqual(results[keys[1]], null);

                    pettyCache.bulkFetch(keys, func, { ttl: 6000 }, (err, results) => {
                        assert.ifError(err);
                        assert.strictEqual(results[keys[0]], 2);
                        assert.strictEqual(results[keys[1]], 2);

                        pettyCache.bulkGet(keys, (err, results) => {
                            assert.ifError(err);
                            assert.strictEqual(results[keys[0]], 2);
                            assert.strictEqual(results[keys[1]], 2);
                            resolve();
                        });
                    });
                });
            }, 6001);
        });
    });
});
});

    t.test('PettyCache.bulkGet', { concurrency: true }, async (t) => {
    t.test('PettyCache.bulkGet should return values', { timeout: 6000 }, async () => {
 await new Promise((resolve) => {
        const key1 = Math.random().toString();
        const key2 = Math.random().toString();
        const key3 = Math.random().toString();

        pettyCache.set(key1, '1', () => {
            pettyCache.set(key2, '2', () => {
                pettyCache.set(key3, '3', () => {
                    pettyCache.bulkGet([key1, key2, key3], (err, values) => {
                        assert.strictEqual(Object.keys(values).length, 3);
                        assert.strictEqual(values[key1], '1');
                        assert.strictEqual(values[key2], '2');
                        assert.strictEqual(values[key3], '3');

                        // Call bulkGet again while values are still in memory cache
                        pettyCache.bulkGet([key1, key2, key3], (err, values) => {
                            assert.strictEqual(Object.keys(values).length, 3);
                            assert.strictEqual(values[key1], '1');
                            assert.strictEqual(values[key2], '2');
                            assert.strictEqual(values[key3], '3');

                            // Wait for memory cache to expire
                            setTimeout(() => {
                                // Ensure keys are still in Redis
                                pettyCache.bulkGet([key1, key2, key3], (err, values) => {
                                    assert.strictEqual(Object.keys(values).length, 3);
                                    assert.strictEqual(values[key1], '1');
                                    assert.strictEqual(values[key2], '2');
                                    assert.strictEqual(values[key3], '3');
                                    resolve();
                                });
                            }, 5001);
                        });
                    });
                });
            });
        });
    });
});

    t.test('PettyCache.bulkGet should return null for missing keys', { timeout: 6000 }, async () => {
 await new Promise((resolve) => {
        const key1 = Math.random().toString();
        const key2 = Math.random().toString();
        const key3 = Math.random().toString();

        pettyCache.set(key1, '1', () => {
            pettyCache.set(key2, '2', () => {
                pettyCache.bulkGet([key1, key2, key3], (err, values) => {
                    assert.strictEqual(Object.keys(values).length, 3);
                    assert.strictEqual(values[key1], '1');
                    assert.strictEqual(values[key2], '2');
                    assert.strictEqual(values[key3], null);

                    // Call bulkGet again while values are still in memory cache
                    pettyCache.bulkGet([key1, key2, key3], (err, values) => {
                        assert.strictEqual(Object.keys(values).length, 3);
                        assert.strictEqual(values[key1], '1');
                        assert.strictEqual(values[key2], '2');
                        assert.strictEqual(values[key3], null);

                        // Wait for memory cache to expire
                        setTimeout(() => {
                            // Ensure keys are still in Redis
                            pettyCache.bulkGet([key1, key2, key3], (err, values) => {
                                assert.strictEqual(Object.keys(values).length, 3);
                                assert.strictEqual(values[key1], '1');
                                assert.strictEqual(values[key2], '2');
                                assert.strictEqual(values[key3], null);
                                resolve();
                            });
                        }, 5001);
                    });
                });
            });
        });
    });
});

    t.test('PettyCache.bulkGet should correctly handle falsy values', { timeout: 12000 }, async () => {
 await new Promise((resolve) => {
        const key1 = Math.random().toString();
        const key2 = Math.random().toString();
        const key3 = Math.random().toString();
        const key4 = Math.random().toString();
        const key5 = Math.random().toString();
        const key6 = Math.random().toString();
        const values = {};

        values[key1] = '';
        values[key2] = 0;
        values[key3] = false;
        values[key4] = NaN;
        values[key5] = null;
        values[key6] = undefined;

        async.each(Object.keys(values), (key, callback) => {
            pettyCache.set(key, values[key], { ttl: 6000 }, callback);
        }, (err) => {
            assert.ifError(err);

            const keys = Object.keys(values);

            // Add an additional key to check handling of missing keys
            const key7 = Math.random().toString();
            keys.push(key7);

            pettyCache.bulkGet(keys, (err, data) => {
                assert.ifError(err);
                assert.strictEqual(keys.length, 7);
                assert.strictEqual(Object.keys(data).length, 7);
                assert.strictEqual(data[key1], '');
                assert.strictEqual(data[key2], 0);
                assert.strictEqual(data[key3], false);
                assert.strictEqual(typeof data[key4], 'number');
                assert(isNaN(data[key4]));
                assert.strictEqual(data[key5], null);
                assert.strictEqual(data[key6], undefined);
                assert.strictEqual(data[key7], null);

                // Wait for memory cache to expire
                setTimeout(() => {
                    // Ensure keys are still in Redis
                    pettyCache.bulkGet(keys, (err, data) => {
                        assert.ifError(err);
                        assert.strictEqual(Object.keys(data).length, 7);
                        assert.strictEqual(data[key1], '');
                        assert.strictEqual(data[key2], 0);
                        assert.strictEqual(data[key3], false);
                        assert.strictEqual(typeof data[key4], 'number');
                        assert(isNaN(data[key4]));
                        assert.strictEqual(data[key5], null);
                        assert.strictEqual(data[key6], undefined);
                        assert.strictEqual(data[key7], null);

                        // Wait for Redis cache to expire
                        setTimeout(() => {
                            // Ensure keys are not in Redis
                            pettyCache.bulkGet(keys, (err, data) => {
                                assert.ifError(err);
                                assert.strictEqual(Object.keys(data).length, 7);
                                assert.strictEqual(data[key1], null);
                                assert.strictEqual(data[key2], null);
                                assert.strictEqual(data[key3], null);
                                assert.strictEqual(data[key4], null);
                                assert.strictEqual(data[key5], null);
                                assert.strictEqual(data[key6], null);
                                assert.strictEqual(data[key7], null);
                                resolve();
                            });
                        }, 6001);
                    });
                }, 5001);
            });
        });
    });
});

    t.test('PettyCache.bulkGet should return empty object when no keys are passed', async () => {
 await new Promise((resolve) => {
        pettyCache.bulkGet([], (err, values) => {
            assert.ifError(err);
            assert.deepEqual(values, {});
            resolve();
        });
    });
});
});

    t.test('PettyCache.bulkSet', { concurrency: true }, async (t) => {
    t.test('PettyCache.bulkSet should set values', { timeout: 6000 }, async () => {
 await new Promise((resolve) => {
        const key1 = Math.random().toString();
        const key2 = Math.random().toString();
        const key3 = Math.random().toString();
        const values = {};

        values[key1] = '1';
        values[key2] = 2;
        values[key3] = '3';

        pettyCache.bulkSet(values, (err) => {
            assert.ifError(err);

            pettyCache.get(key1, (err, value) => {
                assert.ifError(err);
                assert.strictEqual(value, '1');

                pettyCache.get(key2, (err, value) => {
                    assert.ifError(err);
                    assert.strictEqual(value, 2);

                    pettyCache.get(key3, (err, value) => {
                        assert.ifError(err);
                        assert.strictEqual(value, '3');

                        // Wait for memory cache to expire
                        setTimeout(() => {
                            pettyCache.get(key1, (err, value) => {
                                assert.ifError(err);
                                assert.strictEqual(value, '1');

                                pettyCache.get(key2, (err, value) => {
                                    assert.ifError(err);
                                    assert.strictEqual(value, 2);

                                    pettyCache.get(key3, (err, value) => {
                                        assert.ifError(err);
                                        assert.strictEqual(value, '3');
                                        resolve();
                                    });
                                });
                            });
                        }, 5001);
                    });
                });
            });
        });
    });
});

    t.test('PettyCache.bulkSet should set values with the specified TTL option', { timeout: 7000 }, async () => {
 await new Promise((resolve) => {
        const key1 = Math.random().toString();
        const key2 = Math.random().toString();
        const key3 = Math.random().toString();
        const values = {};

        values[key1] = '1';
        values[key2] = 2;
        values[key3] = '3';

        pettyCache.bulkSet(values, { ttl: 6000 }, (err) => {
            assert.ifError(err);

            pettyCache.get(key1, (err, value) => {
                assert.ifError(err);
                assert.strictEqual(value, '1');

                pettyCache.get(key2, (err, value) => {
                    assert.ifError(err);
                    assert.strictEqual(value, 2);

                    pettyCache.get(key3, (err, value) => {
                        assert.ifError(err);
                        assert.strictEqual(value, '3');

                        // Wait for Redis cache to expire
                        setTimeout(() => {
                            pettyCache.get(key1, (err, value) => {
                                assert.ifError(err);
                                assert.strictEqual(value, null);

                                pettyCache.get(key2, (err, value) => {
                                    assert.ifError(err);
                                    assert.strictEqual(value, null);

                                    pettyCache.get(key3, (err, value) => {
                                        assert.ifError(err);
                                        assert.strictEqual(value, null);
                                        resolve();
                                    });
                                });
                            });
                        }, 6001);
                    });
                });
            });
        });
    });
});

    t.test('PettyCache.bulkSet should set values with the specified TTL option using max and min', { timeout: 10000 }, async () => {
 await new Promise((resolve) => {
        const key1 = Math.random().toString();
        const key2 = Math.random().toString();
        const key3 = Math.random().toString();
        const values = {};

        values[key1] = '1';
        values[key2] = 2;
        values[key3] = '3';

        pettyCache.bulkSet(values, { ttl: { max: 7000, min: 6000 } }, (err) => {
            assert.ifError(err);

            pettyCache.get(key1, (err, value) => {
                assert.ifError(err);
                assert.strictEqual(value, '1');

                pettyCache.get(key2, (err, value) => {
                    assert.ifError(err);
                    assert.strictEqual(value, 2);

                    pettyCache.get(key3, (err, value) => {
                        assert.ifError(err);
                        assert.strictEqual(value, '3');

                        // Wait for Redis cache to expire
                        setTimeout(() => {
                            pettyCache.get(key1, (err, value) => {
                                assert.ifError(err);
                                assert.strictEqual(value, null);

                                pettyCache.get(key2, (err, value) => {
                                    assert.ifError(err);
                                    assert.strictEqual(value, null);

                                    pettyCache.get(key3, (err, value) => {
                                        assert.ifError(err);
                                        assert.strictEqual(value, null);
                                        resolve();
                                    });
                                });
                            });
                        }, 7001);
                    });
                });
            });
        });
    });
});

    t.test('PettyCache.bulkSet should set values with the specified TTL option using max only', { timeout: 10000 }, async () => {
 await new Promise((resolve) => {
        const key1 = Math.random().toString();
        const key2 = Math.random().toString();
        const key3 = Math.random().toString();
        const values = {};

        values[key1] = '1';
        values[key2] = 2;
        values[key3] = '3';

        pettyCache.bulkSet(values, { ttl: { max: 10000 } }, (err) => {
            assert.ifError(err);

            pettyCache.get(key1, (err, value) => {
                assert.ifError(err);
                assert.strictEqual(value, '1');

                resolve();
            });
        });
    });
});

    t.test('PettyCache.bulkSet should set values with the specified TTL option using min only', { timeout: 10000 }, async () => {
 await new Promise((resolve) => {
        const key1 = Math.random().toString();
        const key2 = Math.random().toString();
        const key3 = Math.random().toString();
        const values = {};

        values[key1] = '1';
        values[key2] = 2;
        values[key3] = '3';

        pettyCache.bulkSet(values, { ttl: { min: 6000 } }, (err) => {
            assert.ifError(err);

            pettyCache.get(key1, (err, value) => {
                assert.ifError(err);
                assert.strictEqual(value, '1');

                resolve();
            });
        });
    });
});
});

    t.test('PettyCache.del', { concurrency: true }, async (t) => {
    t.test('PettyCache.del', async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        pettyCache.set(key, key.split('').reverse().join(''), (err) => {
            assert.ifError(err);

            pettyCache.get(key, (err, value) => {
                assert.strictEqual(value, key.split('').reverse().join(''));

                pettyCache.del(key, (err) => {
                    assert.ifError(err);

                    pettyCache.get(key, (err, value) => {
                        assert.ifError(err);
                        assert.strictEqual(value, null);

                        pettyCache.del(key, (err) => {
                            assert.ifError(err);
                            resolve();
                        });
                    });
                });
            });
        });
    });
});

    t.test('PettyCache.del', async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        pettyCache.set(key, key.split('').reverse().join(''), (err) => {
            assert.ifError(err);

            pettyCache.get(key, async (err, value) => {
                assert.strictEqual(value, key.split('').reverse().join(''));

                await pettyCache.del(key);

                pettyCache.get(key, async (err, value) => {
                    assert.ifError(err);
                    assert.strictEqual(value, null);

                    await pettyCache.del(key);

                    resolve();
                });
            });
        });
    });
});
});

    t.test('PettyCache.fetch', async (t) => {
    t.test('PettyCache.fetch', { timeout: 7000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        pettyCache.fetch(key, (callback) => {
            return callback(null, { foo: 'bar' });
        }, () => {
            pettyCache.fetch(key, () => {
                throw 'This function should not be called';
            }, (err, data) => {
                assert.equal(data.foo, 'bar');

                // Wait for memory cache to expire
                setTimeout(() => {
                    pettyCache.fetch(key, () => {
                        throw 'This function should not be called';
                    }, (err, data) => {
                        assert.strictEqual(data.foo, 'bar');
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});

    t.test('PettyCache.fetch should cache null values returned by func', { timeout: 7000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        pettyCache.fetch(key, (callback) => {
            return callback(null, null);
        }, () => {
            pettyCache.fetch(key, () => {
                throw 'This function should not be called';
            }, (err, data) => {
                assert.strictEqual(data, null);

                // Wait for memory cache to expire
                setTimeout(() => {
                    pettyCache.fetch(key, () => {
                        throw 'This function should not be called';
                    }, (err, data) => {
                        assert.strictEqual(data, null);
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});

    t.test('PettyCache.fetch should cache undefined values returned by func', { timeout: 7000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        pettyCache.fetch(key, (callback) => {
            return callback(null, undefined);
        }, () => {
            pettyCache.fetch(key, () => {
                throw 'This function should not be called';
            }, (err, data) => {
                assert.strictEqual(data, undefined);

                // Wait for memory cache to expire
                setTimeout(() => {
                    pettyCache.fetch(key, () => {
                        throw 'This function should not be called';
                    }, (err, data) => {
                        assert.strictEqual(data, undefined);
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});

    t.test('PettyCache.fetch should lock around func', async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();
        let numberOfFuncCalls = 0;

        const func = (callback) => {
            setTimeout(() => {
                callback(null, ++numberOfFuncCalls);
            }, 100);
        };

        pettyCache.fetch(key, func, () => {});
        pettyCache.fetch(key, func, () => {});
        pettyCache.fetch(key, func, () => {});
        pettyCache.fetch(key, func, () => {});
        pettyCache.fetch(key, func, () => {});
        pettyCache.fetch(key, func, () => {});
        pettyCache.fetch(key, func, () => {});
        pettyCache.fetch(key, func, () => {});
        pettyCache.fetch(key, func, () => {});

        pettyCache.fetch(key, func, (err, data) => {
            assert.equal(data, 1);
            resolve();
        });
    });
});

    t.test('PettyCache.fetch should run func again after TTL', { timeout: 7000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();
        let numberOfFuncCalls = 0;

        const func = (callback) => {
            setTimeout(() => {
                callback(null, ++numberOfFuncCalls);
            }, 100);
        };

        pettyCache.fetch(key, func, { ttl: 6000 }, () => {});

        pettyCache.fetch(key, func, { ttl: 6000 }, (err, data) => {
            assert.equal(data, 1);

            setTimeout(() => {
                pettyCache.fetch(key, func, { ttl: 6000 }, (err, data) => {
                    assert.equal(data, 2);

                    pettyCache.fetch(key, func, { ttl: 6000 }, (err, data) => {
                        assert.equal(data, 2);
                        resolve();
                    });
                });
            }, 6001);
        });
    });
});

    t.test('PettyCache.fetch should lock around Redis', async () => {
 await new Promise((resolve) => {
        redisClient.info('commandstats', (err, info) => {
            const lineBefore = info.split('\n').find(i => i.startsWith('cmdstat_get:'));
            const tokenBefore = lineBefore.split(/:|,/).find(i => i.startsWith('calls='));
            const callsBefore = parseInt(tokenBefore.split('=')[1]);

            const key = Math.random().toString();
            let numberOfFuncCalls = 0;

            const func = (callback) => {
                setTimeout(() => {
                    callback(null, ++numberOfFuncCalls);
                }, 100);
            };

            pettyCache.fetch(key, func);
            pettyCache.fetch(key, func);
            pettyCache.fetch(key, func);
            pettyCache.fetch(key, func);
            pettyCache.fetch(key, func);
            pettyCache.fetch(key, func);
            pettyCache.fetch(key, func);
            pettyCache.fetch(key, func);
            pettyCache.fetch(key, func);

            pettyCache.fetch(key, func, (err, data) => {
                assert.equal(data, 1);

                redisClient.info('commandstats', (err, info) => {
                    const lineAfter = info.split('\n').find(i => i.startsWith('cmdstat_get:'));
                    const tokenAfter = lineAfter.split(/:|,/).find(i => i.startsWith('calls='));
                    const callsAfter = parseInt(tokenAfter.split('=')[1]);

                    assert.strictEqual(callsBefore + 2, callsAfter);

                    resolve();
                });
            });
        });
    });
});

    t.test('PettyCache.fetch should return error if func returns error', async () => {
 await new Promise((resolve) => {
        pettyCache.fetch(Math.random().toString(), (callback) => {
            callback(new Error('PettyCache.fetch should return error if func returns error'));
        }, (err, values) => {
            assert(err);
            assert.strictEqual(err.message, 'PettyCache.fetch should return error if func returns error');
            assert(!values);
            resolve();
        });
    });
});

    t.test('PettyCache.fetch should support async func', { timeout: 7000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        pettyCache.fetch(key, async () => {
            return { foo: 'bar' };
        }, () => {
            pettyCache.fetch(key, async () => {
                throw 'This function should not be called';
            }, (err, data) => {
                assert.ifError(err);
                assert.equal(data.foo, 'bar');

                // Wait for memory cache to expire
                setTimeout(() => {
                    pettyCache.fetch(key, async () => {
                        throw 'This function should not be called';
                    }, (err, data) => {
                        assert.ifError(err);
                        assert.strictEqual(data.foo, 'bar');
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});

    t.test('PettyCache.fetch should support async func with callback', { timeout: 7000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        pettyCache.fetch(key, async (callback) => {
            return callback(null, { foo: 'bar' });
        }, () => {
            pettyCache.fetch(key, async () => {
                throw 'This function should not be called';
            }, (err, data) => {
                assert.ifError(err);
                assert.equal(data.foo, 'bar');

                // Wait for memory cache to expire
                setTimeout(() => {
                    pettyCache.fetch(key, async () => {
                        throw 'This function should not be called';
                    }, (err, data) => {
                        assert.ifError(err);
                        assert.strictEqual(data.foo, 'bar');
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});

    t.test('PettyCache.fetch should support sync func without callback', { timeout: 7000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        pettyCache.fetch(key, () => {
            return { foo: 'bar' };
        }, () => {
            pettyCache.fetch(key, () => {
                throw 'This function should not be called';
            }, (err, data) => {
                assert.ifError(err);
                assert.equal(data.foo, 'bar');

                // Wait for memory cache to expire
                setTimeout(() => {
                    pettyCache.fetch(key, () => {
                        throw 'This function should not be called';
                    }, (err, data) => {
                        assert.ifError(err);
                        assert.strictEqual(data.foo, 'bar');
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});
});

    t.test('PettyCache.fetchAndRefresh', { concurrency: true }, async (t) => {
    t.test('PettyCache.fetchAndRefresh', { timeout: 7000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        pettyCache.fetchAndRefresh(key, (callback) => {
            return callback(null, { foo: 'bar' });
        }, () => {
            pettyCache.fetchAndRefresh(key, () => {
                throw 'This function should not be called';
            }, (err, data) => {
                assert.equal(data.foo, 'bar');

                // Wait for memory cache to expire
                setTimeout(() => {
                    pettyCache.fetchAndRefresh(key, () => {
                        throw 'This function should not be called';
                    }, (err, data) => {
                        assert.strictEqual(data.foo, 'bar');
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});

    t.test('PettyCache.fetchAndRefresh should run func again to refresh', { timeout: 7000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();
        let numberOfFuncCalls = 0;

        const func = (callback) => {
            setTimeout(() => {
                callback(null, ++numberOfFuncCalls);
            }, 100);
        };

        pettyCache.fetchAndRefresh(key, func, { ttl: 6000 });

        pettyCache.fetchAndRefresh(key, func, { ttl: 6000 }, (err, data) => {
            assert.equal(data, 1);

            setTimeout(() => {
                pettyCache.fetchAndRefresh(key, func, { ttl: 6000 }, (err, data) => {
                    assert.equal(data, 2);

                    pettyCache.fetchAndRefresh(key, func, { ttl: 6000 }, (err, data) => {
                        assert.equal(data, 2);
                        resolve();
                    });
                });
            }, 4001);
        });
    });
});

    t.test('PettyCache.fetchAndRefresh should not allow multiple clients to execute func at the same time', { timeout: 7000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();
        let numberOfFuncCalls = 0;

        const func = (callback) => {
            setTimeout(() => {
                callback(null, ++numberOfFuncCalls);
            }, 100);
        };

        pettyCache.fetchAndRefresh(key, func, { ttl: 6000 }, (err, data) => {
            assert.ifError(err);
            assert.equal(data, 1);

            const pettyCache2 = new PettyCache(redisClient);

            pettyCache2.fetchAndRefresh(key, func, { ttl: 6000 }, (err, data) => {
                assert.ifError(err);
                assert.equal(data, 1);

                setTimeout(() => {
                    pettyCache.fetchAndRefresh(key, func, { ttl: 6000 }, (err, data) => {
                        assert.ifError(err);
                        assert.equal(data, 2);

                        pettyCache2.fetchAndRefresh(key, func, { ttl: 6000 }, (err, data) => {
                            assert.ifError(err);
                            assert.equal(data, 2);
                            resolve();
                        });
                    });
                }, 5001);
            });
        });
    });
});

    t.test('PettyCache.fetchAndRefresh should return error if func returns error', { timeout: 7000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        const func = (callback) => {
            callback(new Error('PettyCache.fetchAndRefresh should return error if func returns error'));
        };

        pettyCache.fetchAndRefresh(key, func, { ttl: 6000 }, (err, data) => {
            assert(err);
            assert.strictEqual(err.message, 'PettyCache.fetchAndRefresh should return error if func returns error');
            assert(!data);

            setTimeout(() => {
                pettyCache.fetchAndRefresh(key, func, { ttl: 6000 }, (err, data) => {
                    assert(err);
                    assert.strictEqual(err.message, 'PettyCache.fetchAndRefresh should return error if func returns error');
                    assert(!data);

                    resolve();
                });
            }, 4001);
        });
    });
});

    t.test('PettyCache.fetchAndRefresh should not require options', async () => {
 await new Promise((resolve) => {
        pettyCache.fetchAndRefresh(Math.random().toString(), (callback) => {
            return callback(null, { foo: 'bar' });
        });

        resolve();
    });
});
});

    t.test('PettyCache.get', { concurrency: true }, async (t) => {
    t.test('PettyCache.get should return value', { timeout: 7000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        pettyCache.set(key, 'hello world', () => {
            pettyCache.get(key, (err, value) => {
                assert.equal(value, 'hello world');

                // Wait for memory cache to expire
                setTimeout(() => {
                    pettyCache.get(key, (err, value) => {
                        assert.equal(value, 'hello world');
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});

    t.test('PettyCache.get should return null for missing keys', async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        pettyCache.get(key, (err, value) => {
            assert.strictEqual(value, null);

            pettyCache.get(key, (err, value) => {
                assert.strictEqual(value, null);
                resolve();
            });
        });
    });
});
});

    t.test('PettyCache.mutex', { concurrency: true }, async (t) => {
    t.test('PettyCache.mutex.lock (callbacks)', { concurrency: true }, async (t) => {
        t.test('PettyCache.mutex.lock should lock for 1 second by default', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.mutex.lock(key, err => {
                assert.ifError(err);

                pettyCache.mutex.lock(key, err => {
                    assert(err);

                    setTimeout(() => {
                        pettyCache.mutex.lock(key, err => {
                            assert.ifError(err);
                            resolve();
                        });
                    }, 1001);
                });
            });
        });
});

        t.test('PettyCache.mutex.lock should lock for 2 seconds when ttl parameter is specified', { timeout: 3000 }, async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.mutex.lock(key, { ttl: 2000 }, err => {
                assert.ifError(err);

                pettyCache.mutex.lock(key, err => {
                    assert(err);

                    setTimeout(() => {
                        pettyCache.mutex.lock(key, err => {
                            assert(err);
                        });
                    }, 1001);

                    setTimeout(() => {
                        pettyCache.mutex.lock(key, err => {
                            assert.ifError(err);
                            resolve();
                        });
                    }, 2001);
                });
            });
        });
});

        t.test('PettyCache.mutex.lock should acquire a lock after retries', { timeout: 3000 }, async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.mutex.lock(key, { ttl: 2000 } , err => {
                assert.ifError(err);

                pettyCache.mutex.lock(key, err => {
                    assert(err);

                    pettyCache.mutex.lock(key, { retry: { interval: 500, times: 10 } }, err => {
                        assert.ifError(err);
                        resolve();
                    });
                });
            });
        });
});
    });

    t.test('PettyCache.mutex.lock (promises)', { concurrency: true }, async (t) => {
        t.test('PettyCache.mutex.lock should lock for 1 second by default', async () => {
            const key = Math.random().toString();

            await pettyCache.mutex.lock(key);

            try {
               await pettyCache.mutex.lock(key);
               assert.fail('Should have thrown an error');
            } catch(err) {
                assert.notStrictEqual(err.message, 'Should have thrown an error');
                assert(err);
            }

            await timers.setTimeout(1001);

            await pettyCache.mutex.lock(key);
        });

        t.test('PettyCache.mutex.lock should lock for 2 seconds when ttl parameter is specified', { timeout: 4000 }, async () => {
            const key = Math.random().toString();

            await pettyCache.mutex.lock(key, { ttl: 2000 });

            try {
                await pettyCache.mutex.lock(key);
                assert.fail('Should have thrown an error');
            } catch(err) {
                assert.notStrictEqual(err.message, 'Should have thrown an error');
                assert(err);
            }

            await timers.setTimeout(1001);

            try {
                await pettyCache.mutex.lock(key);
                assert.fail('Should have thrown an error');
            } catch(err) {
                assert.notStrictEqual(err.message, 'Should have thrown an error');
                assert(err);
            }

            await timers.setTimeout(1001);

            await pettyCache.mutex.lock(key);
        });

        t.test('PettyCache.mutex.lock should acquire a lock after retries', { timeout: 4000 }, async () => {
            const key = Math.random().toString();

            await pettyCache.mutex.lock(key, { ttl: 2000 });

            try {
                await pettyCache.mutex.lock(key);
                assert.fail('Should have thrown an error');
            } catch(err) {
                assert.notStrictEqual(err.message, 'Should have thrown an error');
                assert(err);
            }

            await pettyCache.mutex.lock(key, { retry: { interval: 500, times: 10 } });
        });
    });

    t.test('PettyCache.mutex.unlock (callbacks)', { concurrency: true }, async (t) => {
        t.test('PettyCache.mutex.unlock should unlock', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.mutex.lock(key, { ttl: 10000 }, err => {
                assert.ifError(err);

                pettyCache.mutex.lock(key, err => {
                    assert(err);

                    pettyCache.mutex.unlock(key, () => {
                        pettyCache.mutex.lock(key, err => {
                            assert.ifError(err);
                            resolve();
                        });
                    });
                });
            });
        });
});

        t.test('PettyCache.mutex.unlock should work without a callback', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.mutex.lock(key, { ttl: 10000 }, err => {
                assert.ifError(err);

                pettyCache.mutex.unlock(key);
                resolve();
            });
        });
});
    });

    t.test('PettyCache.mutex.unlock (promises)', async (t) => {
        t.test('PettyCache.mutex.unlock should unlock', async () => {
            const key = Math.random().toString();

            await pettyCache.mutex.lock(key, { ttl: 10000 });

            try {
                await pettyCache.mutex.lock(key);
                assert.fail('Should have thrown an error');
            } catch(err) {
                assert.notStrictEqual(err.message, 'Should have thrown an error');
                assert(err);
            }

            await pettyCache.mutex.unlock(key);
            await pettyCache.mutex.lock(key);
        });
    });
});

    t.test('PettyCache.patch', async (t) => {
    const key = Math.random().toString();

    await new Promise((resolve) => { pettyCache.set(key, { a: 1, b: 2, c: 3 }, resolve); });

    t.test('PettyCache.patch should fail if the key does not exist', async () => {
 await new Promise((resolve) => {
        pettyCache.patch('xyz', { b: 3 }, (err) => {
            assert(err, 'No error provided');
            resolve();
        });
    });
});

    t.test('PettyCache.patch should update the values of given object keys', async () => {
 await new Promise((resolve) => {
        pettyCache.patch(key, { b: 4, c: 5 }, (err) => {
            assert(!err, 'Error: ' + err);

            pettyCache.get(key, (err, data) => {
                assert(!err, 'Error: ' + err);
                assert.deepEqual(data, { a: 1, b: 4, c: 5 });
                resolve();
            });
        });
    });
});

    t.test('PettyCache.patch should update the values of given object keys with options', async () => {
 await new Promise((resolve) => {
        pettyCache.patch(key, { b: 5, c: 6 }, { ttl: 10000 }, (err) => {
            assert(!err, 'Error: ' + err);

            pettyCache.get(key, (err, data) => {
                assert(!err, 'Error: ' + err);
                assert.deepEqual(data, { a: 1, b: 5, c: 6 });
                resolve();
            });
        });
    });
});
});

    t.test('PettyCache.semaphore', { concurrency: true }, async (t) => {
    t.test('PettyCache.semaphore.acquireLock', { concurrency: true }, async (t) => {
        t.test('should aquire a lock', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 10 }, (err) => {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, (err, index) => {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.acquireLock(key, (err, index) => {
                        assert.ifError(err);
                        assert.equal(index, 1);
                        resolve();
                    });
                });
            });
        });
});

        t.test('should not aquire a lock', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, (err) => {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, (err, index) => {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.acquireLock(key, (err) => {
                        assert(err);
                        resolve();
                    });
                });
            });
        });
});

        t.test('should aquire a lock after ttl', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, (err) => {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, (err, index) => {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.acquireLock(key, (err) => {
                        assert(err);

                        setTimeout(() => {
                            pettyCache.semaphore.acquireLock(key, (err, index) => {
                                assert.ifError(err);
                                assert.equal(index, 0);
                                resolve();
                            });
                        }, 1001);
                    });
                });
            });
        });
});

        t.test('should aquire a lock with specified options', { timeout: 5000 }, async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 10 }, (err) => {
                assert.ifError(err);

                // callback is optional
                pettyCache.semaphore.acquireLock(key);

                setTimeout(() => {
                    pettyCache.semaphore.acquireLock(key, { retry: { interval: 500, times: 10 }, ttl: 500 }, (err, index) => {
                        assert.ifError(err);
                        assert.equal(index, 1);
                        resolve();
                    });
                }, 1000);
            });
        });
});

        t.test('should fail if the semaphore does not exist', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.acquireLock(key, 0, (err) => {
                assert(err);
                assert.strictEqual(err.message, `Semaphore ${key} doesn't exist.`);
                resolve();
            });
        });
});
    });

    t.test('PettyCache.semaphore.consumeLock', { concurrency: true }, async (t) => {
        t.test('should consume a lock', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err) => {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, (err, index) => {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.acquireLock(key, (err, index) => {
                        assert.ifError(err);
                        assert.equal(index, 1);

                        pettyCache.semaphore.acquireLock(key, (err) => {
                            assert(err);

                            pettyCache.semaphore.consumeLock(key, 0, (err) => {
                                assert.ifError(err);

                                pettyCache.semaphore.acquireLock(key, (err) => {
                                    assert(err);
                                    resolve();
                                });
                            });
                        });
                    });
                });
            });
        });
});

        t.test('should ensure at least one lock is not consumed', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err) => {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, (err, index) => {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.acquireLock(key, (err, index) => {
                        assert.ifError(err);
                        assert.equal(index, 1);

                        pettyCache.semaphore.acquireLock(key, (err) => {
                            assert(err);

                            pettyCache.semaphore.consumeLock(key, 0, (err) => {
                                assert.ifError(err);

                                pettyCache.semaphore.consumeLock(key, 1, (err) => {
                                    assert.ifError(err);

                                    pettyCache.semaphore.acquireLock(key, (err) => {
                                        assert.ifError(err);
                                        assert.equal(index, 1);
                                        resolve();
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
});

        t.test('should fail if the semaphore does not exist', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.consumeLock(key, 0, (err) => {
                assert(err);
                assert.strictEqual(err.message, `Semaphore ${key} doesn't exist.`);
                resolve();
            });
        });
});

        t.test('should fail if index is larger than semaphore', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err) => {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, (err, index) => {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.consumeLock(key, 10, (err) => {
                        assert(err);
                        assert.strictEqual(err.message, `Index 10 for semaphore ${key} is invalid.`);
                        resolve();
                    });
                });
            });
        });
});

        t.test('callback is optional', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err) => {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, (err, index) => {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.acquireLock(key, (err, index) => {
                        assert.ifError(err);
                        assert.equal(index, 1);

                        pettyCache.semaphore.acquireLock(key, (err) => {
                            assert(err);

                            pettyCache.semaphore.consumeLock(key, 0);

                            pettyCache.semaphore.acquireLock(key, (err) => {
                                assert(err);
                                resolve();
                            });
                        });
                    });
                });
            });
        });
});
    });

    t.test('PettyCache.semaphore.expand', { concurrency: true }, async (t) => {
        t.test('should increase the size of a semaphore pool', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err, pool) => {
                assert.ifError(err);
                assert.strictEqual(pool.length, 2);

                pettyCache.semaphore.expand(key, 3, (err) => {
                    assert.ifError(err);

                    pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err, pool) => {
                        assert.ifError(err);
                        assert.strictEqual(pool.length, 3);
                        resolve();
                    });
                });
            });
        });
});

        t.test('should refuse to shrink a pool', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err, pool) => {
                assert.ifError(err);
                assert.strictEqual(pool.length, 2);

                pettyCache.semaphore.expand(key, 1, (err) => {
                    assert(err);
                    assert.strictEqual(err.message, 'Cannot shrink pool, size is 2 and you requested a size of 1.');
                    resolve();
                });
            });
        });
});

        t.test('should succeed if pool size is already equal to the specified size', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err, pool) => {
                assert.ifError(err);
                assert.strictEqual(pool.length, 2);

                pettyCache.semaphore.expand(key, 2, (err) => {
                    assert.ifError(err);

                    pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err, pool) => {
                        assert.ifError(err);
                        assert.strictEqual(pool.length, 2);
                        resolve();
                    });
                });
            });
        });
});

        t.test('callback is optional', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err, pool) => {
                assert.ifError(err);
                assert.strictEqual(pool.length, 2);

                pettyCache.semaphore.expand(key, 3);

                pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err, pool) => {
                    assert.ifError(err);
                    assert.strictEqual(pool.length, 3);
                    resolve();
                });
            });
        });
});

        t.test('should fail if the semaphore does not exist', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.expand(key, 10, (err) => {
                assert(err);
                assert.strictEqual(err.message, `Semaphore ${key} doesn't exist.`);
                resolve();
            });
        });
});
    });

    t.test('PettyCache.semaphore.releaseLock', { concurrency: true }, async (t) => {
        t.test('should release a lock', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, (err) => {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, (err, index) => {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.acquireLock(key, (err) => {
                        assert(err);

                        pettyCache.semaphore.releaseLock(key, 0, (err) => {
                            assert.ifError(err);

                            pettyCache.semaphore.acquireLock(key, (err, index) => {
                                assert.ifError(err);
                                assert.equal(index, 0);
                                resolve();
                            });
                        });
                    });
                });
            });
        });
});

        t.test('should fail to release a lock outside of the semaphore size', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, (err) => {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, (err, index) => {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.releaseLock(key, 10, (err) => {
                        assert(err);
                        assert.strictEqual(err.message, `Index 10 for semaphore ${key} is invalid.`);
                        resolve();
                    });
                });
            });
        });
});

        t.test('callback is optional', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, (err) => {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, (err, index) => {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.acquireLock(key, (err) => {
                        assert(err);

                        pettyCache.semaphore.releaseLock(key, 0);

                        pettyCache.semaphore.acquireLock(key, (err, index) => {
                            assert.ifError(err);
                            assert.equal(index, 0);
                            resolve();
                        });
                    });
                });
            });
        });
});

        t.test('should fail if the semaphore does not exist', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.releaseLock(key, 10, (err) => {
                assert(err);
                assert.strictEqual(err.message, `Semaphore ${key} doesn't exist.`);
                resolve();
            });
        });
});
    });

    t.test('PettyCache.semaphore.reset', { concurrency: true }, async (t) => {
        t.test('should reset all locks', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err) => {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, (err, index) => {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.acquireLock(key, (err, index) => {
                        assert.ifError(err);
                        assert.equal(index, 1);

                        pettyCache.semaphore.acquireLock(key, (err) => {
                            assert(err);

                            pettyCache.semaphore.reset(key, (err) => {
                                assert.ifError(err);

                                pettyCache.semaphore.acquireLock(key, (err, index) => {
                                    assert.ifError(err);
                                    assert.equal(index, 0);
                                    resolve();
                                });
                            });
                        });
                    });
                });
            });
        });
});

        t.test('callback is optional', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err) => {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, (err, index) => {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.acquireLock(key, (err, index) => {
                        assert.ifError(err);
                        assert.equal(index, 1);

                        pettyCache.semaphore.acquireLock(key, (err) => {
                            assert(err);

                            pettyCache.semaphore.reset(key);

                            pettyCache.semaphore.acquireLock(key, (err, index) => {
                                assert.ifError(err);
                                assert.equal(index, 0);
                                resolve();
                            });
                        });
                    });
                });
            });
        });
});

        t.test('should fail if the semaphore does not exist', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.reset(key, (err) => {
                assert(err);
                assert.strictEqual(err.message, `Semaphore ${key} doesn't exist.`);
                resolve();
            });
        });
});
    });

    t.test('PettyCache.semaphore.retrieveOrCreate', { concurrency: true }, async (t) => {
        t.test('should create a new semaphore', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 100 }, (err, semaphore) => {
                assert.ifError(err);
                assert(semaphore);
                assert.equal(semaphore.length, 100);
                assert(semaphore.every(s => s.status === 'available'));

                pettyCache.semaphore.retrieveOrCreate(key, (err, semaphore) => {
                    assert.ifError(err);
                    assert(semaphore);
                    assert.equal(semaphore.length, 100);
                    assert(semaphore.every(s => s.status === 'available'));
                    resolve();
                });
            });
        });
});

        t.test('should have a min size of 1', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 0 }, (err, semaphore) => {
                assert.ifError(err);
                assert(semaphore);
                assert.equal(semaphore.length, 1);
                assert(semaphore.every(s => s.status === 'available'));

                pettyCache.semaphore.retrieveOrCreate(key, (err, semaphore) => {
                    assert.ifError(err);
                    assert(semaphore);
                    assert.equal(semaphore.length, 1);
                    assert(semaphore.every(s => s.status === 'available'));
                    resolve();
                });
            });
        });
});

        t.test('should allow options.size to provide a function', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: (callback) => callback(null, 1 + 1) }, (err, semaphore) => {
                assert.ifError(err);
                assert(semaphore);
                assert.equal(semaphore.length, 2);
                assert(semaphore.every(s => s.status === 'available'));

                pettyCache.semaphore.retrieveOrCreate(key, (err, semaphore) => {
                    assert.ifError(err);
                    assert(semaphore);
                    assert.equal(semaphore.length, 2);
                    assert(semaphore.every(s => s.status === 'available'));
                    resolve();
                });
            });
        });
});

        t.test('callback is optional', async () => {
 await new Promise((resolve) => {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key);

            pettyCache.semaphore.retrieveOrCreate(key, { size: 100 }, (err, semaphore) => {
                assert.ifError(err);
                assert(semaphore);
                assert.equal(semaphore.length, 1);
                assert(semaphore.every(s => s.status === 'available'));
                resolve();
            });
        });
});
    });
});

    t.test('PettyCache.set', { concurrency: true }, async (t) => {
    t.test('PettyCache.set should set a value', { timeout: 7000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        pettyCache.set(key, 'hello world', () => {
            pettyCache.get(key, (err, value) => {
                assert.equal(value, 'hello world');

                // Wait for memory cache to expire
                setTimeout(() => {
                    pettyCache.get(key, (err, value) => {
                        assert.equal(value, 'hello world');
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});

    t.test('PettyCache.set should work without a callback', async () => {
 await new Promise((resolve) => {
        pettyCache.set(Math.random().toString(), 'hello world');
        resolve();
    });
});

    t.test('PettyCache.set should set a value with the specified TTL option', { timeout: 7000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        pettyCache.set(key, 'hello world', { ttl: 6000 },() => {
            pettyCache.get(key, (err, value) => {
                assert.equal(value, 'hello world');

                // Wait for memory cache to expire
                setTimeout(() => {
                    pettyCache.get(key, (err, value) => {
                        assert.equal(value, null);
                        resolve();
                    });
                }, 6001);
            });
        });
    });
});

    t.test('PettyCache.set should set a value with the specified TTL option using max and min', { timeout: 10000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        pettyCache.set(key, 'hello world', { ttl: { max: 7000, min: 6000 } },() => {
            pettyCache.get(key, (err, value) => {
                assert.strictEqual(value, 'hello world');

                // Get again before cache expires
                setTimeout(() => {
                    pettyCache.get(key, (err, value) => {
                        assert.strictEqual(value, 'hello world');

                        // Wait for memory cache to expire
                        setTimeout(() => {
                            pettyCache.get(key, (err, value) => {
                                assert.strictEqual(value, null);
                                resolve();
                            });
                        }, 6001);
                    });
                }, 1000);
            });
        });
    });
});

    t.test('PettyCache.set should set a value with the specified TTL option using min only', { timeout: 10000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        pettyCache.set(key, 'hello world', { ttl: { min: 6000 } },() => {
            pettyCache.get(key, (err, value) => {
                assert.strictEqual(value, 'hello world');
                resolve();
            });
        });
    });
});

    t.test('PettyCache.set should set a value with the specified TTL option using max only', { timeout: 10000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        pettyCache.set(key, 'hello world', { ttl: { max: 10000 } },() => {
            pettyCache.get(key, (err, value) => {
                assert.strictEqual(value, 'hello world');
                resolve();
            });
        });
    });
});

    t.test('PettyCache.set(key, \'\')', { timeout: 11000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        pettyCache.set(key, '', { ttl: 7000 }, (err) => {
            assert.ifError(err);

            pettyCache.get(key, (err, value) => {
                assert.ifError(err);
                assert.strictEqual(value, '');

                // Wait for memory cache to expire
                setTimeout(() => {
                    pettyCache.get(key, (err, value) => {
                        assert.ifError(err);
                        assert.strictEqual(value, '');

                        // Wait for memory cache and Redis cache to expire
                        setTimeout(() => {
                            pettyCache.get(key, (err, value) => {
                                assert.ifError(err);
                                assert.strictEqual(value, null);
                                resolve();
                            });
                        }, 5001);
                    });
                }, 5001);
            });
        });
    });
});

    t.test('PettyCache.set(key, 0)', { timeout: 11000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        pettyCache.set(key, 0, { ttl: 7000 }, (err) => {
            assert.ifError(err);

            pettyCache.get(key, (err, value) => {
                assert.ifError(err);
                assert.strictEqual(value, 0);

                // Wait for memory cache to expire
                setTimeout(() => {
                    pettyCache.get(key, (err, value) => {
                        assert.ifError(err);
                        assert.strictEqual(value, 0);

                        // Wait for memory cache and Redis cache to expire
                        setTimeout(() => {
                            pettyCache.get(key, (err, value) => {
                                assert.ifError(err);
                                assert.strictEqual(value, null);
                                resolve();
                            });
                        }, 5001);
                    });
                }, 5001);
            });
        });
    });
});

    t.test('PettyCache.set(key, false)', { timeout: 11000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        pettyCache.set(key, false, { ttl: 7000 }, (err) => {
            assert.ifError(err);

            pettyCache.get(key, (err, value) => {
                assert.ifError(err);
                assert.strictEqual(value, false);

                // Wait for memory cache to expire
                setTimeout(() => {
                    pettyCache.get(key, (err, value) => {
                        assert.ifError(err);
                        assert.strictEqual(value, false);

                        // Wait for memory cache and Redis cache to expire
                        setTimeout(() => {
                            pettyCache.get(key, (err, value) => {
                                assert.ifError(err);
                                assert.strictEqual(value, null);
                                resolve();
                            });
                        }, 5001);
                    });
                }, 5001);
            });
        });
    });
});

    t.test('PettyCache.set(key, NaN)', { timeout: 11000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        pettyCache.set(key, NaN, { ttl: 7000 }, (err) => {
            assert.ifError(err);

            pettyCache.get(key, (err, value) => {
                assert.ifError(err);
                assert(typeof value === 'number' && isNaN(value));

                // Wait for memory cache to expire
                setTimeout(() => {
                    pettyCache.get(key, (err, value) => {
                        assert.ifError(err);
                        assert(typeof value === 'number' && isNaN(value));

                        // Wait for memory cache and Redis cache to expire
                        setTimeout(() => {
                            pettyCache.get(key, (err, value) => {
                                assert.ifError(err);
                                assert.strictEqual(value, null);
                                resolve();
                            });
                        }, 5001);
                    });
                }, 5001);
            });
        });
    });
});

    t.test('PettyCache.set(key, null)', { timeout: 11000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        pettyCache.set(key, null, { ttl: 7000 }, (err) => {
            assert.ifError(err);

            pettyCache.get(key, (err, value) => {
                assert.ifError(err);
                assert.strictEqual(value, null);

                // Wait for memory cache to expire
                setTimeout(() => {
                    pettyCache.get(key, (err, value) => {
                        assert.ifError(err);
                        assert.strictEqual(value, null);

                        // Wait for memory cache and Redis cache to expire
                        setTimeout(() => {
                            pettyCache.get(key, (err, value) => {
                                assert.ifError(err);
                                assert.strictEqual(value, null);
                                resolve();
                            });
                        }, 5001);
                    });
                }, 5001);
            });
        });
    });
});

    t.test('PettyCache.set(key, undefined)', { timeout: 11000 }, async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        pettyCache.set(key, undefined, { ttl: 7000 }, (err) => {
            assert.ifError(err);

            pettyCache.get(key, (err, value) => {
                assert.ifError(err);
                assert.strictEqual(value, undefined);

                // Wait for memory cache to expire
                setTimeout(() => {
                    pettyCache.get(key, (err, value) => {
                        assert.ifError(err);
                        assert.strictEqual(value, undefined);

                        // Wait for memory cache and Redis cache to expire
                        setTimeout(() => {
                            pettyCache.get(key, (err, value) => {
                                assert.ifError(err);
                                assert.strictEqual(value, null);
                                resolve();
                            });
                        }, 5001);
                    });
                }, 5001);
            });
        });
    });
});
});

    t.test('redisClient', { concurrency: true }, async (t) => {
    t.test('redisClient.mget(falsy keys)', async () => {
 await new Promise((resolve) => {
        const key1 = Math.random().toString();
        const key2 = Math.random().toString();
        const key3 = Math.random().toString();
        const key4 = Math.random().toString();
        const key5 = Math.random().toString();
        const key6 = Math.random().toString();
        const values = {};

        values[key1] = '';
        values[key2] = 0;
        values[key3] = false;
        values[key4] = NaN;
        values[key5] = null;
        values[key6] = undefined;

        async.each(Object.keys(values), (key, callback) => {
            redisClient.psetex(key, 1000, PettyCache.stringify(values[key]), callback);
        }, (err) => {
            assert.ifError(err);

            const keys = Object.keys(values);

            // Add an additional key to check handling of missing keys
            keys.push(Math.random().toString());

            redisClient.mget(keys, (err, data) => {
                assert.ifError(err);
                assert.strictEqual(data.length, 7);
                assert.strictEqual(data[0], '""');
                assert.strictEqual(PettyCache.parse(data[0]), '');
                assert.strictEqual(data[1], '0');
                assert.strictEqual(PettyCache.parse(data[1]), 0);
                assert.strictEqual(data[2], 'false');
                assert.strictEqual(PettyCache.parse(data[2]), false);
                assert.strictEqual(data[3], '"__NaN"');
                assert.strictEqual(typeof PettyCache.parse(data[3]), 'number');
                assert(isNaN(PettyCache.parse(data[3])));
                assert.strictEqual(data[4], '"__null"');
                assert.strictEqual(PettyCache.parse(data[4]), null);
                assert.strictEqual(data[5], '"__undefined"');
                assert.strictEqual(PettyCache.parse(data[5]), undefined);
                assert.strictEqual(data[6], null);
                resolve();
            });
        });
    });
});

    t.test('redisClient.psetex(key, \'\')', async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        redisClient.psetex(key, 1000, PettyCache.stringify(''), (err) => {
            assert.ifError(err);

            redisClient.get(key, (err, data) => {
                assert.ifError(err);
                assert.strictEqual(data, '""');
                assert.strictEqual(PettyCache.parse(data), '');

                // Wait for Redis cache to expire
                setTimeout(() => {
                    redisClient.get(key, (err, data) => {
                        assert.ifError(err);
                        assert.strictEqual(data, null);
                        resolve();
                    });
                }, 1001);
            });
        });
    });
});

    t.test('redisClient.psetex(key, 0)', async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        redisClient.psetex(key, 1000, PettyCache.stringify(0), (err) => {
            assert.ifError(err);

            redisClient.get(key, (err, data) => {
                assert.ifError(err);
                assert.strictEqual(data, '0');
                assert.strictEqual(PettyCache.parse(data), 0);

                // Wait for Redis cache to expire
                setTimeout(() => {
                    redisClient.get(key, (err, data) => {
                        assert.ifError(err);
                        assert.strictEqual(data, null);
                        resolve();
                    });
                }, 1001);
            });
        });
    });
});

    t.test('redisClient.psetex(key, false)', async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        redisClient.psetex(key, 1000, PettyCache.stringify(false), (err) => {
            assert.ifError(err);

            redisClient.get(key, (err, data) => {
                assert.ifError(err);
                assert.strictEqual(data, 'false');
                assert.strictEqual(PettyCache.parse(data), false);

                // Wait for Redis cache to expire
                setTimeout(() => {
                    redisClient.get(key, (err, data) => {
                        assert.ifError(err);
                        assert.strictEqual(data, null);
                        resolve();
                    });
                }, 1001);
            });
        });
    });
});

    t.test('redisClient.psetex(key, NaN)', async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        redisClient.psetex(key, 1000, PettyCache.stringify(NaN), (err) => {
            assert.ifError(err);

            redisClient.get(key, (err, data) => {
                assert.ifError(err);
                assert.strictEqual(data, '"__NaN"');
                assert(isNaN(PettyCache.parse(data)));

                // Wait for Redis cache to expire
                setTimeout(() => {
                    redisClient.get(key, (err, data) => {
                        assert.ifError(err);
                        assert.strictEqual(data, null);
                        resolve();
                    });
                }, 1001);
            });
        });
    });
});

    t.test('redisClient.psetex(key, null)', async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        redisClient.psetex(key, 1000, PettyCache.stringify(null), (err) => {
            assert.ifError(err);

            redisClient.get(key, (err, data) => {
                assert.ifError(err);
                assert.strictEqual(data, '"__null"');
                assert.strictEqual(PettyCache.parse(data), null);

                // Wait for Redis cache to expire
                setTimeout(() => {
                    redisClient.get(key, (err, data) => {
                        assert.ifError(err);
                        assert.strictEqual(data, null);
                        resolve();
                    });
                }, 1001);
            });
        });
    });
});

    t.test('redisClient.psetex(key, undefined)', async () => {
 await new Promise((resolve) => {
        const key = Math.random().toString();

        redisClient.psetex(key, 1000, PettyCache.stringify(undefined), (err) => {
            assert.ifError(err);

            redisClient.get(key, (err, data) => {
                assert.ifError(err);
                assert.strictEqual(data, '"__undefined"');
                assert.strictEqual(PettyCache.parse(data), undefined);

                // Wait for Redis cache to expire
                setTimeout(() => {
                    redisClient.get(key, (err, data) => {
                        assert.ifError(err);
                        assert.strictEqual(data, null);
                        resolve();
                    });
                }, 1001);
            });
        });
    });
});
});

    t.test('Benchmark', async (t) => {
    const emojis = require('./emojis.json');

    t.test('PettyCache should be faster than node-redis', async () => {
 await new Promise((resolve) => {
        let pettyCacheEnd;
        const pettyCacheKey = Math.random().toString();
        let pettyCacheStart;
        let redisEnd;
        const redisKey = Math.random().toString();
        const redisStart = Date.now();

        redisClient.psetex(redisKey, 30000, JSON.stringify(emojis), (err) => {
            assert.ifError(err);

            async.times(500, (n, callback) => {
                redisClient.get(redisKey, (err, data) => {
                    if (err) {
                        return callback(err);
                    }

                    callback(null, JSON.parse(data));
                });
            }, (err) => {
                redisEnd = Date.now();
                assert.ifError(err);
                pettyCacheStart = Date.now();

                pettyCache.set(pettyCacheKey, emojis, (err) => {
                    assert.ifError(err);

                    async.times(500, (n, callback) => {
                        pettyCache.get(pettyCacheKey, (err, data) => {
                            if (err) {
                                return callback(err);
                            }

                            callback(null, data);
                        });
                    }, (err) => {
                        pettyCacheEnd = Date.now();
                        assert.ifError(err);
                        assert(pettyCacheEnd - pettyCacheStart < redisEnd - redisStart);
                        resolve();
                    });
                });
            });
        });
    });
});
});
});
