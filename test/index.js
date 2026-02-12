const { before, describe, it } = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const async = require('async');
const memoryCache = require('memory-cache');
const redis = require('redis');

const PettyCache = require('../index.js');

const redisClient = redis.createClient();
const pettyCache = new PettyCache(redisClient);

describe('new PettyCache()', function() {
    it('new PettyCache()', { timeout: 7000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();
        const newPettyCache = new PettyCache();

        newPettyCache.fetch(key, function(callback) {
            return callback(null, { foo: 'bar' });
        }, function() {
            newPettyCache.fetch(key, function() {
                throw 'This function should not be called';
            }, function(err, data) {
                assert.equal(data.foo, 'bar');

                // Wait for memory cache to expire
                setTimeout(function() {
                    newPettyCache.fetch(key, function() {
                        throw 'This function should not be called';
                    }, function(err, data) {
                        assert.strictEqual(data.foo, 'bar');
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});

    it('new PettyCache(port, host)', { timeout: 7000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();
        const newPettyCache = new PettyCache(6379, 'localhost');

        newPettyCache.fetch(key, function(callback) {
            return callback(null, { foo: 'bar' });
        }, function() {
            newPettyCache.fetch(key, function() {
                throw 'This function should not be called';
            }, function(err, data) {
                assert.equal(data.foo, 'bar');

                // Wait for memory cache to expire
                setTimeout(function() {
                    newPettyCache.fetch(key, function() {
                        throw 'This function should not be called';
                    }, function(err, data) {
                        assert.strictEqual(data.foo, 'bar');
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});

    it('new PettyCache(redisClient)', { timeout: 7000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();
        const redisClient = redis.createClient();
        const newPettyCache = new PettyCache(redisClient);

        newPettyCache.fetch(key, function(callback) {
            return callback(null, { foo: 'bar' });
        }, function() {
            newPettyCache.fetch(key, function() {
                throw 'This function should not be called';
            }, function(err, data) {
                assert.equal(data.foo, 'bar');

                // Wait for memory cache to expire
                setTimeout(function() {
                    newPettyCache.fetch(key, function() {
                        throw 'This function should not be called';
                    }, function(err, data) {
                        assert.strictEqual(data.foo, 'bar');
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});
});

describe('memory-cache', function() {
    it('memoryCache.put(key, \'\')', async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        memoryCache.put(key, '', 1000);
        assert(memoryCache.keys().includes(key));
        assert.strictEqual(memoryCache.get(key), '');

        // Wait for memory cache to expire
        setTimeout(function() {
            assert(!memoryCache.keys().includes(key));
            assert.strictEqual(memoryCache.get(key), null);
            resolve();
        }, 1001);
    });
});

    it('memoryCache.put(key, 0)', async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        memoryCache.put(key, 0, 1000);
        assert(memoryCache.keys().includes(key));
        assert.strictEqual(memoryCache.get(key), 0);

        // Wait for memory cache to expire
        setTimeout(function() {
            assert(!memoryCache.keys().includes(key));
            assert.strictEqual(memoryCache.get(key), null);
            resolve();
        }, 1001);
    });
});

    it('memoryCache.put(key, false)', async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        memoryCache.put(key, false, 1000);
        assert(memoryCache.keys().includes(key));
        assert.strictEqual(memoryCache.get(key), false);

        // Wait for memory cache to expire
        setTimeout(function() {
            assert(!memoryCache.keys().includes(key));
            assert.strictEqual(memoryCache.get(key), null);
            resolve();
        }, 1001);
    });
});

    it('memoryCache.put(key, NaN)', async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        memoryCache.put(key, NaN, 1000);
        assert(memoryCache.keys().includes(key));
        assert(isNaN(memoryCache.get(key)));

        // Wait for memory cache to expire
        setTimeout(function() {
            assert(!memoryCache.keys().includes(key));
            assert.strictEqual(memoryCache.get(key), null);
            resolve();
        }, 1001);
    });
});

    it('memoryCache.put(key, null)', async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        memoryCache.put(key, null, 1000);
        assert(memoryCache.keys().includes(key));
        assert.strictEqual(memoryCache.get(key), null);

        // Wait for memory cache to expire
        setTimeout(function() {
            assert(!memoryCache.keys().includes(key));
            assert.strictEqual(memoryCache.get(key), null);
            resolve();
        }, 1001);
    });
});

    it('memoryCache.put(key, undefined)', async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        memoryCache.put(key, undefined, 1000);
        assert(memoryCache.keys().includes(key));
        assert.strictEqual(memoryCache.get(key), undefined);

        // Wait for memory cache to expire
        setTimeout(function() {
            assert(!memoryCache.keys().includes(key));
            assert.strictEqual(memoryCache.get(key), null);
            resolve();
        }, 1001);
    });
});
});

describe('PettyCache.bulkFetch', function() {
    it('PettyCache.bulkFetch', { timeout: 7000 }, async function() {
 await new Promise(function(resolve) {
        pettyCache.set('a', 1, function() {
            pettyCache.set('b', '2', function() {
                pettyCache.bulkFetch(['a', 'b', 'c', 'd'], function(keys, callback) {
                    assert(keys.length === 2);

                    callback(null, { 'c': [3], 'd': { num: 4 } });
                }, function(err, values) {
                    assert.strictEqual(values.a, 1);
                    assert.strictEqual(values.b, '2');
                    assert.strictEqual(values.c[0], 3);
                    assert.strictEqual(values.d.num, 4);

                    // Call bulkFetch again to ensure memory serialization is working as expected.
                    pettyCache.bulkFetch(['a', 'b', 'c', 'd'], function() {
                        throw 'This function should not be called';
                    }, function(err, values) {
                        assert.strictEqual(values.a, 1);
                        assert.strictEqual(values.b, '2');
                        assert.strictEqual(values.c[0], 3);
                        assert.strictEqual(values.d.num, 4);

                        // Wait for memory cache to expire
                        setTimeout(function() {
                            pettyCache.bulkFetch(['a', 'b', 'c', 'd'], function() {
                                throw 'This function should not be called';
                            }, function(err, values) {
                                assert.strictEqual(values.a, 1);
                                assert.strictEqual(values.b, '2');
                                assert.strictEqual(values.c[0], 3);
                                assert.strictEqual(values.d.num, 4);

                                // Call bulkFetch again to ensure memory serialization is working as expected.
                                pettyCache.bulkFetch(['a', 'b', 'c', 'd'], function() {
                                    throw 'This function should not be called';
                                }, function(err, values) {
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

    it('PettyCache.bulkFetch should cache null values returned by func', { timeout: 7000 }, async function() {
 await new Promise(function(resolve) {
        const key1 = Math.random().toString();
        const key2 = Math.random().toString();

        pettyCache.bulkFetch([key1, key2], function(keys, callback) {
            assert.strictEqual(keys.length, 2);
            assert(keys.some(k => k === key1));
            assert(keys.some(k => k === key2));

            const values = {};

            values[key1] = '1';
            values[key2] = null;

            callback(null, values);
        }, function(err) {
            assert.ifError(err);

            pettyCache.bulkFetch([key1, key2], function() {
                throw 'This function should not be called';
            }, function(err, data) {
                assert.strictEqual(Object.keys(data).length, 2);
                assert.strictEqual(data[key1], '1');
                assert.strictEqual(data[key2], null);

                // Wait for memory cache to expire
                setTimeout(function() {
                    pettyCache.bulkFetch([key1, key2], function() {
                        throw 'This function should not be called';
                    }, function(err, data) {
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

    it('PettyCache.bulkFetch should return empty object when no keys are passed', async function() {
 await new Promise(function(resolve) {
        pettyCache.bulkFetch([], function() {
            throw 'This function should not be called';
        }, function(err, values) {
            assert.ifError(err);
            assert.deepEqual(values, {});
            resolve();
        });
    });
});

    it('PettyCache.bulkFetch should return error if func returns error', async function() {
 await new Promise(function(resolve) {
        pettyCache.bulkFetch([Math.random().toString()], function(keys, callback) {
            callback(new Error('PettyCache.bulkFetch should return error if func returns error'));
        }, function(err, values) {
            assert(err);
            assert.strictEqual(err.message, 'PettyCache.bulkFetch should return error if func returns error');
            assert(!values);
            resolve();
        });
    });
});

    it('PettyCache.bulkFetch should run func again after TTL', { timeout: 7000 }, async function() {
 await new Promise(function(resolve) {
        const keys = [Math.random().toString(), Math.random().toString()];
        let numberOfFuncCalls = 0;

        const func = function(keys, callback) {
            numberOfFuncCalls++;

            const results = {};
            results[keys[0]] = numberOfFuncCalls;
            results[keys[1]] = numberOfFuncCalls;

            callback(null, results);
        };

        pettyCache.bulkFetch(keys, func, { ttl: 6000 }, function(err, results) {
            assert.ifError(err);
            assert.strictEqual(results[keys[0]], 1);
            assert.strictEqual(results[keys[1]], 1);

            pettyCache.bulkGet(keys, function(err, results) {
                assert.ifError(err);
                assert.strictEqual(results[keys[0]], 1);
                assert.strictEqual(results[keys[1]], 1);
            });

            setTimeout(function() {
                pettyCache.bulkGet(keys, function(err, results) {
                    assert.ifError(err);
                    assert.strictEqual(results[keys[0]], null);
                    assert.strictEqual(results[keys[1]], null);

                    pettyCache.bulkFetch(keys, func, { ttl: 6000 }, function(err, results) {
                        assert.ifError(err);
                        assert.strictEqual(results[keys[0]], 2);
                        assert.strictEqual(results[keys[1]], 2);

                        pettyCache.bulkGet(keys, function(err, results) {
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

describe('PettyCache.bulkGet', function() {
    it('PettyCache.bulkGet should return values', { timeout: 6000 }, async function() {
 await new Promise(function(resolve) {
        const key1 = Math.random().toString();
        const key2 = Math.random().toString();
        const key3 = Math.random().toString();

        pettyCache.set(key1, '1', function() {
            pettyCache.set(key2, '2', function() {
                pettyCache.set(key3, '3', function() {
                    pettyCache.bulkGet([key1, key2, key3], function(err, values) {
                        assert.strictEqual(Object.keys(values).length, 3);
                        assert.strictEqual(values[key1], '1');
                        assert.strictEqual(values[key2], '2');
                        assert.strictEqual(values[key3], '3');

                        // Call bulkGet again while values are still in memory cache
                        pettyCache.bulkGet([key1, key2, key3], function(err, values) {
                            assert.strictEqual(Object.keys(values).length, 3);
                            assert.strictEqual(values[key1], '1');
                            assert.strictEqual(values[key2], '2');
                            assert.strictEqual(values[key3], '3');

                            // Wait for memory cache to expire
                            setTimeout(function() {
                                // Ensure keys are still in Redis
                                pettyCache.bulkGet([key1, key2, key3], function(err, values) {
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

    it('PettyCache.bulkGet should return null for missing keys', { timeout: 6000 }, async function() {
 await new Promise(function(resolve) {
        const key1 = Math.random().toString();
        const key2 = Math.random().toString();
        const key3 = Math.random().toString();

        pettyCache.set(key1, '1', function() {
            pettyCache.set(key2, '2', function() {
                pettyCache.bulkGet([key1, key2, key3], function(err, values) {
                    assert.strictEqual(Object.keys(values).length, 3);
                    assert.strictEqual(values[key1], '1');
                    assert.strictEqual(values[key2], '2');
                    assert.strictEqual(values[key3], null);

                    // Call bulkGet again while values are still in memory cache
                    pettyCache.bulkGet([key1, key2, key3], function(err, values) {
                        assert.strictEqual(Object.keys(values).length, 3);
                        assert.strictEqual(values[key1], '1');
                        assert.strictEqual(values[key2], '2');
                        assert.strictEqual(values[key3], null);

                        // Wait for memory cache to expire
                        setTimeout(function() {
                            // Ensure keys are still in Redis
                            pettyCache.bulkGet([key1, key2, key3], function(err, values) {
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

    it('PettyCache.bulkGet should correctly handle falsy values', { timeout: 12000 }, async function() {
 await new Promise(function(resolve) {
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

        async.each(Object.keys(values), function(key, callback) {
            pettyCache.set(key, values[key], { ttl: 6000 }, callback);
        }, function(err) {
            assert.ifError(err);

            const keys = Object.keys(values);

            // Add an additional key to check handling of missing keys
            const key7 = Math.random().toString();
            keys.push(key7);

            pettyCache.bulkGet(keys, function(err, data) {
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
                setTimeout(function() {
                    // Ensure keys are still in Redis
                    pettyCache.bulkGet(keys, function(err, data) {
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
                        setTimeout(function() {
                            // Ensure keys are not in Redis
                            pettyCache.bulkGet(keys, function(err, data) {
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

    it('PettyCache.bulkGet should return empty object when no keys are passed', async function() {
 await new Promise(function(resolve) {
        pettyCache.bulkGet([], function(err, values) {
            assert.ifError(err);
            assert.deepEqual(values, {});
            resolve();
        });
    });
});
});

describe('PettyCache.bulkSet', function() {
    it('PettyCache.bulkSet should set values', { timeout: 6000 }, async function() {
 await new Promise(function(resolve) {
        const key1 = Math.random().toString();
        const key2 = Math.random().toString();
        const key3 = Math.random().toString();
        const values = {};

        values[key1] = '1';
        values[key2] = 2;
        values[key3] = '3';

        pettyCache.bulkSet(values, function(err) {
            assert.ifError(err);

            pettyCache.get(key1, function(err, value) {
                assert.ifError(err);
                assert.strictEqual(value, '1');

                pettyCache.get(key2, function(err, value) {
                    assert.ifError(err);
                    assert.strictEqual(value, 2);

                    pettyCache.get(key3, function(err, value) {
                        assert.ifError(err);
                        assert.strictEqual(value, '3');

                        // Wait for memory cache to expire
                        setTimeout(function() {
                            pettyCache.get(key1, function(err, value) {
                                assert.ifError(err);
                                assert.strictEqual(value, '1');

                                pettyCache.get(key2, function(err, value) {
                                    assert.ifError(err);
                                    assert.strictEqual(value, 2);

                                    pettyCache.get(key3, function(err, value) {
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

    it('PettyCache.bulkSet should set values with the specified TTL option', { timeout: 7000 }, async function() {
 await new Promise(function(resolve) {
        const key1 = Math.random().toString();
        const key2 = Math.random().toString();
        const key3 = Math.random().toString();
        const values = {};

        values[key1] = '1';
        values[key2] = 2;
        values[key3] = '3';

        pettyCache.bulkSet(values, { ttl: 6000 }, function(err) {
            assert.ifError(err);

            pettyCache.get(key1, function(err, value) {
                assert.ifError(err);
                assert.strictEqual(value, '1');

                pettyCache.get(key2, function(err, value) {
                    assert.ifError(err);
                    assert.strictEqual(value, 2);

                    pettyCache.get(key3, function(err, value) {
                        assert.ifError(err);
                        assert.strictEqual(value, '3');

                        // Wait for Redis cache to expire
                        setTimeout(function() {
                            pettyCache.get(key1, function(err, value) {
                                assert.ifError(err);
                                assert.strictEqual(value, null);

                                pettyCache.get(key2, function(err, value) {
                                    assert.ifError(err);
                                    assert.strictEqual(value, null);

                                    pettyCache.get(key3, function(err, value) {
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

    it('PettyCache.bulkSet should set values with the specified TTL option using max and min', { timeout: 10000 }, async function() {
 await new Promise(function(resolve) {
        const key1 = Math.random().toString();
        const key2 = Math.random().toString();
        const key3 = Math.random().toString();
        const values = {};

        values[key1] = '1';
        values[key2] = 2;
        values[key3] = '3';

        pettyCache.bulkSet(values, { ttl: { max: 7000, min: 6000 } }, function(err) {
            assert.ifError(err);

            pettyCache.get(key1, function(err, value) {
                assert.ifError(err);
                assert.strictEqual(value, '1');

                pettyCache.get(key2, function(err, value) {
                    assert.ifError(err);
                    assert.strictEqual(value, 2);

                    pettyCache.get(key3, function(err, value) {
                        assert.ifError(err);
                        assert.strictEqual(value, '3');

                        // Wait for Redis cache to expire
                        setTimeout(function() {
                            pettyCache.get(key1, function(err, value) {
                                assert.ifError(err);
                                assert.strictEqual(value, null);

                                pettyCache.get(key2, function(err, value) {
                                    assert.ifError(err);
                                    assert.strictEqual(value, null);

                                    pettyCache.get(key3, function(err, value) {
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

    it('PettyCache.bulkSet should set values with the specified TTL option using max only', { timeout: 10000 }, async function() {
 await new Promise(function(resolve) {
        const key1 = Math.random().toString();
        const key2 = Math.random().toString();
        const key3 = Math.random().toString();
        const values = {};

        values[key1] = '1';
        values[key2] = 2;
        values[key3] = '3';

        pettyCache.bulkSet(values, { ttl: { max: 10000 } }, function(err) {
            assert.ifError(err);

            pettyCache.get(key1, function(err, value) {
                assert.ifError(err);
                assert.strictEqual(value, '1');

                resolve();
            });
        });
    });
});

    it('PettyCache.bulkSet should set values with the specified TTL option using min only', { timeout: 10000 }, async function() {
 await new Promise(function(resolve) {
        const key1 = Math.random().toString();
        const key2 = Math.random().toString();
        const key3 = Math.random().toString();
        const values = {};

        values[key1] = '1';
        values[key2] = 2;
        values[key3] = '3';

        pettyCache.bulkSet(values, { ttl: { min: 6000 } }, function(err) {
            assert.ifError(err);

            pettyCache.get(key1, function(err, value) {
                assert.ifError(err);
                assert.strictEqual(value, '1');

                resolve();
            });
        });
    });
});
});

describe('PettyCache.del', function() {
    it('PettyCache.del', async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        pettyCache.set(key, key.split('').reverse().join(''), function(err) {
            assert.ifError(err);

            pettyCache.get(key, function(err, value) {
                assert.strictEqual(value, key.split('').reverse().join(''));

                pettyCache.del(key, function(err) {
                    assert.ifError(err);

                    pettyCache.get(key, function(err, value) {
                        assert.ifError(err);
                        assert.strictEqual(value, null);

                        pettyCache.del(key, function(err) {
                            assert.ifError(err);
                            resolve();
                        });
                    });
                });
            });
        });
    });
});

    it('PettyCache.del', async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        pettyCache.set(key, key.split('').reverse().join(''), function(err) {
            assert.ifError(err);

            pettyCache.get(key, async function(err, value) {
                assert.strictEqual(value, key.split('').reverse().join(''));

                await pettyCache.del(key);

                pettyCache.get(key, async function(err, value) {
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

describe('PettyCache.fetch', function() {
    it('PettyCache.fetch', { timeout: 7000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        pettyCache.fetch(key, function(callback) {
            return callback(null, { foo: 'bar' });
        }, function() {
            pettyCache.fetch(key, function() {
                throw 'This function should not be called';
            }, function(err, data) {
                assert.equal(data.foo, 'bar');

                // Wait for memory cache to expire
                setTimeout(function() {
                    pettyCache.fetch(key, function() {
                        throw 'This function should not be called';
                    }, function(err, data) {
                        assert.strictEqual(data.foo, 'bar');
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});

    it('PettyCache.fetch should cache null values returned by func', { timeout: 7000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        pettyCache.fetch(key, function(callback) {
            return callback(null, null);
        }, function() {
            pettyCache.fetch(key, function() {
                throw 'This function should not be called';
            }, function(err, data) {
                assert.strictEqual(data, null);

                // Wait for memory cache to expire
                setTimeout(function() {
                    pettyCache.fetch(key, function() {
                        throw 'This function should not be called';
                    }, function(err, data) {
                        assert.strictEqual(data, null);
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});

    it('PettyCache.fetch should cache undefined values returned by func', { timeout: 7000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        pettyCache.fetch(key, function(callback) {
            return callback(null, undefined);
        }, function() {
            pettyCache.fetch(key, function() {
                throw 'This function should not be called';
            }, function(err, data) {
                assert.strictEqual(data, undefined);

                // Wait for memory cache to expire
                setTimeout(function() {
                    pettyCache.fetch(key, function() {
                        throw 'This function should not be called';
                    }, function(err, data) {
                        assert.strictEqual(data, undefined);
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});

    it('PettyCache.fetch should lock around func', async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();
        let numberOfFuncCalls = 0;

        const func = function(callback) {
            setTimeout(function() {
                callback(null, ++numberOfFuncCalls);
            }, 100);
        };

        pettyCache.fetch(key, func, function() {});
        pettyCache.fetch(key, func, function() {});
        pettyCache.fetch(key, func, function() {});
        pettyCache.fetch(key, func, function() {});
        pettyCache.fetch(key, func, function() {});
        pettyCache.fetch(key, func, function() {});
        pettyCache.fetch(key, func, function() {});
        pettyCache.fetch(key, func, function() {});
        pettyCache.fetch(key, func, function() {});

        pettyCache.fetch(key, func, function(err, data) {
            assert.equal(data, 1);
            resolve();
        });
    });
});

    it('PettyCache.fetch should run func again after TTL', { timeout: 7000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();
        let numberOfFuncCalls = 0;

        const func = function(callback) {
            setTimeout(function() {
                callback(null, ++numberOfFuncCalls);
            }, 100);
        };

        pettyCache.fetch(key, func, { ttl: 6000 }, function() {});

        pettyCache.fetch(key, func, { ttl: 6000 }, function(err, data) {
            assert.equal(data, 1);

            setTimeout(function() {
                pettyCache.fetch(key, func, { ttl: 6000 }, function(err, data) {
                    assert.equal(data, 2);

                    pettyCache.fetch(key, func, { ttl: 6000 }, function(err, data) {
                        assert.equal(data, 2);
                        resolve();
                    });
                });
            }, 6001);
        });
    });
});

    it('PettyCache.fetch should lock around Redis', async function() {
 await new Promise(function(resolve) {
        redisClient.info('commandstats', function(err, info) {
            const lineBefore = info.split('\n').find(i => i.startsWith('cmdstat_get:'));
            const tokenBefore = lineBefore.split(/:|,/).find(i => i.startsWith('calls='));
            const callsBefore = parseInt(tokenBefore.split('=')[1]);

            const key = Math.random().toString();
            let numberOfFuncCalls = 0;

            const func = function(callback) {
                setTimeout(function() {
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

            pettyCache.fetch(key, func, function(err, data) {
                assert.equal(data, 1);

                redisClient.info('commandstats', function(err, info) {
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

    it('PettyCache.fetch should return error if func returns error', async function() {
 await new Promise(function(resolve) {
        pettyCache.fetch(Math.random().toString(), function(callback) {
            callback(new Error('PettyCache.fetch should return error if func returns error'));
        }, function(err, values) {
            assert(err);
            assert.strictEqual(err.message, 'PettyCache.fetch should return error if func returns error');
            assert(!values);
            resolve();
        });
    });
});

    it('PettyCache.fetch should support async func', { timeout: 7000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        pettyCache.fetch(key, async () => {
            return { foo: 'bar' };
        }, function() {
            pettyCache.fetch(key, async () => {
                throw 'This function should not be called';
            }, function(err, data) {
                assert.ifError(err);
                assert.equal(data.foo, 'bar');

                // Wait for memory cache to expire
                setTimeout(function() {
                    pettyCache.fetch(key, async () => {
                        throw 'This function should not be called';
                    }, function(err, data) {
                        assert.ifError(err);
                        assert.strictEqual(data.foo, 'bar');
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});

    it('PettyCache.fetch should support async func with callback', { timeout: 7000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        pettyCache.fetch(key, async (callback) => {
            return callback(null, { foo: 'bar' });
        }, function() {
            pettyCache.fetch(key, async () => {
                throw 'This function should not be called';
            }, function(err, data) {
                assert.ifError(err);
                assert.equal(data.foo, 'bar');

                // Wait for memory cache to expire
                setTimeout(function() {
                    pettyCache.fetch(key, async () => {
                        throw 'This function should not be called';
                    }, function(err, data) {
                        assert.ifError(err);
                        assert.strictEqual(data.foo, 'bar');
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});

    it('PettyCache.fetch should support sync func without callback', { timeout: 7000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        pettyCache.fetch(key, () => {
            return { foo: 'bar' };
        }, function() {
            pettyCache.fetch(key, () => {
                throw 'This function should not be called';
            }, function(err, data) {
                assert.ifError(err);
                assert.equal(data.foo, 'bar');

                // Wait for memory cache to expire
                setTimeout(function() {
                    pettyCache.fetch(key, () => {
                        throw 'This function should not be called';
                    }, function(err, data) {
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

describe('PettyCache.fetchAndRefresh', function() {
    it('PettyCache.fetchAndRefresh', { timeout: 7000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        pettyCache.fetchAndRefresh(key, function(callback) {
            return callback(null, { foo: 'bar' });
        }, function() {
            pettyCache.fetchAndRefresh(key, function() {
                throw 'This function should not be called';
            }, function(err, data) {
                assert.equal(data.foo, 'bar');

                // Wait for memory cache to expire
                setTimeout(function() {
                    pettyCache.fetchAndRefresh(key, function() {
                        throw 'This function should not be called';
                    }, function(err, data) {
                        assert.strictEqual(data.foo, 'bar');
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});

    it('PettyCache.fetchAndRefresh should run func again to refresh', { timeout: 7000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();
        let numberOfFuncCalls = 0;

        const func = function(callback) {
            setTimeout(function() {
                callback(null, ++numberOfFuncCalls);
            }, 100);
        };

        pettyCache.fetchAndRefresh(key, func, { ttl: 6000 });

        pettyCache.fetchAndRefresh(key, func, { ttl: 6000 }, function(err, data) {
            assert.equal(data, 1);

            setTimeout(function() {
                pettyCache.fetchAndRefresh(key, func, { ttl: 6000 }, function(err, data) {
                    assert.equal(data, 2);

                    pettyCache.fetchAndRefresh(key, func, { ttl: 6000 }, function(err, data) {
                        assert.equal(data, 2);
                        resolve();
                    });
                });
            }, 4001);
        });
    });
});

    it('PettyCache.fetchAndRefresh should not allow multiple clients to execute func at the same time', { timeout: 7000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();
        let numberOfFuncCalls = 0;

        const func = function(callback) {
            setTimeout(function() {
                callback(null, ++numberOfFuncCalls);
            }, 100);
        };

        pettyCache.fetchAndRefresh(key, func, { ttl: 6000 }, function(err, data) {
            assert.ifError(err);
            assert.equal(data, 1);

            const pettyCache2 = new PettyCache(redisClient);

            pettyCache2.fetchAndRefresh(key, func, { ttl: 6000 }, function(err, data) {
                assert.ifError(err);
                assert.equal(data, 1);

                setTimeout(function() {
                    pettyCache.fetchAndRefresh(key, func, { ttl: 6000 }, function(err, data) {
                        assert.ifError(err);
                        assert.equal(data, 2);

                        pettyCache2.fetchAndRefresh(key, func, { ttl: 6000 }, function(err, data) {
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

    it('PettyCache.fetchAndRefresh should return error if func returns error', { timeout: 7000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        const func = function(callback) {
            callback(new Error('PettyCache.fetchAndRefresh should return error if func returns error'));
        };

        pettyCache.fetchAndRefresh(key, func, { ttl: 6000 }, function(err, data) {
            assert(err);
            assert.strictEqual(err.message, 'PettyCache.fetchAndRefresh should return error if func returns error');
            assert(!data);

            setTimeout(function() {
                pettyCache.fetchAndRefresh(key, func, { ttl: 6000 }, function(err, data) {
                    assert(err);
                    assert.strictEqual(err.message, 'PettyCache.fetchAndRefresh should return error if func returns error');
                    assert(!data);

                    resolve();
                });
            }, 4001);
        });
    });
});

    it('PettyCache.fetchAndRefresh should not require options', async function() {
 await new Promise(function(resolve) {
        pettyCache.fetchAndRefresh(Math.random().toString(), function(callback) {
            return callback(null, { foo: 'bar' });
        });

        resolve();
    });
});
});

describe('PettyCache.get', function() {
    it('PettyCache.get should return value', { timeout: 7000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        pettyCache.set(key, 'hello world', function() {
            pettyCache.get(key, function(err, value) {
                assert.equal(value, 'hello world');

                // Wait for memory cache to expire
                setTimeout(function() {
                    pettyCache.get(key, function(err, value) {
                        assert.equal(value, 'hello world');
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});

    it('PettyCache.get should return null for missing keys', async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        pettyCache.get(key, function(err, value) {
            assert.strictEqual(value, null);

            pettyCache.get(key, function(err, value) {
                assert.strictEqual(value, null);
                resolve();
            });
        });
    });
});
});

describe('PettyCache.mutex', function() {
    describe('PettyCache.mutex.lock (callbacks)', function() {
        it('PettyCache.mutex.lock should lock for 1 second by default', async function() {
 await new Promise(function(resolve) {
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

        it('PettyCache.mutex.lock should lock for 2 seconds when ttl parameter is specified', { timeout: 3000 }, async function() {
 await new Promise(function(resolve) {
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

        it('PettyCache.mutex.lock should acquire a lock after retries', { timeout: 3000 }, async function() {
 await new Promise(function(resolve) {
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

    describe('PettyCache.mutex.lock (promises)', function() {
        it('PettyCache.mutex.lock should lock for 1 second by default', async () => {
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

        it('PettyCache.mutex.lock should lock for 2 seconds when ttl parameter is specified', { timeout: 4000 }, async function() {
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

        it('PettyCache.mutex.lock should acquire a lock after retries', { timeout: 4000 }, async function() {
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

    describe('PettyCache.mutex.unlock (callbacks)', function() {
        it('PettyCache.mutex.unlock should unlock', async function() {
 await new Promise(function(resolve) {
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

        it('PettyCache.mutex.unlock should work without a callback', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.mutex.lock(key, { ttl: 10000 }, err => {
                assert.ifError(err);

                pettyCache.mutex.unlock(key);
                resolve();
            });
        });
});
    });

    describe('PettyCache.mutex.unlock (promises)', function() {
        it('PettyCache.mutex.unlock should unlock', async () => {
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

describe('PettyCache.patch', function() {
    const key = Math.random().toString();

    before(function() { return new Promise(function(resolve) { pettyCache.set(key, { a: 1, b: 2, c: 3 }, resolve); }); });

    it('PettyCache.patch should fail if the key does not exist', async function() {
 await new Promise(function(resolve) {
        pettyCache.patch('xyz', { b: 3 }, function(err) {
            assert(err, 'No error provided');
            resolve();
        });
    });
});

    it('PettyCache.patch should update the values of given object keys', async function() {
 await new Promise(function(resolve) {
        pettyCache.patch(key, { b: 4, c: 5 }, function(err) {
            assert(!err, 'Error: ' + err);

            pettyCache.get(key, function(err, data) {
                assert(!err, 'Error: ' + err);
                assert.deepEqual(data, { a: 1, b: 4, c: 5 });
                resolve();
            });
        });
    });
});

    it('PettyCache.patch should update the values of given object keys with options', async function() {
 await new Promise(function(resolve) {
        pettyCache.patch(key, { b: 5, c: 6 }, { ttl: 10000 }, function(err) {
            assert(!err, 'Error: ' + err);

            pettyCache.get(key, function(err, data) {
                assert(!err, 'Error: ' + err);
                assert.deepEqual(data, { a: 1, b: 5, c: 6 });
                resolve();
            });
        });
    });
});
});

describe('PettyCache.semaphore', function() {
    describe('PettyCache.semaphore.acquireLock', function() {
        it('should aquire a lock', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 10 }, function(err) {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, function(err, index) {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.acquireLock(key, function(err, index) {
                        assert.ifError(err);
                        assert.equal(index, 1);
                        resolve();
                    });
                });
            });
        });
});

        it('should not aquire a lock', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, function(err) {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, function(err, index) {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.acquireLock(key, function(err) {
                        assert(err);
                        resolve();
                    });
                });
            });
        });
});

        it('should aquire a lock after ttl', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, function(err) {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, function(err, index) {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.acquireLock(key, function(err) {
                        assert(err);

                        setTimeout(function() {
                            pettyCache.semaphore.acquireLock(key, function(err, index) {
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

        it('should aquire a lock with specified options', { timeout: 5000 }, async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 10 }, function(err) {
                assert.ifError(err);

                // callback is optional
                pettyCache.semaphore.acquireLock(key);

                setTimeout(function() {
                    pettyCache.semaphore.acquireLock(key, { retry: { interval: 500, times: 10 }, ttl: 500 }, function(err, index) {
                        assert.ifError(err);
                        assert.equal(index, 1);
                        resolve();
                    });
                }, 1000);
            });
        });
});

        it('should fail if the semaphore does not exist', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.acquireLock(key, 0, function(err) {
                assert(err);
                assert.strictEqual(err.message, `Semaphore ${key} doesn't exist.`);
                resolve();
            });
        });
});
    });

    describe('PettyCache.semaphore.consumeLock', function() {
        it('should consume a lock', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, function(err) {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, function(err, index) {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.acquireLock(key, function(err, index) {
                        assert.ifError(err);
                        assert.equal(index, 1);

                        pettyCache.semaphore.acquireLock(key, function(err) {
                            assert(err);

                            pettyCache.semaphore.consumeLock(key, 0, function(err) {
                                assert.ifError(err);

                                pettyCache.semaphore.acquireLock(key, function(err) {
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

        it('should ensure at least one lock is not consumed', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, function(err) {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, function(err, index) {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.acquireLock(key, function(err, index) {
                        assert.ifError(err);
                        assert.equal(index, 1);

                        pettyCache.semaphore.acquireLock(key, function(err) {
                            assert(err);

                            pettyCache.semaphore.consumeLock(key, 0, function(err) {
                                assert.ifError(err);

                                pettyCache.semaphore.consumeLock(key, 1, function(err) {
                                    assert.ifError(err);

                                    pettyCache.semaphore.acquireLock(key, function(err) {
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

        it('should fail if the semaphore does not exist', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.consumeLock(key, 0, function(err) {
                assert(err);
                assert.strictEqual(err.message, `Semaphore ${key} doesn't exist.`);
                resolve();
            });
        });
});

        it('should fail if index is larger than semaphore', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, function(err) {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, function(err, index) {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.consumeLock(key, 10, function(err) {
                        assert(err);
                        assert.strictEqual(err.message, `Index 10 for semaphore ${key} is invalid.`);
                        resolve();
                    });
                });
            });
        });
});

        it('callback is optional', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, function(err) {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, function(err, index) {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.acquireLock(key, function(err, index) {
                        assert.ifError(err);
                        assert.equal(index, 1);

                        pettyCache.semaphore.acquireLock(key, function(err) {
                            assert(err);

                            pettyCache.semaphore.consumeLock(key, 0);

                            pettyCache.semaphore.acquireLock(key, function(err) {
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

    describe('PettyCache.semaphore.expand', function() {
        it('should increase the size of a semaphore pool', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, function(err, pool) {
                assert.ifError(err);
                assert.strictEqual(pool.length, 2);

                pettyCache.semaphore.expand(key, 3, function(err) {
                    assert.ifError(err);

                    pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, function(err, pool) {
                        assert.ifError(err);
                        assert.strictEqual(pool.length, 3);
                        resolve();
                    });
                });
            });
        });
});

        it('should refuse to shrink a pool', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, function(err, pool) {
                assert.ifError(err);
                assert.strictEqual(pool.length, 2);

                pettyCache.semaphore.expand(key, 1, function(err) {
                    assert(err);
                    assert.strictEqual(err.message, 'Cannot shrink pool, size is 2 and you requested a size of 1.');
                    resolve();
                });
            });
        });
});

        it('should succeed if pool size is already equal to the specified size', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, function(err, pool) {
                assert.ifError(err);
                assert.strictEqual(pool.length, 2);

                pettyCache.semaphore.expand(key, 2, function(err) {
                    assert.ifError(err);

                    pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, function(err, pool) {
                        assert.ifError(err);
                        assert.strictEqual(pool.length, 2);
                        resolve();
                    });
                });
            });
        });
});

        it('callback is optional', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, function(err, pool) {
                assert.ifError(err);
                assert.strictEqual(pool.length, 2);

                pettyCache.semaphore.expand(key, 3);

                pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, function(err, pool) {
                    assert.ifError(err);
                    assert.strictEqual(pool.length, 3);
                    resolve();
                });
            });
        });
});

        it('should fail if the semaphore does not exist', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.expand(key, 10, function(err) {
                assert(err);
                assert.strictEqual(err.message, `Semaphore ${key} doesn't exist.`);
                resolve();
            });
        });
});
    });

    describe('PettyCache.semaphore.releaseLock', function() {
        it('should release a lock', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, function(err) {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, function(err, index) {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.acquireLock(key, function(err) {
                        assert(err);

                        pettyCache.semaphore.releaseLock(key, 0, function(err) {
                            assert.ifError(err);

                            pettyCache.semaphore.acquireLock(key, function(err, index) {
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

        it('should fail to release a lock outside of the semaphore size', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, function(err) {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, function(err, index) {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.releaseLock(key, 10, function(err) {
                        assert(err);
                        assert.strictEqual(err.message, `Index 10 for semaphore ${key} is invalid.`);
                        resolve();
                    });
                });
            });
        });
});

        it('callback is optional', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, function(err) {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, function(err, index) {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.acquireLock(key, function(err) {
                        assert(err);

                        pettyCache.semaphore.releaseLock(key, 0);

                        pettyCache.semaphore.acquireLock(key, function(err, index) {
                            assert.ifError(err);
                            assert.equal(index, 0);
                            resolve();
                        });
                    });
                });
            });
        });
});

        it('should fail if the semaphore does not exist', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.releaseLock(key, 10, function(err) {
                assert(err);
                assert.strictEqual(err.message, `Semaphore ${key} doesn't exist.`);
                resolve();
            });
        });
});
    });

    describe('PettyCache.semaphore.reset', function() {
        it('should reset all locks', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, function(err) {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, function(err, index) {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.acquireLock(key, function(err, index) {
                        assert.ifError(err);
                        assert.equal(index, 1);

                        pettyCache.semaphore.acquireLock(key, function(err) {
                            assert(err);

                            pettyCache.semaphore.reset(key, function(err) {
                                assert.ifError(err);

                                pettyCache.semaphore.acquireLock(key, function(err, index) {
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

        it('callback is optional', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, function(err) {
                assert.ifError(err);

                pettyCache.semaphore.acquireLock(key, function(err, index) {
                    assert.ifError(err);
                    assert.equal(index, 0);

                    pettyCache.semaphore.acquireLock(key, function(err, index) {
                        assert.ifError(err);
                        assert.equal(index, 1);

                        pettyCache.semaphore.acquireLock(key, function(err) {
                            assert(err);

                            pettyCache.semaphore.reset(key);

                            pettyCache.semaphore.acquireLock(key, function(err, index) {
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

        it('should fail if the semaphore does not exist', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.reset(key, function(err) {
                assert(err);
                assert.strictEqual(err.message, `Semaphore ${key} doesn't exist.`);
                resolve();
            });
        });
});
    });

    describe('PettyCache.semaphore.retrieveOrCreate', function() {
        it('should create a new semaphore', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 100 }, function(err, semaphore) {
                assert.ifError(err);
                assert(semaphore);
                assert.equal(semaphore.length, 100);
                assert(semaphore.every(s => s.status === 'available'));

                pettyCache.semaphore.retrieveOrCreate(key, function(err, semaphore) {
                    assert.ifError(err);
                    assert(semaphore);
                    assert.equal(semaphore.length, 100);
                    assert(semaphore.every(s => s.status === 'available'));
                    resolve();
                });
            });
        });
});

        it('should have a min size of 1', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: 0 }, function(err, semaphore) {
                assert.ifError(err);
                assert(semaphore);
                assert.equal(semaphore.length, 1);
                assert(semaphore.every(s => s.status === 'available'));

                pettyCache.semaphore.retrieveOrCreate(key, function(err, semaphore) {
                    assert.ifError(err);
                    assert(semaphore);
                    assert.equal(semaphore.length, 1);
                    assert(semaphore.every(s => s.status === 'available'));
                    resolve();
                });
            });
        });
});

        it('should allow options.size to provide a function', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key, { size: (callback) => callback(null, 1 + 1) }, function(err, semaphore) {
                assert.ifError(err);
                assert(semaphore);
                assert.equal(semaphore.length, 2);
                assert(semaphore.every(s => s.status === 'available'));

                pettyCache.semaphore.retrieveOrCreate(key, function(err, semaphore) {
                    assert.ifError(err);
                    assert(semaphore);
                    assert.equal(semaphore.length, 2);
                    assert(semaphore.every(s => s.status === 'available'));
                    resolve();
                });
            });
        });
});

        it('callback is optional', async function() {
 await new Promise(function(resolve) {
            const key = Math.random().toString();

            pettyCache.semaphore.retrieveOrCreate(key);

            pettyCache.semaphore.retrieveOrCreate(key, { size: 100 }, function(err, semaphore) {
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

describe('PettyCache.set', function() {
    it('PettyCache.set should set a value', { timeout: 7000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        pettyCache.set(key, 'hello world', function() {
            pettyCache.get(key, function(err, value) {
                assert.equal(value, 'hello world');

                // Wait for memory cache to expire
                setTimeout(function() {
                    pettyCache.get(key, function(err, value) {
                        assert.equal(value, 'hello world');
                        resolve();
                    });
                }, 6000);
            });
        });
    });
});

    it('PettyCache.set should work without a callback', async function() {
 await new Promise(function(resolve) {
        pettyCache.set(Math.random().toString(), 'hello world');
        resolve();
    });
});

    it('PettyCache.set should set a value with the specified TTL option', { timeout: 7000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        pettyCache.set(key, 'hello world', { ttl: 6000 },function() {
            pettyCache.get(key, function(err, value) {
                assert.equal(value, 'hello world');

                // Wait for memory cache to expire
                setTimeout(function() {
                    pettyCache.get(key, function(err, value) {
                        assert.equal(value, null);
                        resolve();
                    });
                }, 6001);
            });
        });
    });
});

    it('PettyCache.set should set a value with the specified TTL option using max and min', { timeout: 10000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        pettyCache.set(key, 'hello world', { ttl: { max: 7000, min: 6000 } },function() {
            pettyCache.get(key, function(err, value) {
                assert.strictEqual(value, 'hello world');

                // Get again before cache expires
                setTimeout(function() {
                    pettyCache.get(key, function(err, value) {
                        assert.strictEqual(value, 'hello world');

                        // Wait for memory cache to expire
                        setTimeout(function() {
                            pettyCache.get(key, function(err, value) {
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

    it('PettyCache.set should set a value with the specified TTL option using min only', { timeout: 10000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        pettyCache.set(key, 'hello world', { ttl: { min: 6000 } },function() {
            pettyCache.get(key, function(err, value) {
                assert.strictEqual(value, 'hello world');
                resolve();
            });
        });
    });
});

    it('PettyCache.set should set a value with the specified TTL option using max only', { timeout: 10000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        pettyCache.set(key, 'hello world', { ttl: { max: 10000 } },function() {
            pettyCache.get(key, function(err, value) {
                assert.strictEqual(value, 'hello world');
                resolve();
            });
        });
    });
});

    it('PettyCache.set(key, \'\')', { timeout: 11000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        pettyCache.set(key, '', { ttl: 7000 }, function(err) {
            assert.ifError(err);

            pettyCache.get(key, function(err, value) {
                assert.ifError(err);
                assert.strictEqual(value, '');

                // Wait for memory cache to expire
                setTimeout(function() {
                    pettyCache.get(key, function(err, value) {
                        assert.ifError(err);
                        assert.strictEqual(value, '');

                        // Wait for memory cache and Redis cache to expire
                        setTimeout(function() {
                            pettyCache.get(key, function(err, value) {
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

    it('PettyCache.set(key, 0)', { timeout: 11000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        pettyCache.set(key, 0, { ttl: 7000 }, function(err) {
            assert.ifError(err);

            pettyCache.get(key, function(err, value) {
                assert.ifError(err);
                assert.strictEqual(value, 0);

                // Wait for memory cache to expire
                setTimeout(function() {
                    pettyCache.get(key, function(err, value) {
                        assert.ifError(err);
                        assert.strictEqual(value, 0);

                        // Wait for memory cache and Redis cache to expire
                        setTimeout(function() {
                            pettyCache.get(key, function(err, value) {
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

    it('PettyCache.set(key, false)', { timeout: 11000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        pettyCache.set(key, false, { ttl: 7000 }, function(err) {
            assert.ifError(err);

            pettyCache.get(key, function(err, value) {
                assert.ifError(err);
                assert.strictEqual(value, false);

                // Wait for memory cache to expire
                setTimeout(function() {
                    pettyCache.get(key, function(err, value) {
                        assert.ifError(err);
                        assert.strictEqual(value, false);

                        // Wait for memory cache and Redis cache to expire
                        setTimeout(function() {
                            pettyCache.get(key, function(err, value) {
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

    it('PettyCache.set(key, NaN)', { timeout: 11000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        pettyCache.set(key, NaN, { ttl: 7000 }, function(err) {
            assert.ifError(err);

            pettyCache.get(key, function(err, value) {
                assert.ifError(err);
                assert(typeof value === 'number' && isNaN(value));

                // Wait for memory cache to expire
                setTimeout(function() {
                    pettyCache.get(key, function(err, value) {
                        assert.ifError(err);
                        assert(typeof value === 'number' && isNaN(value));

                        // Wait for memory cache and Redis cache to expire
                        setTimeout(function() {
                            pettyCache.get(key, function(err, value) {
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

    it('PettyCache.set(key, null)', { timeout: 11000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        pettyCache.set(key, null, { ttl: 7000 }, function(err) {
            assert.ifError(err);

            pettyCache.get(key, function(err, value) {
                assert.ifError(err);
                assert.strictEqual(value, null);

                // Wait for memory cache to expire
                setTimeout(function() {
                    pettyCache.get(key, function(err, value) {
                        assert.ifError(err);
                        assert.strictEqual(value, null);

                        // Wait for memory cache and Redis cache to expire
                        setTimeout(function() {
                            pettyCache.get(key, function(err, value) {
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

    it('PettyCache.set(key, undefined)', { timeout: 11000 }, async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        pettyCache.set(key, undefined, { ttl: 7000 }, function(err) {
            assert.ifError(err);

            pettyCache.get(key, function(err, value) {
                assert.ifError(err);
                assert.strictEqual(value, undefined);

                // Wait for memory cache to expire
                setTimeout(function() {
                    pettyCache.get(key, function(err, value) {
                        assert.ifError(err);
                        assert.strictEqual(value, undefined);

                        // Wait for memory cache and Redis cache to expire
                        setTimeout(function() {
                            pettyCache.get(key, function(err, value) {
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

describe('redisClient', function() {
    it('redisClient.mget(falsy keys)', async function() {
 await new Promise(function(resolve) {
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

        async.each(Object.keys(values), function(key, callback) {
            redisClient.psetex(key, 1000, PettyCache.stringify(values[key]), callback);
        }, function(err) {
            assert.ifError(err);

            const keys = Object.keys(values);

            // Add an additional key to check handling of missing keys
            keys.push(Math.random().toString());

            redisClient.mget(keys, function(err, data) {
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

    it('redisClient.psetex(key, \'\')', async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        redisClient.psetex(key, 1000, PettyCache.stringify(''), function(err) {
            assert.ifError(err);

            redisClient.get(key, function(err, data) {
                assert.ifError(err);
                assert.strictEqual(data, '""');
                assert.strictEqual(PettyCache.parse(data), '');

                // Wait for Redis cache to expire
                setTimeout(function() {
                    redisClient.get(key, function(err, data) {
                        assert.ifError(err);
                        assert.strictEqual(data, null);
                        resolve();
                    });
                }, 1001);
            });
        });
    });
});

    it('redisClient.psetex(key, 0)', async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        redisClient.psetex(key, 1000, PettyCache.stringify(0), function(err) {
            assert.ifError(err);

            redisClient.get(key, function(err, data) {
                assert.ifError(err);
                assert.strictEqual(data, '0');
                assert.strictEqual(PettyCache.parse(data), 0);

                // Wait for Redis cache to expire
                setTimeout(function() {
                    redisClient.get(key, function(err, data) {
                        assert.ifError(err);
                        assert.strictEqual(data, null);
                        resolve();
                    });
                }, 1001);
            });
        });
    });
});

    it('redisClient.psetex(key, false)', async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        redisClient.psetex(key, 1000, PettyCache.stringify(false), function(err) {
            assert.ifError(err);

            redisClient.get(key, function(err, data) {
                assert.ifError(err);
                assert.strictEqual(data, 'false');
                assert.strictEqual(PettyCache.parse(data), false);

                // Wait for Redis cache to expire
                setTimeout(function() {
                    redisClient.get(key, function(err, data) {
                        assert.ifError(err);
                        assert.strictEqual(data, null);
                        resolve();
                    });
                }, 1001);
            });
        });
    });
});

    it('redisClient.psetex(key, NaN)', async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        redisClient.psetex(key, 1000, PettyCache.stringify(NaN), function(err) {
            assert.ifError(err);

            redisClient.get(key, function(err, data) {
                assert.ifError(err);
                assert.strictEqual(data, '"__NaN"');
                assert(isNaN(PettyCache.parse(data)));

                // Wait for Redis cache to expire
                setTimeout(function() {
                    redisClient.get(key, function(err, data) {
                        assert.ifError(err);
                        assert.strictEqual(data, null);
                        resolve();
                    });
                }, 1001);
            });
        });
    });
});

    it('redisClient.psetex(key, null)', async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        redisClient.psetex(key, 1000, PettyCache.stringify(null), function(err) {
            assert.ifError(err);

            redisClient.get(key, function(err, data) {
                assert.ifError(err);
                assert.strictEqual(data, '"__null"');
                assert.strictEqual(PettyCache.parse(data), null);

                // Wait for Redis cache to expire
                setTimeout(function() {
                    redisClient.get(key, function(err, data) {
                        assert.ifError(err);
                        assert.strictEqual(data, null);
                        resolve();
                    });
                }, 1001);
            });
        });
    });
});

    it('redisClient.psetex(key, undefined)', async function() {
 await new Promise(function(resolve) {
        const key = Math.random().toString();

        redisClient.psetex(key, 1000, PettyCache.stringify(undefined), function(err) {
            assert.ifError(err);

            redisClient.get(key, function(err, data) {
                assert.ifError(err);
                assert.strictEqual(data, '"__undefined"');
                assert.strictEqual(PettyCache.parse(data), undefined);

                // Wait for Redis cache to expire
                setTimeout(function() {
                    redisClient.get(key, function(err, data) {
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

describe('Benchmark', function() {
    const emojis = require('./emojis.json');

    it('PettyCache should be faster than node-redis', async function() {
 await new Promise(function(resolve) {
        let pettyCacheEnd;
        const pettyCacheKey = Math.random().toString();
        let pettyCacheStart;
        let redisEnd;
        const redisKey = Math.random().toString();
        const redisStart = Date.now();

        redisClient.psetex(redisKey, 30000, JSON.stringify(emojis), function(err) {
            assert.ifError(err);

            async.times(500, function(n, callback) {
                redisClient.get(redisKey, function(err, data) {
                    if (err) {
                        return callback(err);
                    }

                    callback(null, JSON.parse(data));
                });
            }, function(err) {
                redisEnd = Date.now();
                assert.ifError(err);
                pettyCacheStart = Date.now();

                pettyCache.set(pettyCacheKey, emojis, function(err) {
                    assert.ifError(err);

                    async.times(500, function(n, callback) {
                        pettyCache.get(pettyCacheKey, function(err, data) {
                            if (err) {
                                return callback(err);
                            }

                            callback(null, data);
                        });
                    }, function(err) {
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
