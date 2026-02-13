const test = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const async = require('async');
const memoryCache = require('memory-cache');
const redis = require('redis');

const PettyCache = require('../index.js');

const redisClient = redis.createClient();
const pettyCache = new PettyCache(redisClient);

test('petty-cache', { concurrency: true }, async (t) => {
    t.test('new PettyCache()', { concurrency: true }, async (t) => {
        t.test('new PettyCache()', (t, done) => {
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
                            done();
                        });
                    }, 5001);
                });
            });
        });

        t.test('new PettyCache(port, host)', (t, done) => {
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
                            done();
                        });
                    }, 5001);
                });
            });
        });

        t.test('new PettyCache(redisClient)', (t, done) => {
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
                            done();
                        });
                    }, 5001);
                });
            });
        });
    });

    t.test('memory-cache', { concurrency: true }, async (t) => {
        t.test('memoryCache.put(key, \'\')', (t, done) => {
            const key = Math.random().toString();

            memoryCache.put(key, '', 100);
            assert(memoryCache.keys().includes(key));
            assert.strictEqual(memoryCache.get(key), '');

            // Wait for memory cache to expire
            setTimeout(() => {
                assert(!memoryCache.keys().includes(key));
                assert.strictEqual(memoryCache.get(key), null);
                done();
            }, 101);
        });

        t.test('memoryCache.put(key, 0)', (t, done) => {
            const key = Math.random().toString();

            memoryCache.put(key, 0, 100);
            assert(memoryCache.keys().includes(key));
            assert.strictEqual(memoryCache.get(key), 0);

            // Wait for memory cache to expire
            setTimeout(() => {
                assert(!memoryCache.keys().includes(key));
                assert.strictEqual(memoryCache.get(key), null);
                done();
            }, 101);
        });

        t.test('memoryCache.put(key, false)', (t, done) => {
            const key = Math.random().toString();

            memoryCache.put(key, false, 100);
            assert(memoryCache.keys().includes(key));
            assert.strictEqual(memoryCache.get(key), false);

            // Wait for memory cache to expire
            setTimeout(() => {
                assert(!memoryCache.keys().includes(key));
                assert.strictEqual(memoryCache.get(key), null);
                done();
            }, 101);
        });

        t.test('memoryCache.put(key, NaN)', (t, done) => {
            const key = Math.random().toString();

            memoryCache.put(key, NaN, 100);
            assert(memoryCache.keys().includes(key));
            assert(isNaN(memoryCache.get(key)));

            // Wait for memory cache to expire
            setTimeout(() => {
                assert(!memoryCache.keys().includes(key));
                assert.strictEqual(memoryCache.get(key), null);
                done();
            }, 101);
        });

        t.test('memoryCache.put(key, null)', (t, done) => {
            const key = Math.random().toString();

            memoryCache.put(key, null, 100);
            assert(memoryCache.keys().includes(key));
            assert.strictEqual(memoryCache.get(key), null);

            // Wait for memory cache to expire
            setTimeout(() => {
                assert(!memoryCache.keys().includes(key));
                assert.strictEqual(memoryCache.get(key), null);
                done();
            }, 101);
        });

        t.test('memoryCache.put(key, undefined)', (t, done) => {
            const key = Math.random().toString();

            memoryCache.put(key, undefined, 100);
            assert(memoryCache.keys().includes(key));
            assert.strictEqual(memoryCache.get(key), undefined);

            // Wait for memory cache to expire
            setTimeout(() => {
                assert(!memoryCache.keys().includes(key));
                assert.strictEqual(memoryCache.get(key), null);
                done();
            }, 101);
        });
    });

    t.test('PettyCache.bulkFetch', { concurrency: true }, async (t) => {
        t.test('PettyCache.bulkFetch', (t, done) => {
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
                                        done();
                                    });
                                });
                            }, 5001);
                        });
                    });
                });
            });
        });

        t.test('PettyCache.bulkFetch should cache null values returned by func', (t, done) => {
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

                            done();
                        });
                    }, 5001);
                });
            });
        });

        t.test('PettyCache.bulkFetch should return empty object when no keys are passed', (t, done) => {
            pettyCache.bulkFetch([], () => {
                throw 'This function should not be called';
            }, (err, values) => {
                assert.ifError(err);
                assert.deepEqual(values, {});
                done();
            });
        });

        t.test('PettyCache.bulkFetch should return error if func returns error', (t, done) => {
            pettyCache.bulkFetch([Math.random().toString()], (keys, callback) => {
                callback(new Error('PettyCache.bulkFetch should return error if func returns error'));
            }, (err, values) => {
                assert(err);
                assert.strictEqual(err.message, 'PettyCache.bulkFetch should return error if func returns error');
                assert(!values);
                done();
            });
        });

        t.test('PettyCache.bulkFetch should run func again after TTL', (t, done) => {
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
                                done();
                            });
                        });
                    });
                }, 6001);
            });
        });
    });

    t.test('PettyCache.bulkGet', { concurrency: true }, async (t) => {
        t.test('PettyCache.bulkGet should return values', (t, done) => {
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
                                        done();
                                    });
                                }, 5001);
                            });
                        });
                    });
                });
            });
        });

        t.test('PettyCache.bulkGet should return null for missing keys', (t, done) => {
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
                                    done();
                                });
                            }, 5001);
                        });
                    });
                });
            });
        });

        t.test('PettyCache.bulkGet should correctly handle falsy values', (t, done) => {
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
                                    done();
                                });
                            }, 6001);
                        });
                    }, 5001);
                });
            });
        });

        t.test('PettyCache.bulkGet should return empty object when no keys are passed', (t, done) => {
            pettyCache.bulkGet([], (err, values) => {
                assert.ifError(err);
                assert.deepEqual(values, {});
                done();
            });
        });
    });

    t.test('PettyCache.bulkSet', { concurrency: true }, async (t) => {
        t.test('PettyCache.bulkSet should set values', (t, done) => {
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
                                            done();
                                        });
                                    });
                                });
                            }, 5001);
                        });
                    });
                });
            });
        });

        t.test('PettyCache.bulkSet should set values with the specified TTL option', (t, done) => {
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
                                            done();
                                        });
                                    });
                                });
                            }, 6001);
                        });
                    });
                });
            });
        });

        t.test('PettyCache.bulkSet should set values with the specified TTL option using max and min', (t, done) => {
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
                                            done();
                                        });
                                    });
                                });
                            }, 7001);
                        });
                    });
                });
            });
        });

        t.test('PettyCache.bulkSet should set values with the specified TTL option using max only', (t, done) => {
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

                    done();
                });
            });
        });

        t.test('PettyCache.bulkSet should set values with the specified TTL option using min only', (t, done) => {
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

                    done();
                });
            });
        });
    });

    t.test('PettyCache.del', { concurrency: true }, async (t) => {
        t.test('PettyCache.del', (t, done) => {
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
                                done();
                            });
                        });
                    });
                });
            });
        });

        t.test('PettyCache.del', (t, done) => {
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

                        done();
                    });
                });
            });
        });
    });

    t.test('PettyCache.fetch', { concurrency: true }, async (t) => {
        t.test('PettyCache.fetch', (t, done) => {
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
                            done();
                        });
                    }, 5001);
                });
            });
        });

        t.test('PettyCache.fetch should cache null values returned by func', (t, done) => {
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
                            done();
                        });
                    }, 5001);
                });
            });
        });

        t.test('PettyCache.fetch should cache undefined values returned by func', (t, done) => {
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
                            done();
                        });
                    }, 5001);
                });
            });
        });

        t.test('PettyCache.fetch should lock around func', (t, done) => {
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
                done();
            });
        });

        t.test('PettyCache.fetch should run func again after TTL', (t, done) => {
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
                            done();
                        });
                    });
                }, 6001);
            });
        });

        t.test('PettyCache.fetch should return error if func returns error', (t, done) => {
            pettyCache.fetch(Math.random().toString(), (callback) => {
                callback(new Error('PettyCache.fetch should return error if func returns error'));
            }, (err, values) => {
                assert(err);
                assert.strictEqual(err.message, 'PettyCache.fetch should return error if func returns error');
                assert(!values);
                done();
            });
        });

        t.test('PettyCache.fetch should support async func', (t, done) => {
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
                            done();
                        });
                    }, 5001);
                });
            });
        });

        t.test('PettyCache.fetch should return error if async func throws error', (t, done) => {
            pettyCache.fetch(Math.random().toString(), async () => {
                throw new Error('PettyCache.fetch should return error if async func throws error');
            }, (err, data) => {
                assert(err);
                assert.strictEqual(err.message, 'PettyCache.fetch should return error if async func throws error');
                assert(!data);
                done();
            });
        });

        t.test('PettyCache.fetch should support async func with callback', (t, done) => {
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
                            done();
                        });
                    }, 5001);
                });
            });
        });

        t.test('PettyCache.fetch should support sync func without callback', (t, done) => {
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
                            done();
                        });
                    }, 5001);
                });
            });
        });
    });

    t.test('PettyCache.fetchAndRefresh', { concurrency: true }, async (t) => {
        t.test('PettyCache.fetchAndRefresh', (t, done) => {
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
                            done();
                        });
                    }, 5001);
                });
            });
        });

        t.test('PettyCache.fetchAndRefresh should run func again to refresh', (t, done) => {
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
                            done();
                        });
                    });
                }, 3001);
            });
        });

        t.test('PettyCache.fetchAndRefresh should not allow multiple clients to execute func at the same time', (t, done) => {
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
                                done();
                            });
                        });
                    }, 5001);
                });
            });
        });

        t.test('PettyCache.fetchAndRefresh should return error if func returns error', (t, done) => {
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

                        done();
                    });
                }, 3001);
            });
        });

        t.test('PettyCache.fetchAndRefresh should not require options', (t, done) => {
            pettyCache.fetchAndRefresh(Math.random().toString(), (callback) => {
                return callback(null, { foo: 'bar' });
            });

            done();
        });
    });

    t.test('PettyCache.get', { concurrency: true }, async (t) => {
        t.test('PettyCache.get should return value', (t, done) => {
            const key = Math.random().toString();

            pettyCache.set(key, 'hello world', () => {
                pettyCache.get(key, (err, value) => {
                    assert.equal(value, 'hello world');

                    // Wait for memory cache to expire
                    setTimeout(() => {
                        pettyCache.get(key, (err, value) => {
                            assert.equal(value, 'hello world');
                            done();
                        });
                    }, 5001);
                });
            });
        });

        t.test('PettyCache.get should return null for missing keys', (t, done) => {
            const key = Math.random().toString();

            pettyCache.get(key, (err, value) => {
                assert.strictEqual(value, null);

                pettyCache.get(key, (err, value) => {
                    assert.strictEqual(value, null);
                    done();
                });
            });
        });
    });

    t.test('PettyCache.mutex', { concurrency: true }, async (t) => {
        t.test('PettyCache.mutex.lock (callbacks)', { concurrency: true }, async (t) => {
            t.test('PettyCache.mutex.lock should lock for 1 second by default', (t, done) => {
                const key = Math.random().toString();

                pettyCache.mutex.lock(key, err => {
                    assert.ifError(err);

                    pettyCache.mutex.lock(key, err => {
                        assert(err);

                        setTimeout(() => {
                            pettyCache.mutex.lock(key, err => {
                                assert.ifError(err);
                                done();
                            });
                        }, 1001);
                    });
                });
            });

            t.test('PettyCache.mutex.lock should lock for 2 seconds when ttl parameter is specified', (t, done) => {
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
                                done();
                            });
                        }, 2001);
                    });
                });
            });

            t.test('PettyCache.mutex.lock should acquire a lock after retries', (t, done) => {
                const key = Math.random().toString();

                pettyCache.mutex.lock(key, { ttl: 2000 } , err => {
                    assert.ifError(err);

                    pettyCache.mutex.lock(key, err => {
                        assert(err);

                        pettyCache.mutex.lock(key, { retry: { interval: 500, times: 10 } }, err => {
                            assert.ifError(err);
                            done();
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

            t.test('PettyCache.mutex.lock should lock for 2 seconds when ttl parameter is specified', async () => {
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

            t.test('PettyCache.mutex.lock should acquire a lock after retries', async () => {
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
            t.test('PettyCache.mutex.unlock should unlock', (t, done) => {
                const key = Math.random().toString();

                pettyCache.mutex.lock(key, { ttl: 10000 }, err => {
                    assert.ifError(err);

                    pettyCache.mutex.lock(key, err => {
                        assert(err);

                        pettyCache.mutex.unlock(key, () => {
                            pettyCache.mutex.lock(key, err => {
                                assert.ifError(err);
                                done();
                            });
                        });
                    });
                });
            });

            t.test('PettyCache.mutex.unlock should work without a callback', (t, done) => {
                const key = Math.random().toString();

                pettyCache.mutex.lock(key, { ttl: 10000 }, err => {
                    assert.ifError(err);

                    pettyCache.mutex.unlock(key);
                    done();
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

    t.test('PettyCache.patch', { concurrency: true }, async (t) => {
        t.test('PettyCache.patch should fail if the key does not exist', (t, done) => {
            pettyCache.patch(Math.random().toString(), { b: 3 }, (err) => {
                assert(err, 'No error provided');
                done();
            });
        });

        t.test('PettyCache.patch should update the values of given object keys', (t, done) => {
            const key = Math.random().toString();

            pettyCache.set(key, { a: 1, b: 2, c: 3 }, () => {
                pettyCache.patch(key, { b: 4, c: 5 }, (err) => {
                    assert(!err, 'Error: ' + err);

                    pettyCache.get(key, (err, data) => {
                        assert(!err, 'Error: ' + err);
                        assert.deepEqual(data, { a: 1, b: 4, c: 5 });
                        done();
                    });
                });
            });
        });

        t.test('PettyCache.patch should update the values of given object keys with options', (t, done) => {
            const key = Math.random().toString();

            pettyCache.set(key, { a: 1, b: 2, c: 3 }, () => {
                pettyCache.patch(key, { b: 5, c: 6 }, { ttl: 10000 }, (err) => {
                    assert(!err, 'Error: ' + err);

                    pettyCache.get(key, (err, data) => {
                        assert(!err, 'Error: ' + err);
                        assert.deepEqual(data, { a: 1, b: 5, c: 6 });
                        done();
                    });
                });
            });
        });
    });

    t.test('PettyCache.semaphore', { concurrency: true }, async (t) => {
        t.test('PettyCache.semaphore.acquireLock', { concurrency: true }, async (t) => {
            t.test('should aquire a lock', (t, done) => {
                const key = Math.random().toString();

                pettyCache.semaphore.retrieveOrCreate(key, { size: 10 }, (err) => {
                    assert.ifError(err);

                    pettyCache.semaphore.acquireLock(key, (err, index) => {
                        assert.ifError(err);
                        assert.equal(index, 0);

                        pettyCache.semaphore.acquireLock(key, (err, index) => {
                            assert.ifError(err);
                            assert.equal(index, 1);
                            done();
                        });
                    });
                });
            });

            t.test('should not aquire a lock', (t, done) => {
                const key = Math.random().toString();

                pettyCache.semaphore.retrieveOrCreate(key, (err) => {
                    assert.ifError(err);

                    pettyCache.semaphore.acquireLock(key, (err, index) => {
                        assert.ifError(err);
                        assert.equal(index, 0);

                        pettyCache.semaphore.acquireLock(key, (err) => {
                            assert(err);
                            done();
                        });
                    });
                });
            });

            t.test('should aquire a lock after ttl', (t, done) => {
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
                                    done();
                                });
                            }, 1001);
                        });
                    });
                });
            });

            t.test('should aquire a lock with specified options', (t, done) => {
                const key = Math.random().toString();

                pettyCache.semaphore.retrieveOrCreate(key, { size: 10 }, (err) => {
                    assert.ifError(err);

                    // callback is optional
                    pettyCache.semaphore.acquireLock(key);

                    setTimeout(() => {
                        pettyCache.semaphore.acquireLock(key, { retry: { interval: 500, times: 10 }, ttl: 500 }, (err, index) => {
                            assert.ifError(err);
                            assert.equal(index, 1);
                            done();
                        });
                    }, 1000);
                });
            });

            t.test('should fail if the semaphore does not exist', (t, done) => {
                const key = Math.random().toString();

                pettyCache.semaphore.acquireLock(key, 0, (err) => {
                    assert(err);
                    assert.strictEqual(err.message, `Semaphore ${key} doesn't exist.`);
                    done();
                });
            });
        });

        t.test('PettyCache.semaphore.consumeLock', { concurrency: true }, async (t) => {
            t.test('should consume a lock', (t, done) => {
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
                                        done();
                                    });
                                });
                            });
                        });
                    });
                });
            });

            t.test('should ensure at least one lock is not consumed', (t, done) => {
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
                                            done();
                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            });

            t.test('should fail if the semaphore does not exist', (t, done) => {
                const key = Math.random().toString();

                pettyCache.semaphore.consumeLock(key, 0, (err) => {
                    assert(err);
                    assert.strictEqual(err.message, `Semaphore ${key} doesn't exist.`);
                    done();
                });
            });

            t.test('should fail if index is larger than semaphore', (t, done) => {
                const key = Math.random().toString();

                pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err) => {
                    assert.ifError(err);

                    pettyCache.semaphore.acquireLock(key, (err, index) => {
                        assert.ifError(err);
                        assert.equal(index, 0);

                        pettyCache.semaphore.consumeLock(key, 10, (err) => {
                            assert(err);
                            assert.strictEqual(err.message, `Index 10 for semaphore ${key} is invalid.`);
                            done();
                        });
                    });
                });
            });

            t.test('callback is optional', (t, done) => {
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
                                    done();
                                });
                            });
                        });
                    });
                });
            });
        });

        t.test('PettyCache.semaphore.expand', { concurrency: true }, async (t) => {
            t.test('should increase the size of a semaphore pool', (t, done) => {
                const key = Math.random().toString();

                pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err, pool) => {
                    assert.ifError(err);
                    assert.strictEqual(pool.length, 2);

                    pettyCache.semaphore.expand(key, 3, (err) => {
                        assert.ifError(err);

                        pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err, pool) => {
                            assert.ifError(err);
                            assert.strictEqual(pool.length, 3);
                            done();
                        });
                    });
                });
            });

            t.test('should refuse to shrink a pool', (t, done) => {
                const key = Math.random().toString();

                pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err, pool) => {
                    assert.ifError(err);
                    assert.strictEqual(pool.length, 2);

                    pettyCache.semaphore.expand(key, 1, (err) => {
                        assert(err);
                        assert.strictEqual(err.message, 'Cannot shrink pool, size is 2 and you requested a size of 1.');
                        done();
                    });
                });
            });

            t.test('should succeed if pool size is already equal to the specified size', (t, done) => {
                const key = Math.random().toString();

                pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err, pool) => {
                    assert.ifError(err);
                    assert.strictEqual(pool.length, 2);

                    pettyCache.semaphore.expand(key, 2, (err) => {
                        assert.ifError(err);

                        pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err, pool) => {
                            assert.ifError(err);
                            assert.strictEqual(pool.length, 2);
                            done();
                        });
                    });
                });
            });

            t.test('callback is optional', (t, done) => {
                const key = Math.random().toString();

                pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err, pool) => {
                    assert.ifError(err);
                    assert.strictEqual(pool.length, 2);

                    pettyCache.semaphore.expand(key, 3);

                    pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err, pool) => {
                        assert.ifError(err);
                        assert.strictEqual(pool.length, 3);
                        done();
                    });
                });
            });

            t.test('should fail if the semaphore does not exist', (t, done) => {
                const key = Math.random().toString();

                pettyCache.semaphore.expand(key, 10, (err) => {
                    assert(err);
                    assert.strictEqual(err.message, `Semaphore ${key} doesn't exist.`);
                    done();
                });
            });
        });

        t.test('PettyCache.semaphore.releaseLock', { concurrency: true }, async (t) => {
            t.test('should release a lock', (t, done) => {
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
                                    done();
                                });
                            });
                        });
                    });
                });
            });

            t.test('should fail to release a lock outside of the semaphore size', (t, done) => {
                const key = Math.random().toString();

                pettyCache.semaphore.retrieveOrCreate(key, (err) => {
                    assert.ifError(err);

                    pettyCache.semaphore.acquireLock(key, (err, index) => {
                        assert.ifError(err);
                        assert.equal(index, 0);

                        pettyCache.semaphore.releaseLock(key, 10, (err) => {
                            assert(err);
                            assert.strictEqual(err.message, `Index 10 for semaphore ${key} is invalid.`);
                            done();
                        });
                    });
                });
            });

            t.test('callback is optional', (t, done) => {
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
                                done();
                            });
                        });
                    });
                });
            });

            t.test('should fail if the semaphore does not exist', (t, done) => {
                const key = Math.random().toString();

                pettyCache.semaphore.releaseLock(key, 10, (err) => {
                    assert(err);
                    assert.strictEqual(err.message, `Semaphore ${key} doesn't exist.`);
                    done();
                });
            });
        });

        t.test('PettyCache.semaphore.reset', { concurrency: true }, async (t) => {
            t.test('should reset all locks', (t, done) => {
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
                                        done();
                                    });
                                });
                            });
                        });
                    });
                });
            });

            t.test('callback is optional', (t, done) => {
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
                                    done();
                                });
                            });
                        });
                    });
                });
            });

            t.test('should fail if the semaphore does not exist', (t, done) => {
                const key = Math.random().toString();

                pettyCache.semaphore.reset(key, (err) => {
                    assert(err);
                    assert.strictEqual(err.message, `Semaphore ${key} doesn't exist.`);
                    done();
                });
            });
        });

        t.test('PettyCache.semaphore.retrieveOrCreate', { concurrency: true }, async (t) => {
            t.test('should create a new semaphore', (t, done) => {
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
                        done();
                    });
                });
            });

            t.test('should have a min size of 1', (t, done) => {
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
                        done();
                    });
                });
            });

            t.test('should allow options.size to provide a function', (t, done) => {
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
                        done();
                    });
                });
            });

            t.test('callback is optional', (t, done) => {
                const key = Math.random().toString();

                pettyCache.semaphore.retrieveOrCreate(key);

                pettyCache.semaphore.retrieveOrCreate(key, { size: 100 }, (err, semaphore) => {
                    assert.ifError(err);
                    assert(semaphore);
                    assert.equal(semaphore.length, 1);
                    assert(semaphore.every(s => s.status === 'available'));
                    done();
                });
            });
        });
    });

    t.test('PettyCache.set', { concurrency: true }, async (t) => {
        t.test('PettyCache.set should set a value', (t, done) => {
            const key = Math.random().toString();

            pettyCache.set(key, 'hello world', () => {
                pettyCache.get(key, (err, value) => {
                    assert.equal(value, 'hello world');

                    // Wait for memory cache to expire
                    setTimeout(() => {
                        pettyCache.get(key, (err, value) => {
                            assert.equal(value, 'hello world');
                            done();
                        });
                    }, 5001);
                });
            });
        });

        t.test('PettyCache.set should work without a callback', (t, done) => {
            pettyCache.set(Math.random().toString(), 'hello world');
            done();
        });

        t.test('PettyCache.set should set a value with the specified TTL option', (t, done) => {
            const key = Math.random().toString();

            pettyCache.set(key, 'hello world', { ttl: 6000 },() => {
                pettyCache.get(key, (err, value) => {
                    assert.equal(value, 'hello world');

                    // Wait for memory cache to expire
                    setTimeout(() => {
                        pettyCache.get(key, (err, value) => {
                            assert.equal(value, null);
                            done();
                        });
                    }, 6001);
                });
            });
        });

        t.test('PettyCache.set should set a value with the specified TTL option using max and min', (t, done) => {
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
                                    done();
                                });
                            }, 6001);
                        });
                    }, 1000);
                });
            });
        });

        t.test('PettyCache.set should set a value with the specified TTL option using min only', (t, done) => {
            const key = Math.random().toString();

            pettyCache.set(key, 'hello world', { ttl: { min: 6000 } },() => {
                pettyCache.get(key, (err, value) => {
                    assert.strictEqual(value, 'hello world');
                    done();
                });
            });
        });

        t.test('PettyCache.set should set a value with the specified TTL option using max only', (t, done) => {
            const key = Math.random().toString();

            pettyCache.set(key, 'hello world', { ttl: { max: 10000 } },() => {
                pettyCache.get(key, (err, value) => {
                    assert.strictEqual(value, 'hello world');
                    done();
                });
            });
        });

        t.test('PettyCache.set(key, \'\')', (t, done) => {
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
                                    done();
                                });
                            }, 5001);
                        });
                    }, 5001);
                });
            });
        });

        t.test('PettyCache.set(key, 0)', (t, done) => {
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
                                    done();
                                });
                            }, 5001);
                        });
                    }, 5001);
                });
            });
        });

        t.test('PettyCache.set(key, false)', (t, done) => {
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
                                    done();
                                });
                            }, 5001);
                        });
                    }, 5001);
                });
            });
        });

        t.test('PettyCache.set(key, NaN)', (t, done) => {
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
                                    done();
                                });
                            }, 5001);
                        });
                    }, 5001);
                });
            });
        });

        t.test('PettyCache.set(key, null)', (t, done) => {
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
                                    done();
                                });
                            }, 5001);
                        });
                    }, 5001);
                });
            });
        });

        t.test('PettyCache.set(key, undefined)', (t, done) => {
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
                                    done();
                                });
                            }, 5001);
                        });
                    }, 5001);
                });
            });
        });
    });

    t.test('redisClient', { concurrency: true }, async (t) => {
        t.test('redisClient.mget(falsy keys)', (t, done) => {
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
                redisClient.psetex(key, 100, PettyCache.stringify(values[key]), callback);
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
                    done();
                });
            });
        });

        t.test('redisClient.psetex(key, \'\')', (t, done) => {
            const key = Math.random().toString();

            redisClient.psetex(key, 100, PettyCache.stringify(''), (err) => {
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
                            done();
                        });
                    }, 101);
                });
            });
        });

        t.test('redisClient.psetex(key, 0)', (t, done) => {
            const key = Math.random().toString();

            redisClient.psetex(key, 100, PettyCache.stringify(0), (err) => {
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
                            done();
                        });
                    }, 101);
                });
            });
        });

        t.test('redisClient.psetex(key, false)', (t, done) => {
            const key = Math.random().toString();

            redisClient.psetex(key, 100, PettyCache.stringify(false), (err) => {
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
                            done();
                        });
                    }, 101);
                });
            });
        });

        t.test('redisClient.psetex(key, NaN)', (t, done) => {
            const key = Math.random().toString();

            redisClient.psetex(key, 100, PettyCache.stringify(NaN), (err) => {
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
                            done();
                        });
                    }, 101);
                });
            });
        });

        t.test('redisClient.psetex(key, null)', (t, done) => {
            const key = Math.random().toString();

            redisClient.psetex(key, 100, PettyCache.stringify(null), (err) => {
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
                            done();
                        });
                    }, 101);
                });
            });
        });

        t.test('redisClient.psetex(key, undefined)', (t, done) => {
            const key = Math.random().toString();

            redisClient.psetex(key, 100, PettyCache.stringify(undefined), (err) => {
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
                            done();
                        });
                    }, 101);
                });
            });
        });
    });

    t.test('Benchmark', async (t) => {
        const emojis = require('./emojis.json');

        t.test('PettyCache should be faster than node-redis', (t, done) => {
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
                            done();
                        });
                    });
                });
            });
        });
    });
});

test('PettyCache.fetch should return error if Redis GET fails', (t, done) => {
    const stubClient = redis.createClient();
    const originalGet = stubClient.get.bind(stubClient);

    stubClient.get = (key, callback) => callback(new Error('Redis GET error'));

    const pettyCache = new PettyCache(stubClient);

    pettyCache.fetch(Math.random().toString(), (callback) => {
        callback(null, 'value');
    }, (err) => {
        stubClient.get = originalGet;

        assert(err);
        assert.strictEqual(err.message, 'Redis GET error');

        done();
    });
});

test('PettyCache.get should return error if Redis GET fails', (t, done) => {
    const stubClient = redis.createClient();
    const originalGet = stubClient.get.bind(stubClient);

    stubClient.get = (key, callback) => callback(new Error('Redis GET error'));

    const pettyCache = new PettyCache(stubClient);

    pettyCache.get(Math.random().toString(), (err) => {
        stubClient.get = originalGet;

        assert(err);
        assert.strictEqual(err.message, 'Redis GET error');

        done();
    });
});

test('PettyCache.bulkFetch should return error if Redis MGET fails', (t, done) => {
    const stubClient = redis.createClient();
    const originalMget = stubClient.mget.bind(stubClient);

    stubClient.mget = (keys, callback) => callback(new Error('Redis MGET error'));

    const pettyCache = new PettyCache(stubClient);

    pettyCache.bulkFetch([Math.random().toString()], (keys, callback) => {
        callback(null, {});
    }, (err) => {
        stubClient.mget = originalMget;

        assert(err);
        assert.strictEqual(err.message, 'Redis MGET error');

        done();
    });
});

test('PettyCache.bulkGet should return error if Redis MGET fails', (t, done) => {
    const stubClient = redis.createClient();
    const originalMget = stubClient.mget.bind(stubClient);

    stubClient.mget = (keys, callback) => callback(new Error('Redis MGET error'));

    const pettyCache = new PettyCache(stubClient);

    pettyCache.bulkGet([Math.random().toString()], (err) => {
        stubClient.mget = originalMget;

        assert(err);
        assert.strictEqual(err.message, 'Redis MGET error');

        done();
    });
});

test('PettyCache.mutex.lock should return error if Redis SET fails', (t, done) => {
    const stubClient = redis.createClient();
    const originalSet = stubClient.set.bind(stubClient);

    stubClient.set = (...args) => args[args.length - 1](new Error('Redis SET error'));

    const pettyCache = new PettyCache(stubClient);

    pettyCache.mutex.lock(Math.random().toString(), (err) => {
        stubClient.set = originalSet;

        assert(err);
        assert.strictEqual(err.message, 'Redis SET error');

        done();
    });
});

test('PettyCache.del should return error if Redis DEL fails', (t, done) => {
    const stubClient = redis.createClient();
    const originalDel = stubClient.del.bind(stubClient);

    stubClient.del = (key, callback) => callback(new Error('Redis DEL error'));

    const pettyCache = new PettyCache(stubClient);

    pettyCache.del(Math.random().toString(), (err) => {
        stubClient.del = originalDel;

        assert(err);
        assert.strictEqual(err.message, 'Redis DEL error');

        done();
    });
});

test('PettyCache.mutex.unlock should return error if Redis DEL fails', (t, done) => {
    const stubClient = redis.createClient();
    const originalDel = stubClient.del.bind(stubClient);

    stubClient.del = (key, callback) => callback(new Error('Redis DEL error'));

    const pettyCache = new PettyCache(stubClient);

    pettyCache.mutex.unlock(Math.random().toString(), (err) => {
        stubClient.del = originalDel;

        assert(err);
        assert.strictEqual(err.message, 'Redis DEL error');

        done();
    });
});

test('PettyCache.patch should return error if Redis GET fails', (t, done) => {
    const stubClient = redis.createClient();
    const originalGet = stubClient.get.bind(stubClient);

    stubClient.get = (key, callback) => callback(new Error('Redis GET error'));

    const pettyCache = new PettyCache(stubClient);

    pettyCache.patch(Math.random().toString(), { a: 1 }, (err) => {
        stubClient.get = originalGet;

        assert(err);
        assert.strictEqual(err.message, 'Redis GET error');

        done();
    });
});

test('PettyCache.mutex.lock should return error if Redis SET returns unexpected response', (t, done) => {
    const stubClient = redis.createClient();
    const originalSet = stubClient.set.bind(stubClient);

    stubClient.set = (...args) => args[args.length - 1](null, 'UNEXPECTED');

    const pettyCache = new PettyCache(stubClient);

    pettyCache.mutex.lock(Math.random().toString(), (err) => {
        stubClient.set = originalSet;

        assert(err);
        assert.strictEqual(err.message, 'UNEXPECTED');

        done();
    });
});

test('PettyCache.semaphore.retrieveOrCreate should return error if Redis GET fails', (t, done) => {
    const stubClient = redis.createClient();
    const originalGet = stubClient.get.bind(stubClient);

    stubClient.get = (key, callback) => callback(new Error('Redis GET error'));

    const pettyCache = new PettyCache(stubClient);

    pettyCache.semaphore.retrieveOrCreate(Math.random().toString(), (err) => {
        stubClient.get = originalGet;

        assert(err);
        assert.strictEqual(err.message, 'Redis GET error');

        done();
    });
});

test('PettyCache.semaphore.acquireLock should return error if Redis GET fails', (t, done) => {
    const stubClient = redis.createClient();
    const originalGet = stubClient.get.bind(stubClient);

    stubClient.get = (key, callback) => callback(new Error('Redis GET error'));

    const pettyCache = new PettyCache(stubClient);

    pettyCache.semaphore.acquireLock(Math.random().toString(), (err) => {
        stubClient.get = originalGet;

        assert(err);
        assert.strictEqual(err.message, 'Redis GET error');

        done();
    });
});

test('PettyCache.semaphore.consumeLock should return error if Redis GET fails', (t, done) => {
    const stubClient = redis.createClient();
    const originalGet = stubClient.get.bind(stubClient);

    stubClient.get = (key, callback) => callback(new Error('Redis GET error'));

    const pettyCache = new PettyCache(stubClient);

    pettyCache.semaphore.consumeLock(Math.random().toString(), 0, (err) => {
        stubClient.get = originalGet;

        assert(err);
        assert.strictEqual(err.message, 'Redis GET error');

        done();
    });
});

test('PettyCache.semaphore.expand should return error if Redis GET fails', (t, done) => {
    const stubClient = redis.createClient();
    const originalGet = stubClient.get.bind(stubClient);

    stubClient.get = (key, callback) => callback(new Error('Redis GET error'));

    const pettyCache = new PettyCache(stubClient);

    pettyCache.semaphore.expand(Math.random().toString(), 10, (err) => {
        stubClient.get = originalGet;

        assert(err);
        assert.strictEqual(err.message, 'Redis GET error');

        done();
    });
});

test('PettyCache.semaphore.releaseLock should return error if Redis GET fails', (t, done) => {
    const stubClient = redis.createClient();
    const originalGet = stubClient.get.bind(stubClient);

    stubClient.get = (key, callback) => callback(new Error('Redis GET error'));

    const pettyCache = new PettyCache(stubClient);

    pettyCache.semaphore.releaseLock(Math.random().toString(), 0, (err) => {
        stubClient.get = originalGet;

        assert(err);
        assert.strictEqual(err.message, 'Redis GET error');

        done();
    });
});

test('PettyCache.semaphore.reset should return error if Redis GET fails', (t, done) => {
    const stubClient = redis.createClient();
    const originalGet = stubClient.get.bind(stubClient);

    stubClient.get = (key, callback) => callback(new Error('Redis GET error'));

    const pettyCache = new PettyCache(stubClient);

    pettyCache.semaphore.reset(Math.random().toString(), (err) => {
        stubClient.get = originalGet;

        assert(err);
        assert.strictEqual(err.message, 'Redis GET error');

        done();
    });
});

test('PettyCache.semaphore.retrieveOrCreate should return error if Redis SET fails', (t, done) => {
    const stubClient = redis.createClient();
    const originalSet = stubClient.set.bind(stubClient);

    stubClient.set = (...args) => {
        if (args.includes('NX')) {
            return originalSet(...args);
        }

        args[args.length - 1](new Error('Redis SET error'));
    };

    const pettyCache = new PettyCache(stubClient);

    pettyCache.semaphore.retrieveOrCreate(Math.random().toString(), (err) => {
        stubClient.set = originalSet;

        assert(err);
        assert.strictEqual(err.message, 'Redis SET error');

        done();
    });
});

test('PettyCache.semaphore.acquireLock should return error if Redis SET fails', (t, done) => {
    const key = Math.random().toString();

    pettyCache.semaphore.retrieveOrCreate(key, (err) => {
        assert.ifError(err);

        const stubClient = redis.createClient();
        const originalSet = stubClient.set.bind(stubClient);

        stubClient.set = (...args) => {
            if (args.includes('NX')) {
                return originalSet(...args);
            }

            args[args.length - 1](new Error('Redis SET error'));
        };

        const stubCache = new PettyCache(stubClient);

        stubCache.semaphore.acquireLock(key, (err) => {
            stubClient.set = originalSet;

            assert(err);
            assert.strictEqual(err.message, 'Redis SET error');

            done();
        });
    });
});

test('PettyCache.semaphore.consumeLock should return error if Redis SET fails', (t, done) => {
    const key = Math.random().toString();

    pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err) => {
        assert.ifError(err);

        pettyCache.semaphore.acquireLock(key, (err, index) => {
            assert.ifError(err);

            const stubClient = redis.createClient();
            const originalSet = stubClient.set.bind(stubClient);

            stubClient.set = (...args) => {
                if (args.includes('NX')) {
                    return originalSet(...args);
                }

                args[args.length - 1](new Error('Redis SET error'));
            };

            const stubCache = new PettyCache(stubClient);

            stubCache.semaphore.consumeLock(key, index, (err) => {
                stubClient.set = originalSet;

                assert(err);
                assert.strictEqual(err.message, 'Redis SET error');

                done();
            });
        });
    });
});

test('PettyCache.semaphore.expand should return error if Redis SET fails', (t, done) => {
    const key = Math.random().toString();

    pettyCache.semaphore.retrieveOrCreate(key, { size: 2 }, (err) => {
        assert.ifError(err);

        const stubClient = redis.createClient();
        const originalSet = stubClient.set.bind(stubClient);

        stubClient.set = (...args) => {
            if (args.includes('NX')) {
                return originalSet(...args);
            }

            args[args.length - 1](new Error('Redis SET error'));
        };

        const stubCache = new PettyCache(stubClient);

        stubCache.semaphore.expand(key, 5, (err) => {
            stubClient.set = originalSet;

            assert(err);
            assert.strictEqual(err.message, 'Redis SET error');

            done();
        });
    });
});

test('PettyCache.semaphore.releaseLock should return error if Redis SET fails', (t, done) => {
    const key = Math.random().toString();

    pettyCache.semaphore.retrieveOrCreate(key, (err) => {
        assert.ifError(err);

        pettyCache.semaphore.acquireLock(key, (err, index) => {
            assert.ifError(err);

            const stubClient = redis.createClient();
            const originalSet = stubClient.set.bind(stubClient);

            stubClient.set = (...args) => {
                if (args.includes('NX')) {
                    return originalSet(...args);
                }

                args[args.length - 1](new Error('Redis SET error'));
            };

            const stubCache = new PettyCache(stubClient);

            stubCache.semaphore.releaseLock(key, index, (err) => {
                stubClient.set = originalSet;

                assert(err);
                assert.strictEqual(err.message, 'Redis SET error');

                done();
            });
        });
    });
});

test('PettyCache.semaphore.reset should return error if Redis SET fails', (t, done) => {
    const key = Math.random().toString();

    pettyCache.semaphore.retrieveOrCreate(key, (err) => {
        assert.ifError(err);

        const stubClient = redis.createClient();
        const originalSet = stubClient.set.bind(stubClient);

        stubClient.set = (...args) => {
            if (args.includes('NX')) {
                return originalSet(...args);
            }

            args[args.length - 1](new Error('Redis SET error'));
        };

        const stubCache = new PettyCache(stubClient);

        stubCache.semaphore.reset(key, (err) => {
            stubClient.set = originalSet;

            assert(err);
            assert.strictEqual(err.message, 'Redis SET error');

            done();
        });
    });
});

test('PettyCache.fetch should lock around Redis', (t, done) => {
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

                done();
            });
        });
    });
});