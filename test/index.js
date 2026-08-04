const test = require('node:test');
const assert = require('node:assert');
const timers = require('node:timers/promises');

const memoryCache = require('memory-cache');
const redis = require('redis');

const PettyCache = require('../index.js');

const redisClient = redis.createClient();
const pettyCache = new PettyCache(redisClient);

test('petty-cache', { concurrency: true }, async (t) => {
    t.test('new PettyCache()', { concurrency: true }, async (t) => {
        t.test('new PettyCache()', async () => {
            const key = Math.random().toString();
            const newPettyCache = new PettyCache();

            const data = await newPettyCache.fetch(key, async () => ({ foo: 'bar' }));

            assert.equal(data.foo, 'bar');

            const cached = await newPettyCache.fetch(key, () => {
                throw 'This function should not be called';
            });

            assert.equal(cached.foo, 'bar');

            // Wait for memory cache to expire
            await timers.setTimeout(5001);

            const fromRedis = await newPettyCache.fetch(key, () => {
                throw 'This function should not be called';
            });

            assert.strictEqual(fromRedis.foo, 'bar');
        });

        t.test('new PettyCache(port, host)', async () => {
            const key = Math.random().toString();
            const newPettyCache = new PettyCache(6379, 'localhost');

            const data = await newPettyCache.fetch(key, async () => ({ foo: 'bar' }));

            assert.equal(data.foo, 'bar');

            const cached = await newPettyCache.fetch(key, () => {
                throw 'This function should not be called';
            });

            assert.equal(cached.foo, 'bar');

            // Wait for memory cache to expire
            await timers.setTimeout(5001);

            const fromRedis = await newPettyCache.fetch(key, () => {
                throw 'This function should not be called';
            });

            assert.strictEqual(fromRedis.foo, 'bar');
        });

        t.test('new PettyCache(redisClient)', async () => {
            const key = Math.random().toString();
            const redisClient = redis.createClient();
            const newPettyCache = new PettyCache(redisClient);

            const data = await newPettyCache.fetch(key, async () => ({ foo: 'bar' }));

            assert.equal(data.foo, 'bar');

            const cached = await newPettyCache.fetch(key, () => {
                throw 'This function should not be called';
            });

            assert.equal(cached.foo, 'bar');

            // Wait for memory cache to expire
            await timers.setTimeout(5001);

            const fromRedis = await newPettyCache.fetch(key, () => {
                throw 'This function should not be called';
            });

            assert.strictEqual(fromRedis.foo, 'bar');
        });
    });

    t.test('memory-cache', { concurrency: true }, async (t) => {
        t.test('memoryCache.put(key, \'\')', (t, done) => {
            const key = Math.random().toString();

            memoryCache.put(key, '', 200);
            assert(memoryCache.keys().includes(key));
            assert.strictEqual(memoryCache.get(key), '');

            // Wait for memory cache to expire
            setTimeout(() => {
                assert(!memoryCache.keys().includes(key));
                assert.strictEqual(memoryCache.get(key), null);
                done();
            }, 500);
        });

        t.test('memoryCache.put(key, 0)', (t, done) => {
            const key = Math.random().toString();

            memoryCache.put(key, 0, 200);
            assert(memoryCache.keys().includes(key));
            assert.strictEqual(memoryCache.get(key), 0);

            // Wait for memory cache to expire
            setTimeout(() => {
                assert(!memoryCache.keys().includes(key));
                assert.strictEqual(memoryCache.get(key), null);
                done();
            }, 500);
        });

        t.test('memoryCache.put(key, false)', (t, done) => {
            const key = Math.random().toString();

            memoryCache.put(key, false, 200);
            assert(memoryCache.keys().includes(key));
            assert.strictEqual(memoryCache.get(key), false);

            // Wait for memory cache to expire
            setTimeout(() => {
                assert(!memoryCache.keys().includes(key));
                assert.strictEqual(memoryCache.get(key), null);
                done();
            }, 500);
        });

        t.test('memoryCache.put(key, NaN)', (t, done) => {
            const key = Math.random().toString();

            memoryCache.put(key, NaN, 200);
            assert(memoryCache.keys().includes(key));
            assert(isNaN(memoryCache.get(key)));

            // Wait for memory cache to expire
            setTimeout(() => {
                assert(!memoryCache.keys().includes(key));
                assert.strictEqual(memoryCache.get(key), null);
                done();
            }, 500);
        });

        t.test('memoryCache.put(key, null)', (t, done) => {
            const key = Math.random().toString();

            memoryCache.put(key, null, 200);
            assert(memoryCache.keys().includes(key));
            assert.strictEqual(memoryCache.get(key), null);

            // Wait for memory cache to expire
            setTimeout(() => {
                assert(!memoryCache.keys().includes(key));
                assert.strictEqual(memoryCache.get(key), null);
                done();
            }, 500);
        });

        t.test('memoryCache.put(key, undefined)', (t, done) => {
            const key = Math.random().toString();

            memoryCache.put(key, undefined, 200);
            assert(memoryCache.keys().includes(key));
            assert.strictEqual(memoryCache.get(key), undefined);

            // Wait for memory cache to expire
            setTimeout(() => {
                assert(!memoryCache.keys().includes(key));
                assert.strictEqual(memoryCache.get(key), null);
                done();
            }, 500);
        });
    });

    t.test('PettyCache.bulkFetch', { concurrency: true }, async (t) => {
        t.test('PettyCache.bulkFetch', async () => {
            // Use per-run keys so values left in Redis by a previous test run can't expire mid-test
            const prefix = Math.random().toString();
            const keyA = `${prefix}-a`;
            const keyB = `${prefix}-b`;
            const keyC = `${prefix}-c`;
            const keyD = `${prefix}-d`;

            await pettyCache.set(keyA, 1);
            await pettyCache.set(keyB, '2');

            const values = await pettyCache.bulkFetch([keyA, keyB, keyC, keyD], async (keys) => {
                assert(keys.length === 2);

                const data = {};

                data[keyC] = [3];
                data[keyD] = { num: 4 };

                return data;
            });

            assert.strictEqual(values[keyA], 1);
            assert.strictEqual(values[keyB], '2');
            assert.strictEqual(values[keyC][0], 3);
            assert.strictEqual(values[keyD].num, 4);

            // Call bulkFetch again to ensure memory serialization is working as expected.
            const fromMemory = await pettyCache.bulkFetch([keyA, keyB, keyC, keyD], () => {
                throw 'This function should not be called';
            });

            assert.strictEqual(fromMemory[keyA], 1);
            assert.strictEqual(fromMemory[keyB], '2');
            assert.strictEqual(fromMemory[keyC][0], 3);
            assert.strictEqual(fromMemory[keyD].num, 4);

            // Wait for memory cache to expire
            await timers.setTimeout(5001);

            const fromRedis = await pettyCache.bulkFetch([keyA, keyB, keyC, keyD], () => {
                throw 'This function should not be called';
            });

            assert.strictEqual(fromRedis[keyA], 1);
            assert.strictEqual(fromRedis[keyB], '2');
            assert.strictEqual(fromRedis[keyC][0], 3);
            assert.strictEqual(fromRedis[keyD].num, 4);

            // Call bulkFetch again to ensure memory serialization is working as expected.
            const fromMemoryAgain = await pettyCache.bulkFetch([keyA, keyB, keyC, keyD], () => {
                throw 'This function should not be called';
            });

            assert.strictEqual(fromMemoryAgain[keyA], 1);
            assert.strictEqual(fromMemoryAgain[keyB], '2');
            assert.strictEqual(fromMemoryAgain[keyC][0], 3);
            assert.strictEqual(fromMemoryAgain[keyD].num, 4);
        });

        t.test('PettyCache.bulkFetch should cache null values returned by func', async () => {
            const key1 = Math.random().toString();
            const key2 = Math.random().toString();

            const values = await pettyCache.bulkFetch([key1, key2], async (keys) => {
                assert.strictEqual(keys.length, 2);
                assert(keys.some(k => k === key1));
                assert(keys.some(k => k === key2));

                const data = {};

                data[key1] = '1';
                data[key2] = null;

                return data;
            });

            assert.strictEqual(Object.keys(values).length, 2);
            assert.strictEqual(values[key1], '1');
            assert.strictEqual(values[key2], null);

            const fromMemory = await pettyCache.bulkFetch([key1, key2], () => {
                throw 'This function should not be called';
            });

            assert.strictEqual(Object.keys(fromMemory).length, 2);
            assert.strictEqual(fromMemory[key1], '1');
            assert.strictEqual(fromMemory[key2], null);

            // Wait for memory cache to expire
            await timers.setTimeout(5001);

            const fromRedis = await pettyCache.bulkFetch([key1, key2], () => {
                throw 'This function should not be called';
            });

            assert.strictEqual(Object.keys(fromRedis).length, 2);
            assert.strictEqual(fromRedis[key1], '1');
            assert.strictEqual(fromRedis[key2], null);
        });

        t.test('PettyCache.bulkFetch should return values (promises)', async () => {
            const key1 = Math.random().toString();
            const key2 = Math.random().toString();

            await pettyCache.set(key1, '1');

            const values = await pettyCache.bulkFetch([key1, key2], async () => {
                const data = {};
                data[key2] = '2';
                return data;
            });

            assert.strictEqual(values[key1], '1');
            assert.strictEqual(values[key2], '2');
        });

        t.test('PettyCache.bulkFetch should return empty object when no keys are passed (promises)', async () => {
            const values = await pettyCache.bulkFetch([], () => {
                throw new Error('This function should not be called');
            });
            assert.deepEqual(values, {});
        });

        t.test('PettyCache.bulkFetch should return values with options (promises)', async () => {
            const key = Math.random().toString();
            const values = await pettyCache.bulkFetch([key], async (keys) => {
                const result = {};
                keys.forEach(k => { result[k] = 'value'; });
                return result;
            }, { ttl: 6000 });
            assert.deepEqual(values, { [key]: 'value' });
        });

        t.test('PettyCache.bulkFetch should support async func (promises)', async () => {
            const key1 = Math.random().toString();
            const key2 = Math.random().toString();

            await pettyCache.set(key1, '1');

            const values = await pettyCache.bulkFetch([key1, key2], async (keys) => {
                const result = {};
                keys.forEach(k => { result[k] = 'value'; });
                return result;
            });

            assert.strictEqual(values[key1], '1');
            assert.strictEqual(values[key2], 'value');
        });

        t.test('PettyCache.bulkFetch should reject if async func throws error (promises)', async () => {
            await assert.rejects(
                pettyCache.bulkFetch([Math.random().toString()], async () => {
                    throw new Error('PettyCache.bulkFetch should reject if async func throws error');
                }),
                { message: 'PettyCache.bulkFetch should reject if async func throws error' }
            );
        });

        t.test('PettyCache.bulkFetch should run func again after TTL', async () => {
            const keys = [Math.random().toString(), Math.random().toString()];
            let numberOfFuncCalls = 0;

            const func = async (keys) => {
                numberOfFuncCalls++;

                const results = {};
                results[keys[0]] = numberOfFuncCalls;
                results[keys[1]] = numberOfFuncCalls;

                return results;
            };

            const results = await pettyCache.bulkFetch(keys, func, { ttl: 6000 });

            assert.strictEqual(results[keys[0]], 1);
            assert.strictEqual(results[keys[1]], 1);

            const cached = await pettyCache.bulkGet(keys);

            assert.strictEqual(cached[keys[0]], 1);
            assert.strictEqual(cached[keys[1]], 1);

            // Wait for the TTL to expire
            await timers.setTimeout(6001);

            const expired = await pettyCache.bulkGet(keys);

            assert.strictEqual(expired[keys[0]], null);
            assert.strictEqual(expired[keys[1]], null);

            const refetched = await pettyCache.bulkFetch(keys, func, { ttl: 6000 });

            assert.strictEqual(refetched[keys[0]], 2);
            assert.strictEqual(refetched[keys[1]], 2);

            const recached = await pettyCache.bulkGet(keys);

            assert.strictEqual(recached[keys[0]], 2);
            assert.strictEqual(recached[keys[1]], 2);
        });
    });

    t.test('PettyCache.bulkGet', { concurrency: true }, async (t) => {
        t.test('PettyCache.bulkGet should return values', async () => {
            const key1 = Math.random().toString();
            const key2 = Math.random().toString();
            const key3 = Math.random().toString();

            await pettyCache.set(key1, '1');
            await pettyCache.set(key2, '2');
            await pettyCache.set(key3, '3');

            const values = await pettyCache.bulkGet([key1, key2, key3]);

            assert.strictEqual(Object.keys(values).length, 3);
            assert.strictEqual(values[key1], '1');
            assert.strictEqual(values[key2], '2');
            assert.strictEqual(values[key3], '3');

            // Call bulkGet again while values are still in memory cache
            const fromMemory = await pettyCache.bulkGet([key1, key2, key3]);

            assert.strictEqual(Object.keys(fromMemory).length, 3);
            assert.strictEqual(fromMemory[key1], '1');
            assert.strictEqual(fromMemory[key2], '2');
            assert.strictEqual(fromMemory[key3], '3');

            // Wait for memory cache to expire
            await timers.setTimeout(5001);

            // Ensure keys are still in Redis
            const fromRedis = await pettyCache.bulkGet([key1, key2, key3]);

            assert.strictEqual(Object.keys(fromRedis).length, 3);
            assert.strictEqual(fromRedis[key1], '1');
            assert.strictEqual(fromRedis[key2], '2');
            assert.strictEqual(fromRedis[key3], '3');
        });

        t.test('PettyCache.bulkGet should return null for missing keys', async () => {
            const key1 = Math.random().toString();
            const key2 = Math.random().toString();
            const key3 = Math.random().toString();

            await pettyCache.set(key1, '1');
            await pettyCache.set(key2, '2');

            const values = await pettyCache.bulkGet([key1, key2, key3]);

            assert.strictEqual(Object.keys(values).length, 3);
            assert.strictEqual(values[key1], '1');
            assert.strictEqual(values[key2], '2');
            assert.strictEqual(values[key3], null);

            // Call bulkGet again while values are still in memory cache
            const fromMemory = await pettyCache.bulkGet([key1, key2, key3]);

            assert.strictEqual(Object.keys(fromMemory).length, 3);
            assert.strictEqual(fromMemory[key1], '1');
            assert.strictEqual(fromMemory[key2], '2');
            assert.strictEqual(fromMemory[key3], null);

            // Wait for memory cache to expire
            await timers.setTimeout(5001);

            // Ensure keys are still in Redis
            const fromRedis = await pettyCache.bulkGet([key1, key2, key3]);

            assert.strictEqual(Object.keys(fromRedis).length, 3);
            assert.strictEqual(fromRedis[key1], '1');
            assert.strictEqual(fromRedis[key2], '2');
            assert.strictEqual(fromRedis[key3], null);
        });

        t.test('PettyCache.bulkGet should correctly handle falsy values', async () => {
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

            await Promise.all(Object.keys(values).map(key => pettyCache.set(key, values[key], { ttl: 6000 })));

            const keys = Object.keys(values);

            // Add an additional key to check handling of missing keys
            const key7 = Math.random().toString();
            keys.push(key7);

            const data = await pettyCache.bulkGet(keys);

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
            await timers.setTimeout(5001);

            // Ensure keys are still in Redis
            const fromRedis = await pettyCache.bulkGet(keys);

            assert.strictEqual(Object.keys(fromRedis).length, 7);
            assert.strictEqual(fromRedis[key1], '');
            assert.strictEqual(fromRedis[key2], 0);
            assert.strictEqual(fromRedis[key3], false);
            assert.strictEqual(typeof fromRedis[key4], 'number');
            assert(isNaN(fromRedis[key4]));
            assert.strictEqual(fromRedis[key5], null);
            assert.strictEqual(fromRedis[key6], undefined);
            assert.strictEqual(fromRedis[key7], null);

            // Wait for Redis cache to expire
            await timers.setTimeout(6001);

            // Ensure keys are not in Redis
            const expired = await pettyCache.bulkGet(keys);

            assert.strictEqual(Object.keys(expired).length, 7);
            assert.strictEqual(expired[key1], null);
            assert.strictEqual(expired[key2], null);
            assert.strictEqual(expired[key3], null);
            assert.strictEqual(expired[key4], null);
            assert.strictEqual(expired[key5], null);
            assert.strictEqual(expired[key6], null);
            assert.strictEqual(expired[key7], null);
        });

        t.test('PettyCache.bulkGet should return values (promises)', async () => {
            const key1 = Math.random().toString();
            const key2 = Math.random().toString();

            await pettyCache.set(key1, '1');
            await pettyCache.set(key2, '2');

            const values = await pettyCache.bulkGet([key1, key2]);
            assert.strictEqual(values[key1], '1');
            assert.strictEqual(values[key2], '2');
        });

        t.test('PettyCache.bulkGet should return null for missing keys (promises)', async () => {
            const key1 = Math.random().toString();
            const key2 = Math.random().toString();

            await pettyCache.set(key1, '1');

            const values = await pettyCache.bulkGet([key1, key2]);
            assert.strictEqual(values[key1], '1');
            assert.strictEqual(values[key2], null);
        });

        t.test('PettyCache.bulkGet should return empty object when no keys are passed (promises)', async () => {
            const values = await pettyCache.bulkGet([]);
            assert.deepEqual(values, {});
        });
    });

    t.test('PettyCache.bulkSet', { concurrency: true }, async (t) => {
        t.test('PettyCache.bulkSet should set values', async () => {
            const key1 = Math.random().toString();
            const key2 = Math.random().toString();
            const key3 = Math.random().toString();
            const values = {};

            values[key1] = '1';
            values[key2] = 2;
            values[key3] = '3';

            await pettyCache.bulkSet(values);

            assert.strictEqual(await pettyCache.get(key1), '1');
            assert.strictEqual(await pettyCache.get(key2), 2);
            assert.strictEqual(await pettyCache.get(key3), '3');

            // Wait for memory cache to expire
            await timers.setTimeout(5001);

            assert.strictEqual(await pettyCache.get(key1), '1');
            assert.strictEqual(await pettyCache.get(key2), 2);
            assert.strictEqual(await pettyCache.get(key3), '3');
        });

        t.test('PettyCache.bulkSet should set values with the specified TTL option', async () => {
            const key1 = Math.random().toString();
            const key2 = Math.random().toString();
            const key3 = Math.random().toString();
            const values = {};

            values[key1] = '1';
            values[key2] = 2;
            values[key3] = '3';

            await pettyCache.bulkSet(values, { ttl: 6000 });

            assert.strictEqual(await pettyCache.get(key1), '1');
            assert.strictEqual(await pettyCache.get(key2), 2);
            assert.strictEqual(await pettyCache.get(key3), '3');

            // Wait for Redis cache to expire
            await timers.setTimeout(6001);

            assert.strictEqual(await pettyCache.get(key1), null);
            assert.strictEqual(await pettyCache.get(key2), null);
            assert.strictEqual(await pettyCache.get(key3), null);
        });

        t.test('PettyCache.bulkSet should set values with the specified TTL option using max and min', async () => {
            const key1 = Math.random().toString();
            const key2 = Math.random().toString();
            const key3 = Math.random().toString();
            const values = {};

            values[key1] = '1';
            values[key2] = 2;
            values[key3] = '3';

            await pettyCache.bulkSet(values, { ttl: { max: 7000, min: 6000 } });

            assert.strictEqual(await pettyCache.get(key1), '1');
            assert.strictEqual(await pettyCache.get(key2), 2);
            assert.strictEqual(await pettyCache.get(key3), '3');

            // Wait for Redis cache to expire
            await timers.setTimeout(7001);

            assert.strictEqual(await pettyCache.get(key1), null);
            assert.strictEqual(await pettyCache.get(key2), null);
            assert.strictEqual(await pettyCache.get(key3), null);
        });

        t.test('PettyCache.bulkSet should set values with the specified TTL option using max only', async () => {
            const key1 = Math.random().toString();
            const key2 = Math.random().toString();
            const key3 = Math.random().toString();
            const values = {};

            values[key1] = '1';
            values[key2] = 2;
            values[key3] = '3';

            await pettyCache.bulkSet(values, { ttl: { max: 10000 } });

            assert.strictEqual(await pettyCache.get(key1), '1');
        });

        t.test('PettyCache.bulkSet should set values with the specified TTL option using min only', async () => {
            const key1 = Math.random().toString();
            const key2 = Math.random().toString();
            const key3 = Math.random().toString();
            const values = {};

            values[key1] = '1';
            values[key2] = 2;
            values[key3] = '3';

            await pettyCache.bulkSet(values, { ttl: { min: 6000 } });

            assert.strictEqual(await pettyCache.get(key1), '1');
        });

        t.test('PettyCache.bulkSet should set values (promises)', async () => {
            const key1 = Math.random().toString();
            const key2 = Math.random().toString();
            const values = {};

            values[key1] = '1';
            values[key2] = 2;

            await pettyCache.bulkSet(values);

            assert.strictEqual(await pettyCache.get(key1), '1');
            assert.strictEqual(await pettyCache.get(key2), 2);
        });

        t.test('PettyCache.bulkSet should set values with options (promises)', async () => {
            const key1 = Math.random().toString();
            const values = {};

            values[key1] = 'hello';

            await pettyCache.bulkSet(values, { ttl: 10000 });

            assert.strictEqual(await pettyCache.get(key1), 'hello');
        });
    });

    t.test('PettyCache.del', { concurrency: true }, async (t) => {
        t.test('PettyCache.del', async () => {
            const key = Math.random().toString();

            await pettyCache.set(key, key.split('').reverse().join(''));

            assert.strictEqual(await pettyCache.get(key), key.split('').reverse().join(''));

            await pettyCache.del(key);

            assert.strictEqual(await pettyCache.get(key), null);

            // Deleting a key that no longer exists should not error
            await pettyCache.del(key);
        });

        t.test('PettyCache.del (promises)', async () => {
            const key = Math.random().toString();

            await pettyCache.set(key, 'value');
            assert.strictEqual(await pettyCache.get(key), 'value');

            await pettyCache.del(key);
            assert.strictEqual(await pettyCache.get(key), null);
        });
    });

    t.test('PettyCache.fetch', { concurrency: true }, async (t) => {
        t.test('PettyCache.fetch', async () => {
            const key = Math.random().toString();

            const data = await pettyCache.fetch(key, async () => ({ foo: 'bar' }));

            assert.equal(data.foo, 'bar');

            const fromMemory = await pettyCache.fetch(key, () => {
                throw 'This function should not be called';
            });

            assert.equal(fromMemory.foo, 'bar');

            // Wait for memory cache to expire
            await timers.setTimeout(5001);

            const fromRedis = await pettyCache.fetch(key, () => {
                throw 'This function should not be called';
            });

            assert.strictEqual(fromRedis.foo, 'bar');
        });

        t.test('PettyCache.fetch should cache null values returned by func', async () => {
            const key = Math.random().toString();

            const data = await pettyCache.fetch(key, async () => null);

            assert.strictEqual(data, null);

            const fromMemory = await pettyCache.fetch(key, () => {
                throw 'This function should not be called';
            });

            assert.strictEqual(fromMemory, null);

            // Wait for memory cache to expire
            await timers.setTimeout(5001);

            const fromRedis = await pettyCache.fetch(key, () => {
                throw 'This function should not be called';
            });

            assert.strictEqual(fromRedis, null);
        });

        t.test('PettyCache.fetch should cache undefined values returned by func', async () => {
            const key = Math.random().toString();

            const data = await pettyCache.fetch(key, async () => undefined);

            assert.strictEqual(data, undefined);

            const fromMemory = await pettyCache.fetch(key, () => {
                throw 'This function should not be called';
            });

            assert.strictEqual(fromMemory, undefined);

            // Wait for memory cache to expire
            await timers.setTimeout(5001);

            const fromRedis = await pettyCache.fetch(key, () => {
                throw 'This function should not be called';
            });

            assert.strictEqual(fromRedis, undefined);
        });

        t.test('PettyCache.fetch should lock around func', async () => {
            const key = Math.random().toString();
            let numberOfFuncCalls = 0;

            const func = async () => {
                await timers.setTimeout(100);
                return ++numberOfFuncCalls;
            };

            const results = await Promise.all(Array.from({ length: 10 }, () => pettyCache.fetch(key, func)));

            results.forEach(data => assert.equal(data, 1));
        });

        t.test('PettyCache.fetch should run func again after TTL', async () => {
            const key = Math.random().toString();
            let numberOfFuncCalls = 0;

            const func = async () => {
                await timers.setTimeout(100);
                return ++numberOfFuncCalls;
            };

            const data = await pettyCache.fetch(key, func, { ttl: 6000 });

            assert.equal(data, 1);

            // Wait for the TTL to expire
            await timers.setTimeout(6001);

            const refetched = await pettyCache.fetch(key, func, { ttl: 6000 });

            assert.equal(refetched, 2);

            const cached = await pettyCache.fetch(key, func, { ttl: 6000 });

            assert.equal(cached, 2);
        });

        t.test('PettyCache.fetch should support sync func without callback', async () => {
            const key = Math.random().toString();

            const data = await pettyCache.fetch(key, () => {
                return { foo: 'bar' };
            });

            assert.equal(data.foo, 'bar');

            const fromMemory = await pettyCache.fetch(key, () => {
                throw 'This function should not be called';
            });

            assert.equal(fromMemory.foo, 'bar');

            // Wait for memory cache to expire
            await timers.setTimeout(5001);

            const fromRedis = await pettyCache.fetch(key, () => {
                throw 'This function should not be called';
            });

            assert.strictEqual(fromRedis.foo, 'bar');
        });

        t.test('PettyCache.fetch should return value (promises)', async () => {
            const key = Math.random().toString();

            const data = await pettyCache.fetch(key, async () => ({ foo: 'bar' }));

            assert.strictEqual(data.foo, 'bar');

            const cached = await pettyCache.fetch(key, () => {
                throw 'This function should not be called';
            });

            assert.strictEqual(cached.foo, 'bar');
        });

        t.test('PettyCache.fetch should support async func (promises)', async () => {
            const key = Math.random().toString();

            const data = await pettyCache.fetch(key, async () => {
                return { foo: 'bar' };
            });

            assert.strictEqual(data.foo, 'bar');
        });

        t.test('PettyCache.fetch should reject if async func throws error (promises)', async () => {
            await assert.rejects(
                pettyCache.fetch(Math.random().toString(), async () => {
                    throw new Error('PettyCache.fetch should reject if async func throws error');
                }),
                { message: 'PettyCache.fetch should reject if async func throws error' }
            );
        });

        t.test('PettyCache.fetch should return value with options (promises)', async () => {
            const key = Math.random().toString();

            const data = await pettyCache.fetch(key, async () => 'value', { ttl: 6000 });

            assert.strictEqual(data, 'value');
        });
    });

    t.test('PettyCache.fetchAndRefresh', { concurrency: true }, async (t) => {
        t.test('PettyCache.fetchAndRefresh', async () => {
            const key = Math.random().toString();

            const data = await pettyCache.fetchAndRefresh(key, async () => ({ foo: 'bar' }));

            assert.equal(data.foo, 'bar');

            const fromMemory = await pettyCache.fetchAndRefresh(key, () => {
                throw 'This function should not be called';
            });

            assert.equal(fromMemory.foo, 'bar');

            // Wait for memory cache to expire
            await timers.setTimeout(5001);

            const fromRedis = await pettyCache.fetchAndRefresh(key, () => {
                throw 'This function should not be called';
            });

            assert.strictEqual(fromRedis.foo, 'bar');
        });

        t.test('PettyCache.fetchAndRefresh should run func again to refresh', async () => {
            const key = Math.random().toString();
            let numberOfFuncCalls = 0;

            const func = async () => {
                await timers.setTimeout(100);
                return ++numberOfFuncCalls;
            };

            const data = await pettyCache.fetchAndRefresh(key, func, { ttl: 6000 });

            assert.equal(data, 1);

            // Wait for the background refresh interval (ttl.min / 2)
            await timers.setTimeout(3001);

            const refreshed = await pettyCache.fetchAndRefresh(key, func, { ttl: 6000 });

            assert.equal(refreshed, 2);

            const cached = await pettyCache.fetchAndRefresh(key, func, { ttl: 6000 });

            assert.equal(cached, 2);
        });

        t.test('PettyCache.fetchAndRefresh should not allow multiple clients to execute func at the same time', async () => {
            const key = Math.random().toString();
            let numberOfFuncCalls = 0;

            const func = async () => {
                await timers.setTimeout(100);
                return ++numberOfFuncCalls;
            };

            const data = await pettyCache.fetchAndRefresh(key, func, { ttl: 6000 });

            assert.equal(data, 1);

            const pettyCache2 = new PettyCache(redisClient);

            const data2 = await pettyCache2.fetchAndRefresh(key, func, { ttl: 6000 });

            assert.equal(data2, 1);

            // Wait for the background refresh; the interval mutex should allow only one client to refresh
            await timers.setTimeout(5001);

            const refreshed = await pettyCache.fetchAndRefresh(key, func, { ttl: 6000 });

            assert.equal(refreshed, 2);

            const refreshed2 = await pettyCache2.fetchAndRefresh(key, func, { ttl: 6000 });

            assert.equal(refreshed2, 2);
        });

        t.test('PettyCache.fetchAndRefresh should reject if func throws error', async () => {
            const key = Math.random().toString();

            const func = async () => {
                throw new Error('PettyCache.fetchAndRefresh should reject if func throws error');
            };

            await assert.rejects(
                pettyCache.fetchAndRefresh(key, func, { ttl: 6000 }),
                { message: 'PettyCache.fetchAndRefresh should reject if func throws error' }
            );

            // Wait past a refresh interval; failed refreshes are swallowed and fetch fails again
            await timers.setTimeout(3001);

            await assert.rejects(
                pettyCache.fetchAndRefresh(key, func, { ttl: 6000 }),
                { message: 'PettyCache.fetchAndRefresh should reject if func throws error' }
            );
        });

        t.test('PettyCache.fetchAndRefresh should not require options', async () => {
            const data = await pettyCache.fetchAndRefresh(Math.random().toString(), async () => ({ foo: 'bar' }));

            assert.equal(data.foo, 'bar');
        });

        t.test('PettyCache.fetchAndRefresh should support async func and refresh it (promises)', async () => {
            const key = Math.random().toString();
            let numberOfFuncCalls = 0;

            const func = async () => ++numberOfFuncCalls;

            const data = await pettyCache.fetchAndRefresh(key, func, { ttl: 6000 });

            assert.strictEqual(data, 1);

            // Wait for the background refresh interval (ttl.min / 2)
            await timers.setTimeout(3500);

            assert.ok(numberOfFuncCalls >= 2, `func should have been called by the refresh interval (calls: ${numberOfFuncCalls})`);
            assert.ok(await pettyCache.get(key) >= 2, 'the refreshed value should have been stored in cache');
        });

    });

    t.test('PettyCache.get', { concurrency: true }, async (t) => {
        t.test('PettyCache.get should return value', async () => {
            const key = Math.random().toString();

            await pettyCache.set(key, 'hello world');

            assert.equal(await pettyCache.get(key), 'hello world');

            // Wait for memory cache to expire
            await timers.setTimeout(5001);

            assert.equal(await pettyCache.get(key), 'hello world');
        });

        t.test('PettyCache.get should return null for missing keys', async () => {
            const key = Math.random().toString();

            assert.strictEqual(await pettyCache.get(key), null);
            assert.strictEqual(await pettyCache.get(key), null);
        });

        t.test('PettyCache.get should return value (promises)', async () => {
            const key = Math.random().toString();

            await pettyCache.set(key, 'hello world');
            const value = await pettyCache.get(key);
            assert.equal(value, 'hello world');

            // Wait for memory cache to expire
            await new Promise(resolve => setTimeout(resolve, 5001));
            const value2 = await pettyCache.get(key);
            assert.equal(value2, 'hello world');
        });

        t.test('PettyCache.get should return null for missing keys (promises)', async () => {
            const key = Math.random().toString();
            const value = await pettyCache.get(key);
            assert.strictEqual(value, null);
        });
    });

    t.test('PettyCache.mutex', { concurrency: true }, async (t) => {
        t.test('PettyCache.mutex.lock', { concurrency: true }, async (t) => {
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

        t.test('PettyCache.mutex.unlock', { concurrency: true }, async (t) => {
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
        t.test('PettyCache.patch should update the values of given object keys (promises)', async () => {
            const key = Math.random().toString();

            await pettyCache.set(key, { a: 1, b: 2, c: 3 });
            await pettyCache.patch(key, { b: 4, c: 5 });
            const data = await pettyCache.get(key);
            assert.deepEqual(data, { a: 1, b: 4, c: 5 });
        });

        t.test('PettyCache.patch should reject if the key does not exist (promises)', async () => {
            await assert.rejects(() => pettyCache.patch(Math.random().toString(), { b: 3 }), { message: /does not exist/ });
        });

        t.test('PettyCache.patch should update the values of given object keys with options (promises)', async () => {
            const key = Math.random().toString();

            await pettyCache.set(key, { a: 1, b: 2, c: 3 });
            await pettyCache.patch(key, { b: 4, c: 5 }, { ttl: 6000 });
            const data = await pettyCache.get(key);
            assert.deepEqual(data, { a: 1, b: 4, c: 5 });
        });
    });

    t.test('PettyCache.semaphore', { concurrency: true }, async (t) => {
        t.test('PettyCache.semaphore.acquireLock', { concurrency: true }, async (t) => {
            t.test('should aquire a lock', async () => {
                const key = Math.random().toString();

                await pettyCache.semaphore.retrieveOrCreate(key, { size: 10 });

                assert.equal(await pettyCache.semaphore.acquireLock(key), 0);
                assert.equal(await pettyCache.semaphore.acquireLock(key), 1);
            });

            t.test('should not aquire a lock', async () => {
                const key = Math.random().toString();

                await pettyCache.semaphore.retrieveOrCreate(key);

                assert.equal(await pettyCache.semaphore.acquireLock(key), 0);

                await assert.rejects(
                    pettyCache.semaphore.acquireLock(key),
                    { message: `Semaphore ${key} doesn't have any available slots.` }
                );
            });

            t.test('should aquire a lock after ttl', async () => {
                const key = Math.random().toString();

                await pettyCache.semaphore.retrieveOrCreate(key);

                assert.equal(await pettyCache.semaphore.acquireLock(key), 0);

                await assert.rejects(pettyCache.semaphore.acquireLock(key));

                await timers.setTimeout(1001);

                assert.equal(await pettyCache.semaphore.acquireLock(key), 0);
            });

            t.test('should aquire a lock with specified options', async () => {
                const key = Math.random().toString();

                await pettyCache.semaphore.retrieveOrCreate(key, { size: 10 });

                await pettyCache.semaphore.acquireLock(key);

                await timers.setTimeout(1000);

                const index = await pettyCache.semaphore.acquireLock(key, { retry: { interval: 500, times: 10 }, ttl: 500 });

                assert.equal(index, 1);
            });

            t.test('should fail if the semaphore does not exist', async () => {
                const key = Math.random().toString();

                await assert.rejects(
                    pettyCache.semaphore.acquireLock(key, {}),
                    { message: `Semaphore ${key} doesn't exist.` }
                );
            });
        });

        t.test('PettyCache.semaphore.consumeLock', { concurrency: true }, async (t) => {
            t.test('should consume a lock', async () => {
                const key = Math.random().toString();

                await pettyCache.semaphore.retrieveOrCreate(key, { size: 2 });

                assert.equal(await pettyCache.semaphore.acquireLock(key), 0);
                assert.equal(await pettyCache.semaphore.acquireLock(key), 1);

                await assert.rejects(pettyCache.semaphore.acquireLock(key));

                await pettyCache.semaphore.consumeLock(key, 0);

                await assert.rejects(pettyCache.semaphore.acquireLock(key));
            });

            t.test('should ensure at least one lock is not consumed', async () => {
                const key = Math.random().toString();

                await pettyCache.semaphore.retrieveOrCreate(key, { size: 2 });

                assert.equal(await pettyCache.semaphore.acquireLock(key), 0);
                assert.equal(await pettyCache.semaphore.acquireLock(key), 1);

                await assert.rejects(pettyCache.semaphore.acquireLock(key));

                await pettyCache.semaphore.consumeLock(key, 0);
                await pettyCache.semaphore.consumeLock(key, 1);

                assert.equal(await pettyCache.semaphore.acquireLock(key), 1);
            });

            t.test('should fail if the semaphore does not exist', async () => {
                const key = Math.random().toString();

                await assert.rejects(
                    pettyCache.semaphore.consumeLock(key, 0),
                    { message: `Semaphore ${key} doesn't exist.` }
                );
            });

            t.test('should fail if index is larger than semaphore', async () => {
                const key = Math.random().toString();

                await pettyCache.semaphore.retrieveOrCreate(key, { size: 2 });

                assert.equal(await pettyCache.semaphore.acquireLock(key), 0);

                await assert.rejects(
                    pettyCache.semaphore.consumeLock(key, 10),
                    { message: `Index 10 for semaphore ${key} is invalid.` }
                );
            });
        });

        t.test('PettyCache.semaphore.expand', { concurrency: true }, async (t) => {
            t.test('should increase the size of a semaphore pool', async () => {
                const key = Math.random().toString();

                const pool = await pettyCache.semaphore.retrieveOrCreate(key, { size: 2 });

                assert.strictEqual(pool.length, 2);

                await pettyCache.semaphore.expand(key, 3);

                const expanded = await pettyCache.semaphore.retrieveOrCreate(key, { size: 2 });

                assert.strictEqual(expanded.length, 3);
            });

            t.test('should refuse to shrink a pool', async () => {
                const key = Math.random().toString();

                const pool = await pettyCache.semaphore.retrieveOrCreate(key, { size: 2 });

                assert.strictEqual(pool.length, 2);

                await assert.rejects(
                    pettyCache.semaphore.expand(key, 1),
                    { message: 'Cannot shrink pool, size is 2 and you requested a size of 1.' }
                );
            });

            t.test('should succeed if pool size is already equal to the specified size', async () => {
                const key = Math.random().toString();

                const pool = await pettyCache.semaphore.retrieveOrCreate(key, { size: 2 });

                assert.strictEqual(pool.length, 2);

                await pettyCache.semaphore.expand(key, 2);

                const unchanged = await pettyCache.semaphore.retrieveOrCreate(key, { size: 2 });

                assert.strictEqual(unchanged.length, 2);
            });

            t.test('should fail if the semaphore does not exist', async () => {
                const key = Math.random().toString();

                await assert.rejects(
                    pettyCache.semaphore.expand(key, 10),
                    { message: `Semaphore ${key} doesn't exist.` }
                );
            });
        });

        t.test('PettyCache.semaphore.releaseLock', { concurrency: true }, async (t) => {
            t.test('should release a lock', async () => {
                const key = Math.random().toString();

                await pettyCache.semaphore.retrieveOrCreate(key);

                assert.equal(await pettyCache.semaphore.acquireLock(key), 0);

                await assert.rejects(pettyCache.semaphore.acquireLock(key));

                await pettyCache.semaphore.releaseLock(key, 0);

                assert.equal(await pettyCache.semaphore.acquireLock(key), 0);
            });

            t.test('should fail to release a lock outside of the semaphore size', async () => {
                const key = Math.random().toString();

                await pettyCache.semaphore.retrieveOrCreate(key);

                assert.equal(await pettyCache.semaphore.acquireLock(key), 0);

                await assert.rejects(
                    pettyCache.semaphore.releaseLock(key, 10),
                    { message: `Index 10 for semaphore ${key} is invalid.` }
                );
            });

            t.test('should fail if the semaphore does not exist', async () => {
                const key = Math.random().toString();

                await assert.rejects(
                    pettyCache.semaphore.releaseLock(key, 10),
                    { message: `Semaphore ${key} doesn't exist.` }
                );
            });
        });

        t.test('PettyCache.semaphore.reset', { concurrency: true }, async (t) => {
            t.test('should reset all locks', async () => {
                const key = Math.random().toString();

                await pettyCache.semaphore.retrieveOrCreate(key, { size: 2 });

                assert.equal(await pettyCache.semaphore.acquireLock(key), 0);
                assert.equal(await pettyCache.semaphore.acquireLock(key), 1);

                await assert.rejects(pettyCache.semaphore.acquireLock(key));

                await pettyCache.semaphore.reset(key);

                assert.equal(await pettyCache.semaphore.acquireLock(key), 0);
            });

            t.test('should fail if the semaphore does not exist', async () => {
                const key = Math.random().toString();

                await assert.rejects(
                    pettyCache.semaphore.reset(key),
                    { message: `Semaphore ${key} doesn't exist.` }
                );
            });
        });

        t.test('PettyCache.semaphore.retrieveOrCreate', { concurrency: true }, async (t) => {
            t.test('should create a new semaphore', async () => {
                const key = Math.random().toString();

                const semaphore = await pettyCache.semaphore.retrieveOrCreate(key, { size: 100 });

                assert(semaphore);
                assert.equal(semaphore.length, 100);
                assert(semaphore.every(s => s.status === 'available'));

                const retrieved = await pettyCache.semaphore.retrieveOrCreate(key);

                assert(retrieved);
                assert.equal(retrieved.length, 100);
                assert(retrieved.every(s => s.status === 'available'));
            });

            t.test('should have a min size of 1', async () => {
                const key = Math.random().toString();

                const semaphore = await pettyCache.semaphore.retrieveOrCreate(key, { size: 0 });

                assert(semaphore);
                assert.equal(semaphore.length, 1);
                assert(semaphore.every(s => s.status === 'available'));

                const retrieved = await pettyCache.semaphore.retrieveOrCreate(key);

                assert(retrieved);
                assert.equal(retrieved.length, 1);
                assert(retrieved.every(s => s.status === 'available'));
            });

            t.test('should retrieve an existing semaphore regardless of the specified size', async () => {
                const key = Math.random().toString();

                await pettyCache.semaphore.retrieveOrCreate(key);

                const semaphore = await pettyCache.semaphore.retrieveOrCreate(key, { size: 100 });

                assert(semaphore);
                assert.equal(semaphore.length, 1);
                assert(semaphore.every(s => s.status === 'available'));
            });
        });

        t.test('PettyCache.semaphore should support promises for the full lock lifecycle (promises)', async () => {
            const key = Math.random().toString();

            const semaphore = await pettyCache.semaphore.retrieveOrCreate(key, { size: 2 });

            assert.strictEqual(semaphore.length, 2);
            assert.ok(semaphore.every(s => s.status === 'available'));

            const index = await pettyCache.semaphore.acquireLock(key);

            assert.strictEqual(index, 0);

            await pettyCache.semaphore.releaseLock(key, index);
            await pettyCache.semaphore.consumeLock(key, index);
            await pettyCache.semaphore.expand(key, 3);

            const pool = await pettyCache.semaphore.reset(key);

            assert.strictEqual(pool.length, 3);
            assert.ok(pool.every(s => s.status === 'available'));
        });

        t.test('PettyCache.semaphore.retrieveOrCreate should allow options.size to provide an async function (promises)', async () => {
            const key = Math.random().toString();

            const semaphore = await pettyCache.semaphore.retrieveOrCreate(key, { size: async () => 3 });

            assert.strictEqual(semaphore.length, 3);
        });

        t.test('PettyCache.semaphore.acquireLock should reject if the semaphore does not exist (promises)', async () => {
            await assert.rejects(
                pettyCache.semaphore.acquireLock(Math.random().toString()),
                { message: /doesn't exist/ }
            );
        });

        t.test('PettyCache.semaphore.acquireLock should retry until a slot becomes available (promises)', async () => {
            const key = Math.random().toString();

            await pettyCache.semaphore.retrieveOrCreate(key, { size: 1 });

            // Acquire the pool's only slot with a short TTL
            const index = await pettyCache.semaphore.acquireLock(key, { ttl: 500 });

            assert.strictEqual(index, 0);

            // Retries until the first lock's TTL expires and its slot can be reclaimed
            const retriedIndex = await pettyCache.semaphore.acquireLock(key, { retry: { interval: 200, times: 10 } });

            assert.strictEqual(retriedIndex, 0);
        });

        t.test('PettyCache.semaphore.expand should reject when shrinking (promises)', async () => {
            const key = Math.random().toString();

            await pettyCache.semaphore.retrieveOrCreate(key, { size: 2 });

            await assert.rejects(
                pettyCache.semaphore.expand(key, 1),
                { message: /Cannot shrink pool/ }
            );
        });
    });

    t.test('PettyCache.set', { concurrency: true }, async (t) => {
        t.test('PettyCache.set should set a value', async () => {
            const key = Math.random().toString();

            await pettyCache.set(key, 'hello world');

            assert.equal(await pettyCache.get(key), 'hello world');

            // Wait for memory cache to expire
            await timers.setTimeout(5001);

            assert.equal(await pettyCache.get(key), 'hello world');
        });

        t.test('PettyCache.set should set a value with the specified TTL option', async () => {
            const key = Math.random().toString();

            await pettyCache.set(key, 'hello world', { ttl: 6000 });

            assert.equal(await pettyCache.get(key), 'hello world');

            // Wait for Redis cache to expire
            await timers.setTimeout(6001);

            assert.equal(await pettyCache.get(key), null);
        });

        t.test('PettyCache.set should set a value with the specified TTL option using max and min', async () => {
            const key = Math.random().toString();

            await pettyCache.set(key, 'hello world', { ttl: { max: 7000, min: 6000 } });

            assert.strictEqual(await pettyCache.get(key), 'hello world');

            // Get again before cache expires
            await timers.setTimeout(1000);

            assert.strictEqual(await pettyCache.get(key), 'hello world');

            // Wait for Redis cache to expire
            await timers.setTimeout(6001);

            assert.strictEqual(await pettyCache.get(key), null);
        });

        t.test('PettyCache.set should set a value with the specified TTL option using min only', async () => {
            const key = Math.random().toString();

            await pettyCache.set(key, 'hello world', { ttl: { min: 6000 } });

            assert.strictEqual(await pettyCache.get(key), 'hello world');
        });

        t.test('PettyCache.set should set a value with the specified TTL option using max only', async () => {
            const key = Math.random().toString();

            await pettyCache.set(key, 'hello world', { ttl: { max: 10000 } });

            assert.strictEqual(await pettyCache.get(key), 'hello world');
        });

        t.test('PettyCache.set(key, \'\')', async () => {
            const key = Math.random().toString();

            await pettyCache.set(key, '', { ttl: 7000 });

            assert.strictEqual(await pettyCache.get(key), '');

            // Wait for memory cache to expire
            await timers.setTimeout(5001);

            assert.strictEqual(await pettyCache.get(key), '');

            // Wait for memory cache and Redis cache to expire
            await timers.setTimeout(5001);

            assert.strictEqual(await pettyCache.get(key), null);
        });

        t.test('PettyCache.set(key, 0)', async () => {
            const key = Math.random().toString();

            await pettyCache.set(key, 0, { ttl: 7000 });

            assert.strictEqual(await pettyCache.get(key), 0);

            // Wait for memory cache to expire
            await timers.setTimeout(5001);

            assert.strictEqual(await pettyCache.get(key), 0);

            // Wait for memory cache and Redis cache to expire
            await timers.setTimeout(5001);

            assert.strictEqual(await pettyCache.get(key), null);
        });

        t.test('PettyCache.set(key, false)', async () => {
            const key = Math.random().toString();

            await pettyCache.set(key, false, { ttl: 7000 });

            assert.strictEqual(await pettyCache.get(key), false);

            // Wait for memory cache to expire
            await timers.setTimeout(5001);

            assert.strictEqual(await pettyCache.get(key), false);

            // Wait for memory cache and Redis cache to expire
            await timers.setTimeout(5001);

            assert.strictEqual(await pettyCache.get(key), null);
        });

        t.test('PettyCache.set(key, NaN)', async () => {
            const key = Math.random().toString();

            await pettyCache.set(key, NaN, { ttl: 7000 });

            const value = await pettyCache.get(key);

            assert(typeof value === 'number' && isNaN(value));

            // Wait for memory cache to expire
            await timers.setTimeout(5001);

            const fromRedis = await pettyCache.get(key);

            assert(typeof fromRedis === 'number' && isNaN(fromRedis));

            // Wait for memory cache and Redis cache to expire
            await timers.setTimeout(5001);

            assert.strictEqual(await pettyCache.get(key), null);
        });

        t.test('PettyCache.set(key, null)', async () => {
            const key = Math.random().toString();

            await pettyCache.set(key, null, { ttl: 7000 });

            assert.strictEqual(await pettyCache.get(key), null);

            // Wait for memory cache to expire
            await timers.setTimeout(5001);

            assert.strictEqual(await pettyCache.get(key), null);

            // Wait for memory cache and Redis cache to expire
            await timers.setTimeout(5001);

            assert.strictEqual(await pettyCache.get(key), null);
        });

        t.test('PettyCache.set(key, undefined)', async () => {
            const key = Math.random().toString();

            await pettyCache.set(key, undefined, { ttl: 7000 });

            assert.strictEqual(await pettyCache.get(key), undefined);

            // Wait for memory cache to expire
            await timers.setTimeout(5001);

            assert.strictEqual(await pettyCache.get(key), undefined);

            // Wait for memory cache and Redis cache to expire
            await timers.setTimeout(5001);

            assert.strictEqual(await pettyCache.get(key), null);
        });

        t.test('PettyCache.set should set a value (promises)', async () => {
            const key = Math.random().toString();

            await pettyCache.set(key, 'hello world');
            const value = await pettyCache.get(key);
            assert.equal(value, 'hello world');

            // Wait for memory cache to expire
            await new Promise(resolve => setTimeout(resolve, 5001));
            const value2 = await pettyCache.get(key);
            assert.equal(value2, 'hello world');
        });

        t.test('PettyCache.set should set a value with options (promises)', async () => {
            const key = Math.random().toString();

            await pettyCache.set(key, 'hello world', { ttl: 6000 });
            const value = await pettyCache.get(key);
            assert.equal(value, 'hello world');
        });

        t.test('PettyCache.set should return a promise when no callback is provided', async () => {
            const key = Math.random().toString();
            const result = pettyCache.set(key, 'hello world');
            assert(result instanceof Promise);
            await result;
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

            Promise.all(Object.keys(values).map(key => new Promise((resolve, reject) => {
                redisClient.psetex(key, 100, PettyCache.stringify(values[key]), err => err ? reject(err) : resolve());
            }))).then(() => {
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

    t.test('Benchmark', { concurrency: true }, async (t) => {
        const emojis = require('./emojis.json');

        t.test('PettyCache should be faster than node-redis', async () => {
            const pettyCacheKey = Math.random().toString();
            const redisKey = Math.random().toString();
            const redisStart = Date.now();

            await new Promise((resolve, reject) => {
                redisClient.psetex(redisKey, 30000, JSON.stringify(emojis), err => err ? reject(err) : resolve());
            });

            await Promise.all(Array.from({ length: 500 }, () => new Promise((resolve, reject) => {
                redisClient.get(redisKey, (err, data) => {
                    if (err) {
                        return reject(err);
                    }

                    resolve(JSON.parse(data));
                });
            })));

            const redisEnd = Date.now();
            const pettyCacheStart = Date.now();

            await pettyCache.set(pettyCacheKey, emojis);
            await Promise.all(Array.from({ length: 500 }, () => pettyCache.get(pettyCacheKey)));

            const pettyCacheEnd = Date.now();

            assert(pettyCacheEnd - pettyCacheStart < redisEnd - redisStart);
        });
    });
});

test('PettyCache.fetch should return error if Redis GET fails', async () => {
    const stubClient = redis.createClient();
    const originalGet = stubClient.get.bind(stubClient);

    stubClient.get = (key, callback) => callback(new Error('Redis GET error'));

    const pettyCache = new PettyCache(stubClient);

    await assert.rejects(
        pettyCache.fetch(Math.random().toString(), async () => 'value'),
        { message: 'Redis GET error' }
    );

    stubClient.get = originalGet;
});

test('PettyCache.get should return error if Redis GET fails', async () => {
    const stubClient = redis.createClient();
    const originalGet = stubClient.get.bind(stubClient);

    stubClient.get = (key, callback) => callback(new Error('Redis GET error'));

    const pettyCache = new PettyCache(stubClient);

    await assert.rejects(
        pettyCache.get(Math.random().toString()),
        { message: 'Redis GET error' }
    );

    stubClient.get = originalGet;
});

test('PettyCache.bulkFetch should return error if Redis MGET fails', async () => {
    const stubClient = redis.createClient();
    const originalMget = stubClient.mget.bind(stubClient);

    stubClient.mget = (keys, callback) => callback(new Error('Redis MGET error'));

    const pettyCache = new PettyCache(stubClient);

    await assert.rejects(
        pettyCache.bulkFetch([Math.random().toString()], async () => ({})),
        { message: 'Redis MGET error' }
    );

    stubClient.mget = originalMget;
});

test('PettyCache.bulkGet should return error if Redis MGET fails', async () => {
    const stubClient = redis.createClient();
    const originalMget = stubClient.mget.bind(stubClient);

    stubClient.mget = (keys, callback) => callback(new Error('Redis MGET error'));

    const pettyCache = new PettyCache(stubClient);

    await assert.rejects(
        pettyCache.bulkGet([Math.random().toString()]),
        { message: 'Redis MGET error' }
    );

    stubClient.mget = originalMget;
});

test('PettyCache.bulkSet should return error if Redis batch exec fails', async () => {
    const stubClient = redis.createClient();
    const originalBatch = stubClient.batch.bind(stubClient);

    stubClient.batch = () => {
        const batch = originalBatch();
        batch.exec = (callback) => callback(new Error('Redis EXEC error'));
        return batch;
    };

    const pettyCache = new PettyCache(stubClient);
    const values = {};
    values[Math.random().toString()] = 'value';

    await assert.rejects(
        pettyCache.bulkSet(values),
        { message: 'Redis EXEC error' }
    );

    stubClient.batch = originalBatch;
});

test('PettyCache.bulkFetch should return error if bulkSet fails', async () => {
    const stubClient = redis.createClient();
    const originalBatch = stubClient.batch.bind(stubClient);

    stubClient.batch = () => {
        const batch = originalBatch();
        batch.exec = (callback) => callback(new Error('Redis EXEC error'));
        return batch;
    };

    const pettyCache = new PettyCache(stubClient);
    const key = Math.random().toString();

    await assert.rejects(
        pettyCache.bulkFetch([key], async (keys) => {
            const data = {};
            data[keys[0]] = 'value';
            return data;
        }),
        { message: 'Redis EXEC error' }
    );

    stubClient.batch = originalBatch;
});

test('PettyCache.mutex.lock should return error if Redis SET fails', async () => {
    const stubClient = redis.createClient();
    const originalSet = stubClient.set.bind(stubClient);

    stubClient.set = (...args) => args[args.length - 1](new Error('Redis SET error'));

    const pettyCache = new PettyCache(stubClient);

    await assert.rejects(
        pettyCache.mutex.lock(Math.random().toString()),
        { message: 'Redis SET error' }
    );

    stubClient.set = originalSet;
});

test('PettyCache.del should return error if Redis DEL fails', async () => {
    const stubClient = redis.createClient();
    const originalDel = stubClient.del.bind(stubClient);

    stubClient.del = (key, callback) => callback(new Error('Redis DEL error'));

    const pettyCache = new PettyCache(stubClient);

    await assert.rejects(
        pettyCache.del(Math.random().toString()),
        { message: 'Redis DEL error' }
    );

    stubClient.del = originalDel;
});

test('PettyCache.mutex.unlock should return error if Redis DEL fails', async () => {
    const stubClient = redis.createClient();
    const originalDel = stubClient.del.bind(stubClient);

    stubClient.del = (key, callback) => callback(new Error('Redis DEL error'));

    const pettyCache = new PettyCache(stubClient);

    await assert.rejects(
        pettyCache.mutex.unlock(Math.random().toString()),
        { message: 'Redis DEL error' }
    );

    stubClient.del = originalDel;
});

test('PettyCache.patch should return error if Redis GET fails', async () => {
    const stubClient = redis.createClient();
    const originalGet = stubClient.get.bind(stubClient);

    stubClient.get = (key, callback) => callback(new Error('Redis GET error'));

    const pettyCache = new PettyCache(stubClient);

    await assert.rejects(
        pettyCache.patch(Math.random().toString(), { a: 1 }),
        { message: 'Redis GET error' }
    );

    stubClient.get = originalGet;
});

test('PettyCache.mutex.lock should return error if Redis SET returns unexpected response', async () => {
    const stubClient = redis.createClient();
    const originalSet = stubClient.set.bind(stubClient);

    stubClient.set = (...args) => args[args.length - 1](null, 'UNEXPECTED');

    const pettyCache = new PettyCache(stubClient);

    await assert.rejects(
        pettyCache.mutex.lock(Math.random().toString()),
        { message: 'UNEXPECTED' }
    );

    stubClient.set = originalSet;
});

test('PettyCache.semaphore.retrieveOrCreate should return error if Redis GET fails', async () => {
    const stubClient = redis.createClient();
    const originalGet = stubClient.get.bind(stubClient);

    stubClient.get = (key, callback) => callback(new Error('Redis GET error'));

    const pettyCache = new PettyCache(stubClient);

    await assert.rejects(
        pettyCache.semaphore.retrieveOrCreate(Math.random().toString()),
        { message: 'Redis GET error' }
    );

    stubClient.get = originalGet;
});

test('PettyCache.semaphore.acquireLock should return error if Redis GET fails', async () => {
    const stubClient = redis.createClient();
    const originalGet = stubClient.get.bind(stubClient);

    stubClient.get = (key, callback) => callback(new Error('Redis GET error'));

    const pettyCache = new PettyCache(stubClient);

    await assert.rejects(
        pettyCache.semaphore.acquireLock(Math.random().toString()),
        { message: 'Redis GET error' }
    );

    stubClient.get = originalGet;
});

test('PettyCache.semaphore.consumeLock should return error if Redis GET fails', async () => {
    const stubClient = redis.createClient();
    const originalGet = stubClient.get.bind(stubClient);

    stubClient.get = (key, callback) => callback(new Error('Redis GET error'));

    const pettyCache = new PettyCache(stubClient);

    await assert.rejects(
        pettyCache.semaphore.consumeLock(Math.random().toString(), 0),
        { message: 'Redis GET error' }
    );

    stubClient.get = originalGet;
});

test('PettyCache.semaphore.expand should return error if Redis GET fails', async () => {
    const stubClient = redis.createClient();
    const originalGet = stubClient.get.bind(stubClient);

    stubClient.get = (key, callback) => callback(new Error('Redis GET error'));

    const pettyCache = new PettyCache(stubClient);

    await assert.rejects(
        pettyCache.semaphore.expand(Math.random().toString(), 10),
        { message: 'Redis GET error' }
    );

    stubClient.get = originalGet;
});

test('PettyCache.semaphore.releaseLock should return error if Redis GET fails', async () => {
    const stubClient = redis.createClient();
    const originalGet = stubClient.get.bind(stubClient);

    stubClient.get = (key, callback) => callback(new Error('Redis GET error'));

    const pettyCache = new PettyCache(stubClient);

    await assert.rejects(
        pettyCache.semaphore.releaseLock(Math.random().toString(), 0),
        { message: 'Redis GET error' }
    );

    stubClient.get = originalGet;
});

test('PettyCache.semaphore.reset should return error if Redis GET fails', async () => {
    const stubClient = redis.createClient();
    const originalGet = stubClient.get.bind(stubClient);

    stubClient.get = (key, callback) => callback(new Error('Redis GET error'));

    const pettyCache = new PettyCache(stubClient);

    await assert.rejects(
        pettyCache.semaphore.reset(Math.random().toString()),
        { message: 'Redis GET error' }
    );

    stubClient.get = originalGet;
});

test('PettyCache.semaphore.retrieveOrCreate should return error if Redis SET fails', async () => {
    const stubClient = redis.createClient();
    const originalSet = stubClient.set.bind(stubClient);

    stubClient.set = (...args) => {
        if (args.includes('NX')) {
            return originalSet(...args);
        }

        args[args.length - 1](new Error('Redis SET error'));
    };

    const pettyCache = new PettyCache(stubClient);

    await assert.rejects(
        pettyCache.semaphore.retrieveOrCreate(Math.random().toString()),
        { message: 'Redis SET error' }
    );

    stubClient.set = originalSet;
});

test('PettyCache.semaphore.acquireLock should return error if Redis SET fails', async () => {
    const key = Math.random().toString();

    await pettyCache.semaphore.retrieveOrCreate(key);

    const stubClient = redis.createClient();
    const originalSet = stubClient.set.bind(stubClient);

    stubClient.set = (...args) => {
        if (args.includes('NX')) {
            return originalSet(...args);
        }

        args[args.length - 1](new Error('Redis SET error'));
    };

    const stubCache = new PettyCache(stubClient);

    await assert.rejects(
        stubCache.semaphore.acquireLock(key),
        { message: 'Redis SET error' }
    );

    stubClient.set = originalSet;
});

test('PettyCache.semaphore.consumeLock should return error if Redis SET fails', async () => {
    const key = Math.random().toString();

    await pettyCache.semaphore.retrieveOrCreate(key, { size: 2 });

    const index = await pettyCache.semaphore.acquireLock(key);

    const stubClient = redis.createClient();
    const originalSet = stubClient.set.bind(stubClient);

    stubClient.set = (...args) => {
        if (args.includes('NX')) {
            return originalSet(...args);
        }

        args[args.length - 1](new Error('Redis SET error'));
    };

    const stubCache = new PettyCache(stubClient);

    await assert.rejects(
        stubCache.semaphore.consumeLock(key, index),
        { message: 'Redis SET error' }
    );

    stubClient.set = originalSet;
});

test('PettyCache.semaphore.expand should return error if Redis SET fails', async () => {
    const key = Math.random().toString();

    await pettyCache.semaphore.retrieveOrCreate(key, { size: 2 });

    const stubClient = redis.createClient();
    const originalSet = stubClient.set.bind(stubClient);

    stubClient.set = (...args) => {
        if (args.includes('NX')) {
            return originalSet(...args);
        }

        args[args.length - 1](new Error('Redis SET error'));
    };

    const stubCache = new PettyCache(stubClient);

    await assert.rejects(
        stubCache.semaphore.expand(key, 5),
        { message: 'Redis SET error' }
    );

    stubClient.set = originalSet;
});

test('PettyCache.semaphore.releaseLock should return error if Redis SET fails', async () => {
    const key = Math.random().toString();

    await pettyCache.semaphore.retrieveOrCreate(key);

    const index = await pettyCache.semaphore.acquireLock(key);

    const stubClient = redis.createClient();
    const originalSet = stubClient.set.bind(stubClient);

    stubClient.set = (...args) => {
        if (args.includes('NX')) {
            return originalSet(...args);
        }

        args[args.length - 1](new Error('Redis SET error'));
    };

    const stubCache = new PettyCache(stubClient);

    await assert.rejects(
        stubCache.semaphore.releaseLock(key, index),
        { message: 'Redis SET error' }
    );

    stubClient.set = originalSet;
});

test('PettyCache.semaphore.reset should return error if Redis SET fails', async () => {
    const key = Math.random().toString();

    await pettyCache.semaphore.retrieveOrCreate(key);

    const stubClient = redis.createClient();
    const originalSet = stubClient.set.bind(stubClient);

    stubClient.set = (...args) => {
        if (args.includes('NX')) {
            return originalSet(...args);
        }

        args[args.length - 1](new Error('Redis SET error'));
    };

    const stubCache = new PettyCache(stubClient);

    await assert.rejects(
        stubCache.semaphore.reset(key),
        { message: 'Redis SET error' }
    );

    stubClient.set = originalSet;
});

test('PettyCache.semaphore.retrieveOrCreate should return error if mutex lock fails', async () => {
    const stubClient = redis.createClient();
    const stubCache = new PettyCache(stubClient);

    stubCache.mutex.lock = () => Promise.reject(new Error('mutex lock error'));

    await assert.rejects(
        stubCache.semaphore.retrieveOrCreate(Math.random().toString()),
        { message: 'mutex lock error' }
    );
});

test('PettyCache.semaphore.acquireLock should return error if mutex lock fails', async () => {
    const stubClient = redis.createClient();
    const stubCache = new PettyCache(stubClient);

    stubCache.mutex.lock = () => Promise.reject(new Error('mutex lock error'));

    await assert.rejects(
        stubCache.semaphore.acquireLock(Math.random().toString()),
        { message: 'mutex lock error' }
    );
});

test('PettyCache.semaphore.consumeLock should return error if mutex lock fails', async () => {
    const stubClient = redis.createClient();
    const stubCache = new PettyCache(stubClient);

    stubCache.mutex.lock = () => Promise.reject(new Error('mutex lock error'));

    await assert.rejects(
        stubCache.semaphore.consumeLock(Math.random().toString(), 0),
        { message: 'mutex lock error' }
    );
});

test('PettyCache.semaphore.expand should return error if mutex lock fails', async () => {
    const stubClient = redis.createClient();
    const stubCache = new PettyCache(stubClient);

    stubCache.mutex.lock = () => Promise.reject(new Error('mutex lock error'));

    await assert.rejects(
        stubCache.semaphore.expand(Math.random().toString(), 10),
        { message: 'mutex lock error' }
    );
});

test('PettyCache.semaphore.releaseLock should return error if mutex lock fails', async () => {
    const stubClient = redis.createClient();
    const stubCache = new PettyCache(stubClient);

    stubCache.mutex.lock = () => Promise.reject(new Error('mutex lock error'));

    await assert.rejects(
        stubCache.semaphore.releaseLock(Math.random().toString(), 0),
        { message: 'mutex lock error' }
    );
});

test('PettyCache.semaphore.reset should return error if mutex lock fails', async () => {
    const stubClient = redis.createClient();
    const stubCache = new PettyCache(stubClient);

    stubCache.mutex.lock = () => Promise.reject(new Error('mutex lock error'));

    await assert.rejects(
        stubCache.semaphore.reset(Math.random().toString()),
        { message: 'mutex lock error' }
    );
});

test('PettyCache.semaphore.retrieveOrCreate should return error if size function fails', async () => {
    await assert.rejects(
        pettyCache.semaphore.retrieveOrCreate(Math.random().toString(), { size: async () => { throw new Error('size error'); } }),
        { message: 'size error' }
    );
});

test('PettyCache.fetch should return error if inner Redis GET fails (double-checked lock)', async () => {
    const stubClient = redis.createClient();
    const originalGet = stubClient.get.bind(stubClient);
    let getCallCount = 0;

    stubClient.get = (...args) => {
        getCallCount++;

        if (getCallCount === 1) {
            const callback = args[args.length - 1];
            return callback(null, null);
        }

        stubClient.get = originalGet;
        const callback = args[args.length - 1];
        callback(new Error('Redis GET error'));
    };

    const pettyCache = new PettyCache(stubClient);

    await assert.rejects(
        pettyCache.fetch(Math.random().toString(), async () => 'value'),
        { message: 'Redis GET error' }
    );

    stubClient.get = originalGet;
});

test('PettyCache.fetch should return cached value from inner Redis GET (double-checked lock)', async () => {
    const stubClient = redis.createClient();
    const originalGet = stubClient.get.bind(stubClient);
    let getCallCount = 0;

    stubClient.get = (...args) => {
        getCallCount++;

        if (getCallCount === 1) {
            const callback = args[args.length - 1];
            return callback(null, null);
        }

        stubClient.get = originalGet;
        const callback = args[args.length - 1];
        callback(null, JSON.stringify('cached-value'));
    };

    const pettyCache = new PettyCache(stubClient);

    const value = await pettyCache.fetch(Math.random().toString(), async () => 'should-not-be-used');

    assert.strictEqual(value, 'cached-value');

    stubClient.get = originalGet;
});

test('PettyCache.fetch should return cached value from inner memory cache (double-checked lock)', async () => {
    const stubClient = redis.createClient();
    const originalGet = stubClient.get.bind(stubClient);
    const key = Math.random().toString();
    let stubCache;

    stubClient.get = (...args) => {
        stubClient.get = originalGet;

        // Populate memory cache synchronously via set before returning
        stubCache.set(key, 'cached-value').catch(() => {});

        const callback = args[args.length - 1];
        callback(null, null);
    };

    stubCache = new PettyCache(stubClient);

    const value = await stubCache.fetch(key, async () => 'should-not-be-used');

    assert.strictEqual(value, 'cached-value');

    stubClient.get = originalGet;
});

test('PettyCache.get should return cached value from double-checked lock', async () => {
    const key = Math.random().toString();

    // Put value directly in Redis (not memory cache)
    await new Promise((resolve, reject) => {
        redisClient.psetex(key, 10000, JSON.stringify('test-value'), err => err ? reject(err) : resolve());
    });

    // Two concurrent gets - second should hit memory cache inside lock
    const values = await Promise.all([pettyCache.get(key), pettyCache.get(key)]);

    assert.strictEqual(values[0], 'test-value');
    assert.strictEqual(values[1], 'test-value');
});

test('PettyCache.set should return error if Redis PSETEX fails', async () => {
    const stubClient = redis.createClient();
    const originalPsetex = stubClient.psetex.bind(stubClient);

    stubClient.psetex = (...args) => {
        stubClient.psetex = originalPsetex;
        const callback = args[args.length - 1];
        callback(new Error('Redis PSETEX error'));
    };

    const pettyCache = new PettyCache(stubClient);

    await assert.rejects(
        pettyCache.set(Math.random().toString(), 'value'),
        { message: 'Redis PSETEX error' }
    );

    stubClient.psetex = originalPsetex;
});

test('PettyCache.patch should return error if Redis PSETEX fails', async () => {
    const stubClient = redis.createClient();
    const originalPsetex = stubClient.psetex.bind(stubClient);
    const key = Math.random().toString();

    const pettyCache = new PettyCache(stubClient);

    // First set a value so patch has something to patch
    await pettyCache.set(key, { a: 1 });

    // Now stub psetex to fail on the next call (patch's inner set)
    stubClient.psetex = (...args) => {
        stubClient.psetex = originalPsetex;
        const callback = args[args.length - 1];
        callback(new Error('Redis PSETEX error'));
    };

    await assert.rejects(
        pettyCache.patch(key, { b: 2 }),
        { message: 'Redis PSETEX error' }
    );

    stubClient.psetex = originalPsetex;
});

test('PettyCache.fetch should lock around Redis', async () => {
    const infoBefore = await new Promise((resolve, reject) => {
        redisClient.info('commandstats', (err, info) => err ? reject(err) : resolve(info));
    });

    const lineBefore = infoBefore.split('\n').find(i => i.startsWith('cmdstat_get:'));
    const tokenBefore = lineBefore.split(/:|,/).find(i => i.startsWith('calls='));
    const callsBefore = parseInt(tokenBefore.split('=')[1]);

    const key = Math.random().toString();
    let numberOfFuncCalls = 0;

    const func = async () => {
        await timers.setTimeout(100);
        return ++numberOfFuncCalls;
    };

    const results = await Promise.all(Array.from({ length: 10 }, () => pettyCache.fetch(key, func)));

    results.forEach(data => assert.equal(data, 1));

    const infoAfter = await new Promise((resolve, reject) => {
        redisClient.info('commandstats', (err, info) => err ? reject(err) : resolve(info));
    });

    const lineAfter = infoAfter.split('\n').find(i => i.startsWith('cmdstat_get:'));
    const tokenAfter = lineAfter.split(/:|,/).find(i => i.startsWith('calls='));
    const callsAfter = parseInt(tokenAfter.split('=')[1]);

    assert.strictEqual(callsBefore + 2, callsAfter);
});

test('PettyCache should reject callback-style usage with a TypeError', async () => {
    const noop = () => {};

    await assert.rejects(pettyCache.bulkFetch(['k'], async () => ({}), noop), TypeError);
    await assert.rejects(pettyCache.bulkFetch(['k'], async () => ({}), {}, noop), TypeError);
    await assert.rejects(pettyCache.bulkGet(['k'], noop), TypeError);
    await assert.rejects(pettyCache.bulkSet({ k: 1 }, noop), TypeError);
    await assert.rejects(pettyCache.del('k', noop), TypeError);
    await assert.rejects(pettyCache.fetch('k', async () => 'v', noop), TypeError);
    await assert.rejects(pettyCache.fetchAndRefresh('k', async () => 'v', noop), TypeError);
    await assert.rejects(pettyCache.get('k', noop), TypeError);
    await assert.rejects(pettyCache.mutex.lock('k', noop), TypeError);
    await assert.rejects(pettyCache.mutex.unlock('k', noop), TypeError);
    await assert.rejects(pettyCache.patch('k', { a: 1 }, noop), TypeError);
    await assert.rejects(pettyCache.semaphore.acquireLock('k', noop), TypeError);
    await assert.rejects(pettyCache.semaphore.consumeLock('k', 0, noop), TypeError);
    await assert.rejects(pettyCache.semaphore.expand('k', 2, noop), TypeError);
    await assert.rejects(pettyCache.semaphore.releaseLock('k', 0, noop), TypeError);
    await assert.rejects(pettyCache.semaphore.reset('k', noop), TypeError);
    await assert.rejects(pettyCache.semaphore.retrieveOrCreate('k', noop), TypeError);
    await assert.rejects(pettyCache.set('k', 'v', noop), TypeError);
});
