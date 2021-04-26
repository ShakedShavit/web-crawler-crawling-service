const redisClient = require('../db/redis');

//#region Crawling state hash data manipulation/fetching in/from Redis (each client/search has their own state)

// get [workersCounter, isCrawlingDone, currentLevel, pageCounter, maxPages, maxLevel]
const getHashValuesFromRedis = async (hashKey, fieldsArr) => {
    try {
        const values = await redisClient.hmgetAsync(hashKey, ...fieldsArr);
        console.log(...fieldsArr, " " , values, '9');
        return values;
    } catch (err) {
        console.log(err.message, '12');
        throw new Error(err.message);
    }
}

const getHashValFromRedis = async (hashKey, field) => {
    try {
        const value = await redisClient.hgetAsync(hashKey, field);
        // if (value == null) throw new Error('value is null or undefined');
        console.log('get', field, value, '20');
        return value;
    } catch (err) {
        console.log(err.message, '23');
        throw new Error(err.message);
    }
}

const doesKeyExistInRedis = async (key) => {
    try {
        const doesKeyExist = await redisClient.existsAsync(key);
        return doesKeyExist;
    } catch (err) {
        throw new Error(err.message);
    }
}

const incHashIntValInRedis = async (hashKey, field, factor = 1) => {
    try {
        const doesKeyExist = await doesKeyExistInRedis(hashKey);
        // Return 0 or 1 (!0 equals True, !1 equals False)
        if (!doesKeyExist) throw new Error('key does not exist in redis');

        if (typeof factor !== 'number') {
            let prevFactor = factor;
            factor = parseInt(factor);
            if (isNaN(factor)) throw new Error(`factor's type must be number. factor (${prevFactor}) input is of type ${typeof prevFactor}`);
        }

        const value = await redisClient.hincrbyAsync(hashKey, field, factor);
        console.log('value:', value, 'factor:', factor, field);
        return value;
    } catch (err) {
        console.log(err.message, '37');
        throw new Error(err.message);
    }
}

const setHashStrValInRedis = async (hashKey, field, value) => {
    try {
        const doesKeyExist = await doesKeyExistInRedis(hashKey);
        if (!doesKeyExist) throw new Error('key does not exist in redis');

        if (typeof value !== 'string') throw new Error(`value's type must be string. value (${value}) input is of type ${typeof value}`);

        await redisClient.hsetAsync(hashKey, field, value);
        console.log('set', field, value, '67');
        return value;
    } catch (err) {
        console.log(err.message, '50');
        throw new Error(err.message);
    }
}

//#endregion

const getStrValFromRedis = async (key) => {
    try {
        const value = await redisClient.getAsync(key);
        console.log('get', key, value, '60');
        return value;
    } catch (err) {
        console.log(err.message, '62');
        throw new Error(err.message);
    }
}

const setStrWithExInRedis = async (key, value, exSec = 300) => {
    try {
        if (typeof value !== 'string') throw new Error(`value's type must be string. value (${value}) input is of type ${typeof value}`);

        await redisClient.setexAsync(key, exSec, value);
    } catch (err) {
        console.log(err.message, '73');
        throw new Error(err.message);
    }
}

// Pops (and returns) last el of list and pushes it in another list (both lists could be the same list)
const getLastElOfListAndPushItToDestListInRedis = async (sourceKey, destKey = sourceKey) => {
    try {
        const lastEl = await redisClient.rpoplpushAsync(sourceKey, destKey);
        if (lastEl == null) throw new Error('source list does not exist or is empty');
        return lastEl;
    } catch (err) {
        console.log(err.message);
        throw new Error(err.message);
    }
}

module.exports = {
    doesKeyExistInRedis,
    getHashValuesFromRedis,
    getHashValFromRedis,
    incHashIntValInRedis,
    setHashStrValInRedis,
    getStrValFromRedis,
    setStrWithExInRedis,
    getLastElOfListAndPushItToDestListInRedis
}
