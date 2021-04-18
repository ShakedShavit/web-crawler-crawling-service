const redisClient = require('../db/redis');

//#region Crawling state hash data manipulation/fetching in/from Redis (each client/search has their own state)

// get [workersCounter, isCrawlingDone, currentLevel, pageCounter, maxPages, maxLevel]
const getHashValuesFromRedis = async (hashKey, fieldsArr) => {
    try {
        const values = await redisClient.hmgetAsync(hashKey, ...fieldsArr);
        console.log(values, '9');
        return values;
    } catch (err) {
        console.log(err.message, '12');
        throw new Error(err.message);
    }
}

const getHashValFromRedis = async (hashKey, field) => {
    try {
        const value = await redisClient.hgetAsync(hashKey, field);
        console.log(value, '20');
        return value;
    } catch (err) {
        console.log(err.message, '23');
        throw new Error(err.message);
    }
}

const incHashIntValInRedis = async (hashKey, field, factor = 1) => {
    try {
        if (typeof factor !== 'number') {
            let prevFactor = factor;
            factor = parseInt(factor);
            if (isNaN(factor)) throw new Error(`factor's type must be number. factor (${prevFactor}) input is of type ${typeof prevFactor}`);
        }

        const value = await redisClient.hincrbyAsync(hashKey, field, factor);
        console.log(value, '35');
        return value;
    } catch (err) {
        console.log(err.message, '37');
        throw new Error(err.message);
    }
}

const setHashStrValInRedis = async (hashKey, field, value) => {
    try {
        if (typeof value !== 'string') throw new Error(`value's type must be string. value (${value}) input is of type ${typeof value}`);

        await redisClient.hsetAsync(hashKey, field, value);
        console.log(value, '47');
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
        console.log(value, '60');
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

module.exports = {
    getHashValuesFromRedis,
    getHashValFromRedis,
    incHashIntValInRedis,
    setHashStrValInRedis,
    getStrValFromRedis,
    setStrWithExInRedis
}
