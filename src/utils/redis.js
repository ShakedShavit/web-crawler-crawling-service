const redisClient = require("../db/redis");

//#region Crawling state hash data manipulation/fetching in/from Redis (each client/search has their own state)

// get [workersCounter, isCrawlingDone, currentLevel, pageCounter, maxPages, maxLevel]
const getHashValuesFromRedis = async (hashKey, fieldsArr) => {
    try {
        return redisClient.hmgetAsync(hashKey, ...fieldsArr);
    } catch (err) {
        throw new Error(err.message);
    }
};

const getHashValFromRedis = async (hashKey, field) => {
    try {
        return redisClient.hgetAsync(hashKey, field);
    } catch (err) {
        throw new Error(err.message);
    }
};

const doesKeyExistInRedis = async (key) => {
    try {
        return redisClient.existsAsync(key);
    } catch (err) {
        throw new Error(err.message);
    }
};

const incHashIntValInRedis = async (hashKey, field, factor = 1) => {
    try {
        const doesKeyExist = await doesKeyExistInRedis(hashKey);
        // Return 0 or 1 (!0 equals True, !1 equals False)
        if (!doesKeyExist) throw new Error("key does not exist in redis");

        if (typeof factor !== "number") {
            let prevFactor = factor;
            factor = parseInt(factor);
            if (isNaN(factor))
                throw new Error(
                    `factor's type must be number. factor (${prevFactor}) input is of type ${typeof prevFactor}`
                );
        }
        if (factor === 0) return 0;

        return redisClient.hincrbyAsync(hashKey, field, factor);
    } catch (err) {
        throw new Error(err.message);
    }
};

const setHashStrValInRedis = async (hashKey, field, value) => {
    try {
        const doesKeyExist = await doesKeyExistInRedis(hashKey);
        if (!doesKeyExist) throw new Error("key does not exist in redis");

        if (typeof value !== "string")
            throw new Error(
                `value's type must be string. value (${value}) input is of type ${typeof value}`
            );

        return redisClient.hsetAsync(hashKey, field, value);
    } catch (err) {
        throw new Error(err.message);
    }
};

//#endregion

const getStrValFromRedis = async (key) => {
    try {
        return redisClient.getAsync(key);
    } catch (err) {
        throw new Error(err.message);
    }
};

const setStrWithExInRedis = async (key, value, exSec = 300) => {
    try {
        if (typeof value !== "string")
            throw new Error(
                `value's type must be string. value (${value}) input is of type ${typeof value}`
            );

        return redisClient.setexAsync(key, exSec, value);
    } catch (err) {
        console.log(err.message, "73");
        throw new Error(err.message);
    }
};

// Pops (and returns) last el of list and pushes it in another list (both lists could be the same list)
const getLastElOfListAndPushItToDestListInRedis = async (sourceKey, destKey = sourceKey) => {
    try {
        return redisClient.rpoplpushAsync(sourceKey, destKey);
    } catch (err) {
        throw new Error(err.message);
    }
};

const appendElementsToListInRedis = async (key, elementsArr) => {
    try {
        return redisClient.rpushAsync(key, ...elementsArr);
    } catch (err) {
        throw new Error(err.message);
    }
};

const getElementsFromListInRedis = async (key, start = 0, end = -1) => {
    try {
        return redisClient.lrangeAsync(key, start, end);
    } catch (err) {
        throw new Error(err.message);
    }
};

const trimListInRedis = async (key, start = 0, end = -1) => {
    try {
        return redisClient.ltrimAsync(key, start, end);
    } catch (err) {
        throw new Error(err.message);
    }
};

const removeElementFromListInRedis = async (key, element, count = 0) => {
    try {
        return redisClient.lremAsync(key, count, element);
    } catch (err) {
        throw new Error(err.message);
    }
};

module.exports = {
    doesKeyExistInRedis,
    getHashValuesFromRedis,
    getHashValFromRedis,
    incHashIntValInRedis,
    setHashStrValInRedis,
    getStrValFromRedis,
    setStrWithExInRedis,
    getLastElOfListAndPushItToDestListInRedis,
    appendElementsToListInRedis,
    getElementsFromListInRedis,
    trimListInRedis,
    removeElementFromListInRedis,
};
