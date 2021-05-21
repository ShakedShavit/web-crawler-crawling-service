const {
    getHashValuesFromRedis,
    getHashValFromRedis,
    incHashIntValInRedis,
    getStrValFromRedis,
    setStrWithExInRedis,
    appendElementsToListInRedis
} = require('../utils/redis');
const getPageInfo = require('./cheerio');

const getHasReachedNextLevel = async (messageLevel, queueRedisHashKey, currLvlQueueField) => {
    let currentLevel = await getHashValFromRedis(queueRedisHashKey, currLvlQueueField);
    return messageLevel > parseInt(currentLevel);
}

const getWrkCounterAndWrkReachedNextLvl = async (hashKey, workerCountersFieldsArr) => {
    try {
        let [workersCounter, workersReachedNextLevelCounter] = await getHashValuesFromRedis(hashKey, workerCountersFieldsArr);
        return {workersCounter: parseInt(workersCounter), workersReachedNextLevelCounter: parseInt(workersReachedNextLevelCounter)};
    } catch (err) {
        throw new Error("couldn't fetch workersCounter and/or workersReachedNextLevelCounter from redis");
    }
}

const waitForWorkersToReachNextLevel = async (hashKey, queueHashFields, messageLevel) => {
    try {
        let hasCheckedCurrLvlAgain = false;
        let workersMap = await getWrkCounterAndWrkReachedNextLvl(hashKey, [queueHashFields[0], queueHashFields[6]]);
        while (workersMap.workersReachedNextLevelCounter !== workersMap.workersCounter) {
            await new Promise(resolve => setTimeout(resolve, 500));
            // Checks if the crawler really did reach next level (might be that another worker at the same time has changed the current level)
            if (!hasCheckedCurrLvlAgain) {
                if (!await getHasReachedNextLevel(messageLevel, hashKey, queueHashFields[2])) break;
                hasCheckedCurrLvlAgain = true;
            }
            workersMap = await getWrkCounterAndWrkReachedNextLvl(hashKey, [queueHashFields[0], queueHashFields[6]]);
        }
        if (workersMap.workersCounter > 1) await new Promise(resolve => setTimeout(resolve, 1000)); // Gives a chance to the rest of the workers to notice the change
        await incHashIntValInRedis(hashKey, queueHashFields[6], -1);
    } catch(err) {
        await incHashIntValInRedis(hashKey, queueHashFields[6], -1);
        throw new Error(err.message);
    }
}

const getHasReachedMaxLevel = async (queueRedisHashKey, levelField, maxDepth, currLevel = -1) => {
    if (!maxDepth) return false;
    try {
        if (currLevel === -1) currLevel = await getHashValFromRedis(queueRedisHashKey, levelField);
        return parseInt(currLevel) >= maxDepth;
    } catch(err) { return false; }
}

const getHasReachedMaxPages = async (queueRedisHashKey, pageCounterField, maxPages, pageCounter = -1) => {
    if (!maxPages) return false;
    try {
        if (pageCounter === -1) pageCounter = await getHashValFromRedis(queueRedisHashKey, pageCounterField);
        return parseInt(pageCounter) >= maxPages;
    } catch (err) { return false; }
}

const getLinksAndAddPageToTree = async (message, treeRedisListKey, hasReachedLimit = false) => {
    const messageUrl = message.url;
    const messageLevel = message.level;
    const parentUrl = message.parentUrl;
    try {
        // Get page from db, and if it doesn't exist than create it and save it on db
        let page = await getStrValFromRedis(messageUrl);
        if (!page) {
            page = await getPageInfo(messageUrl);
            if (!page.error) setStrWithExInRedis(messageUrl, JSON.stringify(page));
        } else page = JSON.parse(page);

        const newPageObj = {
            title: page.title,
            level: messageLevel,
            url: messageUrl,
            parentUrl
        };
        if (!hasReachedLimit) newPageObj.children = page.error || [];

        appendElementsToListInRedis(treeRedisListKey, [JSON.stringify(newPageObj)]);

        return page.links;
    } catch (err) {
        console.log(err);
        return [];
    }
}

module.exports = {
    getWrkCounterAndWrkReachedNextLvl,
    waitForWorkersToReachNextLevel,
    getHasReachedMaxLevel,
    getHasReachedMaxPages,
    getLinksAndAddPageToTree,
    getHasReachedNextLevel
}