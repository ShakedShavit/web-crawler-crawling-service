const {
    getHashValuesFromRedis,
    getHashValFromRedis,
    incHashIntValInRedis,
    getStrValFromRedis,
    setStrWithExInRedis,
    appendElementsToListInRedis
} = require('../utils/redis');
const getPageInfo = require('./cheerio');

const getWrkCounterAndWrkReachedNextLvl = async (hashKey, workerCountersFieldsArr) => {
    try {
        let [workersCounter, workersReachedNextLevelCounter] = await getHashValuesFromRedis(hashKey, workerCountersFieldsArr);
        return {workersCounter: parseInt(workersCounter), workersReachedNextLevelCounter: parseInt(workersReachedNextLevelCounter)};
    } catch (err) {
        throw new Error("couldn't fetch workersCounter and/or workersReachedNextLevelCounter from redis");
    }
}

const waitForWorkersToReachNextLevel = async (hashKey, workerCountersFieldsArr, workersReachedNextLevelCounterField) => {
    try {
        let workersMap = await getWrkCounterAndWrkReachedNextLvl(hashKey, workerCountersFieldsArr);
        while (workersMap.workersReachedNextLevelCounter !== workersMap.workersCounter) {
            await new Promise(resolve => setTimeout(resolve, 500));
            workersMap = await getWrkCounterAndWrkReachedNextLvl(hashKey, workerCountersFieldsArr);
        }
        if (workersMap.workersCounter > 1) await new Promise(resolve => setTimeout(resolve, 1000)); // Gives a chance to the rest of the workers to notice the change
        await incHashIntValInRedis(hashKey, workersReachedNextLevelCounterField, -1);
    } catch(err) {
        throw new Error(err.message);
    }
}

const getHasReachedMaxLevel = async (queueRedisHashKey, levelField, maxDepth, currLevel = -1) => {
    if (!maxDepth) return false;
    try {
        if (currLevel === -1) currLevel = await getHashValFromRedis(queueRedisHashKey, levelField);
        return parseInt(currLevel) >= maxDepth - 1;
    } catch(err) { return false; }
}

const getHasReachedMaxPages = async (queueRedisHashKey, pageCounterField, maxPages, pageCounter = -1) => {
    if (!maxPages) return false;
    try {
        if (pageCounter === -1) pageCounter = await getHashValFromRedis(queueRedisHashKey, pageCounterField);
        return parseInt(pageCounter) >= maxPages;
    } catch (err) { return false; }
}

// Add new page obj directly to JSON formatted tree (without parsing it)
const getUpdatedJsonTree = (treeJSON, newPageObj, parentUrl) => {
    let newPageJSON = JSON.stringify(newPageObj);
    let searchString = `${parentUrl}","children":[`;
    let insertIndex = treeJSON.indexOf(searchString);
    if (insertIndex === -1) return newPageJSON; // If the tree is empty (first page insertion)
    insertIndex += searchString.length;
    if (treeJSON[insertIndex] === '{') newPageJSON += ',';
    return treeJSON.slice(0, insertIndex) + newPageJSON + treeJSON.slice(insertIndex);
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

        // let treeJSON = await getHashValFromRedis(queueRedisHashKey, treeQueueField);

        // let isUrlInTree = treeJSON.includes(`,"url":"${messageUrl}"`);
        // if (!isUrlInTree && !hasReachedLimit) newPageObj.children = page.error || [];

        // let updatedTree = getUpdatedJsonTree(treeJSON, newPageObj, parentUrl);
        // await setHashStrValInRedis(queueRedisHashKey, treeQueueField, updatedTree);

        return page.links;
    } catch (err) {
        console.log(err);
        return [];
    }
}

const getHasReachedNextLevel = async (messageLevel, queueRedisHashKey, currLvlQueueField) => {
    let currentLevel = await getHashValFromRedis(queueRedisHashKey, currLvlQueueField);
    return messageLevel > parseInt(currentLevel);
}

module.exports = {
    getWrkCounterAndWrkReachedNextLvl,
    waitForWorkersToReachNextLevel,
    getHasReachedMaxLevel,
    getHasReachedMaxPages,
    getLinksAndAddPageToTree,
    getHasReachedNextLevel
}