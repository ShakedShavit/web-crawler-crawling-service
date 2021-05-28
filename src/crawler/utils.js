const {
    getHashValFromRedis,
    getStrValFromRedis,
    setStrWithExInRedis,
    appendElementsToListInRedis,
    getElementsFromListInRedis
} = require('../utils/redis');
const getPageInfo = require('./cheerio');

const getHasReachedMaxLevel = async (maxDepth, currLevel = -1, queueRedisHashKey, levelField) => {
    if (!maxDepth) return false;
    try {
        if (currLevel === -1) currLevel = await getHashValFromRedis(queueRedisHashKey, levelField);
        return parseInt(currLevel) >= maxDepth;
    } catch(err) { return false; }
}

const getHasReachedMaxPages = async (maxPages, pageCounter = -1, queueRedisHashKey, pageCounterField) => {
    if (!maxPages) return false;
    try {
        if (pageCounter === -1) pageCounter = await getHashValFromRedis(queueRedisHashKey, pageCounterField);
        return parseInt(pageCounter) >= maxPages;
    } catch (err) { return false; }
}

const getLinksAndAddPageToTree = async (message, crawlInfo) => {
    const { url: messageUrl, level: messageLevel, parentUrl } = message;
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
        if (!crawlInfo.hasReachedLimit) newPageObj.children = page.error || [];

        if (!crawlInfo.hasReachedLimit && page.links.length !== 0) {
            const getTreePromise = getHashValFromRedis(crawlInfo.queueRedisHashKey, crawlInfo.queueHashFields[4]);
            const getNewPagesPromise = getElementsFromListInRedis(crawlInfo.treeRedisListKey, 0, -1);
            await Promise.allSettled([getTreePromise, getNewPagesPromise])
            .then((values) => {
                if (values[0].value.includes(`"url":"${messageUrl}"`) ||
                values[1].value.some(p => p.includes(`"url":"${messageUrl}"`))) {
                    if (!!newPageObj.children) delete newPageObj.children;
                    page.links = [];
                }
            });
        }
        
        appendElementsToListInRedis(crawlInfo.treeRedisListKey, [JSON.stringify(newPageObj)]);

        return page.links;
    } catch (err) {
        console.log(err);
        return [];
    }
}

module.exports = {
    getHasReachedMaxLevel,
    getHasReachedMaxPages,
    getLinksAndAddPageToTree
}