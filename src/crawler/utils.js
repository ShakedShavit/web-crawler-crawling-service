const {
    getStrValFromRedis,
    setStrWithExInRedis,
    appendElementsToListInRedis,
    incHashIntValInRedis
} = require('../utils/redis');
const getPageInfo = require('./cheerio');

const getLinksAndAddPageToTree = async (message, crawlInfo) => {
    const { messageUrl, messageLevel, parentUrl } = message;
    try {
        // Get page from db, and if it doesn't exist than create it and save it on db
        console.log(messageUrl, message);
        let page = await getStrValFromRedis(messageUrl);
        if (!page) {
            page = await getPageInfo(messageUrl);
            if (!page.error) setStrWithExInRedis(messageUrl, JSON.stringify(page));
        } else page = JSON.parse(page);

        const newPageObj = {
            title: page.title,
            level: messageLevel,
            url: messageUrl,
            children: [],
            parentUrl,
            linksLength: page.links.length,
            childrenCounter: 0
        };

        appendElementsToListInRedis(crawlInfo.treeRedisListKey, [JSON.stringify(newPageObj)]);
        // Add the links length to the hash key of the next level links length (for the API/main server to use)
        incHashIntValInRedis(crawlInfo.crawlRedisHashKey, crawlInfo.redisHashFields[4], page.links.length);

        return page.links;
    } catch (err) {
        console.log(err);
        return [];
    }
}

module.exports = getLinksAndAddPageToTree;