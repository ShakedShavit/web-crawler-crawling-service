const {
    doesKeyExistInRedis,
    getHashValuesFromRedis
} = require('../utils/redis');
const {
    sendMessageToQueue,
    pollMessagesFromQueue,
    deleteMessagesFromQueue
} = require('../utils/sqs');
const getLinksAndAddPageToTree = require('../crawler/utils');

//  pages-list:<queueUrl> - Holds new pages information (as JSON) collected to be added to the tree

//  data about particular search, shared by all workers through redis
//  queue-workers:<queueUrl>: {                       
//      workersCounter: number              
//      num of workers currently working on it (when worker starts crawling this url increment this). defaults to 0

//      isCrawlingDone: boolean             
//      is crawling done (turn true when one of the workers poll zero messages). defaults to false

//      currentLevel: number
//      the current search level (only when all the workers surpass this the workers can continue). defaults to 0

//      pageCounter: number
//      the current search page (for the maxDepth)

//      maxPages: number
//      max search pages, is set in the REST API

//      maxDepth: number
//      max search levels, is set in the REST API

//      workersReachedNextLevelCounter: number
//      increment this when a worker reaches currentLevel + 1, and stop the workers process until it finishes. defaults and resets to 0 (each time this reaches the workersCounter)

//      tree: JSON
//      the entire tree of the queueUrl is saved (in the form of JSON)
//}

const sendNewMessages = async (messageUrl, messageLevel, nextQueueUrl, links = []) => {
    try {
        for (let link of links)
            sendMessageToQueue(nextQueueUrl, link, messageLevel + 1, messageUrl)
            .catch((err) => { throw new Error(err.message); });
    } catch (err) {
        throw new Error(err.message);
    }
}

const crawl = async (crawlInfo) => {
    const crawlRedisHashKey = crawlInfo.crawlRedisHashKey;
    const redisHashFields = crawlInfo.redisHashFields;
    try {
        do {
            let doesQueueHashExist = await doesKeyExistInRedis(crawlRedisHashKey);
            if (!doesQueueHashExist || doesQueueHashExist === 'false') throw new Error(`${crawlRedisHashKey} does not exist in redis`);

            // If other crawlers finished the scraping
            let [currQueueUrl, nextQueueUrl, isCrawlingDone] = await getHashValuesFromRedis(crawlRedisHashKey, [redisHashFields[2], redisHashFields[3], redisHashFields[0]]);
            if (isCrawlingDone === "true") break;

            let messages;
            try { messages = await pollMessagesFromQueue(currQueueUrl, 10); }
            catch (error) {
                await new Promise(resolve => setTimeout(resolve, 1500));
                continue;
            }

            deleteMessagesFromQueue(currQueueUrl, messages);

            for (let message of messages) {
                let messageUrl = message.Body;
                let messageLevel = parseInt(message.MessageAttributes.level.StringValue);
                let parentUrl = message.MessageAttributes.parentUrl.StringValue;
                getLinksAndAddPageToTree({ messageUrl, messageLevel, parentUrl }, crawlInfo)
                .then(links => {
                    if (links.length !== 0) {
                        if (!!nextQueueUrl) 
                            sendNewMessages(messageUrl, messageLevel, nextQueueUrl, links)
                            .catch((err) => { throw new Error(err.message); })
                    }
                });
            }
        } while (true);
    } catch (err) { throw new Error(err); } // Would cause re-crawling with new queue
}

module.exports = crawl;