const {
    doesKeyExistInRedis,
    getHashValuesFromRedis,
    getHashValFromRedis,
    incHashIntValInRedis,
    setHashStrValInRedis,
    appendElementsToListInRedis,
    getElementsFromListInRedis,
    trimListInRedis,
    removeElementFromListInRedis
} = require('../utils/redis');
const {
    sendMessageToQueue,
    pollMessagesFromQueue,
    deleteMessagesFromQueue,
    deleteMessagesBatchFromQueue
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

let arr = [];

const sendNewMessages = async (messageUrl, messageLevel, nextQueueUrl, links = []) => {
    console.log("\n", messageUrl, links.length);
    for (let link of links)
        sendMessageToQueue(nextQueueUrl, link, messageLevel + 1, messageUrl)
        .then(() => arr.push(link));
}

const crawl = async (crawlInfo) => {
    const crawlRedisHashKey = crawlInfo.crawlRedisHashKey;
    const redisHashFields = crawlInfo.redisHashFields;
    arr = []
    try {
        do {
            let doesQueueHashExist = await doesKeyExistInRedis(crawlRedisHashKey);
            if (!doesQueueHashExist || doesQueueHashExist === 'false') throw new Error(`${crawlRedisHashKey} does not exist in redis`);

            // If other crawlers finished the scraping
            let [currQueueUrl, nextQueueUrl, isCrawlingDone] = await getHashValuesFromRedis(crawlRedisHashKey, [redisHashFields[2], redisHashFields[3], redisHashFields[0]]);
            if (isCrawlingDone === "true") break;
            console.log(currQueueUrl);
            //if (isCrawlingDone === 'true') break; // Exit condition
            // let currLevel = await getHashValFromRedis(crawlRedisHashKey, redisHashFields[1]);
            let messages;
            try { messages = await pollMessagesFromQueue(currQueueUrl, 10); }
            catch (error) {
                await new Promise(resolve => setTimeout(resolve, 1500));
                continue;
            }
            //if (messages.length === 0) {
                //let workersProcessingQueue = await getElementsFromListInRedis(crawlInfo.currProcessingRedisListKey, 0, -1);
                //if (workersProcessingQueue?.length > 0) continue;
                //await setHashStrValInRedis(crawlRedisHashKey, redisHashFields[0], 'true');
            //}
            deleteMessagesFromQueue(currQueueUrl, messages);
            //await appendElementsToListInRedis(crawlInfo.currProcessingRedisListKey, [`${process.env.WORKER_ID}`]);

            for (let message of messages) {
                let messageUrl = message.Body;
                let messageLevel = parseInt(message.MessageAttributes.level.StringValue);
                let parentUrl = message.MessageAttributes.parentUrl.StringValue;
                getLinksAndAddPageToTree({ messageUrl, messageLevel, parentUrl }, crawlInfo)
                .then(links => {
                    if (links.length !== 0) {
                        if (!!nextQueueUrl) sendNewMessages(messageUrl, messageLevel, nextQueueUrl, links);
                        // .then(() => removeElementFromListInRedis(crawlInfo.currProcessingRedisListKey, `${process.env.WORKER_ID}`, 1));
                    } // else {
                        // removeElementFromListInRedis(crawlInfo.currProcessingRedisListKey, `${process.env.WORKER_ID}`, 1);
                    // }
                });
            }
        } while (true);
        console.log("arr", arr);
    } catch (err) { 
        console.log("arr", arr);
        throw new Error(err); } // Would cause re-crawling with new queue
}

module.exports = crawl;