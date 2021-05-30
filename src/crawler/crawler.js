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
const {
    getHasReachedMaxLevel,
    getHasReachedMaxPages,
    getLinksAndAddPageToTree,
} = require('../crawler/utils');

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

const sendNewMessages = async (message, links = [], crawlInfo) => {
    const { url: messageUrl, level: messageLevel } = message;
    let pagesToAddNum = Infinity;
    let pageCounter;
    const linksLength = links.length;

    console.log(messageUrl, linksLength);
    try {
        pageCounter = await getHashValFromRedis(crawlInfo.queueRedisHashKey, crawlInfo.queueHashFields[1]);
        pageCounter = parseInt(pageCounter);

        if (!!crawlInfo.maxPages) pagesToAddNum = crawlInfo.maxPages - pageCounter;
        let pagesCounterInc = (pagesToAddNum > linksLength || !crawlInfo.maxPages) ? linksLength : pagesToAddNum;
        if (pagesCounterInc > 0) await incHashIntValInRedis(crawlInfo.queueRedisHashKey, crawlInfo.queueHashFields[1], pagesCounterInc);

        if (await getHasReachedMaxPages(crawlInfo.maxPages, pageCounter + pagesCounterInc))
            crawlInfo.hasReachedMaxPages = true;
    } catch (err) { throw new Error(err); }

    let sendMessagePromises = [];
    for (let i = 0; i < linksLength && i < pagesToAddNum; i++)
        sendMessagePromises.push(sendMessageToQueue(crawlInfo.queueUrl, links[i], messageLevel + 1, messageUrl, pageCounter + i));
    await Promise.allSettled(sendMessagePromises).then(() => {});
    // return new Promise((resolve, reject) => {
    //     Promise.allSettled(sendMessagePromises).then(() => resolve());
    // });
}

const crawl = async (crawlInfo) => {
    const queueRedisHashKey = crawlInfo.queueRedisHashKey;
    const queueHashFields = crawlInfo.queueHashFields;
    try {
        let doesQueueHashExist = await doesKeyExistInRedis(queueRedisHashKey);
        if (!doesQueueHashExist || doesQueueHashExist === 'false') throw new Error(`${queueRedisHashKey} does not exist in redis`);
        let [maxPages, maxDepth] = await getHashValuesFromRedis(queueRedisHashKey, [queueHashFields[2], queueHashFields[3]]);
        if (!!maxPages) maxPages = parseInt(maxPages);
        if (!!maxDepth) maxDepth = parseInt(maxDepth);
        crawlInfo.maxPages = maxPages;
        crawlInfo.maxDepth = maxDepth;
    } catch (err) { throw new Error(err); }

    try {
        do {
            // If other crawlers finished the scraping
            if (await getHashValFromRedis(queueRedisHashKey, queueHashFields[0]) === 'true') break; // Exit condition
    
let date = new Date();
console.log('\n* ', date.getMinutes(), date.getSeconds(), ' *\n');

            const messages = await pollMessagesFromQueue(crawlInfo.queueUrl, crawlInfo.hasReachedLimit ? 10 : 3);
            if (messages.length === 0) {
                let workersProcessingQueue = await getElementsFromListInRedis(crawlInfo.currProcessingRedisListKey, 0, -1);
                if (workersProcessingQueue?.length > 0) continue;

                await setHashStrValInRedis(queueRedisHashKey, queueHashFields[0], 'true');
                break;
            }
            await appendElementsToListInRedis(crawlInfo.currProcessingRedisListKey, [`${process.env.WORKER_ID}`]);

            //#region Extracting messages information
            // Extract info from messages that holds the message info
            let messagesDeleteObjects = [];
            let messagesInfoArr = [];
            for (let i = 0; i < messages.length; i++) {
                let message = messages[i];
                messagesInfoArr.push({
                    url: message.Body,
                    level: parseInt(message.MessageAttributes.level.StringValue),
                    parentUrl: message.MessageAttributes.parentUrl.StringValue
                });
                messagesDeleteObjects.push({
                    Id: `${i}`,
                    ReceiptHandle: message.ReceiptHandle
                });
            }
            //#endregion
            //#region Deleting messages
            // Deleting the messages before processing, so other crawlers could get the messages
            deleteMessagesBatchFromQueue(crawlInfo.queueUrl, messagesDeleteObjects)
            .then(({ Failed }) => {
                messagesDeleteObjects = [];
                Failed.forEach(message => messagesDeleteObjects.push(messages[parseInt(message.Id)]) );
                if (Failed.length !== 0) deleteMessagesFromQueue(crawlInfo.queueUrl, messagesDeleteObjects);
            })
            .catch(err => deleteMessagesFromQueue(crawlInfo.queueUrl, messages) );
            //#endregion
            
            let getLinksPromises = [];
            for (let message of messagesInfoArr) {
                getLinksPromises.push(new Promise((resolve, reject) => {
                    getLinksAndAddPageToTree(message, crawlInfo)
                    .then(links => resolve(links))
                    .catch(err => reject(err));
                }));
            }

            if (!crawlInfo.hasReachedLimit) {
                let currProcessWorker = await getElementsFromListInRedis(crawlInfo.currProcessingRedisListKey, 0, 0);
                while (currProcessWorker[0] !== `${process.env.WORKER_ID}`) {
                    await new Promise(resolve => setTimeout(resolve, 1500));
                    currProcessWorker = await getElementsFromListInRedis(crawlInfo.currProcessingRedisListKey, 0, 0);
                }
            }

            for (let i = 0; i < messagesInfoArr.length; i++) {
                let message = messagesInfoArr[i];
                let messageLevel = message.level;

                if (!crawlInfo.hasReachedMaxLevel && await getHasReachedMaxLevel(crawlInfo.maxDepth, messageLevel))
                    crawlInfo.hasReachedMaxLevel = true;

                if (crawlInfo.hasReachedLimit) continue;
                let links = await getLinksPromises[i];
                if (links.length !== 0) await sendNewMessages(message, links, crawlInfo);
            }
            await Promise.allSettled(getLinksPromises).then(() => {});
            
            await removeElementFromListInRedis(crawlInfo.currProcessingRedisListKey, `${process.env.WORKER_ID}`, 1);
        } while (true);
    } catch (err) { throw new Error(err); } // Would cause re-crawling with new queue
}

module.exports = crawl;