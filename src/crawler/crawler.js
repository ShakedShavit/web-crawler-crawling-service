const {
    doesKeyExistInRedis,
    getHashValuesFromRedis,
    getHashValFromRedis,
    incHashIntValInRedis,
    setHashStrValInRedis
} = require('../utils/redis');
const {
    sendMessageToQueue,
    pollMessagesFromQueue,
    deleteMessagesFromQueue,
    deleteMessagesBatchFromQueue
} = require('../utils/sqs');
const {
    getWrkCounterAndWrkReachedNextLvl,
    waitForWorkersToReachNextLevel,
    getHasReachedMaxLevel,
    getHasReachedMaxPages,
    getLinksAndAddPageToTree,
    getHasReachedNextLevel
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

const processMessage = async (message, links = [], crawlInfo) => {
    const linksLength = links.length;
    if (links.length === 0 || crawlInfo.hasReachedLimit) return;
    const messageUrl = message.url;
    const messageLevel = message.level;
    const linksLevel = messageLevel + 1;
    let pageCounter;

    for (let i = 0; i < linksLength; i++) {
        let link = links[i];
        if ((i !== 0 && links.slice(0, i).includes(link))) continue;
        try {
            pageCounter = await getHashValFromRedis(crawlInfo.queueRedisHashKey, crawlInfo.queueHashFields[3]);
            if (await getHasReachedMaxPages(crawlInfo.queueRedisHashKey, crawlInfo.queueHashFields[3], crawlInfo.maxPages, pageCounter)) {
                crawlInfo.hasReachedMaxPages = true;
                break; // Exit
            }
        } catch (err) {
            throw new Error(err);
        }
        crawlInfo.processesRunning++;
        sendMessageToQueue(crawlInfo.queueUrl, link, linksLevel, messageUrl, parseInt(pageCounter))
            .catch(async (res) => {
                if (!!crawlInfo.maxPages && !crawlInfo.hasReachedLimit)
                    await incHashIntValInRedis(crawlInfo.queueRedisHashKey, crawlInfo.queueHashFields[3], -1);
            })
            .finally(() => crawlInfo.processesRunning-- );
        // Update page counter
        await incHashIntValInRedis(crawlInfo.queueRedisHashKey, crawlInfo.queueHashFields[3]);
    }
}

const crawl = async (crawlInfo) => {
    const queueRedisHashKey = crawlInfo.queueRedisHashKey;
    const queueHashFields = crawlInfo.queueHashFields;
    let isCrawlingDone = 'false';
    let wasQueueEmptyPrevPoll = false;

    try {
        let doesQueueHashExist = await doesKeyExistInRedis(queueRedisHashKey);
        if (!doesQueueHashExist || doesQueueHashExist === 'false') throw new Error(`${queueRedisHashKey} does not exist in redis`);

        let [currentLevel, maxPages, maxDepth] = await getHashValuesFromRedis(queueRedisHashKey, [queueHashFields[2], queueHashFields[4], queueHashFields[5]]);
        if (currentLevel == null || currentLevel == "null") throw new Error(`current level in ${queueRedisHashKey} hash is null`);
        if (!!maxPages) maxPages = parseInt(maxPages);
        if (!!maxDepth) maxDepth = parseInt(maxDepth);
        crawlInfo.maxPages = maxPages;
        crawlInfo.maxDepth = maxDepth;
        crawlInfo.currentLevel = currentLevel;
    } catch (err) { throw new Error(err); }

    try {
        await incHashIntValInRedis(queueRedisHashKey, queueHashFields[0]);
        do {
            // If other crawlers finished the scraping
            let isCrawlingDone = await getHashValFromRedis(queueRedisHashKey, queueHashFields[1]);
            if (isCrawlingDone === 'true') break; // Exit condition
    
let date = new Date();
console.log('\n* ', date.getMinutes(), date.getSeconds(), ' *\n');

            const messages = await pollMessagesFromQueue(crawlInfo.queueUrl, crawlInfo.hasReachedLimit ? 10 : 3);
            if (messages.length === 0) {
                // Could have simplify this section by using GetQueueAttributes (using sqs api),
                // but it would not work properly because I am deleting the messages before processing them.

                // Might be that all the messages were polled and deleted (by other crawlers) but not processed yet (because they reached next level and are waiting for this one to update), if so, than increment the counter and re-try
                let workersMap = await getWrkCounterAndWrkReachedNextLvl(queueRedisHashKey, [queueHashFields[0], queueHashFields[6]]);
                if (!wasQueueEmptyPrevPoll && workersMap.workersCounter !== 1 && workersMap.workersReachedNextLevelCounter !== workersMap.workersCounter) {
                    await incHashIntValInRedis(queueRedisHashKey, queueHashFields[6]);
                    wasQueueEmptyPrevPoll = true;
                    continue;
                } else if (wasQueueEmptyPrevPoll && workersMap.workersCounter !== 1 && workersMap.workersReachedNextLevelCounter !== workersMap.workersCounter) continue;
                
                while (!crawlInfo.areProcessesDone)
                    await new Promise(resolve => setTimeout(resolve, 1500));

                await setHashStrValInRedis(queueRedisHashKey, queueHashFields[1], 'true');
                break;
            }

            if (wasQueueEmptyPrevPoll) {
                await incHashIntValInRedis(queueRedisHashKey, queueHashFields[6], -1);
                wasQueueEmptyPrevPoll = false;
            }

            // Extract info from messages that holds the message info
            let messagesInfoArr = [];
            let messagesDeleteObjects = [];
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
            // Deleting the messages before processing, so other crawlers could get the messages
            crawlInfo.processesRunning++;
            deleteMessagesBatchFromQueue(crawlInfo.queueUrl, messagesDeleteObjects)
            .then(({ BatchResultErrorEntry }) => {
                messagesDeleteObjects = [];
                BatchResultErrorEntry.forEach(message => {
                    messagesDeleteObjects.push(messages[parseInt(message.Id)]);
                });
                if (BatchResultErrorEntry.length != 0) {
                    deleteMessagesFromQueue(crawlInfo.queueUrl, messagesDeleteObjects)
                    .finally(() => crawlInfo.processesRunning--);
                } else crawlInfo.processesRunning--;
            })
            .catch(err => {
                deleteMessagesFromQueue(crawlInfo.queueUrl, messages)
                .finally(() => crawlInfo.processesRunning--);
            });
            console.log('deleting messages', messages.length);

            for (let message of messagesInfoArr) {
                let messageLevel = message.level;
                // If reached next level, stop to check if all other workers have reached it as well
                if (!crawlInfo.hasReachedMaxLevel && await getHasReachedNextLevel(messageLevel, queueRedisHashKey, queueHashFields[2])) {
                    while (!crawlInfo.areProcessesDone)
                        await new Promise(resolve => setTimeout(resolve, 1500));

                    console.log('\n reached next level \n');
                    await incHashIntValInRedis(queueRedisHashKey, queueHashFields[6]);
                    await waitForWorkersToReachNextLevel(queueRedisHashKey, crawlInfo.queueHashFields, messageLevel);

                    // If no other worker has changes the current level in hash yet, than increment it (make it equal to message level)
                    let newCurrentLevel = await getHashValFromRedis(queueRedisHashKey, queueHashFields[2]);
                    if (parseInt(newCurrentLevel) !== messageLevel) await setHashStrValInRedis(queueRedisHashKey, queueHashFields[2], `${messageLevel}`);
                    console.log('newCurrentLevel: ', newCurrentLevel, " is changing it:", parseInt(newCurrentLevel) !== messageLevel);
                    if (await getHasReachedMaxLevel(queueRedisHashKey, queueHashFields[2], crawlInfo.maxDepth, messageLevel))
                        crawlInfo.hasReachedMaxLevel = true;
                }

                crawlInfo.processesRunning++;
                getLinksAndAddPageToTree(message, crawlInfo.treeRedisListKey, crawlInfo.hasReachedLimit)
                .then(links => {
                    processMessage(message, links, crawlInfo).finally(res => crawlInfo.processesRunning-- );
                });
            }
        } while (isCrawlingDone === 'false'); // while(true) will accomplish the same here

        await incHashIntValInRedis(queueRedisHashKey, queueHashFields[0], -1);
    } catch (err) {
        try {
            await incHashIntValInRedis(queueRedisHashKey, queueHashFields[0], -1);
        } catch (error) { throw new Error({ err, error }); }
        // Would cause re-crawling with new queue
        throw new Error(err);
    }
}

module.exports = crawl;