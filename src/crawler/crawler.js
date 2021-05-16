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
    getHasReachedLimits,
    getLinksAndAddPageToTree
} = require('../crawler/utils');

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

const processMessage = async (message, queueUrl, queueRedisHashKey, allQueueHashFields, maxPages, maxDepth, hasReachedMaxLevel = false, hasReachedMaxPages = false) => {
    const messageUrl = message.url;
    const messageLevel = message.level;
    try {
        const links = await getLinksAndAddPageToTree(message, queueRedisHashKey, allQueueHashFields[7], hasReachedMaxLevel || hasReachedMaxPages);
        if (hasReachedMaxLevel) return { hasReachedMaxLevel, hasReachedMaxPages };

        // If reached next level, stop to check if all other workers have reached it as well
        let currentLevel = await getHashValFromRedis(queueRedisHashKey, allQueueHashFields[2]);
        currentLevel = parseInt(currentLevel);
        if (messageLevel > currentLevel) {
            console.log('\n reached next level \n');
            await incHashIntValInRedis(queueRedisHashKey, allQueueHashFields[6]);
            await waitForWorkersToReachNextLevel(queueRedisHashKey, [allQueueHashFields[0], allQueueHashFields[6]], allQueueHashFields[6]);

            // If no other worker has changes the current level in hash yet, than increment it (make it equal to message level)
            let newCurrentLevel = await getHashValFromRedis(queueRedisHashKey, allQueueHashFields[2]);
            if (parseInt(newCurrentLevel) !== messageLevel) await setHashStrValInRedis(queueRedisHashKey, allQueueHashFields[2], `${messageLevel}`);
            console.log('newCurrentLevel: ', newCurrentLevel, " is changing it:", parseInt(newCurrentLevel) !== messageLevel);
            if (await getHasReachedMaxLevel(queueRedisHashKey, allQueueHashFields[2], maxDepth, newCurrentLevel)) return { hasReachedMaxLevel: true, hasReachedMaxPages };
        }

        const linksLength = links.length;
        if (!links || linksLength === 0  || hasReachedMaxPages) { hasReachedMaxLevel, hasReachedMaxPages };
        
        const linksLevel = messageLevel + 1;

        let linkErrCounter = 0;
        let pageCounter;
        for (let i = 0; i < linksLength; i++) {
            let link = links[i];
            console.log(`\n\n${link}`);
            if ((i !== 0 && links.slice(0, i).includes(link))) continue;
            try {
                pageCounter = await getHashValFromRedis(queueRedisHashKey, allQueueHashFields[3]);
                if (await getHasReachedMaxPages(queueRedisHashKey, allQueueHashFields[3], maxPages, pageCounter))
                    return { hasReachedMaxLevel, hasReachedMaxPages: true }; // Exit
                
                await sendMessageToQueue(queueUrl, link, linksLevel, messageUrl, parseInt(pageCounter));
            } catch (err) {
                if (++linkErrCounter > 2 && linksLength > 2) throw new Error(err);
                continue; // Do not increment page counter
            }
            // Update page counter
            await incHashIntValInRedis(queueRedisHashKey, allQueueHashFields[3]);
        }

        return { hasReachedMaxLevel, hasReachedMaxPages };
    } catch (err) {
        console.log(err.message);
        return { hasReachedMaxLevel, hasReachedMaxPages, err: err.message };
    }
}

const crawl = async (queueUrl) => {
    const queueRedisHashKey = `queue-workers:${queueUrl}`;
    const allQueueHashFields = [
        'workersCounter',
        'isCrawlingDone',
        'currentLevel',
        'pageCounter',
        'maxPages',
        'maxDepth',
        'workersReachedNextLevelCounter',
        'tree'
    ];

    try {
        let doesQueueHashExist = await doesKeyExistInRedis(queueRedisHashKey);
        if (!doesQueueHashExist) throw new Error(`queue-workers:${queueUrl} does not exist in redis`);

        let [currentLevel, maxPages, maxDepth] = await getHashValuesFromRedis(queueRedisHashKey, [allQueueHashFields[2], allQueueHashFields[4], allQueueHashFields[5]]);
        if (currentLevel == null || currentLevel == "null") throw new Error(`current level in queue-workers:${queueUrl} hash is null`);
        if (!!maxPages) maxPages = parseInt(maxPages);
        if (!!maxDepth) maxDepth = parseInt(maxDepth);

        await incHashIntValInRedis(queueRedisHashKey, allQueueHashFields[0]);

        let isCrawlingDone = 'false';
        let wasQueueEmptyPrevPoll = false;
        let hasReachedMaxLevel = false;
        let hasReachedMaxPages = false;
        do {
            // If other crawlers finished the scraping
            isCrawlingDone = await getHashValFromRedis(queueRedisHashKey, allQueueHashFields[1]);
            if (isCrawlingDone === 'true') break; // Exit condition
    
let date = new Date();
console.log('\n* ', date.getMinutes(), date.getSeconds(), ' *\n');

            // if (!hasReachedMaxLevel && !hasReachedMaxPages) {
            //     let limits = await getHasReachedLimits(queueRedisHashKey, allQueueHashFields, maxDepth, maxPages);
            //     hasReachedMaxLevel = limits[0];
            //     hasReachedMaxPages = limits[1];
            // } else {
            //     hasReachedMaxLevel = hasReachedMaxLevel || await getHasReachedMaxLevel(queueRedisHashKey, allQueueHashFields[2], maxDepth, -1);
            //     hasReachedMaxPages = hasReachedMaxPages || await getHasReachedMaxPages(queueRedisHashKey, allQueueHashFields[3], maxPages, -1);
            // }

            const messages = await pollMessagesFromQueue(queueUrl, hasReachedMaxLevel || hasReachedMaxPages ? 10 : 3);

            if (messages.length === 0) {
                // Could have simplify this section by using GetQueueAttributes (using sqs api),
                // but it would not work properly because I am deleting the messages before processing them.

                // Might be that all the messages were polled and deleted (by other crawlers) but not processed yet (because they reached next level and are waiting for this one to update), if so, than increment the counter and re-try
                let workersMap = await getWrkCounterAndWrkReachedNextLvl(queueRedisHashKey, [allQueueHashFields[0], allQueueHashFields[6]]);
                if (!wasQueueEmptyPrevPoll && workersMap.workersCounter !== 1 && workersMap.workersReachedNextLevelCounter !== workersMap.workersCounter) {
                    await incHashIntValInRedis(queueRedisHashKey, allQueueHashFields[6]);
                    wasQueueEmptyPrevPoll = true;
                    continue;
                } else if (wasQueueEmptyPrevPoll && workersMap.workersCounter !== 1 && workersMap.workersReachedNextLevelCounter !== workersMap.workersCounter) continue;
                
                await setHashStrValInRedis(queueRedisHashKey, allQueueHashFields[1], 'true');
                break;
            }

            if (wasQueueEmptyPrevPoll) {
                await incHashIntValInRedis(queueRedisHashKey, allQueueHashFields[6], -1);
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
            deleteMessagesBatchFromQueue(queueUrl, messagesDeleteObjects)
            // .then(({ BatchResultErrorEntry }) => {
            //     messagesDeleteObjects = [];
            //     BatchResultErrorEntry.forEach(message => {
            //         messagesDeleteObjects.push(messages[parseInt(message.Id)]);
            //     });
            //     if (BatchResultErrorEntry.length != 0) deleteMessagesFromQueue(queueUrl, messagesDeleteObjects);
            // })
            // .catch(err => {
            //     deleteMessagesFromQueue(queueUrl, messages);
            // });
            console.log('deleting messages', messages.length);

            for (let message of messagesInfoArr) {
                console.log('*********LOOP');
                ({ hasReachedMaxLevel, hasReachedMaxPages } = await processMessage(message, queueUrl, queueRedisHashKey, allQueueHashFields, maxPages, maxDepth, hasReachedMaxLevel, hasReachedMaxPages));
                console.log({ hasReachedMaxLevel, hasReachedMaxPages });
            }
        } while (isCrawlingDone === 'false'); // while(true) will accomplish the same here

        await incHashIntValInRedis(queueRedisHashKey, allQueueHashFields[0], -1);
    } catch (err) {
        console.log(err, '174');
        try {
            await incHashIntValInRedis(queueRedisHashKey, allQueueHashFields[0], -1);
        } catch (error) {
            console.log(error);
            throw new Error({ err, error });
        }
        // Would cause re-crawling with new queue
        throw new Error(err);
    }
}

module.exports = crawl;