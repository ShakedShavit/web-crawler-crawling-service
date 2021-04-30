const {
    doesKeyExistInRedis,
    getHashValuesFromRedis,
    getHashValFromRedis,
    incHashIntValInRedis,
    setHashStrValInRedis,
    getStrValFromRedis,
    setStrWithExInRedis
} = require('./redis');
const {
    sendMessageToQueue,
    pollMessagesFromQueue,
    deleteMessagesFromQueue
} = require('./sqs');
const getPageInfo = require('./cheerio');

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


const waitForWorkersToReachNextLevel = async (hashKey, workerCountersFieldsArr, currentLevelField, workersReachedNextLevelCounterField) => {
    let [workersCounter, workersReachedNextLevelCounter] = await getHashValuesFromRedis(hashKey, workerCountersFieldsArr);
    workersCounter = parseInt(workersCounter);
    workersReachedNextLevelCounter = parseInt(workersReachedNextLevelCounter);
    if (workersReachedNextLevelCounter !== workersCounter) {
        await setTimeout(() => {}, 1000);
        await waitForWorkersToReachNextLevel(hashKey, workerCountersFieldsArr, currentLevelField, workersReachedNextLevelCounterField);
    }
    await incHashIntValInRedis(hashKey, workersReachedNextLevelCounterField, -1);
}

// Add new page obj directly to JSON formatted tree (without parsing it)
const getUpdatedJsonTree = (treeJSON, newPageObj, parentUrl) => {
    let newPageJSON = JSON.stringify(newPageObj);
    console.log(treeJSON, '57');
    console.log();
    console.log(newPageJSON, '58');
    console.log();
    console.log(parentUrl, '62');
    let searchString = `${parentUrl}","children":[`;
    let insertIndex = treeJSON.indexOf(searchString);
    if (insertIndex === -1) return newPageJSON; // If the tree is empty (first page insertion)
    insertIndex += searchString.length;
    if (treeJSON[insertIndex] === '{') newPageJSON += ',';
    return treeJSON.slice(0, insertIndex) + newPageJSON + treeJSON.slice(insertIndex);
}

const processMessage = async (message, queueUrl, queueRedisHashKey, allQueueHashFields, maxPages, maxDepth) => {
    try {
        const messageLevel = parseInt(message.Attributes.MessageGroupId);
        const messageUrl = message.Body;
        const parentUrl = message.MessageAttributes.parentUrl.StringValue;

        // If reached next level, stop to check if all other workers have reached it as well
        let currentLevel = await getHashValFromRedis(queueRedisHashKey, allQueueHashFields[2]);
        currentLevel = parseInt(currentLevel);
        if (messageLevel > currentLevel) {
            console.log('\n reached next level \n');
            await incHashIntValInRedis(queueRedisHashKey, allQueueHashFields[6]);
            // Recursive func that stops when all other workers get to the next level
            await waitForWorkersToReachNextLevel(queueRedisHashKey, [allQueueHashFields[0], allQueueHashFields[6]], allQueueHashFields[2], allQueueHashFields[6]);

            // If no other worker has changes the current level in hash yet, than increment it (make it equal to message level)
            let newCurrentLevel = await getHashValFromRedis(queueRedisHashKey, allQueueHashFields[2]);
            if (parseInt(newCurrentLevel) !== messageLevel) await setHashStrValInRedis(queueRedisHashKey, allQueueHashFields[2], messageLevel.toString());
            console.log('newCurrentLevel: ', newCurrentLevel, " is changing it:", parseInt(newCurrentLevel) !== messageLevel);
        }

        // Get page from db, and if it doesn't exist than create it and save it on db
        let page = await getStrValFromRedis(messageUrl);
        if (!page) {
             page = await getPageInfo(messageUrl);
             await setStrWithExInRedis(messageUrl, JSON.stringify(page));
        } else {
             page = JSON.parse(page);
        }

        //#region Update tree in Redis
        const treeJSON = await getHashValFromRedis(queueRedisHashKey, allQueueHashFields[7]);
        const newPageObj = {
            title: page.title,
            level: messageLevel,
            url: messageUrl,
            children: []
        };
        await setHashStrValInRedis(queueRedisHashKey, allQueueHashFields[7], getUpdatedJsonTree(treeJSON, newPageObj, parentUrl));
        //#endregion

        // If messageUrl has already been processed for this queue than move to next message
        if (treeJSON.includes(messageUrl) || !page.links) return false;

        // If messageLevel = maxLevel than don't send new messages
        if (!!maxDepth && messageLevel + 1 >= maxDepth) {
            console.log('maxDepth:', maxDepth, 'messageLevel:', messageLevel, '128');
            return false;
        }

        let pageCounter;
        const linksLength = page.links.length;
        for (let i = 0; i < linksLength; i++) {
            let link = page.links[i];

            // If crawl limits have been reached
            if (!!maxPages) {
                pageCounter = await getHashValFromRedis(queueRedisHashKey, allQueueHashFields[3]);
                pageCounter = parseInt(pageCounter);

                if (pageCounter >= maxPages) {
                    console.log('pageCounter:', pageCounter, 'maxPages:', maxPages, '128');
                    break;
                }
            }

            await sendMessageToQueue(queueUrl, link, messageLevel + 1, messageUrl);
            // Update page counter
            if (!!maxPages) await incHashIntValInRedis(queueRedisHashKey, allQueueHashFields[3]);
        }
    } catch (err) {
        console.log(err.message);
        throw new Error(err.message);
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

        const crawlRecursive = async () => {
            // If other crawlers finished the scraping
            let isCrawlingDone = await getHashValFromRedis(queueRedisHashKey, allQueueHashFields[1]);
            if (isCrawlingDone === 'true') return; // Exit condition
    
            const messages = await pollMessagesFromQueue(queueUrl);

            if (messages.length === 0) {
                await setHashStrValInRedis(queueRedisHashKey, allQueueHashFields[1], 'true');
                return; // Exit condition
            }
    
            for (let message of messages) {
                await processMessage(message, queueUrl, queueRedisHashKey, allQueueHashFields, maxPages, maxDepth);
            }
    
            await deleteMessagesFromQueue(queueUrl, messages);

            await crawlRecursive();
        }

        await crawlRecursive();
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