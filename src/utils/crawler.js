const startCrawlingProcess = require('../index');
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

//  data about particular search, shared by all workers through redis
// queueUrl-worker: {                       
//      workersCounter: number              
//      num of workers currently working on it (when worker starts crawling this url increment this). defaults to 0

//      isCrawlingDone: boolean             
//      is crawling done (turn true when one of the workers poll zero messages). defaults to false

//      currentLevel: number
//      the current search level (only when all the workers surpass this the workers can continue). defaults to 0

//      pageCounter: number
//      the current search page (for the maxLevel)

//      maxPages: number
//      max search pages, is set in the REST API

//      maxLevel: number
//      max search levels, is set in the REST API

//      workersReachedNextLevelCounter: number
//      increment this when a worker reaches currentLevel + 1, and stop the workers process until it finishes. defaults and resets to 0 (each time this reaches the workersCounter)

//      tree: JSON
//      the entire tree of the queueUrl is saved (in the form of JSON)
//}



const waitForWorkersToReachNextLevel = async (hashKey, workerCountersFieldsArr, currentLevelField) => {
    const [workersCounter, workersReachedNextLevelCounter] = await getHashValuesFromRedis(hashKey, workerCountersFieldsArr);
    if (workersReachedNextLevelCounter !== workersCounter) {
        await setTimeout(() => {}, 1000);
        await waitForWorkersToReachNextLevel(hashKey, workerCountersFieldsArr, currentLevelField);
    }
    await incHashIntValInRedis(hashKey, currentLevelField);
    return;
}


// TODO: when starting the crawl process set the initial values in redis (pageCounter ...), nope, do tht in the main API instead

// TODO: if maxPages or maxLevel are not specified

// TODO: delete queue hash when deleting queue - IN MAIN API NOT HERE!!!!

const insertPageToTree = async (treeJSON, newPageJSON, parentUrl) => {
    let searchString = `${parentUrl}","children":[`;
    let insertIndex = treeJSON.indexOf(searchString) + searchString.length;
    if (treeJSON[insertIndex] === '{') newPageJSON += ',';
    return treeJSON.slice(0, insertIndex) + newPageJSON + treeJSON.slice(insertIndex);
}

const processMessage = async (message, queueUrl) => {
    try {
        const messageLevel = message.GroupId;
        const messageUrl = message.Body;
        const parentUrl = message.Attributes.parentUrl;

        // If reached next level, stop to check if all other workers have reached it as well
        const currentLevel = await getHashValFromRedis(queueRedisHashKey, allQueueHashFields[2]);
        if (messageLevel > currentLevel) {
            await incHashIntValInRedis(queueRedisHashKey, allQueueHashFields[6]);

            // Recursive func that stops when all other workers get to the next level
            await waitForWorkersToReachNextLevel(queueRedisHashKey, [allQueueHashFields[0], allQueueHashFields[6]], allQueueHashFields[2]);
        }

        // Get page from db, and if it doesn't exist than create it and save it on db
        let page = await getStrValFromRedis(messageUrl);
        if (!page) {
             page = await getPageInfo(messageUrl);
             await setStrWithExInRedis(messageUrl, JSON.stringify(page));
        } else {
             page = JSON.parse(page);
        }

        //#region Update tree in redis
        const treeJSON = await getHashValFromRedis(queueRedisHashKey, allQueueHashFields[7]);
        const newPageJSON = JSON.stringify({
            title: page.title,
            level: messageLevel,
            messageUrl,
            children: []
        });
        await setHashStrValInRedis(queueRedisHashKey, allQueueHashFields[7], insertPageToTree(treeJSON, newPageJSON, parentUrl));
        //#endregion

        // If messageUrl has already been processed for this queue than move to next message
        if (treeJSON.includes(messageUrl) || !page.links) return false;

        let pageCounter;
        if (!!maxPages) pageCounter = await getHashValFromRedis(queueRedisHashKey, allQueueHashFields[3]);

        for (let i = 0; i < page.links.length; i++) {
            let link = page.links[i];

            // If crawl limits have been reached
            if ((!!maxPages && pageCounter + i >= maxPages) || (!!maxDepth && messageLevel + 1 >= maxDepth)) return true;

            await sendMessageToQueue(queueUrl, link, messageLevel + 1, messageUrl);
        }
    } catch (err) {
        console.log(err.message);
        throw new Error(err.message);
    }
}



const crawl = async (queueUrl) => {
    const queueRedisHashKey = `${queueUrl}-workers`;
    const allQueueHashFields = ['workersCounter', 'isCrawlingDone', 'currentLevel', 'pageCounter', 'maxPages', 'maxLevel', 'workersReachedNextLevelCounter', 'tree'];

    try {
        let doesQueueHashExist = await doesKeyExistInRedis(queueRedisHashKey);
        if (!doesQueueHashExist) throw new Error(`${queueUrl}-workers does not exist in redis`);

        const [currentLevel, maxPages, maxLevel] = await getHashValuesFromRedis(queueRedisHashKey, [allQueueHashFields[2], allQueueHashFields[4], allQueueHashFields[5]]);
        if (currentLevel == null) throw new Error('current level in queueUrl-workers hash is null');

        const crawlRecursive = async () => {
            // If other crawlers finished the scraping
            let isCrawlingDone = await getHashValFromRedis(queueRedisHashKey, allQueueHashFields[1]);
            if (isCrawlingDone === 'true') {
                // TODO: restart interval to find new queue
                return; // Exit condition
            }
    
            const messages = await pollMessagesFromQueue(queueUrl);

            if (messages.length === 0) {
                await setHashStrValInRedis(queueRedisHashKey, allQueueHashFields[1], 'true');
                // TODO: restart interval to find new queue
                return; // Exit condition
            }
    
            for (let message of messages) {
                let hasCrawlReachedLimit = await processMessage(message, queueUrl, currentLevel, maxPages, maxLevel);
                if (hasCrawlReachedLimit) {
                    await setHashStrValInRedis(queueRedisHashKey, allQueueHashFields[1], 'true');
                    // TODO: restart interval to find new queue
                    return; // Exit condition
                }
            }
    
            await deleteMessagesFromQueue(queueUrl, messages);

            await crawlRecursive();
        }

        await crawlRecursive();
    } catch (err) {
        console.log(err);

        try {
            await incHashIntValInRedis(queueRedisHashKey, allQueueHashFields[0], -1);
            // TODO restart interval to find new queue
        } catch (error) {
            console.log(error);
            throw new Error({ err, error });
        }
        throw new Error(err);
    }
}

module.exports = crawl;