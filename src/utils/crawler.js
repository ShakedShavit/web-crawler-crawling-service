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

const getLinksAndAddPageToTree = async (message, queueRedisHashKey, queueTreeField, isMsgLastLevel) => {
    const messageUrl = message.url;
    const messageLevel = message.level;
    const parentUrl = message.parentUrl;
    try {
        // Get page from db, and if it doesn't exist than create it and save it on db
        let page = await getStrValFromRedis(messageUrl);
        if (!page) {
             page = await getPageInfo(messageUrl, !isMsgLastLevel);
             if (!isMsgLastLevel) await setStrWithExInRedis(messageUrl, JSON.stringify(page));
        } else page = JSON.parse(page);

        const newPageObj = {
            title: page.title,
            level: messageLevel,
            url: messageUrl
        };
        const treeJSON = await getHashValFromRedis(queueRedisHashKey, queueTreeField);

        const isUrlInTree = treeJSON.includes(`,"url":"${messageUrl}"`)
        if (!isUrlInTree && !isMsgLastLevel) newPageObj.children = [];

        const updatedTree = getUpdatedJsonTree(treeJSON, newPageObj, parentUrl);
        await setHashStrValInRedis(queueRedisHashKey, queueTreeField, updatedTree);

        return isUrlInTree ? [] : page.links;
    } catch (err) {
        console.log(err);
        return [];
    }
}

const processMessage = async (message, queueUrl, queueRedisHashKey, allQueueHashFields, maxPages, maxDepth) => {
    const messageUrl = message.url;
    const messageLevel = message.level;

    const isMsgLastLevel = !!maxDepth && messageLevel >= maxDepth - 1;
    try {
        const links = await getLinksAndAddPageToTree(message, queueRedisHashKey, allQueueHashFields[7], isMsgLastLevel);

        const linksLength = links.length;
        // If message level is equal to max level than don't send new messages
        if (!links || linksLength === 0 || isMsgLastLevel) {
            console.log('maxDepth:', maxDepth, 'messageLevel:', messageLevel, '128');
            return;
        }

        // If reached next level, stop to check if all other workers have reached it as well
        let currentLevel = await getHashValFromRedis(queueRedisHashKey, allQueueHashFields[2]);
        currentLevel = parseInt(currentLevel);
        if (messageLevel < currentLevel) console.log("\n\n\n\n\n\n\n\n***********\n\n\n\n\n\n\n\n");
        if (messageLevel > currentLevel) {
            console.log('\n reached next level \n');
            await incHashIntValInRedis(queueRedisHashKey, allQueueHashFields[6]);
            await waitForWorkersToReachNextLevel(queueRedisHashKey, [allQueueHashFields[0], allQueueHashFields[6]], allQueueHashFields[6]);

            // If no other worker has changes the current level in hash yet, than increment it (make it equal to message level)
            let newCurrentLevel = await getHashValFromRedis(queueRedisHashKey, allQueueHashFields[2]);
            if (parseInt(newCurrentLevel) !== messageLevel) await setHashStrValInRedis(queueRedisHashKey, allQueueHashFields[2], `${messageLevel}`);
            console.log('newCurrentLevel: ', newCurrentLevel, " is changing it:", parseInt(newCurrentLevel) !== messageLevel);
        }
        
        const linksLevel = messageLevel + 1;

        let sendMsgErrCounter = 0;
        let pageCounter;
        for (let i = 0; i < linksLength; i++) {
            let link = links[i];
            console.log(`\n\n${link}`);

            if ((i !== 0 && links.slice(0, i).includes(link))) continue;

            // If crawl limits have been reached
            if (!!maxPages) {
                pageCounter = await getHashValFromRedis(queueRedisHashKey, allQueueHashFields[3]);
                pageCounter = parseInt(pageCounter);

                if (pageCounter >= maxPages) {
                    console.log('pageCounter:', pageCounter, 'maxPages:', maxPages, '128');
                    break; // Exit
                }
            }
            
            try {
                await sendMessageToQueue(queueUrl, link, linksLevel, messageUrl, pageCounter);
            } catch (err) {
                if (++sendMsgErrCounter > 2 && linksLength > 2) throw new Error(err);
                continue; // Do not increment page counter
            }
            // Update page counter
            if (!!maxPages) await incHashIntValInRedis(queueRedisHashKey, allQueueHashFields[3]);
        }
    } catch (err) {
        console.log(err.message);
        return err.message;
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

        let wasQueueEmptyPrevPoll = false;
        let isCrawlingDone = 'false';
        do {
            // If other crawlers finished the scraping
            isCrawlingDone = await getHashValFromRedis(queueRedisHashKey, allQueueHashFields[1]);
            if (isCrawlingDone === 'true') break; // Exit condition
    
let date = new Date();
console.log('\n* ', date.getMinutes(), date.getSeconds(), ' *\n');

            const messages = await pollMessagesFromQueue(queueUrl);

            if (messages.length === 0) {
                // Might be that all the messages were polled (by other crawlers) but not processed yet (because they reached next level and are waiting for this one to update), if so, than increment the counter and re-try
                let workersMap = await getWrkCounterAndWrkReachedNextLvl(queueRedisHashKey, [allQueueHashFields[0], allQueueHashFields[6]]);
                if (!wasQueueEmptyPrevPoll && workersMap.workersCounter !== 1 && workersMap.workersReachedNextLevelCounter !== workersMap.workersCounter) {
                    await incHashIntValInRedis(queueRedisHashKey, allQueueHashFields[6]);
                    wasQueueEmptyPrevPoll = true;
                    continue;
                } else if (wasQueueEmptyPrevPoll && workersMap.workersCounter !== 1 && workersMap.workersReachedNextLevelCounter !== workersMap.workersCounter) continue;
                
                await setHashStrValInRedis(queueRedisHashKey, allQueueHashFields[1], 'true');
                break; // Exit condition
            }

            if (wasQueueEmptyPrevPoll) {
                await incHashIntValInRedis(queueRedisHashKey, allQueueHashFields[6], -1);
                wasQueueEmptyPrevPoll = false;
            }

            // Get object that holds the message info
            let messagesInfoArr = [];
            for (let message of messages) {
                messagesInfoArr.push({
                    url: message.Body,
                    level: parseInt(message.MessageAttributes.level.StringValue),
                    parentUrl: message.MessageAttributes.parentUrl.StringValue
                });
            }
            await deleteMessagesFromQueue(queueUrl, messages);

            for (let message of messagesInfoArr) {
                await processMessage(message, queueUrl, queueRedisHashKey, allQueueHashFields, maxPages, maxDepth);
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