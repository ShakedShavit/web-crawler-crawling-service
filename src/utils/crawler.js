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
        const [currentLevel, maxPages, maxLevel] = await getHashValuesFromRedis(queueRedisHashKey, [allQueueHashFields[2], allQueueHashFields[4], allQueueHashFields[5]]);

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
        try {
            await incHashIntValInRedis(queueRedisHashKey, allQueueHashFields[0], -1);
        } catch (error) {
            console.log(error);
        }
        console.log(err);
        throw new Error(err.message);
    }
}



























const crawl = async (queueUrl) => {
    try {
        // TODO: if isCrawlingDone for this queue in redis is true exit
        // if (redis.get(crawlingDone + queueUrl)) return; // Exit condition

        const messages = await pollMessagesFromQueue(queueUrl);
        if (messages.length === 0) return; // Exit condition

        for (let message of messages) {
            // let currentLevel = message.attr.groupId;
            // let pageCounter = await redis.get(queueUrl + '-pageCounter');

            // TODO: if message counter attr matches maxPages than break and change crawlingDone in redis to true
            // if (pageCounter >== maxPages || currentLevel >== maxDepth) {
            //      await redis.set(crawlingDone + queueUrl, true));
            //      break;
            // }

            let url = message.Body;
            let parentUrl = message.attr.parentUrl;

            // TODO: look for page obj in redis, if it exists than continue
            // TODO: save page obj in redis ([key: url, value: JSON(pageObj)])
            // let page = await redis.get(url);
            // if (!page) {
            //      page = await getPageInfo();
            //      await redis.set(url, page);
            // }

            // TODO: add url (message) to queueUrl tree
            let treeJSON = await redis.get(queueUrl + '-tree');
            let newPageJSON = JSON.stringify({
                title: page.title,
                level: currentLevel,
                url,
                children: []
            });
            let searchString = `${parentUrl}","children":[`;
            let insertIndex = treeJSON.indexOf(searchString) + searchString.length;
            if (treeJSON[insertIndex] === '{') newPageJSON += ',';
            treeJSON = treeJSON.slice(0, insertIndex) + newPageJSON + treeJSON.slice(insertIndex);
            await redis.set(queueUrl + '-tree', treeJSON);


            // TODO: if url is in urlList don't send new links (messages)
            // let allUrls = await redis.get(queueUrl + '-urlList');
            // if (allUrls.includes(url)) continue;

            // TODO: add url to urlList
            // await redis.set(queueUrl + '-urlList', [...allUrls, url]);

            let didReachMaxPages = false;
            for (let i = 0; i < page.links.length; i++) {
                let link = page.links[i];

                // TODO: if pages counter plus link index is equal to max pages break
                // if (pageCounter + linkIndex + 1 >= =maxPages) {
                //     await redis.set(crawlingDone + queueUrl, true));
                //     didReachMaxPages = true;
                //     break;
                // }


                // TODO: send new message with the link and level
                // await sendMessageToQueue(queueUrl, link, url, currentLevel + 1);
            }
            // if (didReachMaxPages) break;
        }

        // TODO: delete messages in queue
        //await deleteMessagesFromQueue(queueUrl, messages);

        crawl();
    } catch (err) {
        console.log(err);
        throw new Error(err.message);
    }
}