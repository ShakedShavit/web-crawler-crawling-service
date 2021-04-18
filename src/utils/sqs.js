const AWS = require('aws-sdk');
const getAllLinksInPage = require('./cheerio');

const sqs = new AWS.SQS({
    apiVersion: '2012-11-05',
    region: process.env.AWS_REGION
});

const sendMessageToQueue = async (QueueUrl, url, level, parentUrl) => {
    try {
        const { MessageId } = await sqs.sendMessage({
            QueueUrl,
            MessageAttributes: {
                'level': {
                    DataType: 'Number',
                    StringValue: `${level}`
                },
                'parentUrl': {
                    DataType: 'String',
                    StringValue: `${parentUrl}`
                }
            },
            MessageBody: url,
            // MessageDeduplicationId: `${url}`,  // Required for FIFO queues
            MessageGroupId: `${level}`  // Required for FIFO queues
        }).promise();

        return MessageId;
    } catch (err) {
        console.log(err);
        throw new Error(err.message);
    }
}

const pollMessagesFromQueue = async (QueueUrl) => {
    try {
        const { Messages } = await sqs.receiveMessage({
            QueueUrl,
            MaxNumberOfMessages: 10,
            MessageAttributeNames: [
                "All"
            ],
            // AttributeNames: [
            //     "All"
            // ],
            VisibilityTimeout: 30,
            WaitTimeSeconds: 10
        }).promise();

        return Messages || [];
    } catch (err) {
        console.log(err);
        throw new Error(err);
    }
}

const deleteMessagesFromQueue = async (QueueUrl, messages) => {
    try {
        const messagesDeletionFuncs = messages.map((message) => {
            return sqs.deleteMessage({
                QueueUrl,
                ReceiptHandle: message.ReceiptHandle
            }).promise();
        });
        Promise.allSettled(messagesDeletionFuncs)
            .then(data => console.log(data))
            .catch(err => { throw new Error(err) });
    } catch (err) {
        console.log(err);
        throw new Error(err);
    }
}

const deleteQueue = async (QueueUrl) => {
    try {
        await sqs.deleteQueue({ QueueUrl }).promise();
        console.log('deleted');
    } catch (err) {
        throw new Error(err.message);
    }
}


let receiveMessagesProcessResolved = { isTrue: true }; //Has to be object (instead of simple bool value)
let messagesCounter;
let urlArr = [];

const handleMessagesFromQueue = async (queueUrl, maxDepth, maxPages, clearSearchInterval) => {
    receiveMessagesProcessResolved.isTrue = false;
    try {
        const messages = await pollMessagesFromQueue(queueUrl);

        // Is search complete
        if (messages.length === 0) {
            await clearSearchInterval();
            console.log('Search Complete');
            return;
        }

        console.log('new poll batch');

        for (let message of messages) {
            if (receiveMessagesProcessResolved.isTrue) break; // Queue deleted before processing finished

            console.log();
            console.log('message');

            let currentMessageLevel = parseInt(message.MessageAttributes.level.StringValue);
            if (currentMessageLevel >= maxDepth) {
                console.log('Reached max depth');
                break;
            }
            if (messagesCounter >= maxPages) {
                console.log('Reached max pages');
                break;
            }

            if (urlArr.includes(message.Body)) break;
            urlArr.push(message.Body);

            let shouldSaveSiteOnRedis = false;

            let links = await getSiteLinksFromRedis(message.Body); //
            if (links == undefined || links.length === 0) {
                shouldSaveSiteOnRedis = true;
                links = await getAllLinksInPage(message.Body);
            }

            for (let link of links) {
                if (receiveMessagesProcessResolved.isTrue) {
                    shouldSaveSiteOnRedis = false;
                    break; // Queue deleted before processing finished
                }
                
                await sendMessageToQueue(queueUrl, link, currentMessageLevel + 1);
                
                new Site(link, currentMessageLevel + 1, message.Body);

                messagesCounter++;

                if (messagesCounter >= maxPages) {
                    shouldSaveSiteOnRedis = false;
                    break;
                }
            }

            if (shouldSaveSiteOnRedis) {
                await setNewSiteInRedis(message.Body, links); //
            }
        }

        if (receiveMessagesProcessResolved.isTrue) return; // Queue deleted before processing finished
        
        await deleteMessagesFromQueue(queueUrl, messages);

        receiveMessagesProcessResolved.isTrue = true;

        return false;
    } catch (err) {
        console.log(err);
        receiveMessagesProcessResolved.isTrue = true;
    }
}

module.exports = {
    sqs,
    sendMessageToQueue,
    deleteQueue,
    handleMessagesFromQueue,
    receiveMessagesProcessResolved
};