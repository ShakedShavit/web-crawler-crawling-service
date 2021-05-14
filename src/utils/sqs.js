const AWS = require('aws-sdk');

const sqs = new AWS.SQS({
    apiVersion: '2012-11-05',
    region: process.env.AWS_REGION
});

const sendMessageToQueue = async (QueueUrl, url, level, parentUrl, pageCounter) => {
    const workerId = process.env.WORKER_ID || 0;
    // workerId and pageCounter should suffice (but the more info the less it is likely that the id will be a duplicate)
    let MessageDeduplicationId = `${url.slice(4)},${level},${workerId},${pageCounter + 1}`;
    // Removes all non alphanumeric and punctuation characters
    MessageDeduplicationId = MessageDeduplicationId.replace(/[^.,\/#!$%\^&\*;:{}=\-_`~()\w]/g, '')
    let messageIdLen = MessageDeduplicationId.length;
    if (messageIdLen > 128) MessageDeduplicationId = MessageDeduplicationId.slice(messageIdLen - 128);
    console.log(MessageDeduplicationId);
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
            MessageGroupId: '0', // Every message should have the same group ID
            MessageDeduplicationId
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
            //     "MessageGroupId"
            // ],
            VisibilityTimeout: 120,
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

const deleteMessagesBatchFromQueue = async (QueueUrl, messages) => {
    try {
        const { BatchResultErrorEntry } = await sqs.deleteMessageBatch({
            QueueUrl,
            Entries: messages
        }).promise();

        return BatchResultErrorEntry || [];
    } catch (err) {
        console.log(err);
        throw new Error(err);
    }
}

module.exports = {
    sendMessageToQueue,
    pollMessagesFromQueue,
    deleteMessagesFromQueue,
    deleteMessagesBatchFromQueue
};