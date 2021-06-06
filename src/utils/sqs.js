const AWS = require('aws-sdk');

const sqs = new AWS.SQS({
    apiVersion: '2012-11-05',
    region: process.env.AWS_REGION
});
const sendMessageToQueue = (QueueUrl, url, level, parentUrl) => {
    let MessageDeduplicationId = `${url.slice(4)}${level}${Math.random().toString(36).substr(2, 9)}`;
    // Removes all non alphanumeric and punctuation characters
    MessageDeduplicationId = MessageDeduplicationId.replace(/[^.,\/#!$%\^&\*;:{}=\-_`~()\w]/g, '');
    let messageIdLen = MessageDeduplicationId.length;
    if (messageIdLen > 128) MessageDeduplicationId = MessageDeduplicationId.slice(messageIdLen - 128);
    try {
        return sqs.sendMessage({
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
    } catch (err) {
        console.log(err);
        throw new Error(err.message);
    }
}

const pollMessagesFromQueue = async (QueueUrl, MaxNumberOfMessages = 10) => {
    try {
        const { Messages } = await sqs.receiveMessage({
            QueueUrl,
            MaxNumberOfMessages,
            MessageAttributeNames: [
                "All"
            ],
            // AttributeNames: [
            //     "MessageGroupId"
            // ],
            VisibilityTimeout: 120,
            WaitTimeSeconds: 5
        }).promise();

        return Messages || [];
    } catch (err) {
        console.log(err.message);
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

const deleteMessagesBatchFromQueue = (QueueUrl, messages) => {
    try {
        return sqs.deleteMessageBatch({
            QueueUrl,
            Entries: messages
        }).promise();
        // .then(({ BatchResultErrorEntry }) => { return BatchResultErrorEntry })
        // .catch(err => { throw new Error(err) });

        // return BatchResultErrorEntry || [];
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