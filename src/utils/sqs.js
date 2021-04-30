const AWS = require('aws-sdk');

const sqs = new AWS.SQS({
    apiVersion: '2012-11-05',
    region: process.env.AWS_REGION
});

const sendMessageToQueue = async (QueueUrl, url, level, parentUrl) => {
    let MessageDeduplicationId = url;
    const urlLen = url.length;
    // const parentUrlLen = parentUrl.length;

    const msgDuplicationIdAddition = `,${level}`;
    const maxMsgIdLen = 128 - msgDuplicationIdAddition.length;

    if (urlLen > maxMsgIdLen) MessageDeduplicationId = url.slice(urlLen - maxMsgIdLen);
    MessageDeduplicationId += msgDuplicationIdAddition;

    // const msgDuplicationIdLen =  + parentUrlLen;

    // if (msgDuplicationIdLen <= maxMsgIdLen) MessageDeduplicationId = url + parentUrl + msgDuplicationIdAddition;
    // else {
    //     let extraLength = msgDuplicationIdLen - maxMsgIdLen;
    //     MessageDeduplicationId = url.slice((extraLength + extraLength % 2) / 2) + parentUrl.slice((extraLength - extraLength % 2) / 2) + msgDuplicationIdAddition;
    //     if (MessageDeduplicationId.length > 128) MessageDeduplicationId = MessageDeduplicationId.slice(MessageDeduplicationId.length - 128);
    // }

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
            MessageDeduplicationId,  // Required for FIFO queues
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
            AttributeNames: [
                "MessageGroupId"
            ],
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

module.exports = {
    sendMessageToQueue,
    pollMessagesFromQueue,
    deleteMessagesFromQueue
};