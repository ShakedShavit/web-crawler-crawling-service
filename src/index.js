const express = require('express');
const cors = require('cors');
const { getLastElOfListAndPushItToDestListInRedis } = require('./utils/redis');
const crawl = require('./crawler/crawler');

const port = process.env.PORT || 5001;

const app = express();
app.use(cors());
app.use(express.json());

const getQueueUrlFromRedis = (queueUrlListKey) => {
    return new Promise((resolve, reject) => {
        getLastElOfListAndPushItToDestListInRedis(queueUrlListKey)
        .then((res) => {
            if (!res) reject(res);
            resolve(res);
        })
        .catch((err) => {
            reject(err);
        });
    });
}

const startCrawlingProcess = async () => {
    const queueListKey = 'queue-url-list';
    let crawlInfo = {};
    let queueUrl;
    while (true) {
        try {
            queueUrl = await getQueueUrlFromRedis(queueListKey);
        } catch (err) {
            await new Promise(resolve => setTimeout(resolve, 2000));
            continue;
        }

        crawlInfo = {
            queueUrl,
            queueHashFields: [
                'isCrawlingDone',
                'pageCounter',
                'maxPages',
                'maxDepth',
                'tree'
            ],
            hasReachedMaxLevel: false,
            hasReachedMaxPages: false,
            
            get queueRedisHashKey() { return `queue-workers:${this.queueUrl}`; },
            get treeRedisListKey() { return `pages-list:${this.queueUrl}`; },
            get currProcessingRedisListKey() { return `curr-processes-list:${this.queueUrl}`; },
            get hasReachedLimit() { return this.hasReachedMaxLevel || this.hasReachedMaxPages; }
        };

        try {
            console.time('a');
            console.log(crawlInfo);
            await crawl(crawlInfo);
            console.timeEnd('a');
            await new Promise(resolve => setTimeout(resolve, 2000));
        } catch (err) {
            console.timeEnd('a');
            console.log(err.message, err, '64');
            continue;
        }
    }
}

startCrawlingProcess();

// app.post('start-scraping', (req, res) => {
//     try {
//         if (!req.body.queueUrl) {
//             return res.status(400).send({
//                 status: 400,
//                 message: 'missing sqs queue url'
//             });
//         }

//         res.status(200).send();
//     } catch (err) {
//         console.log(err);
//         res.status(400).send(err.message);
//     }
// });

// app.post('stop-scraping', (req, res) => {
//     try {
//         if (!req.body.queueUrl) {
//             return res.status(400).send({
//                 status: 400,
//                 message: 'missing sqs queue url'
//             });
//         }
//     } catch (err) {
//         console.log(err);
//         res.status(400).send(err.message);
//     }

// });

app.listen(port, () => {
    console.log(`Server connected to port: ${port}`);
});