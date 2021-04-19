const express = require('express');
const cors = require('cors');
const { getLastElOfListAndPushItToDestListInRedis } = require('./utils/redis');
const crawl = require('./utils/crawler');

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

let queueListKey = 'queueUrlList';

startCrawlingProcess = () => {
    getQueueUrlFromRedis(queueListKey)
    .then((queueUrl) => {
        crawl(queueUrl).then(() => {
            startCrawlingProcess();
        }).catch((err) => {
            startCrawlingProcess();
        }); // if fails or success restart
    })
    .catch((err) => {
        console.log(err, '28');
        // If it fails or the list is empty, wait and retry
        setTimeout(() => {
            startCrawlingProcess();
        }, 1500);
    });
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

module.exports = startCrawlingProcess;