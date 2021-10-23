const { getElementsFromListInRedis } = require("./utils/redis");
const crawl = require("./crawler/crawler");

const getCrawlNameFromRedis = (crawlListKey) => {
    return new Promise((resolve, reject) => {
        getElementsFromListInRedis(crawlListKey, 0, 0)
            .then(([res]) => {
                if (!res) reject(res);
                resolve(res);
            })
            .catch((err) => {
                reject(err);
            });
    });
};

const startCrawlingProcess = async () => {
    const crawlListKey = "crawl-name-list";
    let crawlName;
    let crawlInfo = {};
    while (true) {
        try {
            crawlName = await getCrawlNameFromRedis(crawlListKey);
        } catch (err) {
            console.log("researching...");
            await new Promise((resolve) => setTimeout(resolve, 2000));
            continue;
        }

        crawlInfo = {
            crawlName,
            redisHashFields: [
                "isCrawlingDone",
                "currentLevel",
                "currQueueUrl",
                "nextQueueUrl",
                "nextLvlLinksLen",
            ],
            get crawlRedisHashKey() {
                return `workers:${this.crawlName}`;
            },
            get treeRedisListKey() {
                return `pages-list:${this.crawlName}`;
            },
        };

        try {
            console.time("CRAWL_DURATION");
            console.log(crawlInfo);
            await crawl(crawlInfo);
            console.timeEnd("CRAWL_DURATION");
            await new Promise((resolve) => setTimeout(resolve, 2000));
        } catch (err) {
            console.timeEnd("CRAWL_DURATION");
            console.log(err.message, err);
            continue;
        }
    }
};

startCrawlingProcess();
