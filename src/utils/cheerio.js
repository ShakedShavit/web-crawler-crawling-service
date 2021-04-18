const cheerio = require('cheerio');
const fetch = require('node-fetch');
const urlJoin = require('url-join');

const getPageInfo = async (url) => {
    const links = [];

    try {
        const res = await fetch(url);
        const html = await res.text();
        const $ = cheerio.load(html);
    
        const title = $('title').text();

        $('a').each((index, element) => {
            let hrefVal = $(element).attr('href');
            if (!hrefVal) return;
                
            if (hrefVal[0] === '/') { // This means that the path afterwards is added to the core site's url (instead of the current path)
                let endOfCoreSiteUrlIndex = url.indexOf('/', 8);
                let modifiedUrl = url;
                if (endOfCoreSiteUrlIndex !== -1) { // If the parent url does end with a - /
                    modifiedUrl = url.slice(0, endOfCoreSiteUrlIndex);
                }
                hrefVal = urlJoin(modifiedUrl, hrefVal);
            } else if (hrefVal.slice(0, 2) === './') {
                hrefVal = urlJoin(url, hrefVal);
                hrefVal = hrefVal.replace('/./', '/');
                hrefVal = hrefVal.replace('./', '/');
            }
            else if (hrefVal.slice(0, 4) !== 'http') return;
    
            links.push(hrefVal);
        });

        return { 'title': title, 'links': links };
    } catch (err) {
        console.log(err);
        throw new Error(err.message);
    }
}

module.exports = getPageInfo;