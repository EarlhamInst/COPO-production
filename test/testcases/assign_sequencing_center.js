const puppeteer = require('puppeteer'); // v20.7.4 or later
const url =  process.env.BROWSERLESS_HTTP_URL + '/sessions' ;

let browser = null;

const get_browser_id  = async () => {
    const res = await fetch(url);
    const data = await res.json();
    return data[data.length-1].browserId;
};

(async () => {
    browser_id = await get_browser_id();
    const browser = await puppeteer.connect({ browserWSEndpoint: process.env.BROWSERLESS_WS_URL + '/devtools/browser/' + browser_id  });    
    const page = await browser.newPage();
    const timeout = 5000;
    page.setDefaultTimeout(timeout);

    {
        const targetPage = page;
        await targetPage.setViewport({
            width: 1512,
            height: 684
        })
    }
    {
        const targetPage = page;
        const promises = [];
        const startWaitingForEvents = () => {
            promises.push(targetPage.waitForNavigation());
        }
        startWaitingForEvents();
        await targetPage.goto(process.env.COPO_WEB_URL + '/admin/login/?next=/admin/');
        await Promise.all(promises);
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Username:)'),
            targetPage.locator('#id_username'),
            targetPage.locator('::-p-xpath(//*[@id=\\"id_username\\"])'),
            targetPage.locator(':scope >>> #id_username')
        ])
            .setTimeout(timeout)
            .fill('admin');
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Password:)'),
            targetPage.locator('#id_password'),
            targetPage.locator('::-p-xpath(//*[@id=\\"id_password\\"])'),
            targetPage.locator(':scope >>> #id_password')
        ])
            .setTimeout(timeout)
            .fill('admin');
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Log in)'),
            targetPage.locator('div.submit-row > input'),
            targetPage.locator('::-p-xpath(//*[@id=\\"login-form\\"]/div[3]/input)'),
            targetPage.locator(':scope >>> div.submit-row > input'),
            targetPage.locator('::-p-text(Log in)')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 45.3359375,
                y: 28.5,
              },
            });
    }
    {
        const targetPage = page;
        const promises = [];
        const startWaitingForEvents = () => {
            promises.push(targetPage.waitForNavigation());
        }
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Sequencing centres[role=\\"link\\"])'),
            targetPage.locator('tr.model-sequencingcentre > th > a'),
            targetPage.locator('::-p-xpath(//*[@id=\\"nav-sidebar\\"]/div[4]/table/tbody/tr[2]/th/a)'),
            targetPage.locator(':scope >>> tr.model-sequencingcentre > th > a'),
            targetPage.locator('::-p-text(Sequencing centres)')
        ])
            .setTimeout(timeout)
            .on('action', () => startWaitingForEvents())
            .click({
              offset: {
                x: 110,
                y: 9,
              },
            });
        await Promise.all(promises);
    }
    {
        const targetPage = page;
        const promises = [];
        const startWaitingForEvents = () => {
            promises.push(targetPage.waitForNavigation());
        }
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(EI[role=\\"link\\"])'),
            targetPage.locator('tr:nth-of-type(19) a'),
            targetPage.locator('::-p-xpath(//*[@id=\\"result_list\\"]/tbody/tr[19]/th/a)'),
            targetPage.locator(':scope >>> tr:nth-of-type(19) a'),
            targetPage.locator('::-p-text(EI)')
        ])
            .setTimeout(timeout)
            .on('action', () => startWaitingForEvents())
            .click({
              offset: {
                x: 8,
                y: 6.6875,
              },
            });
        await Promise.all(promises);
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Users:)'),
            targetPage.locator('#id_users'),
            targetPage.locator('::-p-xpath(//*[@id=\\"id_users\\"])'),
            targetPage.locator(':scope >>> #id_users')
        ])
            .setTimeout(timeout)
            .fill('1');
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(debby)'),
             targetPage.locator('::-p-xpath(//*[@id=\\"id_users\\"]/option[normalize-space(.)=\"debby\"])'),
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 27,
                y: 3.8125,
              },
            });
    }
    {
        const targetPage = page;
        const promises = [];
        const startWaitingForEvents = () => {
            promises.push(targetPage.waitForNavigation());
        }
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Save)'),
            targetPage.locator('input.default'),
            targetPage.locator('::-p-xpath(//*[@id=\\"sequencingcentre_form\\"]/div/div/input[1])'),
            targetPage.locator(':scope >>> input.default')
        ])
            .setTimeout(timeout)
            .on('action', () => startWaitingForEvents())
            .click({
              offset: {
                x: 31,
                y: 19,
              },
            });
        await Promise.all(promises);
    }
    {
        const targetPage = page;
        const promises = [];
        const startWaitingForEvents = () => {
            promises.push(targetPage.waitForNavigation());
        }
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(LOG OUT)'),
            targetPage.locator('#logout-form > button'),
            targetPage.locator('::-p-xpath(//*[@id=\\"logout-form\\"]/button)'),
            targetPage.locator(':scope >>> #logout-form > button'),
            targetPage.locator('::-p-text(Log out)')
        ])
            .setTimeout(timeout)
            .on('action', () => startWaitingForEvents())
            .click({
              offset: {
                x: 27.890625,
                y: 3.5546875,
              },
            });
        await Promise.all(promises);
    }

    await browser.disconnect();

})().catch(err => {
    console.error(err);
    process.exit(1);
});
