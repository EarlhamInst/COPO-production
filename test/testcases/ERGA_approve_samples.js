const puppeteer = require('puppeteer'); // v20.7.4 or later

const url =  process.env.BROWSERLESS_HTTP_URL + '/sessions' ;
const get_browser_id  = async () => {
  const res = await fetch(url);
  const data = await res.json();
  return data[0].browserId;
};

let browser = null;

(async () => {
    browser_id = await get_browser_id();
    browser = await puppeteer.connect({ browserWSEndpoint: process.env.BROWSERLESS_WS_URL + '/devtools/browser/' + browser_id + '?--user-data-dir=/tmp/puppeteer' });
    const page = await browser.newPage();
    const timeout = 5000;
    page.setDefaultTimeout(timeout);

    {
        const targetPage = page;
        await targetPage.setViewport({
            width: 1512,
            height: 533
        })
    }
    {
        const targetPage = page;
        const promises = [];
        const startWaitingForEvents = () => {
            promises.push(targetPage.waitForNavigation());
        }
        startWaitingForEvents();
        await targetPage.goto(process.env.COPO_WEB_URL+"/copo");
        await Promise.all(promises);
    }
    {
        const targetPage = page;
        const promises = [];
        const startWaitingForEvents = () => {
            promises.push(targetPage.waitForNavigation());
        }
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria()'),
            targetPage.locator('#accept_reject_shortcut'),
            targetPage.locator('::-p-xpath(//*[@id=\\"accept_reject_shortcut\\"])'),
            targetPage.locator(':scope >>> #accept_reject_shortcut')
        ])
            .setTimeout(timeout)
            .on('action', () => startWaitingForEvents())
            .click({
              offset: {
                x: 36.78125,
                y: 16,
              },
            });
        await Promise.all(promises);
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Type of manifest to accept/reject samples for)'),
            targetPage.locator('#group_id'),
            targetPage.locator('::-p-xpath(//*[@id=\\"group_id\\"])'),
            targetPage.locator(':scope >>> #group_id')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 49.2734375,
                y: 15,
              },
            });
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Type of manifest to accept/reject samples for)'),
            targetPage.locator('#group_id'),
            targetPage.locator('::-p-xpath(//*[@id=\\"group_id\\"])'),
            targetPage.locator(':scope >>> #group_id')
        ])
            .setTimeout(timeout)
            .fill('erga');
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Select all visible) >>>> ::-p-aria([role=\\"generic\\"])'),
            targetPage.locator('button.select-all > span'),
            targetPage.locator('::-p-xpath(//*[@id=\\"edit-buttons\\"]/button[1]/span)'),
            targetPage.locator(':scope >>> button.select-all > span'),
            targetPage.locator('::-p-text(Select all visible)')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 38.078125,
                y: 10.703125,
              },
            });
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria( Accept)'),
            targetPage.locator('button.positive'),
            targetPage.locator('::-p-xpath(//*[@id=\\"accept_reject_button\\"]/button[2])'),
            targetPage.locator(':scope >>> button.positive')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 77.1015625,
                y: 17.203125,
              },
            });
    }   
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Okay)'),
            targetPage.locator('#code_okay'),
            targetPage.locator('::-p-xpath(//*[@id=\\"code_okay\\"])'),
            targetPage.locator(':scope >>> #code_okay'),
            targetPage.locator('::-p-text(Okay)')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 33.328125,
                y: 15.0234375,
              },
            });
    }
    {
      const targetPage = page;
      await  targetPage.locator('::-p-xpath(//*[@id=\\"dtol_sample_info\\" and contains(@value,\\"Bioimage was not submitted\\")])').setTimeout(120000).wait()
      console.info("done")
     }
     /*
    {
      const targetPage = page;
      targetPage.waitForXPath('//*[@id="dtol_sample_info" and contains(text(),"Bioimage not submitted")]', {timeout: 120000})
      .then(() => console.log('Sample accessed'));
    }*/
 
})().catch(err => {
    console.error(err);
    process.exit(1);
}).finally(() => {
    if (browser != null)
        browser.disconnect();
})

