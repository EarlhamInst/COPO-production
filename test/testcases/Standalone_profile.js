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
            height: 555
        })
    }
    {
        const targetPage = page;
        const promises = [];
        const startWaitingForEvents = () => {
            promises.push(targetPage.waitForNavigation());
        }
        startWaitingForEvents();
        await targetPage.goto(process.env.COPO_WEB_URL + '/copo');
        await Promise.all(promises);
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria() >>>> ::-p-aria([role=\\"generic\\"])'),
            targetPage.locator('div.global-page-title button.primary > i'),
            targetPage.locator('::-p-xpath(/html/body/div[5]/div/div[1]/div[2]/div[2]/div[1]/span[2]/button[2]/i)'),
            targetPage.locator(':scope >>> div.global-page-title button.primary > i')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 3.5,
                y: 6.359375,
              },
            });
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Title required )'),
            targetPage.locator('#copo\\.profile\\.title'),
            targetPage.locator('::-p-xpath(//*[@id=\\"copo.profile.title\\"])'),
            targetPage.locator(':scope >>> #copo\\.profile\\.title')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 103,
                y: 12.015625,
              },
            });
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Title required )'),
            targetPage.locator('#copo\\.profile\\.title'),
            targetPage.locator('::-p-xpath(//*[@id=\\"copo.profile.title\\"])'),
            targetPage.locator(':scope >>> #copo\\.profile\\.title')
        ])
            .setTimeout(timeout)
            .fill('T');
    }
    {
        const targetPage = page;
        await targetPage.keyboard.up('t');
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Title required )'),
            targetPage.locator('#copo\\.profile\\.title'),
            targetPage.locator('::-p-xpath(//*[@id=\\"copo.profile.title\\"])'),
            targetPage.locator(':scope >>> #copo\\.profile\\.title')
        ])
            .setTimeout(timeout)
            .fill('test S');
    }
    {
        const targetPage = page;
        await targetPage.keyboard.up('s');
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Title required )'),
            targetPage.locator('#copo\\.profile\\.title'),
            targetPage.locator('::-p-xpath(//*[@id=\\"copo.profile.title\\"])'),
            targetPage.locator(':scope >>> #copo\\.profile\\.title')
        ])
            .setTimeout(timeout)
            .fill('test Standalone profile');
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Description required )'),
            targetPage.locator('#copo\\.profile\\.description'),
            targetPage.locator('::-p-xpath(//*[@id=\\"copo.profile.description\\"])'),
            targetPage.locator(':scope >>> #copo\\.profile\\.description')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 58,
                y: 30.0234375,
              },
            });
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Description required )'),
            targetPage.locator('#copo\\.profile\\.description'),
            targetPage.locator('::-p-xpath(//*[@id=\\"copo.profile.description\\"])'),
            targetPage.locator(':scope >>> #copo\\.profile\\.description')
        ])
            .setTimeout(timeout)
            .fill('It is S');
    }
    {
        const targetPage = page;
        await targetPage.keyboard.up('s');
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Description required )'),
            targetPage.locator('#copo\\.profile\\.description'),
            targetPage.locator('::-p-xpath(//*[@id=\\"copo.profile.description\\"])'),
            targetPage.locator(':scope >>> #copo\\.profile\\.description')
        ])
            .setTimeout(timeout)
            .fill('It is Standalone profile');
    }
    {
        const targetPage = page;
        const promises = [];
        const startWaitingForEvents = () => {
            promises.push(targetPage.waitForNavigation());
        }
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria( Save)'),
            targetPage.locator('#btnFormSave'),
            targetPage.locator('::-p-xpath(//*[@id=\\"btnFormSave\\"])'),
            targetPage.locator(':scope >>> #btnFormSave'),
            targetPage.locator('::-p-text(Save)')
        ])
            .setTimeout(timeout)
            .on('action', () => startWaitingForEvents())
            .click({
              offset: {
                x: 40.359375,
                y: 12.03125,
              },
            });
        await Promise.all(promises);
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('#copo_profiles_table > div:nth-of-type(1) div.panel-heading i'),
            targetPage.locator('::-p-xpath(//*[@id=\\"ellipsisID\\"])'),
            targetPage.locator(':scope >>> #copo_profiles_table > div:nth-of-type(1) div.panel-heading i')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 2.265625,
                y: 8.7265625,
              },
            });
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria( Edit)'),
            targetPage.locator('#editProfileBtn'),
            targetPage.locator('::-p-xpath(//*[@id=\\"editProfileBtn\\"])'),
            targetPage.locator(':scope >>> #editProfileBtn')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 15.015625,
                y: 17.7265625,
              },
            });
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Description required )'),
            targetPage.locator('#copo\\.profile\\.description'),
            targetPage.locator('::-p-xpath(//*[@id=\\"copo.profile.description\\"])'),
            targetPage.locator(':scope >>> #copo\\.profile\\.description')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 163,
                y: 24.0234375,
              },
            });
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Description required )'),
            targetPage.locator('#copo\\.profile\\.description'),
            targetPage.locator('::-p-xpath(//*[@id=\\"copo.profile.description\\"])'),
            targetPage.locator(':scope >>> #copo\\.profile\\.description')
        ])
            .setTimeout(timeout)
            .fill('It is Standalone profile.  It is an updated');
    }
    {
        const targetPage = page;
        const promises = [];
        const startWaitingForEvents = () => {
            promises.push(targetPage.waitForNavigation());
        }
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria( Save)'),
            targetPage.locator('#btnFormSave'),
            targetPage.locator('::-p-xpath(//*[@id=\\"btnFormSave\\"])'),
            targetPage.locator(':scope >>> #btnFormSave'),
            targetPage.locator('::-p-text(Save)')
        ])
            .setTimeout(timeout)
            .on('action', () => startWaitingForEvents())
            .click({
              offset: {
                x: 36.359375,
                y: 13.03125,
              },
            });
        await Promise.all(promises);
    }

})().catch(err => {
    console.error(err);
    process.exit(1);
}).finally(() => {
    if (browser != null)
        browser.disconnect();
})
