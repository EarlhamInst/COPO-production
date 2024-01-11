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
            height: 444
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
                x: 7.5,
                y: 5.359375,
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
            .fill('test ERGA profile');
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
                x: 19,
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
            .fill('It is ERGA profile');
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Profile Type )'),
            targetPage.locator('#copo\\.profile\\.type'),
            targetPage.locator('::-p-xpath(//*[@id=\\"copo.profile.type\\"])'),
            targetPage.locator(':scope >>> #copo\\.profile\\.type')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 106,
                y: 18.03125,
              },
            });
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Profile Type )'),
            targetPage.locator('#copo\\.profile\\.type'),
            targetPage.locator('::-p-xpath(//*[@id=\\"copo.profile.type\\"])'),
            targetPage.locator(':scope >>> #copo\\.profile\\.type')
        ])
            .setTimeout(timeout)
            .fill('European Reference Genome Atlas (ERGA)');
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Select sequencing centre)'),
            targetPage.locator('#\\35 66aa805-68f8-4cb3-b5ee-af540b6ed92b div:nth-of-type(5) input'),
            targetPage.locator('::-p-xpath(//*[@id=\\"566aa805-68f8-4cb3-b5ee-af540b6ed92b\\"]/div/div/div[2]/div/div/div/div[3]/div/div/form/div[5]/div/div/div/span/span[1]/span/ul/li/input)'),
            targetPage.locator(':scope >>> #\\35 66aa805-68f8-4cb3-b5ee-af540b6ed92b div:nth-of-type(5) input')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 91,
                y: 11.546875,
              },
            });
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(EARLHAM INSTITUTE[role=\\"treeitem\\"])'),
            targetPage.locator('::-p-xpath(//*[contains(@id, \\"-EI\\")])'),
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 95,
                y: 18.546875,
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
                x: 44.359375,
                y: 19.578125,
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
                y: 10.7265625,
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
                x: 16.015625,
                y: 16.7265625,
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
                x: 142,
                y: 20.0234375,
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
            .fill('It is ERGA profile. It is a updated');
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Select associated type\\(s\\))'),
            targetPage.locator('div:nth-of-type(4) input'),
            targetPage.locator(':scope >>> div:nth-of-type(4) input')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 65,
                y: 9.0390625,
              },
            });
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Biodiversity Genomics Europe \\(BGE\\)[role=\\"treeitem\\"])'),
            targetPage.locator('::-p-xpath(//*[contains(@id=, "-BGE\\"])'),
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 142,
                y: 12.046875,
              },
            });
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria( Save)'),
            targetPage.locator('#btnFormSave'),
            targetPage.locator('::-p-xpath(//*[@id=\\"btnFormSave\\"])'),
            targetPage.locator(':scope >>> #btnFormSave'),
            targetPage.locator('::-p-text(Save)')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 42.359375,
                y: 12.578125,
              },
            });
    }

})().catch(err => {
    console.error(err);
    process.exit(1);
}).finally(() => {
    if (browser != null)
        browser.disconnect();
})
