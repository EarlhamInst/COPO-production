const puppeteer = require('puppeteer'); // v20.7.4 or later
const url =  process.env.BROWSERLESS_HTTP_URL + '/sessions' ;

let browser = null;

const get_browser_id  = async () => {
    const res = await fetch(url);
    const data = await res.json();
    return data[0].browserId;
};

(async () => 
{
    let browser_id = await get_browser_id();
    let browserWSEndpoint = process.env.BROWSERLESS_WS_URL + '/devtools/browser/' + browser_id + '?--user-data-dir=/tmp/puppeteer';
    browser = await puppeteer.connect({ browserWSEndpoint:  browserWSEndpoint });
    const page = await browser.newPage();
    const timeout = 10000;
    page.setDefaultTimeout(timeout);
    {
        const targetPage = page;
        await targetPage.setViewport({
            width: 1920,
            height: 624
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
        const promises = [];
        const startWaitingForEvents = () => {
            promises.push(targetPage.waitForNavigation());
        }
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria( Sign in with Orcid.org) >>>> ::-p-aria([role=\\"generic\\"])'),
            targetPage.locator('span'),
            targetPage.locator('::-p-xpath(//*[@id=\\"login_orcid\\"]/a/span)'),
            targetPage.locator(':scope >>> span'),
            targetPage.locator('::-p-text(Sign in with)')
        ])
            .setTimeout(timeout)
            .on('action', () => startWaitingForEvents())
            .click({
              offset: {
                x: 66.671875,
                y: 7.875,
              },
            });
        await Promise.all(promises);
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Reject Unnecessary Cookies)'),
            targetPage.locator('#onetrust-reject-all-handler'),
            targetPage.locator('::-p-xpath(//*[@id=\\"onetrust-reject-all-handler\\"])'),
            targetPage.locator(':scope >>> #onetrust-reject-all-handler')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 299,
                y: 22.421875,
              },
            });
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Email or 16-digit ORCID iD)'),
            targetPage.locator('#username-input'),
            targetPage.locator('::-p-xpath(//*[@id=\\"username-input\\"])'),
            targetPage.locator(':scope >>> #username-input')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 174.234375,
                y: 4,
              },
            });
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Email or 16-digit ORCID iD)'),
            targetPage.locator('#username-input'),
            targetPage.locator('::-p-xpath(//*[@id=\\"username-input\\"])'),
            targetPage.locator(':scope >>> #username-input')
        ])
            .setTimeout(timeout)
            .fill(process.env.COPO_WEB_USER);
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Password)'),
            targetPage.locator('#password'),
            targetPage.locator('::-p-xpath(//*[@id=\\"password\\"])'),
            targetPage.locator(':scope >>> #password')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 157.234375,
                y: 3.671875,
              },
            });
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Password)'),
            targetPage.locator('#password'),
            targetPage.locator('::-p-xpath(//*[@id=\\"password\\"])'),
            targetPage.locator(':scope >>> #password')
        ])
            .setTimeout(timeout)
            .fill(process.env.COPO_WEB_USER_PW);
    }
    {
        const targetPage = page;
        const promises = [];
        const startWaitingForEvents = () => {
            promises.push(targetPage.waitForNavigation());
        }
        await puppeteer.Locator.race([
            targetPage.locator('app-form-sign-in span.mat-button-wrapper'),
            targetPage.locator('::-p-xpath(//*[@id=\\"signin-button\\"]/span[1])'),
            targetPage.locator(':scope >>> app-form-sign-in span.mat-button-wrapper'),
        ])
            .setTimeout(timeout)
            .on('action', () => startWaitingForEvents())
            .click({
              offset: {
                x: 23.578125,
                y: 9.34375,
              },
            });
        await Promise.all(promises);
    }
    {
        const targetPage = page;
        await waitForElement({
            type: 'waitForElement',
            target: 'main',
            selectors: [
                'aria/I agree, dismiss this dialog',
                '#acceptCookies',
                'xpath///*[@id="acceptCookies"]',
                'pierce/#acceptCookies',
                'text/I agree, dismiss\n'
            ]
        }, targetPage, timeout);
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(I agree, dismiss this dialog)'),
            targetPage.locator('#acceptCookies'),
            targetPage.locator('::-p-xpath(//*[@id=\\"acceptCookies\\"])'),
            targetPage.locator(':scope >>> #acceptCookies'),
            targetPage.locator('::-p-text(I agree, dismiss\n)')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 60.53125,
                y: 20.1875,
              },
            });
    }

    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Enter email address)'),
            targetPage.locator('#emaddres'),
            targetPage.locator('::-p-xpath(//*[@id=\\"emaddres\\"])'),
            targetPage.locator(':scope >>> #emaddres')
        ])
            .setTimeout(timeout)
            .fill('debby.ku@earlham.ac.uk');
    }

    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(I Agree)'),
            targetPage.locator('#gdpr_check'),
            targetPage.locator('::-p-xpath(//*[@id=\\"gdpr_check\\"])'),
            targetPage.locator(':scope >>> #gdpr_check')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 5,
                y: 11.078125,
              },
            });
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Submit)'),
            targetPage.locator('#submit'),
            targetPage.locator('::-p-xpath(//*[@id=\\"submit\\"])'),
            targetPage.locator(':scope >>> #submit')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 31.578125,
                y: 11.078125,
              },
            });
    }

    /*
    {
        const targetPage = page;
        await targetPage.setViewport({
            width: 1583,
            height: 212
        })
    }
    {
        const targetPage = page;
        const promises = [];
        const startWaitingForEvents = () => {
            promises.push(targetPage.waitForNavigation());
        }
        startWaitingForEvents();
        await targetPage.goto('http://copo-new.cyverseuk.org:8000/copo');
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
                x: 8.5,
                y: 5.359375,
              },
            });
    }
*/
    /*
    {
        const targetPage = page;
        const promises = [];
        const startWaitingForEvents = () => {
            promises.push(targetPage.waitForNavigation());
        }
        startWaitingForEvents();
        await targetPage.goto('http://copo-new.cyverseuk.org:8000/copo/auth/logout/');
        await Promise.all(promises);
    }
    {
        const targetPage = page;
        const promises = [];
        const startWaitingForEvents = () => {
            promises.push(targetPage.waitForNavigation());
        }
        startWaitingForEvents();
        await targetPage.goto('https://orcid.org/my-orcid');
        await Promise.all(promises);
    }*/

})().catch(err => {
    console.error(err);
    process.exit(1);
}).finally(() => {
    if (browser != null)
        browser.disconnect();
})
