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
    const browser = await puppeteer.connect({ browserWSEndpoint: process.env.BROWSERLESS_WS_URL + '/devtools/browser/' + browser_id });    
    const page = await browser.newPage();
    const timeout = 5000;
    page.setDefaultTimeout(timeout);

    {
        const targetPage = page;
        await targetPage.setViewport({
            width: 1900,
            height: 668
        })
    }
    {
        const targetPage = page;
        const promises = [];
        const startWaitingForEvents = () => {
            promises.push(targetPage.waitForNavigation());
        }
        startWaitingForEvents();
        await targetPage.goto(process.env.COPO_WEB_URL+'/admin/login/?next=/admin/');
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
        const promises = [];
        const startWaitingForEvents = () => {
            promises.push(targetPage.waitForNavigation());
        }
        await targetPage.keyboard.down('Enter');
        await Promise.all(promises);
    }
    {
        const targetPage = page;
        await targetPage.keyboard.up('Enter');
    }
    {
        const targetPage = page;
        const promises = [];
        const startWaitingForEvents = () => {
            promises.push(targetPage.waitForNavigation());
        }
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Users[role=\\"link\\"])'),
            targetPage.locator('tr.model-user > th > a'),
            targetPage.locator('::-p-xpath(//*[@id=\\"content-main\\"]/div[3]/table/tbody/tr[2]/th/a)'),
            targetPage.locator(':scope >>> tr.model-user > th > a'),
            targetPage.locator('::-p-text(Users)')
        ])
            .setTimeout(timeout)
            .on('action', () => startWaitingForEvents())
            .click({
              offset: {
                x: 27,
                y: 10,
              },
            });
        await Promise.all(promises);
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Search[role=\\"textbox\\"])'),
            targetPage.locator('#searchbar'),
            targetPage.locator('::-p-xpath(//*[@id=\\"searchbar\\"])'),
            targetPage.locator(':scope >>> #searchbar')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 54.203125,
                y: 4,
              },
            });
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Search[role=\\"textbox\\"])'),
            targetPage.locator('#searchbar'),
            targetPage.locator('::-p-xpath(//*[@id=\\"searchbar\\"])'),
            targetPage.locator(':scope >>> #searchbar')
        ])
            .setTimeout(timeout)
            .fill('debby');
    }
    {
        const targetPage = page;
        const promises = [];
        const startWaitingForEvents = () => {
            promises.push(targetPage.waitForNavigation());
        }
        await targetPage.keyboard.down('Enter');
        await Promise.all(promises);
    }
    {
        const targetPage = page;
        await targetPage.keyboard.up('Enter');
    }
    {
        const targetPage = page;
        const promises = [];
        const startWaitingForEvents = () => {
            promises.push(targetPage.waitForNavigation());
        }
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(debby[role=\\"link\\"])'),
            targetPage.locator('#content-start tr:nth-of-type(2) a'),
            targetPage.locator('::-p-xpath(//*[@id=\\"result_list\\"]/tbody/tr[2]/th/a)'),
            targetPage.locator(':scope >>> #content-start tr:nth-of-type(2) a')
        ])
            .setTimeout(timeout)
            .on('action', () => startWaitingForEvents())
            .click({
              offset: {
                x: 32,
                y: 6.609375,
              },
            });
        await Promise.all(promises);
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('#id_groups_add_all_link'),
            targetPage.locator('::-p-xpath(//*[@id=\\"id_groups_add_all_link\\"])'),
            targetPage.locator(':scope >>> #id_groups_add_all_link')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 46.546875,
                y: 10.40625,
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
            targetPage.locator('::-p-xpath(//*[@id=\\"user_form\\"]/div/div/input[1])'),
            targetPage.locator(':scope >>> input.default')
        ])
            .setTimeout(timeout)
            .on('action', () => startWaitingForEvents())
            .click({
              offset: {
                x: 35,
                y: 17.8125,
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
            targetPage.locator('::-p-aria(debby[role=\\"rowheader\\"]) >>>> ::-p-aria([role=\\"link\\"])'),
            targetPage.locator('#content-start tr:nth-of-type(2) a'),
            targetPage.locator('::-p-xpath(//*[@id=\\"result_list\\"]/tbody/tr[2]/th/a)'),
            targetPage.locator(':scope >>> #content-start tr:nth-of-type(2) a')
        ])
            .setTimeout(timeout)
            .on('action', () => startWaitingForEvents())
            .click({
              offset: {
                x: 34,
                y: 4.609375,
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
                'xpath///*[@id="id_groups_from"]/option'
            ],
            count: 0
        }, targetPage, timeout);
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(LOG OUT)'),
            targetPage.locator('#logout-form > button'),
            targetPage.locator('::-p-xpath(//*[@id=\\"logout-form\\"]/button)'),
            targetPage.locator(':scope >>> #logout-form > button'),
            targetPage.locator('::-p-text(Log out)')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 33.890625,
                y: 9.8125,
              },
            });
    }

    await browser.disconnect();

    async function waitForElement(step, frame, timeout) {
      const {
        count = 1,
        operator = '>=',
        visible = true,
        properties,
        attributes,
      } = step;
      const compFn = {
        '==': (a, b) => a === b,
        '>=': (a, b) => a >= b,
        '<=': (a, b) => a <= b,
      }[operator];
      await waitForFunction(async () => {
        const elements = await querySelectorsAll(step.selectors, frame);
        let result = compFn(elements.length, count);
        const elementsHandle = await frame.evaluateHandle((...elements) => {
          return elements;
        }, ...elements);
        await Promise.all(elements.map((element) => element.dispose()));
        if (result && (properties || attributes)) {
          result = await elementsHandle.evaluate(
            (elements, properties, attributes) => {
              for (const element of elements) {
                if (attributes) {
                  for (const [name, value] of Object.entries(attributes)) {
                    if (element.getAttribute(name) !== value) {
                      return false;
                    }
                  }
                }
                if (properties) {
                  if (!isDeepMatch(properties, element)) {
                    return false;
                  }
                }
              }
              return true;

              function isDeepMatch(a, b) {
                if (a === b) {
                  return true;
                }
                if ((a && !b) || (!a && b)) {
                  return false;
                }
                if (!(a instanceof Object) || !(b instanceof Object)) {
                  return false;
                }
                for (const [key, value] of Object.entries(a)) {
                  if (!isDeepMatch(value, b[key])) {
                    return false;
                  }
                }
                return true;
              }
            },
            properties,
            attributes
          );
        }
        await elementsHandle.dispose();
        return result === visible;
      }, timeout);
    }

    async function querySelectorsAll(selectors, frame) {
      for (const selector of selectors) {
        const result = await querySelectorAll(selector, frame);
        if (result.length) {
          return result;
        }
      }
      return [];
    }

    async function querySelectorAll(selector, frame) {
      if (!Array.isArray(selector)) {
        selector = [selector];
      }
      if (!selector.length) {
        throw new Error('Empty selector provided to querySelectorAll');
      }
      let elements = [];
      for (let i = 0; i < selector.length; i++) {
        const part = selector[i];
        if (i === 0) {
          elements = await frame.$$(part);
        } else {
          const tmpElements = elements;
          elements = [];
          for (const el of tmpElements) {
            elements.push(...(await el.$$(part)));
          }
        }
        if (elements.length === 0) {
          return [];
        }
        if (i < selector.length - 1) {
          const tmpElements = [];
          for (const el of elements) {
            const newEl = (await el.evaluateHandle(el => el.shadowRoot ? el.shadowRoot : el)).asElement();
            if (newEl) {
              tmpElements.push(newEl);
            }
          }
          elements = tmpElements;
        }
      }
      return elements;
    }

    async function waitForFunction(fn, timeout) {
      let isActive = true;
      const timeoutId = setTimeout(() => {
        isActive = false;
      }, timeout);
      while (isActive) {
        const result = await fn();
        if (result) {
          clearTimeout(timeoutId);
          return;
        }
        await new Promise(resolve => setTimeout(resolve, 100));
      }
      throw new Error('Timed out');
    }
})().catch(err => {
    console.error(err);
    process.exit(1);
});
