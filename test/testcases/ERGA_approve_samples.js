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
      const timeout = 120000;
      const targetPage = page;
      await waitForElement({
          type: 'waitForElement',
          timeout: 30000,
          target: 'main',
          selectors: [
              [
                  'aria/[role="alert"]',
                  'aria/[role="paragraph"]'
              ],
              '#dtol_sample_info',
              'xpath///*[@id="dtol_sample_info" and contains(text(),"Bioimage not submitted")]',
              'pierce/#dtol_sample_info',
              'text/Bioimage not'
          ]
      }, targetPage, timeout);
  }

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
}).finally(() => {
    if (browser != null)
        browser.disconnect();
})

