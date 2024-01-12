const puppeteer = require('puppeteer'); // v20.7.4 or later

const url =  process.env.BROWSERLESS_HTTP_URL + '/sessions' ;
const get_browser_id  = async () => {
  const res = await fetch(url);
  const data = await res.json();
  return data[0].browserId;
};

let browser = null;

(async () => {
    let browser_id = await get_browser_id();
    browser = await puppeteer.connect({ browserWSEndpoint: process.env.BROWSERLESS_WS_URL + '/devtools/browser/' + browser_id +  '?--user-data-dir=/tmp/puppeteer' });
    const page = await browser.newPage();
    const timeout = 10000;
    page.setDefaultTimeout(timeout);

    {
        const targetPage = page;
        await targetPage.setViewport({
            width: 1512,
            height: 566
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
            targetPage.locator('div.upward'),
            targetPage.locator('::-p-xpath(//*[starts-with(@id, \\"menu_\\")]/div[1])'),
            targetPage.locator(':scope >>> div.upward')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 43,
                y: 20.6015625,
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
            targetPage.locator('div.upward a:nth-of-type(6)'),
            targetPage.locator('::-p-xpath(//*[starts-with(@id, \\"menu_\\")]/div[1]/div/a[6])'),
            targetPage.locator(':scope >>> div.upward a:nth-of-type(6)')
        ])
            .setTimeout(timeout)
            .on('action', () => startWaitingForEvents())
            .click({
              offset: {
                x: 74,
                y: 20.6015625,
              },
            });
        await Promise.all(promises);
    }
    
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('div.global-page-title button.pink > i'),
            targetPage.locator('::-p-xpath(/html/body/div[5]/div/div[1]/div[2]/div[2]/div[1]/span[2]/button[4]/i)'),
            targetPage.locator(':scope >>> div.global-page-title button.pink > i')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 0.5703125,
                y: 10.359375,
              },
            });
    }
    
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Okay)'),
            targetPage.locator('::-p-xpath(//button[contains(.,\\"Okay\\")]'),
            targetPage.locator('::-p-text(Okay)')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 29.328125,
                y: 8.0546875,
              },
            });
    }
    
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('#upload_label'),
            targetPage.locator('::-p-xpath(//*[@id=\\"upload_label\\"])'),
            targetPage.locator(':scope >>> #upload_label'),
            targetPage.locator('::-p-text(Upload Sample)')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 137.4921875,
                y: 17.578125,
              },
            });
    }
    
   
    {
        const targetPage = page;
        //await puppeteer.Locator.race([
        //    targetPage.locator('#file'),
        //    targetPage.locator('::-p-xpath(//*[@id=\\"file\\"])'),
        //    targetPage.locator(':scope >>> #file')
        //]).setTimeout(timeout)
            await targetPage.waitForSelector('input[id=file]')
            const inputUploadHandle = await targetPage.$('input[id=file]')

             // prepare file to upload, I'm using test_to_upload.jpg file on same directory as this script
            // Photo by Ave Calvar Martinez from Pexels https://www.pexels.com/photo/lighthouse-3361704/
            let fileToUpload = '/usr/src/app/workspace/ERGA_SAMPLE_MANIFEST_v2.5_success_several_samples.xlsx';
            // Sets the value of the file input to fileToUpload
            inputUploadHandle.uploadFile(fileToUpload);
    }
   /* 
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Finish)'),
            targetPage.locator('#finish_button'),
            targetPage.locator('::-p-xpath(//*[@id=\\"finish_button\\"])'),
            targetPage.locator(':scope >>> #finish_button')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 59.1171875,
                y: 22.578125,
              },
            });
    }
    */
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Finish)'),
            targetPage.locator('#finish_button'),
            targetPage.locator('::-p-xpath(//*[@id=\\"finish_button\\"])'),
            targetPage.locator(':scope >>> #finish_button')
        ])
            .setTimeout(120000)
            .click({
              offset: {
                x: 28.2578125,
                y: 14.0625,
              },
            });
    }
    {
      const targetPage = page;
      await puppeteer.Locator.race([
          targetPage.locator('::-p-aria(Confirm)'),
          targetPage.locator('#confirmBtnID'),
          targetPage.locator('::-p-xpath(//*[@id=\\"confirmBtnID\\"])'),
          targetPage.locator(':scope >>> #confirmBtnID')
      ])
          .setTimeout(timeout)
          .click({
            offset: {
              x: 39.4453125,
              y: 14.015625,
            },
          });
   }

   {
    const timeout = 30000;
    const targetPage = page;
    await waitForElement({
        type: 'waitForElement',
        timeout: 30000,
        target: 'main',
        selectors: [
            'aria/Confirm',
            '#confirmBtnID',
            'xpath///*[@id="confirmBtnID"]',
            'pierce/#confirmBtnID'
        ],
        count: 0
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
