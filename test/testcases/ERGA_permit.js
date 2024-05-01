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
    const timeout = 20000;
    page.setDefaultTimeout(timeout);

    {
        const targetPage = page;
        await targetPage.setViewport({
            width: 1512,
            height: 502
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
          targetPage.locator('::-p-xpath(//span[contains(text(), \\"'+ process.argv[1].toUpperCase() + '\\")]/ancestor::div[@class=\\"copo-records-panel\\"]//*[starts-with(@id, \\"menu_\\")]/div[1])'),
          targetPage.locator(':scope >>> div.upward')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 66,
                y: 29.625,
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
            targetPage.locator('::-p-xpath(//span[contains(text(), \\"'+ process.argv[1].toUpperCase() + '\\")]/ancestor::div[@class=\\"copo-records-panel\\"]//*[starts-with(@id, \\"menu_\\")]/div[1]/div/a[6])'),
            targetPage.locator(':scope >>> div.upward a:nth-of-type(6)')
        ])
            .setTimeout(timeout)
            .on('action', () => startWaitingForEvents())
            .click({
              offset: {
                x: 124,
                y: 15.625,
              },
            });
        await Promise.all(promises);
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria() >>>> ::-p-aria([role=\\"generic\\"])'),
            targetPage.locator('div.global-page-title button.pink > i'),
            targetPage.locator('::-p-xpath(/html/body/div[5]/div/div[1]/div[2]/div[2]/div[1]/span[2]/button[4]/i)'),
            targetPage.locator(':scope >>> div.global-page-title button.pink > i')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 10.546875,
                y: 11.671875,
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
                x: 19.328125,
                y: 5.109375,
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
                x: 85.484375,
                y: 17.578125,
              },
            });
    }
    {
      const targetPage = page;
      await targetPage.waitForSelector('input[id=file]')
      const inputUploadHandle = await targetPage.$('input[id=file]')
      let fileToUpload = '/usr/src/app/workspace/ERGA_SAMPLE_MANIFEST_with_permit_' + process.argv[1] + '.xlsx';
      // Sets the value of the file input to fileToUpload
      inputUploadHandle.uploadFile(fileToUpload);
  }

  
   {
    const targetPage = page;
    await  targetPage.locator('::-p-xpath(//*[@id=\\"sample_parse_table\\"]/tbody/tr)').setTimeout(120000).wait()
    console.info("upload manifest : done")
   }

    {
        const targetPage = page;
        await targetPage.waitForSelector('input[id=permits]')
        const inputUploadHandle = await targetPage.$('input[id=permits]')
        let fileToUpload = '/usr/src/app/workspace/test_permit.pdf';
        inputUploadHandle.uploadFile(fileToUpload);
    }

    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Confirm)'),
            targetPage.locator('#confirm_button'),
            targetPage.locator('::-p-xpath(//*[@id=\\"confirm_button\\"])'),
            targetPage.locator(':scope >>> #confirm_button'),
            targetPage.locator('::-p-text(Confirm)')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 36.109375,
                y: 11.578125,
              },
            });
    }
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Update)'),
            targetPage.locator('#updateBtnID'),
            targetPage.locator('::-p-xpath(//*[@id=\\"updateBtnID\\"])'),
            targetPage.locator(':scope >>> #updateBtnID')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 50.453125,
                y: 11.03125,
              },
            });
    }

    {
      const targetPage = page;
      await  targetPage.locator('::-p-xpath(//*[@id=\\"page_alert_panel\\"]/div/span[contains(text(), \\"Samples have been updated successfully\\")])').setTimeout(120000).wait()
      console.info("done")
     }

  })().catch(err => {
    console.error(err);
    process.exit(1);
}).finally(() => {
    if (browser != null)
        browser.disconnect();
})
