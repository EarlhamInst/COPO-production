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
    const timeout = 10000;
    page.setDefaultTimeout(timeout);
    {
        const targetPage = page;
        await targetPage.setViewport({
            width: 1512,
            height: 417
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
                x: 68,
                y: 21.7421875,
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
            targetPage.locator('#copo_profiles_table > div:nth-of-type(1) a:nth-of-type(8)'),
            targetPage.locator('::-p-xpath(//*[starts-with(@id, \\"menu_\\")]/div[1]/div/a[8])'),
            targetPage.locator(':scope >>> #copo_profiles_table > div:nth-of-type(1) a:nth-of-type(8)')
        ])
            .setTimeout(timeout)
            .on('action', () => startWaitingForEvents())
            .click({
              offset: {
                x: 66,
                y: 6.7421875,
              },
            });
        await Promise.all(promises);
    }
    
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria() >>>> ::-p-aria([role=\\"generic\\"])'),
            targetPage.locator('div.container button.new-local-file > i'),
            targetPage.locator('::-p-xpath(/html/body/div[5]/div/div[1]/div[2]/div[2]/div[1]/span[2]/button[1]/i)'),
            targetPage.locator(':scope >>> div.container button.new-local-file > i')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 9.2734375,
                y: 9.359375,
              },
            });
    }
    
    {
        const targetPage = page;
        await puppeteer.Locator.race([
            targetPage.locator('::-p-aria(Upload)'),
            targetPage.locator('#upload_local_files_button'),
            targetPage.locator('::-p-xpath(//*[@id=\\"upload_local_files_button\\"])'),
            targetPage.locator(':scope >>> #upload_local_files_button')
        ])
            .setTimeout(timeout)
            .click({
              offset: {
                x: 25.8671875,
                y: 17.5859375,
              },
            });
    }
    
  
    {
      const targetPage = page;  
      await targetPage.waitForSelector('input[id=file]')
      const inputUploadHandle = await targetPage.$('input[id=file]')

        // prepare file to upload, I'm using test_to_upload.jpg file on same directory as this script
      // Photo by Ave Calvar Martinez from Pexels https://www.pexels.com/photo/lighthouse-3361704/
   
      let fileToUploads = ['/usr/src/app/workspace/small1_r1.fastq.gz',
          '/usr/src/app/workspace/small1_r2.fastq.gz',
          '/usr/src/app/workspace/small2_r1.fastq.gz',
          '/usr/src/app/workspace/small2_r2.fastq.gz'
        ];
     
      //let fileToUpload = '/usr/src/app/workspace/small1_r1.fastq.gz';          
      // Sets the value of the file input to fileToUpload
      inputUploadHandle.uploadFile(...fileToUploads);
      
    }
    
    {
      const targetPage = page;
      await  targetPage.locator('::-p-xpath(//*[@id=\\"page_alert_panel\\"]/div/span[contains(text(), \\"have been uploaded!\\")])').setTimeout(120000).wait()
      console.info("done")
    }
  
  })().catch(err => {s
    console.error(err);
    process.exit(1);
  }).finally(() => {
      if (browser != null)
          browser.disconnect();
  })
