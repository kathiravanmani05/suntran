import scrapy
import json
import pandas as pd

class MySpider(scrapy.Spider):
    name = 'ids'
    allowed_domains = ['api-locations.suntransfers.com']
    start_urls = []  # We'll populate this dynamically

    def __init__(self, *args, **kwargs):
        super(MySpider, self).__init__(*args, **kwargs)
        
        # Read the Excel file
        excel_file_path = r'G:\My Drive\2.Kathir\Python_Scripts\my_project\Airport_taxi\sun_mis.xlsx'
        self.df = pd.read_excel(excel_file_path)
        
        # Generate the start URLs
        for i in self.df.index[0:3]:
            ids1 = self.df.loc[i]['Route start']
            ids2 = self.df.loc[i]['Route%20dest']
            ids3 = self.df.loc[i]['Route dest']
            url = f"https://api-locations.suntransfers.com/gateways/14208/destinations?search_term={ids2}&max_results=5"
            self.start_urls.append({'url': url, 'ids1': ids1, 'ids3': ids3})

    def start_requests(self):
        headers = {
            'accept': '*/*',
            'accept-language': 'en',
            'origin': 'https://booking.suntransfers.com',
            'priority': 'u=1, i',
            'referer': 'https://booking.suntransfers.com/',
            'sec-ch-ua': '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
        }
        
        for url_info in self.start_urls:
            yield scrapy.Request(url_info['url'], headers=headers, callback=self.parse, meta={'ids1': url_info['ids1'], 'ids3': url_info['ids3']})

    def parse(self, response):
        ids1 = response.meta['ids1']
        ids3 = response.meta['ids3']
        
        data = json.loads(response.text)
        locations = data.get('locations', [])
        
        for location in locations:
            yield {
                'ids1': ids1,
                'ids3': ids3,
                'id': location['id'],
                'code': location['code'],
                'name': location['name']
            }
