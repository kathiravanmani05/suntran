
import scrapy,io
import pandas as pd
import json,requests,csv


class MyTransfersSpider(scrapy.Spider):
    name = 'mytransfers_spider'
    
    def start_requests(self):
        # Load data from Excel
        #df = pd.read_excel('input.xlsx')
        excel_url = "https://raw.githubusercontent.com/kathiravanmani05/suntran/proxies/input.xlsx"
        excel_data = requests.get(excel_url)
        df = pd.read_excel(io.BytesIO(excel_data.content))
        
        # Iterate over the specific range of rows
        for i in df.index:
            # Get the row data
            row_data = df.loc[i]
            Route_start = row_data['Route start']
            Route_dest = row_data['Route dest']
            start_lat = row_data['Start_lat']
            start_long = row_data['Start_long']
            dest_lat = row_data['dest_lat']
            dest_log = row_data['dest_long']
            
            url = f"https://www.mytransfers.com/api/list?adults=2&arrival_date=2024-08-27+10:00&arrival_lat={start_lat}&arrival_lng={start_long}&arrival_time=0000&children=0&client_api_key=&departure_date=2024-08-28+10:00&departure_lat={dest_lat}&departure_lng={dest_log}&departure_time=0000&destination={Route_dest}&infants=0&lang=en&origin={Route_start}&type=oneway"
            
            headers = {
                'accept': 'application/json, text/plain, */*',
                'accept-language': 'en-US,en;q=0.9',
                'cache-control': 'no-cache',
                'pragma': 'no-cache',
                'priority': 'u=1, i',
                'referer': 'https://www.mytransfers.com/en/search/?adults=2&transfer_type=oneway&from=Pula+Airport+%28PUY%29&to=Pore%C4%8D%2C+Croatia&arrival_date=2024-08-27+10%3A00&departure_date=2024-08-28+10%3A00&arrival_time=0000&departure_time=0000&arrival_lat=44.89751785483413&arrival_lng=13.922019901074123&departure_lat=45.2268187&departure_lng=13.5944023&arrival_type=airport&departure_type=address',
                'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
            }
            
            # Pass the necessary information via meta for the parse method
            meta_data = {
                'Route_start': Route_start,
                'Route_dest': Route_dest,
                'start_lat':start_lat
                
            }
            
            # Yield a request
            yield scrapy.Request(url, headers=headers, callback=self.parse, meta=meta_data)
    
    def parse(self, response):
        
        Route_start = response.meta['Route_start']
        Route_dest = response.meta['Route_dest']
        start_lat = response.meta['start_lat']
        if start_lat is None :
                raise ValueError("All passenger prices are empty")
        
        try:
            datas = json.loads(response.text)
            x_paxs = {i: [] for i in range(1, 17)}  # Initialize dictionary for pax 1 to 16
            
            # Process each item in transferPriceList
            for data in datas['response']['transferPriceList']:
                passanger = data['maxPassengers']
                price = data['price']
                
                if passanger <= 16:  # Only consider vehicles with less than 16 passengers
                    x_paxs[passanger].append(float(price))  # Convert price to float and add to the list
            
            # Find the lowest prices for each passenger count
            lowest_values = {pax: min(prices) if prices else None for pax, prices in x_paxs.items()}
            
            # Extract the lowest prices for specific passenger counts
            pax_prices = {f'pax{i}': lowest_values.get(i) for i in range(1, 17)}
            if all(price is None for price in pax_prices.values()):
                raise ValueError("All passenger prices are empty")
        
            yield {
                'Route_start': Route_start,
                'Route_dest': Route_dest,
                **pax_prices
            }

        except Exception as e:
            # In case of failure, yield empty values
            yield {
                'Route_start': Route_start,
                'Route_dest': Route_dest,
                **{f'pax{i}': None for i in range(1, 17)}
            }
            
            self.log(f'Failed to process: {Route_start} to {Route_dest} - {e}')
