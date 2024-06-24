import scrapy
import pandas as pd
from datetime import datetime
import copy
import aiohttp
import asyncio
from scrapy import Selector
import logging

logger = logging.getLogger(__name__)

class SuntransferPriceSpider(scrapy.Spider):
    name = "suntransfer_1"
    start_urls = ["https://www.suntransfers.com/"]

    input_date = "25-06-2024 10:00"
    dt_object = datetime.strptime(input_date, '%d-%m-%Y %H:%M')

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,/;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6',
        'cache-control': 'max-age=0',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://booking.suntransfers.com',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    }

    booking = {
        'f_outbound_day': '25',
        'f_outbound_month': f"{dt_object.month}-{dt_object.year}",
        'f_outbound_date': dt_object.strftime('%d/%m/%Y'),
        'f_outbound_hours': '10',
        'f_outbound_minutes': '00',
        'f_outbound_time': dt_object.strftime('%H:%M')
    }

    payload = {
        'booking[form_get_quote_now_l]': '',
        'booking[f_departure]': '26134',
        'booking[a_departure][id]': '',
        'booking[a_departure][cod]': '',
        'booking[f_arrival]': '25077',
        'booking[a_arrival][id]': '',
        'booking[a_arrival][cod]': '',
        'booking[f_fromto]': 'ar_1',
        'booking[f_outbound_day]': booking.get('f_outbound_day'),
        'booking[f_outbound_month]': booking.get('f_outbound_month'),
        'booking[f_outbound_date]': booking.get('f_outbound_date'),
        'booking[f_outbound_hours]': booking.get('f_outbound_hours'),
        'booking[f_outbound_minutes]': booking.get('f_outbound_minutes'),
        'booking[f_return_day]': '',
        'booking[f_return_month]': '',
        'booking[f_return_date]': '',
        'booking[f_return_hours]': '',
        'booking[f_return_minutes]': '',
        'booking[f_return_time]': '',
        'booking[f_pax]': '2',
        'booking[f_adults]': '2',
        'booking[f_children]': '0',
        'booking[f_infants]': '0',
        'searchDateTime': '',
        'step': '1',
        'booking[f_outbound_time]': booking.get('f_outbound_time')
    }

    async def parse(self, response):
        excel_url = "https://raw.githubusercontent.com/kathiravanmani05/suntran/main/Batch2_input3.xlsx"
        df = pd.read_excel('Batch2_input3.xlsx')
        
        tasks = []
        for i in range(4800, 5300, 10):  # Process rows in batches of 10
            batch = df.iloc[i:i+10]
            tasks.append(self.process_batch(batch))
        
        results = await asyncio.gather(*tasks)
        
        for batch_result in results:
            for result in batch_result:
                yield result

    async def process_batch(self, batch):
        tasks = [self.process_row(row) for _, row in batch.iterrows()]
        return await asyncio.gather(*tasks)

    async def process_row(self, row_data):
        try:
            from_id = int(row_data['from_alternateId'])
            to_id = int(row_data['to_alternateId'])
            route_dest = row_data.get('route_dest', None)
            route_start = row_data.get('route_start', None)
            airport_code = row_data['CODE']
            url = f"https://booking.suntransfers.com/booking?step=1&iata={airport_code}&fromNoMatches=0"

            temp_payload = self.payload.copy()

            temp_payload['booking[f_departure]'] = from_id
            temp_payload['booking[f_arrival]'] = to_id
            
            stored_pax_values = []
            x_paxs = {i: [] for i in range(1, 17)}
            logger.info(f"{route_dest}_{route_start}")
            
            async with aiohttp.ClientSession() as session:
                for pax_count in range(1, 17):
                    if pax_count in stored_pax_values:
                        continue
                    temp_payload['booking[f_pax]'] = str(pax_count)
                    temp_payload['booking[f_adults]'] = str(pax_count)

                    async with session.post(url, headers=self.headers, data=temp_payload) as resp:
                        print(resp.text())
                        data = await resp.text()
                        response = Selector(text=data)
                        no_results = response.xpath('//text()[contains(.,"We are very sorry, unfortunately we are not able to offer you")]').get()
                        if no_results:
                            break

                        vehicle_lst = response.xpath('//*[contains(@id,"vehicle_list_item")]')
                        
                        for vehicle in vehicle_lst:
                            pax = vehicle.xpath('.//text()[contains(.,"Up to") and contains(.,"passengers")]').get()
                            if pax:
                                pax = pax.replace('Up to ', '').replace(' passengers', '').strip()
                                stored_pax_values.append(int(pax))
                                
                                if int(pax) < 16:
                                    price = vehicle.xpath('.//*[@class="c-pricing__highlight currency"]//text()[contains(.,"€")]').get()
                                    if price:
                                        price = price.replace('€', '').strip()
                                        x_paxs[int(pax)].append(price)
                
                lowest_values = {}
                for passengers, prices in x_paxs.items():
                    if prices:
                        lowest_values[passengers] = min(prices)
                    else:
                        lowest_values[passengers] = None
                logger.info(f"{route_dest}{route_start}{lowest_values}")

                return {
                    'from_id': from_id,
                    'to_id': to_id,
                    'route_dest': route_dest,
                    'route_start': route_start,
                    **{f'pax{passengers}': price for passengers, price in lowest_values.items()}
                }
        except Exception as e:
            logger.error(f"Error processing row {row_data}: {e}")
            from_id = row_data['from_alternateId']
            to_id = row_data['to_alternateId']
            print(from_id, to_id)
            return {
                'from_id': from_id,
                'to_id': to_id,
                'error': str(e)
            }