import scrapy
from scrapy.http import Request, FormRequest
import pandas as pd
from datetime import datetime
import io
import urllib.parse
import requests
import copy
from scrapy import Selector
import logging

logger = logging.getLogger(__name__)

class SuntransferPriceSpider(scrapy.Spider):
    name = "suntransfer_price_new_4"
    start_urls = ["https://www.suntransfers.com/"]

    input_date = "25-06-2024 10:00"
    dt_object = datetime.strptime(input_date, '%d-%m-%Y %H:%M')

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
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

    custom_settings = {
        'CONCURRENT_REQUESTS': 16,
        'DOWNLOAD_TIMEOUT': 15,
        'RETRY_TIMES': 3
    }

    def parse(self, response):
        excel_url = "https://raw.githubusercontent.com/kathiravanmani05/suntran/batch3_run/input.xlsx"
        excel_data = requests.get(excel_url)
        df = pd.read_excel(io.BytesIO(excel_data.content))
        #df = pd.read_excel('input.xlsx')

        for i in df.index[3000:4000]:
            try:
                row_data = df.loc[i]
                from_id = int(row_data['from_alternateId'])
                to_id = int(row_data['to_alternateId'])
                route_dest = row_data.get('route_dest', None)
                route_start = row_data.get('route_start', None)
                aiport_code = row_data['CODE']
                url = f"https://booking.suntransfers.com/booking?step=1&iata={aiport_code}&fromNoMatches=0"

                temp_payload = copy.deepcopy(self.payload)
                temp_payload['booking[f_departure]'] = from_id
                temp_payload['booking[f_arrival]'] = to_id

                yield FormRequest(
                    url=url,
                    method='POST',
                    headers=self.headers,
                    formdata={k: str(v) for k, v in temp_payload.items()},
                    callback=self.parse_prices,
                    meta={'route_dest': route_dest, 'route_start': route_start, 'from_id': from_id, 'to_id': to_id}
                )

            except Exception as e:
                logger.error(f"Error processing row {i}: {e}")

    def parse_prices(self, response):
        route_dest = response.meta['route_dest']
        route_start = response.meta['route_start']
        from_id = response.meta['from_id']
        to_id = response.meta['to_id']

        stored_pax_values = []
        x_paxs = {i: [] for i in range(1, 17)}

        no_results = response.xpath('//text()[contains(.,"We are very sorry, unfortunately we are not able to offer you")]').get()
        if no_results:
            return

        vehicle_lst = response.xpath('//*[contains(@id,"vehicle_list_item")]')
        for vehicle in vehicle_lst:
            pax = vehicle.xpath('.//text()[contains(.,"Up to") and contains(.,"passengers")]').get()
            if pax:
                pax = pax.replace('Up to ', '').replace(' passengers', '').strip()
                stored_pax_values.append(int(pax))

                if int(pax) < 16:
                    price = vehicle.xpath('.//*[@class="c-pricing__pricing"]//text()[contains(.,"€")]').get()
                    if price:
                        price = price.replace('€', '').strip()
                        x_paxs[int(pax)].append(price)

        lowest_values = {passengers: min(prices) if prices else None for passengers, prices in x_paxs.items()}

        yield {
            'from_id': from_id,
            'to_id': to_id,
            'route_dest': route_dest,
            'route_start': route_start,
            **{f'pax{pax}': lowest_values.get(pax) for pax in range(1, 17)}
        }
