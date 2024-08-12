import scrapy,io
from datetime import datetime
import requests,re
import copy
from scrapy import Selector
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class SuntransferPriceSpider(scrapy.Spider):
    name = "suntransfer_price_try2"
    start_urls = ["https://www.suntransfers.com/"]
    input_date = "27-08-2024 10:00"

    # Parse the input string into a datetime object
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

    # Creating booking dictionary with desired keys and values
    booking = {
        'f_outbound_day': '27',
        'f_outbound_month': f"{dt_object.month}-{dt_object.year}",
        'f_outbound_date': dt_object.strftime('%d/%m/%Y'),
        'f_outbound_hours': '10',
        'f_outbound_minutes': '00',
        'f_outbound_time': dt_object.strftime('%H:%M')
    }

    payload = {
        'booking[form_get_quote_now_l]': '',
        'booking[f_departure]': '26134',  # FROM
        'booking[a_departure][id]': '',
        'booking[a_departure][cod]': '',
        'booking[f_arrival]': '25077',  # TO
        'booking[a_arrival][id]': '',
        'booking[a_arrival][cod]': '',
        'booking[f_fromto]': 'ar_1',
        'booking[f_outbound_day]': booking.get('f_outbound_day'), # 07
        'booking[f_outbound_month]': booking.get('f_outbound_month'), # 05-2024
        'booking[f_outbound_date]': booking.get('f_outbound_date'), # 07/05/2024
        'booking[f_outbound_hours]': booking.get('f_outbound_hours'),  # Dynamic outbound hours
        'booking[f_outbound_minutes]': booking.get('f_outbound_minutes'),  # Dynamic outbound minutes
        'booking[f_return_day]': '',  # Dynamic return day
        'booking[f_return_month]': '',  # Dynamic return month
        'booking[f_return_date]': '',  # Dynamic return date
        'booking[f_return_hours]': '',  # Dynamic return hours
        'booking[f_return_minutes]': '',  # Dynamic return minutes
        'booking[f_return_time]': '',  # Dynamic return time
        'booking[f_pax]': '2',  # PAX
        'booking[f_adults]': '2',  # PAX
        'booking[f_children]': '0',
        'booking[f_infants]': '0',
        'searchDateTime': '',
        'step': '1',
        'booking[f_outbound_time]': booking.get('f_outbound_time')  # Dynamic outbound time
    }

    def __init__(self, *args, **kwargs):
        super(SuntransferPriceSpider, self).__init__(*args, **kwargs)
        self.output_data = []

    def get_records_from_excel(self, file_path, sheet_name='Sheet1'):
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error("Error reading Excel file: %s", str(e))
            return []

    def save_to_excel(self, file_path, data, sheet_name='Output'):
        try:
            df = pd.DataFrame(data)
            df.to_excel(file_path, sheet_name=sheet_name, index=False)
            logger.info("Data saved successfully to Excel")
        except Exception as e:
            logger.error("Error saving data to Excel: %s", str(e))

    def parse(self, response):
        #file_path = 'sun_input_final.xlsx'  # Your Excel file path
        excel_url = "https://raw.githubusercontent.com/kathiravanmani05/suntran/proxies/sun_input_final.xlsx"
        excel_data = requests.get(excel_url)
        df = pd.read_excel(io.BytesIO(excel_data.content))
        output_file_path = 'output_data.xlsx'
        #records = self.get_records_from_excel(file_path)
        #records_to_process = records[:3]
        
        for i, row in df.iterrows():  # Use iterrows() to iterate over DataFrame rows
            try:
                output_data = self.parser_data(row)
                self.output_data.append(output_data)
                yield output_data
            except Exception as e:
                pass
                #logger.error(f"Error in row {row['Route_start']}_{row['Route_dest']}: {e}")
        
        self.save_to_excel(output_file_path, self.output_data)

    def parser_data(self, row):
        from_alternateId = row.get('from_alternateId')
        to_alternateId = row.get('to_alternateId')
        output_data = {}
        from_id = int(from_alternateId)
        to_id = int(to_alternateId)
        aiport_code = row.get('code')
        url = f"https://booking.suntransfers.com/booking?step=1&iata={aiport_code}&fromNoMatches=0"
        temp_payload = copy.deepcopy(self.payload)

        temp_payload['booking[f_departure]'] = from_id
        temp_payload['booking[f_arrival]'] = to_id

        stored_pax_values = []
        x_paxs = {i: [] for i in range(1, 17)}

        for i in [2, 4, 6, 8, 12, 16]:
            if i in stored_pax_values:
                continue
            temp_payload['booking[f_pax]'] = str(i)
            temp_payload['booking[f_adults]'] = str(i)

            proxies = {
                "http": "http://ybgfjkyz-rotate:gvxsoym3tw9o@p.webshare.io:80/",
                "https": "http://ybgfjkyz-rotate:gvxsoym3tw9o@p.webshare.io:80/"
            }

            data = requests.post(url, headers=self.headers, data=temp_payload, proxies=proxies)

            response = Selector(text=data.text)
            
            no_results = response.xpath('//text()[contains(.,"We are very sorry, unfortunately we are not able to offer you")]').get()
            if no_results:
                break
            vehicle_lst = response.xpath('//*[contains(@id,"vehicle_list_item")]')
            if i == 2 and len(vehicle_lst) == 0:
                break
            for vehicle in vehicle_lst:
                pax = vehicle.xpath('.//text()[contains(.,"Up to") and contains(.,"passengers")]').get()
                if pax:
                    pax = pax.replace('Up to ', '').replace(' passengers', '').strip()
                    stored_pax_values.append(int(pax))

                    if int(pax) < 16:
                        price = vehicle.xpath('.//*[@class="c-pricing__pricing"]//text()[contains(.,"€")]').get()
                        if price:
                            price = price.replace('€', '').replace(',', '').strip()
                            x_paxs[int(pax)].append(price)

        lowest_values = {}
        value_status = False
        for passengers, prices in x_paxs.items():
            if prices:
                lowest_values[passengers] = min(prices)
                value_status = True  # Find the minimum price
            else:
                lowest_values[passengers] = None
        if value_status:
            output_data['status'] = True
        else:
            output_data['status'] = None
        #import pdb;pdb.set_trace()
        output_data['pax_1'] = lowest_values.get(1)
        output_data['pax_2'] = lowest_values.get(2)
        output_data['pax_3'] = lowest_values.get(3)
        output_data['pax_4'] = lowest_values.get(4)
        output_data['pax_5'] = lowest_values.get(5)
        output_data['pax_6'] = lowest_values.get(6)
        output_data['pax_7'] = lowest_values.get(7)
        output_data['pax_8'] = lowest_values.get(8)
        output_data['pax_9'] = lowest_values.get(9)
        output_data['pax_10'] = lowest_values.get(10)
        output_data['pax_11'] = lowest_values.get(11)
        output_data['pax_12'] = lowest_values.get(12)
        output_data['pax_13'] = lowest_values.get(13)
        output_data['pax_14'] = lowest_values.get(14)
        output_data['pax_15'] = lowest_values.get(15)
        output_data['pax_16'] = lowest_values.get(16)

        output_data['Route_start'] = row.get('route_start')
        output_data['Route_dest'] = row.get('route_dest')
        output_data['Country'] = row.get('Country')
        output_data['Transfer_Type'] = row.get('Transfer_Type')
        output_data['Link'] = url

        return output_data
