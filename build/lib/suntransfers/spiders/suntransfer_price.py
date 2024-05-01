import scrapy
from scrapy.http import Request
import pandas as pd
from datetime import datetime

import urllib.parse
import requests
import copy


class SuntransferPriceSpider(scrapy.Spider):
    name = "suntransfer_price"
    #allowed_domains = ["www.suntransfers.com"]
    start_urls = ["https://www.suntransfers.com/"]

    input_date = "07-05-2024 10:00"
    pax_lst = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]
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
        #'cookie':'_gali=submit-form',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
        }

    # Creating booking dictionary with desired keys and values
    booking = {
        'f_outbound_day': '7',
        'f_outbound_month': f"{dt_object.month}-{dt_object.year}",
        'f_outbound_date': dt_object.strftime('%d/%m/%Y'),
        'f_outbound_hours':'10',
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
    'booking[f_outbound_day]': booking.get('f_outbound_day'),#07
    'booking[f_outbound_month]': booking.get('f_outbound_month'),#05-2024
    'booking[f_outbound_date]': booking.get('f_outbound_date'),#07/05/2024
    'booking[f_outbound_hours]': booking.get('f_outbound_hours'),  # Dynamic outbound hours
    'booking[f_outbound_minutes]': booking.get('f_outbound_minutes'),  # Dynamic outbound minutes
    'booking[f_return_day]': '',  # Dynamic return day
    'booking[f_return_month]': '',  # Dynamic return month
    'booking[f_return_date]': '',  # Dynamic return date
    'booking[f_return_hours]': '',  # Dynamic return hours
    'booking[f_return_minutes]': '',  # Dynamic return minutes
    'booking[f_return_time]': '',  # Dynamic return time
    'booking[f_pax]': '2', #PAX
    'booking[f_adults]': '2', #PAX
    'booking[f_children]': '0',
    'booking[f_infants]': '0',
    'searchDateTime': '',
    'step': '1',
    'booking[f_outbound_time]': booking.get('f_outbound_time')  # Dynamic outbound time
    }


    def parse(self, response):
        df = pd.read_excel('Valid_final_list.xlsx')
        filtered_df = df[df['Valid'].notnull()]
        for i in filtered_df.index:
            row_data = filtered_df.loc[i]
            from_id = int(row_data['ID'])
            to_id = int(row_data['ALTERNATE ID'])
            aiport_code = row_data['CODE']
            temp_pax_lst = copy.deepcopy(self.pax_lst)
            scraped_pax = []
            temp_payload =   copy.deepcopy(self.payload)

            temp_pax = temp_pax_lst.pop(0)
            scraped_pax.append(temp_pax)
            temp_payload['booking[f_departure]'] = from_id
            temp_payload['booking[f_arrival]'] = to_id
            temp_payload['booking[f_pax]'] = str(temp_pax)
            temp_payload['booking[f_pax]'] = str(temp_pax)

            
    
            url = f"https://booking.suntransfers.com/booking?step=1&iata={aiport_code}&fromNoMatches=0"
            encoded_payload = urllib.parse.urlencode(temp_payload, doseq=True)
            meta_data = {'url':url,
                         'from_id':from_id,
                         'to_id':to_id,
                         'aiport_code':aiport_code,
                         'temp_pax_lst':temp_pax_lst,
                         'scraped_pax':scraped_pax,
                         'dont_redirect': True
                         }
            
            
            yield Request(url,method='POST',headers=self.headers,body=temp_payload,callback=self.parse_price,meta={'meta_data':meta_data})

    def parse_price(self,response):

        import pdb;pdb.set_trace()
        print()


            