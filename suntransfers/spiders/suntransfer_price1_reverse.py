import scrapy
from scrapy.http import Request
import pandas as pd
from datetime import datetime
import io
import urllib.parse
import requests
import copy
from scrapy import Selector


class SuntransferPriceSpider(scrapy.Spider):
    name = "suntransfer_price1_reverse"
    #allowed_domains = ["www.suntransfers.com"]
    start_urls = ["https://www.suntransfers.com/"]

    input_date = "07-05-2024 10:00"

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
    'booking[f_fromto]': 'ra_1',
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
        excel_url = "https://raw.githubusercontent.com/kathiravanmani05/suntran/main/ValidInputBatch3_ra.xlsx"
        excel_data = requests.get(excel_url)
        
        # Reading the Excel file
        df = pd.read_excel(io.BytesIO(excel_data.content))
        
        for i in df.index:
            try:
                row_data = df.loc[i]
                to_id = int(row_data['ID'])
                from_id= int(row_data['ALTERNATE ID'])
                aiport_code = row_data['CODE']
                url = f"https://booking.suntransfers.com/booking?step=1&iata={aiport_code}&fromNoMatches=0"

                temp_payload =   copy.deepcopy(self.payload)


                temp_payload['booking[f_departure]'] = from_id
                temp_payload['booking[f_arrival]'] = to_id
                
                stored_pax_values = []
                x_paxs = {i: [] for i in range(1, 17)}
                for i in range(1, 17):
                    print('Loop',i)
                    if i in stored_pax_values:
                        continue
                    temp_payload['booking[f_pax]'] = str(i)
                    temp_payload['booking[f_adults]'] = str(i)

                    
                
                    data = requests.post(url,headers=self.headers,data=temp_payload)
                    
                    response = Selector(text=data.text)
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
                                price = vehicle.xpath('.//*[@class="c-pricing__pricing"]//text()[contains(.,"€")]').get()
                                print(pax,price)
                                if price:
                                    price = price.replace('€', '').strip()
                                    
                                    x_paxs[int(pax)].append(price)
                    
                lowest_values = {}
                
                for passengers, prices in x_paxs.items():
                    if prices:
                        lowest_values[passengers] = min(prices)  # Find the minimum price
                    else:
                        lowest_values[passengers] = None
            
                pax1 = lowest_values.get(1)
                pax2 = lowest_values.get(2)
                pax3 = lowest_values.get(3)
                pax4 = lowest_values.get(4)
                pax5 = lowest_values.get(5)
                pax6 = lowest_values.get(6)
                pax7 = lowest_values.get(7)
                pax8 = lowest_values.get(8)
                pax9 = lowest_values.get(9)
                pax10 = lowest_values.get(10)
                pax11 = lowest_values.get(11)
                pax12 = lowest_values.get(12)
                pax13 = lowest_values.get(13)
                pax14 = lowest_values.get(14)
                pax15 = lowest_values.get(15)
                pax16 = lowest_values.get(16)
                

                yield { 'from_id':from_id,
                        'to_id':to_id,
                        'pax1':pax1,
                        'pax2':pax2,
                        'pax3':pax3,
                        'pax4':pax4,
                        'pax5':pax5,
                        'pax6':pax6,
                        'pax7':pax7,
                        'pax8':pax8,
                        'pax9':pax9,
                        'pax10':pax10,
                        'pax11':pax11,
                        'pax12':pax12,
                        'pax13':pax13,
                        'pax14':pax14,
                        'pax15':pax15,
                        'pax16':pax16,

                    }
            except:
                pass



            
