import scrapy
from datetime import datetime
import requests
import copy
from scrapy import Selector

import logging
logger = logging.getLogger(__name__)

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import scoped_session


from suntransfers.models import Route
# Assuming you already have an engine
user = 'suntransfer1'
password = 'suntransfer1'
host = '34.45.193.112'
port = '3306'  # Default port for MySQL
database = 'suntransfer'
engine = create_engine( f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'  )
Session = sessionmaker(bind=engine)
session = scoped_session(Session)



class SuntransferPriceSpider(scrapy.Spider):
    name = "suntransfer_price_try1"
    start_urls = ["https://www.suntransfers.com/"]
    input_date = "23-07-2024 10:00"

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
        'f_outbound_day': '23',
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

    def __init__(self, *args, **kwargs):
        super(SuntransferPriceSpider, self).__init__(*args, **kwargs)
        self.batch_size = 100

    def get_records_with_conditions(self,batch_size):
        try:
            rows = session.query(Route).filter(
                Route.status == 0,
                Route.retry < 2,
                Route.from_alternateId.isnot(None),
                Route.to_alternateId.isnot(None),
                Route.serial_no >= 1,
                Route.serial_no <= 10
            ).limit(batch_size).all()
            logger.info("Query executed successfully")
            return rows
        except Exception as e:
            logger.error("Error executing query: %s", str(e))
            return []
        


    def parse(self,response):

        batch_number = 0
        while True:
            records = self.get_records_with_conditions(self.batch_size)
            if not records:
                break
            batch_number += 1

            
            for i,row in enumerate(records,1):
                try:
                    output_data = self.parser_data(row)
                    self.save_to_mysql(output_data,i)
                    yield output_data
                except Exception as e:
                    logger.error(f"Error in row  {row.Route_start}_{row.Route_dest} {e}")
            
    
    def save_to_mysql(self,data,counter):

        try:
            with session.no_autoflush:
                # Fetch the existing record
                record = session.query(Route).filter(
                    Route.from_alternateId == data.get('from_alternateId'),
                    Route.to_alternateId == data.get('to_alternateId')
                ).one_or_none()
                
                if record:
                    # Update the fields
                    record.pax_1 = data.get('pax_1')
                    record.pax_2 = data.get('pax_2')
                    record.pax_3 = data.get('pax_3')
                    record.pax_4 = data.get('pax_4')
                    record.pax_5 = data.get('pax_5')
                    record.pax_6 = data.get('pax_6')
                    record.pax_7 = data.get('pax_7')
                    record.pax_8 = data.get('pax_8')
                    record.pax_9 = data.get('pax_9')
                    record.pax_10 = data.get('pax_10')
                    record.pax_11 = data.get('pax_11')
                    record.pax_12 = data.get('pax_12')
                    record.pax_13 = data.get('pax_13')
                    record.pax_14 = data.get('pax_14')
                    record.pax_15 = data.get('pax_15')
                    record.pax_16 = data.get('pax_16')
                    record.retry = data.get('retry', 0)
                    record.status = data.get('status')
                    session.commit()
                else:
                    print("Record not found.")
        except Exception as e:
            session.rollback()
            logger.error(f"An error occurred: {e}")
        

    def parser_data(self,row):
            
            from_alternateId = row.from_alternateId
            to_alternateId = row.to_alternateId
            retry = row.retry
            output_data = {}
            from_id = int(from_alternateId)
            to_id = int(to_alternateId)
            aiport_code = row.code
            url = f"https://booking.suntransfers.com/booking?step=1&iata={aiport_code}&fromNoMatches=0"
            temp_payload =   copy.deepcopy(self.payload)

            temp_payload['booking[f_departure]'] = from_id
            temp_payload['booking[f_arrival]'] = to_id

            stored_pax_values = []
            x_paxs = {i: [] for i in range(1, 17)}
            for i in range(1, 17):
                if i in stored_pax_values:
                    continue
                temp_payload['booking[f_pax]'] = str(i)
                temp_payload['booking[f_adults]'] = str(i)

                proxies={
                        "http": "http://ybgfjkyz-rotate:gvxsoym3tw9o@p.webshare.io:80/",
                        "https": "http://ybgfjkyz-rotate:gvxsoym3tw9o@p.webshare.io:80/"
                    }

            
                data = requests.post(url,headers=self.headers,data=temp_payload,proxies=proxies)
                
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
                try:
                    retry = int(retry)
                    retry = retry + 1
                    output_data['retry'] = retry
                except :
                    output_data['retry'] = 1


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
            output_data['from_alternateId'] = from_alternateId
            output_data['to_alternateId'] = to_alternateId
            return output_data
            







            
